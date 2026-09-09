use std::{
    ffi::c_void,
    io,
    mem::size_of,
    os::windows::{ffi::OsStrExt, io::AsRawHandle},
    path::{Path, PathBuf},
    ptr::null_mut,
    slice,
    time::Duration,
};

use sha2::{Digest, Sha256};
use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions,
};
use windows_sys::{
    core::PWSTR,
    Win32::{
        Foundation::{CloseHandle, LocalFree, ERROR_INSUFFICIENT_BUFFER, ERROR_PIPE_BUSY, HANDLE},
        Security::{
            Authorization::{
                ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
                SDDL_REVISION_1,
            },
            EqualSid, GetLengthSid, GetTokenInformation, IsValidSid, TokenUser,
            PSECURITY_DESCRIPTOR, PSID, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER,
        },
        Storage::FileSystem::SECURITY_IDENTIFICATION,
        System::{
            Pipes::GetNamedPipeServerProcessId,
            Threading::{
                GetCurrentProcess, GetCurrentProcessId, OpenProcess, OpenProcessToken,
                PROCESS_QUERY_LIMITED_INFORMATION,
            },
        },
    },
};

use crate::{database_identity, Endpoint};

const PIPE_PREFIX: &str = r"\\.\pipe\bastet-workstation-";
const CONNECT_BUSY_RETRIES: usize = 100;
const RETRY_DELAY: Duration = Duration::from_millis(10);

pub type ClientStream = NamedPipeClient;
pub type ServerStream = NamedPipeServer;

fn denied(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}

/// An aligned owned copy of a SID. Windows' SID APIs require an aligned PSID;
/// a plain `Vec<u8>` does not provide that guarantee.
#[derive(Clone)]
struct Sid {
    storage: Vec<usize>,
    byte_len: usize,
}

impl Sid {
    unsafe fn copy_from_raw(raw: PSID) -> io::Result<Self> {
        if raw.is_null() || unsafe { IsValidSid(raw) } == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Windows SID",
            ));
        }
        let byte_len = unsafe { GetLengthSid(raw) } as usize;
        if byte_len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "empty Windows SID",
            ));
        }
        let words = byte_len.div_ceil(size_of::<usize>());
        let mut storage = vec![0usize; words];
        unsafe {
            std::ptr::copy_nonoverlapping(
                raw.cast::<u8>(),
                storage.as_mut_ptr().cast::<u8>(),
                byte_len,
            );
        }
        Ok(Self { storage, byte_len })
    }

    fn as_psid(&self) -> PSID {
        self.storage.as_ptr().cast_mut().cast::<c_void>()
    }

    fn as_bytes(&self) -> &[u8] {
        // SAFETY: `storage` contains at least `byte_len` initialized bytes.
        unsafe { slice::from_raw_parts(self.storage.as_ptr().cast::<u8>(), self.byte_len) }
    }

    #[cfg(test)]
    fn as_bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: `storage` contains at least `byte_len` initialized bytes.
        unsafe { slice::from_raw_parts_mut(self.storage.as_mut_ptr().cast::<u8>(), self.byte_len) }
    }
}

struct OwnedHandle(HANDLE);

impl OwnedHandle {
    fn new(handle: HANDLE) -> io::Result<Self> {
        if handle.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self(handle))
        }
    }
}

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        // SAFETY: this wrapper is the unique owner of a non-null HANDLE.
        unsafe {
            CloseHandle(self.0);
        }
    }
}

struct LocalSecurityDescriptor(PSECURITY_DESCRIPTOR);

impl Drop for LocalSecurityDescriptor {
    fn drop(&mut self) {
        // SAFETY: ConvertStringSecurityDescriptorToSecurityDescriptorW allocates
        // this block with LocalAlloc and ownership is unique here.
        unsafe {
            LocalFree(self.0);
        }
    }
}

fn token_user_sid(token: HANDLE) -> io::Result<Sid> {
    let mut required = 0u32;
    // SAFETY: the null-buffer sizing call writes only `required`.
    let sized = unsafe { GetTokenInformation(token, TokenUser, null_mut(), 0, &mut required) };
    if sized != 0
        || io::Error::last_os_error().raw_os_error() != Some(ERROR_INSUFFICIENT_BUFFER as i32)
    {
        return Err(io::Error::last_os_error());
    }
    if (required as usize) < size_of::<TOKEN_USER>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Windows returned a truncated token identity",
        ));
    }

    // `usize` storage gives TOKEN_USER and SID their required alignment.
    let mut buffer = vec![0usize; (required as usize).div_ceil(size_of::<usize>())];
    let mut written = required;
    // SAFETY: the allocation is aligned and has at least `required` bytes.
    if unsafe {
        GetTokenInformation(
            token,
            TokenUser,
            buffer.as_mut_ptr().cast::<c_void>(),
            required,
            &mut written,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    if written > required || (written as usize) < size_of::<TOKEN_USER>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Windows returned an invalid token identity length",
        ));
    }
    // SAFETY: the successful query initialized a TOKEN_USER at this address.
    let user = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
    // SAFETY: TOKEN_USER::User.Sid is documented as a valid SID for a
    // successful TokenUser query and is copied before `buffer` is dropped.
    unsafe { Sid::copy_from_raw(user.User.Sid) }
}

fn process_user_sid(process: HANDLE) -> io::Result<Sid> {
    let mut token = null_mut();
    // SAFETY: `token` is a valid output pointer and the process handle remains
    // alive for the duration of this call.
    if unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let token = OwnedHandle::new(token)?;
    token_user_sid(token.0)
}

fn current_user_sid() -> io::Result<Sid> {
    // SAFETY: GetCurrentProcess returns a process pseudo-handle that must not be
    // closed and remains valid for the process lifetime.
    process_user_sid(unsafe { GetCurrentProcess() })
}

fn sid_to_string(sid: &Sid) -> io::Result<String> {
    let mut raw: PWSTR = null_mut();
    // SAFETY: `sid` is an aligned, validated SID and `raw` is an output pointer.
    if unsafe { ConvertSidToStringSidW(sid.as_psid(), &mut raw) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if raw.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Windows returned an empty SID string",
        ));
    }

    let mut len = 0usize;
    // SAFETY: Windows returned a NUL-terminated UTF-16 LocalAlloc string.
    unsafe {
        while *raw.add(len) != 0 {
            len += 1;
        }
    }
    // SAFETY: the preceding scan found the terminator in the allocated string.
    let result = String::from_utf16(unsafe { slice::from_raw_parts(raw, len) })
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid SID string"));
    // SAFETY: ConvertSidToStringSidW allocates this block with LocalAlloc.
    unsafe {
        LocalFree(raw.cast::<c_void>());
    }
    result
}

fn security_descriptor(sid: &Sid) -> io::Result<LocalSecurityDescriptor> {
    let sid = sid_to_string(sid)?;
    // Protected DACL with one ACE: only the account running this process gets
    // access. The owner is explicit rather than inherited from a default DACL.
    let sddl = format!("O:{sid}D:P(A;;GA;;;{sid})");
    let mut wide: Vec<u16> = sddl.encode_utf16().collect();
    wide.push(0);
    let mut descriptor = null_mut();
    // SAFETY: `wide` is NUL-terminated and descriptor is a valid output pointer.
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            wide.as_ptr(),
            SDDL_REVISION_1,
            &mut descriptor,
            null_mut(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    if descriptor.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Windows returned an empty security descriptor",
        ));
    }
    Ok(LocalSecurityDescriptor(descriptor))
}

fn validate_endpoint(endpoint: &Endpoint) -> io::Result<()> {
    let value = endpoint.path.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "named-pipe endpoint is not Unicode",
        )
    })?;
    let Some(digest) = value.strip_prefix(PIPE_PREFIX) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "only local Bastet named-pipe endpoints are accepted",
        ));
    };
    if digest.len() != 64
        || !digest
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid Bastet named-pipe endpoint",
        ));
    }
    Ok(())
}

pub fn endpoint_for_database(database: &Path) -> io::Result<Endpoint> {
    let identity = database_identity(database)?;
    let sid = current_user_sid()?;
    let mut hasher = Sha256::new();
    hasher.update(b"bastet-workstation-local-ipc-v1\0");
    hasher.update((sid.byte_len as u64).to_le_bytes());
    hasher.update(sid.as_bytes());
    let identity: Vec<u16> = identity.as_os_str().encode_wide().collect();
    hasher.update((identity.len() as u64).to_le_bytes());
    for unit in identity {
        hasher.update(unit.to_le_bytes());
    }
    let digest = hasher.finalize();
    Ok(Endpoint {
        path: PathBuf::from(format!(r"{PIPE_PREFIX}{digest:x}")),
    })
}

fn server_options(first: bool) -> ServerOptions {
    let mut options = ServerOptions::new();
    options
        .first_pipe_instance(first)
        .reject_remote_clients(true);
    options
}

fn create_server(endpoint: &Endpoint, sid: &Sid, first: bool) -> io::Result<NamedPipeServer> {
    validate_endpoint(endpoint)?;
    let descriptor = security_descriptor(sid)?;
    let mut attributes = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor.0,
        bInheritHandle: 0,
    };
    // SAFETY: `attributes` and its descriptor remain valid throughout the
    // synchronous CreateNamedPipeW call. Windows captures the descriptor.
    unsafe {
        server_options(first).create_with_security_attributes_raw(
            &endpoint.path,
            (&mut attributes as *mut SECURITY_ATTRIBUTES).cast::<c_void>(),
        )
    }
}

fn open_client(endpoint: &Endpoint) -> io::Result<NamedPipeClient> {
    validate_endpoint(endpoint)?;
    // Identification is explicit: the server may identify this account for
    // authorization, but cannot impersonate it.
    ClientOptions::new()
        .security_qos_flags(SECURITY_IDENTIFICATION)
        .open(&endpoint.path)
}

fn server_process_id(client: &NamedPipeClient) -> io::Result<u32> {
    let mut process_id = 0u32;
    // SAFETY: this is the live pipe handle that will be returned to the HTTP
    // client, and `process_id` is a valid output pointer.
    if unsafe {
        GetNamedPipeServerProcessId(client.as_raw_handle().cast::<c_void>(), &mut process_id)
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    if process_id == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "named-pipe server did not identify a process",
        ));
    }
    Ok(process_id)
}

fn verify_server_process_sid(client: &NamedPipeClient, expected: &Sid) -> io::Result<()> {
    let process_id = server_process_id(client)?;
    // SAFETY: OpenProcess receives a PID supplied by the kernel for this pipe
    // handle. The returned handle is checked before use.
    let process =
        OwnedHandle::new(unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, process_id) })?;
    let actual = process_user_sid(process.0)?;
    // Re-read from the same pipe handle after opening the process. This closes
    // the ordinary disconnect/reconnect TOCTOU window and fails if the pipe no
    // longer has the same server endpoint.
    if server_process_id(client)? != process_id {
        return Err(denied(
            "named-pipe server identity changed during authentication",
        ));
    }
    // SAFETY: both values are aligned, validated SIDs with live backing storage.
    if unsafe { EqualSid(actual.as_psid(), expected.as_psid()) } == 0 {
        return Err(denied(
            "named-pipe server belongs to another Windows account",
        ));
    }
    Ok(())
}

pub async fn connect(endpoint: &Endpoint) -> io::Result<ClientStream> {
    validate_endpoint(endpoint)?;
    let expected = current_user_sid()?;
    for attempt in 0..=CONNECT_BUSY_RETRIES {
        match open_client(endpoint) {
            Ok(client) => {
                // Authentication is performed on—and without closing—the exact
                // handle returned to Hyper, so there is no preflight/reopen gap.
                verify_server_process_sid(&client, &expected)?;
                return Ok(client);
            }
            Err(error)
                if error.raw_os_error() == Some(ERROR_PIPE_BUSY as i32)
                    && attempt < CONNECT_BUSY_RETRIES =>
            {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("the bounded named-pipe connection loop always returns")
}

pub struct LocalListener {
    endpoint: Endpoint,
    sid: Sid,
    pending: Option<NamedPipeServer>,
}

/// Owns a permanently connected first instance. Keeping both ends alive until
/// Axum has completely stopped makes the pipe name continuously occupied and
/// makes a second daemon's FILE_FLAG_FIRST_PIPE_INSTANCE claim fail closed.
pub struct EndpointGuard {
    _sentinel_server: NamedPipeServer,
    _sentinel_client: NamedPipeClient,
}

pub fn bind(endpoint: &Endpoint) -> io::Result<(LocalListener, EndpointGuard)> {
    validate_endpoint(endpoint)?;
    let sid = current_user_sid()?;

    // Claim the name before any shared store is opened. A pre-existing pipe,
    // including a squatter, makes this fail rather than silently joining it.
    let sentinel_server = create_server(endpoint, &sid, true)?;
    let sentinel_client = open_client(endpoint)?;
    // There was no server instance before our successful FIRST_PIPE_INSTANCE
    // claim. Still require the internal connection to terminate in this exact
    // process, preventing a same-user racing instance from consuming it.
    if server_process_id(&sentinel_client)? != unsafe { GetCurrentProcessId() } {
        return Err(denied("named-pipe singleton connection was intercepted"));
    }
    verify_server_process_sid(&sentinel_client, &sid)?;

    // This is the externally available instance. The connected sentinel keeps
    // the singleton/name alive while pending instances rotate.
    let pending = create_server(endpoint, &sid, false)?;
    Ok((
        LocalListener {
            endpoint: endpoint.clone(),
            sid,
            pending: Some(pending),
        },
        EndpointGuard {
            _sentinel_server: sentinel_server,
            _sentinel_client: sentinel_client,
        },
    ))
}

impl axum::serve::Listener for LocalListener {
    type Io = ServerStream;
    type Addr = ();

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let server = match self.pending.take() {
                Some(server) => server,
                None => match create_server(&self.endpoint, &self.sid, false) {
                    Ok(server) => server,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                },
            };

            if server.connect().await.is_err() {
                drop(server);
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }

            // Never expose the connected stream until a restricted, local-only
            // successor is listening. Clients that race this small interval see
            // PIPE_BUSY and retry rather than reaching an unauthenticated path.
            loop {
                match create_server(&self.endpoint, &self.sid, false) {
                    Ok(next) => {
                        self.pending = Some(next);
                        return (server, ());
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::serve::Listener;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn fixture(name: &str) -> (tempfile::TempDir, Endpoint) {
        let directory = tempfile::tempdir().unwrap();
        let endpoint = endpoint_for_database(&directory.path().join(name)).unwrap();
        (directory, endpoint)
    }

    #[tokio::test]
    async fn same_user_connects_over_the_authenticated_handle_and_io_is_bidirectional() {
        let (_directory, endpoint) = fixture("database.db");
        let (mut listener, guard) = bind(&endpoint).unwrap();
        let mut client = connect(&endpoint).await.unwrap();
        assert_eq!(server_process_id(&client).unwrap(), unsafe {
            GetCurrentProcessId()
        });
        let (mut server, ()) = listener.accept().await;

        client.write_all(b"ping").await.unwrap();
        let mut request = [0u8; 4];
        server.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"ping");
        server.write_all(b"pong").await.unwrap();
        let mut response = [0u8; 4];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"pong");

        drop(server);
        drop(client);
        drop(listener);
        drop(guard);
    }

    #[tokio::test]
    async fn database_identities_are_unique_and_each_endpoint_is_exclusive() {
        let directory = tempfile::tempdir().unwrap();
        let first = endpoint_for_database(&directory.path().join("first.db")).unwrap();
        let second = endpoint_for_database(&directory.path().join("second.db")).unwrap();
        assert_ne!(first, second);

        let (first_listener, first_guard) = bind(&first).unwrap();
        assert!(bind(&first).is_err());
        let (second_listener, second_guard) = bind(&second).unwrap();

        drop(first_listener);
        // Graceful HTTP drain can drop its listener while the guard must still
        // prevent a replacement daemon from opening/recovering the same store.
        assert!(bind(&first).is_err());
        drop(first_guard);
        assert!(bind(&first).is_ok());
        drop(second_listener);
        drop(second_guard);
    }

    #[tokio::test]
    async fn database_endpoint_is_stable_before_and_after_creation() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("stable.db");
        let before = endpoint_for_database(&database).unwrap();
        std::fs::write(&database, b"").unwrap();
        assert_eq!(before, endpoint_for_database(&database).unwrap());
    }

    #[tokio::test]
    async fn connected_server_sid_must_match_the_expected_account() {
        let (_directory, endpoint) = fixture("identity.db");
        let (_listener, _guard) = bind(&endpoint).unwrap();
        let client = open_client(&endpoint).unwrap();
        let expected = current_user_sid().unwrap();
        verify_server_process_sid(&client, &expected).unwrap();

        let mut other = expected.clone();
        let last = other.byte_len - 1;
        other.as_bytes_mut()[last] ^= 1;
        assert_ne!(unsafe { IsValidSid(other.as_psid()) }, 0);
        let error = verify_server_process_sid(&client, &other).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn nonlocal_and_unrecognized_pipe_names_are_refused() {
        let digest = "0".repeat(64);
        let remote = Endpoint {
            path: PathBuf::from(format!(r"\\server\pipe\bastet-workstation-{digest}")),
        };
        let arbitrary = Endpoint {
            path: PathBuf::from(r"\\.\pipe\not-bastet"),
        };
        assert_eq!(
            validate_endpoint(&remote).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            validate_endpoint(&arbitrary).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
