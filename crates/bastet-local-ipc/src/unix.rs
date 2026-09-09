use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io,
    os::{
        fd::AsRawFd,
        unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    time::Duration,
};

use sha2::{Digest, Sha256};
use tokio::net::{UnixListener, UnixStream};

use crate::{database_identity, Endpoint};

pub type ClientStream = UnixStream;
pub type ServerStream = UnixStream;

fn current_uid() -> u32 {
    // SAFETY: geteuid has no arguments or memory preconditions.
    unsafe { libc::geteuid() }
}

fn denied() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "local IPC ownership or permissions are invalid",
    )
}

fn validate_directory(path: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.uid() != current_uid() || metadata.mode() & 0o077 != 0 {
        return Err(denied());
    }
    Ok(())
}

pub fn endpoint_for_database(database: &Path) -> io::Result<Endpoint> {
    let identity = database_identity(database)?;
    // A short, fixed OS temp base avoids sockaddr_un truncation when app-data
    // paths are long. The directory itself is exclusive to this effective UID.
    let directory = PathBuf::from(format!("/tmp/bastet-workstation-{}", current_uid()));
    match DirBuilder::new().mode(0o700).create(&directory) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    validate_directory(&directory)?;
    use std::os::unix::ffi::OsStrExt;
    let digest = format!("{:x}", Sha256::digest(identity.as_os_str().as_bytes()));
    Ok(Endpoint {
        path: directory.join(format!("{}.sock", &digest[..32])),
    })
}

fn validate_socket(endpoint: &Endpoint) -> io::Result<fs::Metadata> {
    validate_directory(endpoint.path.parent().ok_or_else(denied)?)?;
    let metadata = fs::symlink_metadata(&endpoint.path)?;
    if !metadata.file_type().is_socket()
        || metadata.uid() != current_uid()
        || metadata.mode() & 0o077 != 0
    {
        return Err(denied());
    }
    Ok(metadata)
}

pub async fn connect(endpoint: &Endpoint) -> io::Result<ClientStream> {
    validate_socket(endpoint)?;
    let stream = UnixStream::connect(&endpoint.path).await?;
    // Authenticate the connected handle, not only the pathname checked above.
    if stream.peer_cred()?.uid() != current_uid() {
        return Err(denied());
    }
    Ok(stream)
}

pub struct LocalListener {
    listener: UnixListener,
}

/// Kept alive until graceful shutdown and all HTTP connections have completed.
pub struct EndpointGuard {
    _lock: File,
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl Drop for EndpointGuard {
    fn drop(&mut self) {
        if let Ok(metadata) = fs::symlink_metadata(&self.path) {
            if metadata.file_type().is_socket()
                && metadata.dev() == self.device
                && metadata.ino() == self.inode
            {
                let _ = fs::remove_file(&self.path);
            }
        }
        // The persistent lock file is deliberately not unlinked: replacing it
        // could allow two processes to hold locks on different inodes.
    }
}

pub fn bind(endpoint: &Endpoint) -> io::Result<(LocalListener, EndpointGuard)> {
    validate_directory(endpoint.path.parent().ok_or_else(denied)?)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(endpoint.path.with_extension("lock"))?;
    let metadata = lock.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != current_uid()
        || metadata.mode() & 0o077 != 0
        || metadata.nlink() != 1
    {
        return Err(denied());
    }
    // SAFETY: the descriptor remains valid and exclusively owned by `lock`.
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err(io::Error::last_os_error());
    }
    match fs::symlink_metadata(&endpoint.path) {
        Ok(_) => {
            validate_socket(endpoint)?;
            match std::os::unix::net::UnixStream::connect(&endpoint.path) {
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        "local IPC is active",
                    ))
                }
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                    fs::remove_file(&endpoint.path)?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = UnixListener::bind(&endpoint.path)?;
    fs::set_permissions(&endpoint.path, fs::Permissions::from_mode(0o600))?;
    let metadata = validate_socket(endpoint)?;
    let guard = EndpointGuard {
        _lock: lock,
        path: endpoint.path.clone(),
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    Ok((LocalListener { listener }, guard))
}

impl axum::serve::Listener for LocalListener {
    type Io = ServerStream;
    type Addr = ();

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    if stream
                        .peer_cred()
                        .is_ok_and(|peer| peer.uid() == current_uid())
                    {
                        return (stream, ());
                    }
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
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

    fn fixture() -> (tempfile::TempDir, Endpoint) {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let endpoint = Endpoint {
            path: directory.path().join("daemon.sock"),
        };
        (directory, endpoint)
    }

    #[tokio::test]
    async fn same_user_connects_and_endpoint_is_exclusive_until_guard_release() {
        let (_directory, endpoint) = fixture();
        let (mut listener, guard) = bind(&endpoint).unwrap();
        assert!(bind(&endpoint).is_err());
        let mut client = connect(&endpoint).await.unwrap();
        let (mut server, ()) = listener.accept().await;
        client.write_all(b"test").await.unwrap();
        let mut bytes = [0; 4];
        server.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"test");
        drop(listener);
        assert!(bind(&endpoint).is_err());
        drop(server);
        drop(client);
        drop(guard);
        assert!(!endpoint.path.exists());
        assert!(bind(&endpoint).is_ok());
    }

    #[tokio::test]
    async fn stale_socket_is_recovered_but_regular_files_and_symlinks_are_preserved() {
        let (directory, endpoint) = fixture();
        let stale = std::os::unix::net::UnixListener::bind(&endpoint.path).unwrap();
        fs::set_permissions(&endpoint.path, fs::Permissions::from_mode(0o600)).unwrap();
        drop(stale);
        let (listener, guard) = bind(&endpoint).unwrap();
        drop(listener);
        drop(guard);
        fs::write(&endpoint.path, b"preserve").unwrap();
        assert!(bind(&endpoint).is_err());
        assert_eq!(fs::read(&endpoint.path).unwrap(), b"preserve");
        fs::remove_file(&endpoint.path).unwrap();
        let target = directory.path().join("target");
        fs::write(&target, b"preserve").unwrap();
        std::os::unix::fs::symlink(&target, &endpoint.path).unwrap();
        assert!(bind(&endpoint).is_err());
        assert!(connect(&endpoint).await.is_err());
        assert_eq!(fs::read(target).unwrap(), b"preserve");
    }

    #[tokio::test]
    async fn public_directory_and_socket_permissions_fail_closed() {
        let (directory, endpoint) = fixture();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o755)).unwrap();
        assert!(bind(&endpoint).is_err());
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let (_listener, _guard) = bind(&endpoint).unwrap();
        fs::set_permissions(&endpoint.path, fs::Permissions::from_mode(0o666)).unwrap();
        assert!(connect(&endpoint).await.is_err());
    }

    #[tokio::test]
    async fn guard_preserves_replacement_file_and_lock_symlinks_are_rejected() {
        let (directory, endpoint) = fixture();
        let (listener, guard) = bind(&endpoint).unwrap();
        fs::remove_file(&endpoint.path).unwrap();
        fs::write(&endpoint.path, b"replacement").unwrap();
        drop(listener);
        drop(guard);
        assert_eq!(fs::read(&endpoint.path).unwrap(), b"replacement");
        fs::remove_file(endpoint.path.with_extension("lock")).unwrap();
        let target = directory.path().join("keep-lock-target");
        fs::write(&target, b"unchanged").unwrap();
        std::os::unix::fs::symlink(&target, endpoint.path.with_extension("lock")).unwrap();
        assert!(bind(&endpoint).is_err());
        assert_eq!(fs::read(target).unwrap(), b"unchanged");
    }

    #[test]
    fn database_endpoint_is_stable_before_and_after_file_creation() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("state.db");
        let before = endpoint_for_database(&database).unwrap();
        fs::write(&database, []).unwrap();
        let after = endpoint_for_database(&database).unwrap();
        assert_eq!(before, after);
        let alias = directory.path().join("alias.db");
        std::os::unix::fs::symlink(&database, &alias).unwrap();
        assert_eq!(endpoint_for_database(&alias).unwrap(), after);
    }
}
