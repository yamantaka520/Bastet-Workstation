//! Deadline-bounded writes to an adapter child's stdin.
//!
//! This owns only stdin.  It deliberately does not kill or reap the child;
//! [`crate::OwnedAdapterChild`] remains responsible for process cleanup.

use std::{
    io::{self, Write},
    process::ChildStdin,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde::Serialize;

/// The largest accepted stdin frame, including the JSON-lines newline.
pub const MAX_INPUT_FRAME_BYTES: usize = 8 * 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(10);

struct WriteRequest {
    frame: Vec<u8>,
    deadline: Instant,
    result: mpsc::Sender<io::Result<()>>,
}

/// A single-flight, deadline-bounded writer for a child stdin pipe.
///
/// Requests are passed through a zero-capacity channel, so this type never
/// accumulates an unbounded queue of frames. A timeout or I/O failure poisons
/// the writer, closes stdin, and makes all later writes fail.
pub struct ProcessInputWriter {
    sender: Option<SyncSender<WriteRequest>>,
    stopped: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    poisoned: bool,
}

impl ProcessInputWriter {
    /// Starts the dedicated stdin writer thread.
    pub fn spawn(stdin: ChildStdin) -> io::Result<Self> {
        prepare_stdin(&stdin)?;
        let (sender, receiver) = mpsc::sync_channel(0);
        let stopped = Arc::new(AtomicBool::new(false));
        let worker_stopped = Arc::clone(&stopped);
        let thread = thread::Builder::new()
            .name("bastet-process-input".into())
            .spawn(move || write_stdin(stdin, receiver, worker_stopped))?;
        Ok(Self {
            sender: Some(sender),
            stopped,
            thread: Some(thread),
            poisoned: false,
        })
    }

    /// Writes exactly one frame before the absolute `deadline`.
    ///
    /// Frames larger than 8 MiB are rejected before they reach the worker.
    /// A timeout is terminal because the pipe may contain an indeterminate
    /// prefix of the frame.
    pub fn write_frame(&mut self, frame: Vec<u8>, deadline: Instant) -> io::Result<()> {
        if self.poisoned || self.sender.is_none() {
            return Err(unusable_error());
        }
        if frame.len() > MAX_INPUT_FRAME_BYTES {
            return Err(oversized_error());
        }
        if Instant::now() >= deadline {
            self.poison_and_close();
            return Err(timed_out_error());
        }

        let (result_sender, result_receiver) = mpsc::channel();
        let mut request = WriteRequest {
            frame,
            deadline,
            result: result_sender,
        };
        let sender = self.sender.as_ref().expect("checked above");
        loop {
            if Instant::now() >= deadline {
                self.poison_and_close();
                return Err(timed_out_error());
            }
            match sender.try_send(request) {
                Ok(()) => break,
                Err(mpsc::TrySendError::Full(returned)) => {
                    request = returned;
                    if Instant::now() >= deadline {
                        self.poison_and_close();
                        return Err(timed_out_error());
                    }
                    thread::sleep(POLL_INTERVAL);
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    self.poison_and_close();
                    return Err(unusable_error());
                }
            }
        }

        match result_receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                self.poison_and_close();
                Err(error)
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.poison_and_close();
                Err(timed_out_error())
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                self.poison_and_close();
                Err(unusable_error())
            }
        }
    }

    /// Closes stdin and joins the worker. It is safe to call repeatedly.
    pub fn close(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            #[cfg(windows)]
            {
                use std::os::windows::io::AsRawHandle;
                // A synchronous pipe write may be blocked. Repeating this
                // closes the stop-check / I/O-entry race without terminating
                // the worker thread or touching another thread's I/O.
                while !thread.is_finished() {
                    unsafe {
                        windows_sys::Win32::System::IO::CancelSynchronousIo(thread.as_raw_handle());
                    }
                    thread::sleep(POLL_INTERVAL);
                }
            }
            let _ = thread.join();
        }
    }

    fn poison_and_close(&mut self) {
        self.poisoned = true;
        self.close();
    }
}

impl Drop for ProcessInputWriter {
    fn drop(&mut self) {
        self.close();
    }
}

/// Serializes a value as one bounded JSON-lines frame.
///
/// The limit includes the trailing newline, and serialization writes directly
/// into a bounded sink so escaping cannot transiently over-allocate a large
/// intermediate JSON string.
pub fn encode_json_line(value: &impl Serialize) -> io::Result<Vec<u8>> {
    let mut sink = BoundedVec::new(MAX_INPUT_FRAME_BYTES - 1);
    serde_json::to_writer(&mut sink, value).map_err(json_error)?;
    sink.limit = MAX_INPUT_FRAME_BYTES;
    sink.write_all(b"\n")?;
    Ok(sink.bytes)
}

struct BoundedVec {
    bytes: Vec<u8>,
    limit: usize,
}

impl BoundedVec {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }
}

impl Write for BoundedVec {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let new_len = self
            .bytes
            .len()
            .checked_add(buffer.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(oversized_error)?;
        // Use bounded geometric growth, then reserve exactly that capacity.
        // Unbounded Vec growth can exceed the frame budget, while reserving
        // each escaped character separately can require millions of reallocs.
        if self.bytes.capacity() < new_len {
            let capacity = self
                .bytes
                .capacity()
                .max(1024)
                .saturating_mul(2)
                .max(new_len)
                .min(self.limit);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate JSON frame")
                })?;
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn json_error(error: serde_json::Error) -> io::Error {
    match error.io_error_kind() {
        Some(io::ErrorKind::InvalidInput) => oversized_error(),
        Some(kind) => io::Error::new(kind, error),
        None => io::Error::new(io::ErrorKind::InvalidData, error),
    }
}

fn oversized_error() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "stdin frame exceeds 8 MiB")
}

fn timed_out_error() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "stdin write deadline elapsed")
}

fn unusable_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "stdin writer is closed or poisoned",
    )
}

fn write_stdin(mut stdin: ChildStdin, receiver: Receiver<WriteRequest>, stopped: Arc<AtomicBool>) {
    while let Ok(request) = receiver.recv() {
        let result = write_one(&mut stdin, &request.frame, request.deadline, &stopped);
        let terminal = result.is_err();
        let _ = request.result.send(result);
        if terminal || stopped.load(Ordering::Acquire) {
            return;
        }
    }
}

fn write_one(
    stdin: &mut ChildStdin,
    frame: &[u8],
    deadline: Instant,
    stopped: &AtomicBool,
) -> io::Result<()> {
    let mut offset = 0;
    while offset < frame.len() {
        if stopped.load(Ordering::Acquire) {
            return Err(unusable_error());
        }
        if Instant::now() >= deadline {
            return Err(timed_out_error());
        }
        match stdin.write(&frame[offset..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "child stdin accepted no bytes",
                ))
            }
            Ok(written) => offset += written,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => thread::sleep(POLL_INTERVAL),
            Err(error) => return Err(error),
        }
    }
    if Instant::now() >= deadline {
        Err(timed_out_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn prepare_stdin(stdin: &ChildStdin) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let fd = stdin.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(windows)]
fn prepare_stdin(_stdin: &ChildStdin) -> io::Result<()> {
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn prepare_stdin(_stdin: &ChildStdin) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "bounded process-input writer is unsupported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        env, fs,
        io::Read,
        process::{Command, Stdio},
        sync::mpsc,
    };

    const FIXTURE_MODE: &str = "BASTET_PROCESS_INPUT_FIXTURE_MODE";
    const FIXTURE_OUTPUT: &str = "BASTET_PROCESS_INPUT_FIXTURE_OUTPUT";
    const READY: &str = "fixture-ready";
    const COMPLETE: &str = "fixture-complete";

    #[test]
    fn json_escaping_counts_against_the_limit() {
        let value = "\u{0000}".repeat((MAX_INPUT_FRAME_BYTES - 2) / 6 + 1);
        assert_eq!(
            encode_json_line(&value).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn json_exact_bound_includes_newline() {
        // Quotes plus newline leave exactly MAX-3 bytes for an ASCII string.
        let value = "x".repeat(MAX_INPUT_FRAME_BYTES - 3);
        let frame = encode_json_line(&value).unwrap();
        assert_eq!(frame.len(), MAX_INPUT_FRAME_BYTES);
        assert!(frame.capacity() <= MAX_INPUT_FRAME_BYTES);
    }

    #[test]
    fn native_process_fixture() {
        let Ok(mode) = env::var(FIXTURE_MODE) else {
            return;
        };
        println!("{READY}");
        std::io::stdout().flush().unwrap();
        match mode.as_str() {
            "read" => {
                // The fixture itself is bounded too: it only accepts the
                // small positive-test frame and never uses read_to_end.
                let mut received = Vec::with_capacity(1024);
                let mut chunk = [0_u8; 256];
                while received.len() < 1024 {
                    let remaining = 1024 - received.len();
                    let chunk_limit = remaining.min(chunk.len());
                    let read = std::io::stdin().read(&mut chunk[..chunk_limit]).unwrap();
                    if read == 0 {
                        break;
                    }
                    received.extend_from_slice(&chunk[..read]);
                }
                fs::write(env::var(FIXTURE_OUTPUT).unwrap(), received).unwrap();
                println!("{COMPLETE}");
                std::io::stdout().flush().unwrap();
            }
            "never-read" => thread::sleep(Duration::from_secs(30)),
            _ => panic!("unknown process-input fixture mode"),
        }
    }

    fn spawn_fixture(
        mode: &str,
        output: Option<&std::path::Path>,
    ) -> (
        crate::OwnedAdapterChild,
        ProcessInputWriter,
        crate::ProcessOutputReader,
        mpsc::Receiver<String>,
    ) {
        let mut command = Command::new(env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "process_input::tests::native_process_fixture",
                "--nocapture",
            ])
            .env(FIXTURE_MODE, mode)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        if let Some(output) = output {
            command.env(FIXTURE_OUTPUT, output);
        }
        let mut child = crate::OwnedAdapterChild::spawn(&mut command).unwrap();
        let writer = ProcessInputWriter::spawn(child.take_stdin().unwrap()).unwrap();
        let (sender, receiver) = mpsc::channel();
        let reader =
            crate::ProcessOutputReader::spawn(
                child.take_stdout().unwrap(),
                move |line| match line {
                    Ok(line) => sender.send(line).is_ok(),
                    Err(()) => false,
                },
            )
            .unwrap();
        (child, writer, reader, receiver)
    }

    fn wait_for(receiver: &mpsc::Receiver<String>, expected: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let line = receiver
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap();
            if line == expected {
                return;
            }
        }
    }

    #[test]
    fn writes_a_frame_to_a_reading_child() {
        let output = tempfile::NamedTempFile::new().unwrap();
        let (mut child, mut writer, mut reader, receiver) =
            spawn_fixture("read", Some(output.path()));
        wait_for(&receiver, READY);
        writer
            .write_frame(
                b"hello bounded stdin\n".to_vec(),
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap();
        writer.close();
        // Closing the pipe is not evidence that the child has consumed it.
        // Wait for its explicit bounded fixture completion before reading.
        wait_for(&receiver, COMPLETE);
        assert_eq!(fs::read(output.path()).unwrap(), b"hello bounded stdin\n");
        reader.close();
        child.shutdown(Duration::ZERO).unwrap();
    }

    #[test]
    fn blocked_pipe_times_out_and_poison_is_sticky() {
        let (mut child, mut writer, mut reader, receiver) = spawn_fixture("never-read", None);
        wait_for(&receiver, READY);
        let start = Instant::now();
        assert_eq!(
            writer
                .write_frame(
                    vec![b'x'; MAX_INPUT_FRAME_BYTES],
                    start + Duration::from_millis(200)
                )
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
        assert!(start.elapsed() < Duration::from_secs(3));
        assert_eq!(
            writer
                .write_frame(vec![b'x'], Instant::now() + Duration::from_secs(1))
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        writer.close();
        writer.close();
        reader.close();
        child.shutdown(Duration::ZERO).unwrap();
    }
}
