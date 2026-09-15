//! Cancellable line-oriented readers with a bounded individual line size.
//!
//! This deliberately owns only the stdout handle.  Process lifecycle remains
//! the caller's responsibility, so closing a reader never kills a child.

use std::{
    io::{self, Read},
    process::ChildStdout,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

const READ_CHUNK_BYTES: usize = 8 * 1024;
const MAX_LINE_BYTES: usize = 8 * 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// A background stdout reader which can be stopped without waiting for more
/// output from the child.
pub struct ProcessOutputReader {
    stopped: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl ProcessOutputReader {
    /// Starts consuming `stdout` as UTF-8 lines.
    ///
    /// The sink is invoked with `Err(())` for I/O, UTF-8, or line-size errors.
    /// Returning `false` asks the reader to stop.  A line is at most 8 MiB;
    /// an overlong line is rejected rather than accumulated indefinitely.
    /// The sink must not block: cancellation cannot interrupt sink code.
    /// Any downstream queue requires its own total-memory policy.
    pub fn spawn(
        stdout: ChildStdout,
        sink: impl FnMut(Result<String, ()>) -> bool + Send + 'static,
    ) -> io::Result<Self> {
        prepare_stdout(&stdout)?;

        let stopped = Arc::new(AtomicBool::new(false));
        let thread_stopped = Arc::clone(&stopped);
        let thread = thread::Builder::new()
            .name("bastet-process-output".into())
            .spawn(move || read_stdout(stdout, thread_stopped, sink))?;

        Ok(Self {
            stopped,
            thread: Some(thread),
        })
    }

    /// Stops the reader and waits for its thread to exit.
    pub fn close(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            #[cfg(windows)]
            {
                use std::os::windows::io::AsRawHandle;
                // PeekNamedPipe may block on a synchronous pipe. Repeating
                // cancellation also closes the stop-check / I/O-entry race:
                // an earlier ERROR_NOT_FOUND does not mean a later I/O cannot
                // start. Keep the thread handle alive until it has exited.
                while !thread.is_finished() {
                    // SAFETY: this is our live reader thread's owned handle.
                    // Cancellation affects only its synchronous I/O; it never
                    // terminates the thread or closes a borrowed pipe handle.
                    unsafe {
                        windows_sys::Win32::System::IO::CancelSynchronousIo(thread.as_raw_handle());
                    }
                    thread::sleep(POLL_INTERVAL);
                }
            }
            let _ = thread.join();
        }
    }
}

impl Drop for ProcessOutputReader {
    fn drop(&mut self) {
        self.close();
    }
}

fn read_stdout(
    mut stdout: ChildStdout,
    stopped: Arc<AtomicBool>,
    mut sink: impl FnMut(Result<String, ()>) -> bool,
) {
    let mut decoder = LineDecoder::default();
    let mut chunk = [0_u8; READ_CHUNK_BYTES];

    loop {
        if stopped.load(Ordering::Acquire) {
            return;
        }

        match available_bytes(&stdout) {
            Ok(None) => {
                if !stopped.load(Ordering::Acquire) {
                    let _ = decoder.finish(&mut sink);
                }
                return;
            }
            Ok(Some(0)) => thread::sleep(POLL_INTERVAL),
            Ok(Some(available)) => {
                if stopped.load(Ordering::Acquire) {
                    return;
                }
                let to_read = available.min(chunk.len());
                match stdout.read(&mut chunk[..to_read]) {
                    Ok(0) => {
                        if !stopped.load(Ordering::Acquire) {
                            let _ = decoder.finish(&mut sink);
                        }
                        return;
                    }
                    Ok(read) => {
                        if stopped.load(Ordering::Acquire)
                            || !decoder.push(&chunk[..read], &mut sink)
                        {
                            return;
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(POLL_INTERVAL);
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => {
                        let _ = sink(Err(()));
                        return;
                    }
                }
            }
            Err(_) => {
                let _ = sink(Err(()));
                return;
            }
        }
    }
}

#[derive(Default)]
struct LineDecoder {
    partial: Vec<u8>,
}

impl LineDecoder {
    fn push(&mut self, bytes: &[u8], sink: &mut impl FnMut(Result<String, ()>) -> bool) -> bool {
        let mut start = 0;
        while let Some(relative_end) = bytes[start..].iter().position(|byte| *byte == b'\n') {
            let end = start + relative_end;
            self.partial.extend_from_slice(&bytes[start..end]);
            if self.partial.len() > MAX_LINE_BYTES {
                let _ = sink(Err(()));
                return false;
            }
            if !self.emit(sink, true) {
                return false;
            }
            start = end + 1;
        }

        self.partial.extend_from_slice(&bytes[start..]);
        if self.partial.len() > MAX_LINE_BYTES {
            let _ = sink(Err(()));
            return false;
        }
        true
    }

    fn finish(&mut self, sink: &mut impl FnMut(Result<String, ()>) -> bool) -> bool {
        self.partial.is_empty() || self.emit(sink, false)
    }

    fn emit(&mut self, sink: &mut impl FnMut(Result<String, ()>) -> bool, newline: bool) -> bool {
        if newline && self.partial.last() == Some(&b'\r') {
            self.partial.pop();
        }
        let line = match String::from_utf8(std::mem::take(&mut self.partial)) {
            Ok(line) => line,
            Err(_) => {
                let _ = sink(Err(()));
                return false;
            }
        };
        sink(Ok(line))
    }
}

#[cfg(unix)]
fn prepare_stdout(stdout: &ChildStdout) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let fd = stdout.as_raw_fd();
    // SAFETY: `fd` belongs to the live ChildStdout and fcntl does not take
    // ownership of it.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: as above; only the nonblocking status flag is changed.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
fn available_bytes(_stdout: &ChildStdout) -> io::Result<Option<usize>> {
    // Nonblocking reads are the source of truth on Unix.  The loop will read
    // immediately and handle WouldBlock without ever blocking.
    Ok(Some(READ_CHUNK_BYTES))
}

#[cfg(windows)]
fn prepare_stdout(_stdout: &ChildStdout) -> io::Result<()> {
    Ok(())
}

#[cfg(windows)]
fn available_bytes(stdout: &ChildStdout) -> io::Result<Option<usize>> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::ERROR_BROKEN_PIPE;
    use windows_sys::Win32::System::Pipes::PeekNamedPipe;

    let mut available = 0_u32;
    // SAFETY: stdout is a valid pipe handle; null pointers specify that only
    // the byte-count is requested, and `available` is valid writable storage.
    let result = unsafe {
        PeekNamedPipe(
            stdout.as_raw_handle(),
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            &mut available,
            std::ptr::null_mut(),
        )
    };
    if result == 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(ERROR_BROKEN_PIPE as i32) {
            return Ok(None);
        }
        return Err(error);
    }
    Ok(Some(available as usize))
}

#[cfg(not(any(unix, windows)))]
fn prepare_stdout(_stdout: &ChildStdout) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "cancellable process-output reader is unsupported on this platform",
    ))
}

#[cfg(not(any(unix, windows)))]
fn available_bytes(_stdout: &ChildStdout) -> io::Result<Option<usize>> {
    unreachable!("unsupported platforms fail before the reader thread starts")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(chunks: &[&[u8]], finish: bool) -> Vec<Result<String, ()>> {
        let mut decoder = LineDecoder::default();
        let mut received = Vec::new();
        for chunk in chunks {
            if !decoder.push(chunk, &mut |line| {
                received.push(line);
                true
            }) {
                return received;
            }
        }
        if finish {
            assert!(decoder.finish(&mut |line| {
                received.push(line);
                true
            }));
        }
        received
    }

    #[test]
    fn retains_partial_lines_and_strips_line_endings() {
        assert_eq!(
            decode(&[b"first\r", b"\nsecond", b"\nthird"], true),
            vec![Ok("first".into()), Ok("second".into()), Ok("third".into())]
        );
    }

    #[test]
    fn rejects_invalid_utf8() {
        assert_eq!(
            decode(&[b"good\n\xff\n"], false),
            vec![Ok("good".into()), Err(())]
        );
    }

    #[test]
    fn rejects_oversized_line() {
        let mut oversized = vec![b'x'; MAX_LINE_BYTES + 1];
        assert_eq!(decode(&[&oversized], false), vec![Err(())]);
        oversized.push(b'\n');
        assert_eq!(decode(&[&oversized], false), vec![Err(())]);
        assert_eq!(
            decode(&[&oversized[..MAX_LINE_BYTES], b"x\n"], false),
            vec![Err(())]
        );
    }

    #[test]
    fn preserves_final_bare_carriage_return() {
        assert_eq!(decode(&[b"last\r"], true), vec![Ok("last\r".into())]);
    }

    #[test]
    fn reader_stops_while_a_ready_child_still_holds_stdout() {
        use std::io::Write;
        use std::process::{Command, Stdio};
        use std::sync::mpsc;
        use std::time::Instant;
        const MARKER: &str = "BASTET_TEST_SILENT_OUTPUT_CHILD";
        if std::env::var_os(MARKER).is_some() {
            println!("fixture-ready");
            std::io::stdout().flush().unwrap();
            thread::sleep(Duration::from_secs(3));
            return;
        }
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "process_output::tests::reader_stops_while_a_ready_child_still_holds_stdout",
                "--nocapture",
            ])
            .env(MARKER, "1")
            .stdout(Stdio::piped());
        let mut child = crate::OwnedAdapterChild::spawn(&mut command).unwrap();
        let (sender, receiver) = mpsc::channel();
        let mut reader = ProcessOutputReader::spawn(child.take_stdout().unwrap(), move |line| {
            if line.as_deref() == Ok("fixture-ready") {
                let _ = sender.send(());
            }
            true
        })
        .unwrap();
        receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        let closing = Instant::now();
        reader.close();
        assert!(closing.elapsed() < Duration::from_secs(1));
        child.shutdown(Duration::ZERO).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn close_returns_promptly_when_child_keeps_stdout_open() {
        use std::process::Command;
        use std::time::{Duration, Instant};

        let mut command = Command::new("sh");
        command
            .args(["-c", "exec sleep 30"])
            .stdout(std::process::Stdio::piped());
        let mut child = crate::OwnedAdapterChild::spawn(&mut command).expect("spawn sleep");
        let stdout = child.take_stdout().expect("piped stdout");
        let start = Instant::now();
        let mut reader = ProcessOutputReader::spawn(stdout, |_| true).expect("start reader");
        reader.close();
        assert!(start.elapsed() < Duration::from_secs(1));
        child.shutdown(Duration::ZERO).unwrap();
    }
}
