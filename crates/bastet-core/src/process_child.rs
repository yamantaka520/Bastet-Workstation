//! Exclusive ownership of an adapter child from spawn through cleanup.
//!
//! Unix process groups cover descendants that remain in the group, not hostile
//! setsid/setpgid escapes. Windows currently owns only the direct child; atomic
//! Job Object assignment is a separate, required enforcement gate.

use std::{
    io,
    process::{Child, ChildStdin, ChildStdout, Command},
    thread,
    time::{Duration, Instant},
};

/// Cleanup coverage, not a sandbox or proof of adversarial tree containment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessCleanupScope {
    /// Descendants can escape with setsid/setpgid; wrappers may do so normally.
    UnixProcessGroup,
    /// No atomically assigned Windows Job Object is implemented yet.
    DirectChildOnly,
}

/// The host must not install a competing child reaper. An observed ECHILD
/// disarms cleanup rather than risking a signal to a reused process id.
pub struct OwnedAdapterChild {
    // Never expose mutable Child: group signaling must precede reaping, while
    // the unreaped leader still reserves its PID/PGID against reuse.
    child: Option<Child>,
}

impl OwnedAdapterChild {
    pub const fn cleanup_scope(&self) -> ProcessCleanupScope {
        if cfg!(unix) {
            ProcessCleanupScope::UnixProcessGroup
        } else {
            ProcessCleanupScope::DirectChildOnly
        }
    }

    pub fn spawn(command: &mut Command) -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        Ok(Self {
            child: Some(command.spawn()?),
        })
    }

    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child.as_mut()?.stdin.take()
    }

    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.as_mut()?.stdout.take()
    }

    /// Bounded grace, followed by force termination and direct-child reaping.
    /// The OS may still delay reaping an uninterruptible kernel task. This is
    /// not a guarantee that descendants which escaped their group have exited.
    pub fn shutdown(&mut self, grace: Duration) -> io::Result<()> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        child.stdin.take();
        let deadline = Instant::now() + grace.min(Duration::from_millis(500));
        #[cfg(unix)]
        {
            loop {
                match exited_without_reaping(child.id()) {
                    Ok(true) => break,
                    Ok(false) if Instant::now() < deadline => {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Ok(false) => break,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) if error.raw_os_error() == Some(libc::ECHILD) => {
                        // A foreign reaper violated exclusive ownership. Its
                        // old PID is no longer safe to use for any signal.
                        self.child.take();
                        return Err(error);
                    }
                    Err(_) => break,
                }
            }
            let pid = i32::try_from(child.id())
                .map_err(|_| io::Error::other("invalid owned child id"))?;
            if pid <= 1 {
                return Err(io::Error::other("invalid owned child id"));
            }
            // SAFETY: spawn established an isolated group with this id. No
            // wait/try_wait has reaped the leader, so its id cannot be reused.
            let group_result = unsafe { libc::kill(-pid, libc::SIGKILL) };
            let group_error = (group_result != 0).then(io::Error::last_os_error);
            // Also terminate our direct child if it changed its own group.
            let _ = child.kill();
            let wait_result = child.wait().map(|_| ());
            self.child.take();
            if let Some(error) = group_error {
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error);
                }
            }
            wait_result
        }
        #[cfg(not(unix))]
        {
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => {
                        self.child.take();
                        return Ok(());
                    }
                    Ok(None) if Instant::now() < deadline => {
                        thread::sleep(Duration::from_millis(5))
                    }
                    _ => break,
                }
            }
            let _ = child.kill();
            let result = child.wait().map(|_| ());
            self.child.take();
            result
        }
    }
}

impl Drop for OwnedAdapterChild {
    fn drop(&mut self) {
        let _ = self.shutdown(Duration::ZERO);
    }
}

#[cfg(unix)]
fn exited_without_reaping(pid: u32) -> io::Result<bool> {
    // SAFETY: zero initialization is valid for siginfo_t; waitid fills it for
    // this owned child and WNOWAIT deliberately leaves its PID reserved.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if result == 0 {
        Ok(info.si_signo != 0)
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{
        io::{BufRead, BufReader, Read},
        process::Stdio,
        sync::mpsc,
    };

    fn descendant_fixture(exit_leader: bool) -> (OwnedAdapterChild, BufReader<ChildStdout>) {
        let mut command = Command::new("/bin/sh");
        command
            .args([
                "-c",
                if exit_leader {
                    "sleep 3 & printf 'ready\\n'; exit 0"
                } else {
                    "sleep 3 & printf 'ready\\n'; wait"
                },
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = OwnedAdapterChild::spawn(&mut command).unwrap();
        let mut stdout = BufReader::new(child.take_stdout().unwrap());
        let mut ready = String::new();
        stdout.read_line(&mut ready).unwrap();
        assert_eq!(ready, "ready\n");
        (child, stdout)
    }

    #[test]
    fn group_cleanup_closes_descendant_pipe_before_leader_reaping() {
        for exit_leader in [false, true] {
            let (mut child, mut stdout) = descendant_fixture(exit_leader);
            let (sender, receiver) = mpsc::channel();
            let reader = thread::spawn(move || {
                let mut bytes = Vec::new();
                sender.send(stdout.read_to_end(&mut bytes).is_ok()).unwrap();
            });
            child.shutdown(Duration::from_millis(25)).unwrap();
            child.shutdown(Duration::ZERO).unwrap();
            assert!(receiver.recv_timeout(Duration::from_secs(2)).unwrap());
            reader.join().unwrap();
        }
    }

    #[test]
    fn dropping_owned_child_cleans_up_its_group() {
        let (child, mut stdout) = descendant_fixture(false);
        let (sender, receiver) = mpsc::channel();
        let reader = thread::spawn(move || {
            let mut bytes = Vec::new();
            sender.send(stdout.read_to_end(&mut bytes).is_ok()).unwrap();
        });
        drop(child);
        assert!(receiver.recv_timeout(Duration::from_secs(2)).unwrap());
        reader.join().unwrap();
    }

    #[test]
    fn cleanup_does_not_signal_another_owned_group() {
        let (mut target, _target_stdout) = descendant_fixture(false);
        let (mut unrelated, _unrelated_stdout) = descendant_fixture(false);
        let unrelated_id = unrelated.child.as_ref().unwrap().id();
        assert_eq!(
            target.cleanup_scope(),
            ProcessCleanupScope::UnixProcessGroup
        );
        target.shutdown(Duration::ZERO).unwrap();
        assert!(!exited_without_reaping(unrelated_id).unwrap());
        unrelated.shutdown(Duration::ZERO).unwrap();
    }

    #[test]
    fn lost_wait_ownership_disarms_cleanup() {
        const MARKER: &str = "BASTET_TEST_IGNORED_SIGCHLD";
        if std::env::var_os(MARKER).is_none() {
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "process_child::tests::lost_wait_ownership_disarms_cleanup",
                ])
                .env(MARKER, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        // SAFETY: only this dedicated test subprocess changes signal policy.
        // It runs no other tests and exits immediately after this fixture.
        unsafe {
            libc::signal(libc::SIGCHLD, libc::SIG_IGN);
        }
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "exit 0"]);
        let mut child = OwnedAdapterChild::spawn(&mut command).unwrap();
        let result = child.shutdown(Duration::from_millis(500));
        assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::ECHILD));
        assert!(child.child.is_none());
        child.shutdown(Duration::ZERO).unwrap();
    }
}
