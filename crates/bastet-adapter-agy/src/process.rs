use std::{
    io::Write,
    path::{Path, PathBuf},
    process::{ChildStdin, Stdio},
    sync::mpsc::{self, Receiver, RecvTimeoutError},
    time::{Duration, Instant},
};

use bastet_core::{
    configure_adapter_process_environment, AdapterProcessLauncher, DirectAdapterProcessLauncher,
    OwnedAdapterChild, ProcessOutputReader, RunId, WorkspaceEvidenceError, WorkspaceSnapshot,
};
use serde_json::json;
use thiserror::Error;

use crate::{AgyRunStream, AgyRunUpdate, AgyStreamError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgyRunRequest {
    pub run_id: RunId,
    pub model: String,
    pub effort: Option<String>,
    pub prompt: String,
    pub cwd: PathBuf,
    pub read_only: bool,
    pub timeout: Duration,
    pub conversation_id: Option<String>,
}

#[derive(Debug, Error)]
pub enum AgyProcessError {
    #[error("Agy run request is invalid")]
    InvalidRequest,
    #[error("Agy CLI process is unavailable")]
    Unavailable,
    #[error("Agy CLI process timed out")]
    TimedOut,
    #[error("Agy CLI process cleanup could not be confirmed")]
    CleanupUncertain,
    #[error(transparent)]
    Stream(#[from] AgyStreamError),
    #[error(transparent)]
    Workspace(#[from] WorkspaceEvidenceError),
}

pub struct AgyProcess {
    child: OwnedAdapterChild,
    stdin: Option<ChildStdin>,
    lines: Receiver<Result<String, ()>>,
    reader: ProcessOutputReader,
    stream: AgyRunStream,
    timeout: Duration,
    inactivity_deadline: Instant,
    cancellation_requested: bool,
    pending: Option<AgyRunUpdate>,
    pending_terminal: Option<AgyRunUpdate>,
    before: Option<WorkspaceSnapshot>,
}

impl AgyProcess {
    pub fn spawn(executable: PathBuf, request: AgyRunRequest) -> Result<Self, AgyProcessError> {
        let launcher = DirectAdapterProcessLauncher;
        Self::spawn_with_launcher(executable, request, &launcher)
    }

    /// Starts Agy through the supplied launcher. Its environment is cleared and
    /// replaced with the adapter allowlist below, so launcher-provided
    /// environment values are deliberately overridden.
    pub fn spawn_with_launcher(
        executable: PathBuf,
        request: AgyRunRequest,
        launcher: &dyn AdapterProcessLauncher,
    ) -> Result<Self, AgyProcessError> {
        validate_request(&executable, &request)?;
        let before = if request.read_only {
            None
        } else {
            Some(WorkspaceSnapshot::capture(&request.cwd)?)
        };
        let timeout_seconds = request.timeout.as_secs().to_string();
        let timeout_argument = format!("{timeout_seconds}s");
        let is_resume = request.conversation_id.is_some();
        let cwd = request
            .cwd
            .to_str()
            .ok_or(AgyProcessError::InvalidRequest)?;
        // Invoke the launcher once only after all request checks have passed.
        // A launcher rejection is unavailable; never fall back to direct spawn.
        let mut command = launcher
            .command(&executable)
            .map_err(|_| AgyProcessError::Unavailable)?;
        configure_adapter_process_environment(&mut command);
        command
            .args([
                "--input-format",
                "stream-json",
                "--output-format",
                "stream-json",
                "--mode",
                if request.read_only {
                    "plan"
                } else {
                    "accept-edits"
                },
                "--sandbox",
                "--add-dir",
                cwd,
                "--model",
                &request.model,
                "--print-timeout",
                &timeout_argument,
            ])
            .current_dir(&request.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(effort) = &request.effort {
            command.args(["--effort", effort]);
        }
        if let Some(conversation_id) = &request.conversation_id {
            command.args(["--conversation", conversation_id]);
        }
        // From this point the child is exclusively owned, so every fallible
        // initialization path below cleans it up through OwnedAdapterChild.
        let mut child =
            OwnedAdapterChild::spawn(&mut command).map_err(|_| AgyProcessError::Unavailable)?;
        let mut stdin = child.take_stdin().ok_or(AgyProcessError::Unavailable)?;
        let stdout = child.take_stdout().ok_or(AgyProcessError::Unavailable)?;
        serde_json::to_writer(
            &mut stdin,
            &json!({"event":"user","message":{"content":request.prompt}}),
        )
        .map_err(|_| AgyProcessError::Unavailable)?;
        stdin
            .write_all(b"\n")
            .and_then(|_| stdin.flush())
            .map_err(|_| AgyProcessError::Unavailable)?;

        let (sender, lines) = mpsc::channel();
        let reader = ProcessOutputReader::spawn(stdout, move |line| sender.send(line).is_ok())
            .map_err(|_| AgyProcessError::Unavailable)?;
        let mut stream = if let Some(conversation_id) = request.conversation_id {
            AgyRunStream::resuming(request.run_id, conversation_id)?
        } else {
            AgyRunStream::new(request.run_id)
        };
        let pending = if is_resume {
            Some(stream.recovery_started("locally-observed")?)
        } else {
            None
        };
        Ok(Self {
            child,
            stdin: Some(stdin),
            lines,
            reader,
            stream,
            timeout: request.timeout,
            inactivity_deadline: Instant::now() + request.timeout,
            cancellation_requested: false,
            pending,
            pending_terminal: None,
            before,
        })
    }

    pub fn conversation_id(&self) -> Option<&str> {
        self.stream.conversation_id()
    }

    /// Returns the bounded final assistant Markdown retained outside the
    /// lifecycle/cost/write-receipt update stream.
    pub fn final_output(&self) -> Option<&str> {
        self.stream.final_output()
    }

    /// True when a provider answer exceeded the capture bound and was
    /// discarded rather than silently truncated.
    pub fn final_output_overflowed(&self) -> bool {
        self.stream.final_output_overflowed()
    }

    pub fn next_update(&mut self, occurred_at: &str) -> Result<AgyRunUpdate, AgyProcessError> {
        // Preserve the historical API: a caller which is willing to wait for
        // the configured inactivity interval still receives its terminal
        // timeout as an update rather than an observation timeout.
        loop {
            if let Some(update) = self.poll_update(occurred_at, self.timeout)? {
                return Ok(update);
            }
        }
    }

    /// Waits at most `max_wait` for the next observable update. `None` means
    /// only that this observation window elapsed; the provider is still
    /// running and its configured inactivity deadline remains in force.
    pub fn poll_update(
        &mut self,
        occurred_at: &str,
        max_wait: Duration,
    ) -> Result<Option<AgyRunUpdate>, AgyProcessError> {
        if let Some(update) = self.pending.take() {
            return Ok(Some(update));
        }
        if let Some(update) = self.pending_terminal.take() {
            return Ok(Some(update));
        }
        let observation_deadline = Instant::now().checked_add(max_wait);
        loop {
            let now = Instant::now();
            if now >= self.inactivity_deadline {
                self.terminate()?;
                let terminal = self.stream.timed_out(occurred_at)?;
                return self.order_terminal(terminal).map(Some);
            }
            if observation_deadline.is_some_and(|deadline| now >= deadline) {
                return Ok(None);
            }
            let wait = observation_deadline
                .map(|deadline| deadline.saturating_duration_since(now))
                .unwrap_or(Duration::MAX)
                .min(self.inactivity_deadline.saturating_duration_since(now));
            match self.lines.recv_timeout(wait) {
                Ok(Ok(line)) => {
                    // Any provider output is activity, even when it does not
                    // normalize to a public update.
                    self.inactivity_deadline = Instant::now() + self.timeout;
                    if let Some(update) = self.stream.consume_line(&line, occurred_at)? {
                        if is_terminal(&update) {
                            self.stdin.take();
                            self.reap()?;
                            return self.order_terminal(update).map(Some);
                        }
                        return Ok(Some(update));
                    }
                }
                Ok(Err(())) | Err(RecvTimeoutError::Disconnected) => {
                    self.reap()?;
                    let terminal = if self.cancellation_requested {
                        self.stream.cancelled(occurred_at)?
                    } else {
                        self.stream.crashed(occurred_at)?
                    };
                    return self.order_terminal(terminal).map(Some);
                }
                // Re-check both deadlines so a short observation window never
                // becomes a terminal provider timeout.
                Err(RecvTimeoutError::Timeout) => continue,
            }
        }
    }

    pub fn request_cancellation(
        &mut self,
        occurred_at: &str,
    ) -> Result<AgyRunUpdate, AgyProcessError> {
        let update = self.stream.cancellation_started(occurred_at)?;
        self.cancellation_requested = true;
        self.terminate()?;
        Ok(update)
    }

    fn terminate(&mut self) -> Result<(), AgyProcessError> {
        self.stdin.take();
        let result = self.child.shutdown(Duration::ZERO);
        self.reader.close();
        result.map_err(|_| AgyProcessError::CleanupUncertain)
    }

    fn reap(&mut self) -> Result<(), AgyProcessError> {
        self.stdin.take();
        let result = self.child.shutdown(Duration::from_millis(50));
        self.reader.close();
        result.map_err(|_| AgyProcessError::CleanupUncertain)
    }

    fn order_terminal(&mut self, terminal: AgyRunUpdate) -> Result<AgyRunUpdate, AgyProcessError> {
        let Some(before) = self.before.take() else {
            return Ok(terminal);
        };
        let after = WorkspaceSnapshot::capture(before.root())?;
        let Some(receipt) = before.write_receipt(&after)? else {
            return Ok(terminal);
        };
        self.pending_terminal = Some(terminal);
        Ok(AgyRunUpdate::WriteReceipt(receipt))
    }
}

impl Drop for AgyProcess {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

fn validate_request(executable: &Path, request: &AgyRunRequest) -> Result<(), AgyProcessError> {
    if !executable.is_file()
        || !request.cwd.is_absolute()
        || !request.cwd.is_dir()
        || request.prompt.trim().is_empty()
        || request.timeout.is_zero()
        || request.timeout.as_secs() == 0
        || request
            .effort
            .as_deref()
            .is_some_and(|effort| !matches!(effort, "low" | "medium" | "high"))
        || request.model.trim().is_empty()
        || request.model.len() > 128
        || !request
            .model
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/'))
        || request
            .conversation_id
            .as_deref()
            .is_some_and(|id| id.trim().is_empty())
    {
        return Err(AgyProcessError::InvalidRequest);
    }
    Ok(())
}

fn is_terminal(update: &AgyRunUpdate) -> bool {
    matches!(
        update,
        AgyRunUpdate::Lifecycle { event, .. }
            if matches!(
                event.state,
                bastet_core::NormalizedRunState::Succeeded
                    | bastet_core::NormalizedRunState::Failed
                    | bastet_core::NormalizedRunState::Cancelled
                    | bastet_core::NormalizedRunState::Blocked
                    | bastet_core::NormalizedRunState::Uncertain
            )
    )
}

#[cfg(all(test, unix))]
mod tests {
    use std::{cell::Cell, io, os::unix::fs::PermissionsExt, process::Command};

    use bastet_core::NormalizedRunState;

    use super::*;

    struct FixtureProcess {
        process: AgyProcess,
        _root: tempfile::TempDir,
    }

    fn fixture_process(mode: &str, timeout: Duration) -> FixtureProcess {
        let root = tempfile::tempdir().unwrap();
        // Use a committed executable, not a just-written temporary script:
        // parallel process startup must not race writable fixture handles.
        let executable =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/poll-provider.sh");
        let process = AgyProcess::spawn(
            executable,
            AgyRunRequest {
                run_id: RunId::from_bytes([61; 16]),
                model: mode.into(),
                effort: None,
                prompt: "test".into(),
                cwd: root.path().to_path_buf(),
                read_only: true,
                timeout,
                conversation_id: None,
            },
        )
        .unwrap();
        FixtureProcess {
            process,
            _root: root,
        }
    }

    fn descendant_stdout_fixture() -> FixtureProcess {
        let root = tempfile::tempdir().unwrap();
        let executable = root.path().join("stdout-holding-descendant.sh");
        // The leader exits after its running event, while its descendant keeps
        // stdout open. Cancellation must finish the owned group before closing
        // the output reader, so no fixture descendant is left behind.
        std::fs::write(
            &executable,
            "#!/bin/sh\nread -r prompt || exit 1\nsleep 30 &\nprintf '%s\\n' '{\"event\":\"init\",\"conversation_id\":\"descendant-test\",\"init\":{}}'\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let process = AgyProcess::spawn(executable, fixture_request(&root, "descendant")).unwrap();
        FixtureProcess {
            process,
            _root: root,
        }
    }

    struct RejectingLauncher {
        calls: Cell<usize>,
    }

    impl AdapterProcessLauncher for RejectingLauncher {
        fn command(&self, _executable: &Path) -> io::Result<Command> {
            self.calls.set(self.calls.get() + 1);
            Err(io::Error::other("synthetic launcher rejection"))
        }
    }

    struct ShellWrapperLauncher {
        calls: Cell<usize>,
    }

    impl AdapterProcessLauncher for ShellWrapperLauncher {
        fn command(&self, executable: &Path) -> io::Result<Command> {
            self.calls.set(self.calls.get() + 1);
            let mut command = Command::new("sh");
            command.arg(executable);
            Ok(command)
        }
    }

    fn fixture_request(root: &tempfile::TempDir, model: &str) -> AgyRunRequest {
        AgyRunRequest {
            run_id: RunId::from_bytes([62; 16]),
            model: model.into(),
            effort: None,
            prompt: "test".into(),
            cwd: root.path().to_path_buf(),
            read_only: true,
            timeout: Duration::from_secs(10),
            conversation_id: None,
        }
    }

    #[test]
    fn rejected_request_does_not_invoke_the_launcher() {
        let root = tempfile::tempdir().unwrap();
        let launcher = RejectingLauncher {
            calls: Cell::new(0),
        };
        let request = AgyRunRequest {
            prompt: "   ".into(),
            ..fixture_request(&root, "delayed-init")
        };

        assert!(matches!(
            AgyProcess::spawn_with_launcher(
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/poll-provider.sh"),
                request,
                &launcher,
            ),
            Err(AgyProcessError::InvalidRequest)
        ));
        assert_eq!(launcher.calls.get(), 0);
    }

    #[test]
    fn launcher_rejection_is_unavailable_without_a_direct_spawn() {
        let root = tempfile::tempdir().unwrap();
        let launcher = RejectingLauncher {
            calls: Cell::new(0),
        };

        assert!(matches!(
            AgyProcess::spawn_with_launcher(
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/poll-provider.sh"),
                fixture_request(&root, "delayed-init"),
                &launcher,
            ),
            Err(AgyProcessError::Unavailable)
        ));
        assert_eq!(launcher.calls.get(), 1);
    }

    #[test]
    fn wrapper_launcher_receives_the_command_before_agy_arguments_are_applied() {
        let root = tempfile::tempdir().unwrap();
        let launcher = ShellWrapperLauncher {
            calls: Cell::new(0),
        };
        let executable =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/poll-provider.sh");
        let mut process = AgyProcess::spawn_with_launcher(
            executable,
            fixture_request(&root, "delayed-init"),
            &launcher,
        )
        .unwrap();

        // The shell wrapper receives the fixture first; the post-launcher
        // `--model` argument makes that fixture emit this synthetic event.
        process.stdin.as_mut().unwrap().write_all(b"go\n").unwrap();
        process.stdin.as_mut().unwrap().flush().unwrap();
        assert!(matches!(
            process.next_update("now").unwrap(),
            AgyRunUpdate::Lifecycle { event, .. } if event.state == NormalizedRunState::Running
        ));
        assert_eq!(launcher.calls.get(), 1);
    }

    #[test]
    fn short_polls_are_nonterminal_and_later_events_are_observed() {
        let mut fixture = fixture_process("delayed-init", Duration::from_secs(10));
        assert!(fixture
            .process
            .poll_update("now", Duration::from_millis(5))
            .unwrap()
            .is_none());
        let stdin = fixture.process.stdin.as_mut().unwrap();
        stdin.write_all(b"go\n").unwrap();
        stdin.flush().unwrap();
        let observed = fixture
            .process
            .poll_update("later", Duration::from_secs(10))
            .unwrap();
        let Some(AgyRunUpdate::Lifecycle { event, .. }) = observed else {
            panic!("the post-poll provider event must remain observable: {observed:?}");
        };
        assert_eq!(event.state, NormalizedRunState::Running);
    }

    #[test]
    fn legacy_wait_survives_activity_beyond_its_first_observation_window() {
        let mut fixture = fixture_process("activity", Duration::from_secs(10));
        assert!(
            matches!(fixture.process.next_update("started").unwrap(), AgyRunUpdate::Lifecycle { event, .. } if event.state == NormalizedRunState::Running)
        );
        fixture.process.timeout = Duration::from_millis(350);
        fixture.process.inactivity_deadline = Instant::now() + fixture.process.timeout;
        let stdin = fixture.process.stdin.as_mut().unwrap();
        stdin.write_all(b"go\n").unwrap();
        stdin.flush().unwrap();
        // The response arrives after 400ms, but the intermediate text renews
        // the 350ms inactivity limit. The first observation window may expire
        // without a public update; the legacy API must keep waiting, not panic.
        assert!(
            matches!(fixture.process.next_update("finished").unwrap(), AgyRunUpdate::Lifecycle { event, .. } if event.state == NormalizedRunState::Succeeded)
        );
        assert_eq!(fixture.process.final_output(), Some("# Answer"));
    }

    #[test]
    fn configured_inactivity_deadline_still_becomes_terminal_after_short_poll() {
        let mut fixture = fixture_process("silent", Duration::from_secs(10));
        assert!(fixture
            .process
            .poll_update("first", Duration::from_millis(5))
            .unwrap()
            .is_none());
        fixture.process.inactivity_deadline = Instant::now() - Duration::from_secs(1);
        let Some(AgyRunUpdate::Lifecycle { event, .. }) = fixture
            .process
            .poll_update("expired", Duration::ZERO)
            .unwrap()
        else {
            panic!("the configured inactivity deadline must remain terminal");
        };
        assert_eq!(event.state, NormalizedRunState::Failed);
    }

    #[test]
    fn cancellation_remains_responsive_after_a_silent_poll() {
        let mut fixture = fixture_process("cancel", Duration::from_secs(10));
        assert!(
            matches!(fixture.process.next_update("started").unwrap(), AgyRunUpdate::Lifecycle { event, .. } if event.state == NormalizedRunState::Running)
        );
        assert!(fixture
            .process
            .poll_update("first", Duration::from_millis(5))
            .unwrap()
            .is_none());
        let AgyRunUpdate::Lifecycle { event, .. } =
            fixture.process.request_cancellation("cancel").unwrap()
        else {
            panic!("cancellation must emit a lifecycle update");
        };
        assert_eq!(event.state, NormalizedRunState::Cancelling);
        assert!(
            matches!(fixture.process.next_update("terminal").unwrap(), AgyRunUpdate::Lifecycle { event, .. } if event.state == NormalizedRunState::Cancelled)
        );
    }

    #[test]
    fn cleanup_failure_withholds_success_and_remains_failed() {
        const MARKER: &str = "BASTET_TEST_AGY_FOREIGN_REAPER";
        if std::env::var_os(MARKER).is_none() {
            assert!(Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "process::tests::cleanup_failure_withholds_success_and_remains_failed"
                ])
                .env(MARKER, "1")
                .status()
                .unwrap()
                .success());
            return;
        }
        let mut fixture = fixture_process("done", Duration::from_secs(2));
        // SAFETY: this dedicated subprocess has exactly one child, the finite
        // synthetic fixture. Deliberately violate exclusive wait ownership.
        assert!(unsafe { libc::waitpid(-1, std::ptr::null_mut(), 0) } > 0);
        assert!(
            matches!(fixture.process.next_update("started").unwrap(), AgyRunUpdate::Lifecycle { event, .. } if event.state == NormalizedRunState::Running)
        );
        for _ in 0..2 {
            assert!(matches!(
                fixture.process.next_update("terminal"),
                Err(AgyProcessError::CleanupUncertain)
            ));
        }
    }

    #[test]
    fn cancelling_stdout_holding_descendant_closes_the_output_reader_promptly() {
        let mut fixture = descendant_stdout_fixture();
        assert!(matches!(
            fixture.process.next_update("started").unwrap(),
            AgyRunUpdate::Lifecycle { event, .. } if event.state == NormalizedRunState::Running
        ));
        let started = Instant::now();

        fixture.process.request_cancellation("cancel").unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
