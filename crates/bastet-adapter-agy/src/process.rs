use std::{
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver, RecvTimeoutError},
    thread::{self, JoinHandle},
    time::Duration,
};

use bastet_core::{RunId, WorkspaceEvidenceError, WorkspaceSnapshot};
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
    #[error(transparent)]
    Stream(#[from] AgyStreamError),
    #[error(transparent)]
    Workspace(#[from] WorkspaceEvidenceError),
}

pub struct AgyProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<Result<String, ()>>,
    reader: Option<JoinHandle<()>>,
    stream: AgyRunStream,
    timeout: Duration,
    cancellation_requested: bool,
    pending: Option<AgyRunUpdate>,
    pending_terminal: Option<AgyRunUpdate>,
    before: Option<WorkspaceSnapshot>,
}

impl AgyProcess {
    pub fn spawn(executable: PathBuf, request: AgyRunRequest) -> Result<Self, AgyProcessError> {
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
        let mut command = Command::new(&executable);
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
        let mut child = command.spawn().map_err(|_| AgyProcessError::Unavailable)?;
        let mut stdin = child.stdin.take().ok_or(AgyProcessError::Unavailable)?;
        let stdout = child.stdout.take().ok_or(AgyProcessError::Unavailable)?;
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
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let line = line.map_err(|_| ());
                let failed = line.is_err();
                if sender.send(line).is_err() || failed {
                    break;
                }
            }
        });
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
            reader: Some(reader),
            stream,
            timeout: request.timeout,
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
        if let Some(update) = self.pending.take() {
            return Ok(update);
        }
        if let Some(update) = self.pending_terminal.take() {
            return Ok(update);
        }
        loop {
            match self.lines.recv_timeout(self.timeout) {
                Ok(Ok(line)) => {
                    if let Some(update) = self.stream.consume_line(&line, occurred_at)? {
                        if is_terminal(&update) {
                            self.stdin.take();
                            self.reap();
                            return self.order_terminal(update);
                        }
                        return Ok(update);
                    }
                }
                Ok(Err(())) | Err(RecvTimeoutError::Disconnected) => {
                    self.reap();
                    let terminal = if self.cancellation_requested {
                        self.stream.cancelled(occurred_at)?
                    } else {
                        self.stream.crashed(occurred_at)?
                    };
                    return self.order_terminal(terminal);
                }
                Err(RecvTimeoutError::Timeout) => {
                    self.terminate();
                    let terminal = self.stream.timed_out(occurred_at)?;
                    return self.order_terminal(terminal);
                }
            }
        }
    }

    pub fn request_cancellation(
        &mut self,
        occurred_at: &str,
    ) -> Result<AgyRunUpdate, AgyProcessError> {
        let update = self.stream.cancellation_started(occurred_at)?;
        self.cancellation_requested = true;
        self.terminate();
        Ok(update)
    }

    fn terminate(&mut self) {
        self.stdin.take();
        let _ = self.child.kill();
        self.reap();
    }

    fn reap(&mut self) {
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
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
        self.terminate();
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
