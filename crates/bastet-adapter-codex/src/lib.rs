//! Codex CLI discovery and read-only health boundary.

use std::{
    ffi::OsStr,
    io,
    path::{Path, PathBuf},
    process::Command,
};

use bastet_core::EvidenceClass;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const ADAPTER_KIND: &str = "codex_cli";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub success: bool,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

pub trait CommandRunner {
    fn run(&self, executable: &Path, arguments: &[&str]) -> io::Result<CommandOutput>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemCommandRunner;

impl CommandRunner for SystemCommandRunner {
    fn run(&self, executable: &Path, arguments: &[&str]) -> io::Result<CommandOutput> {
        let output = Command::new(executable).args(arguments).output()?;
        Ok(CommandOutput {
            success: output.status.success(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryReport {
    pub adapter_kind: String,
    pub executable: PathBuf,
    pub evidence_class: EvidenceClass,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionReport {
    pub adapter_kind: String,
    pub version: String,
    pub evidence_class: EvidenceClass,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DoctorReport {
    pub adapter_kind: String,
    pub schema_version: u64,
    pub codex_version: String,
    pub overall_status: String,
    pub check_count: usize,
    pub evidence_class: EvidenceClass,
}

#[derive(Debug, Error)]
pub enum CodexAdapterError {
    #[error("Codex CLI executable was not found at the configured path")]
    BinaryMissing,
    #[error("Codex CLI command could not be started: {0}")]
    Spawn(#[source] io::Error),
    #[error("Codex CLI command failed")]
    CommandFailed,
    #[error("Codex CLI returned non-UTF-8 output")]
    NonUtf8Output,
    #[error("Codex CLI output did not match the expected protocol")]
    ProtocolDrift,
}

#[derive(Debug)]
pub struct CodexAdapter<R = SystemCommandRunner> {
    executable: PathBuf,
    runner: R,
}

impl CodexAdapter<SystemCommandRunner> {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self::with_runner(executable, SystemCommandRunner)
    }
}

impl<R: CommandRunner> CodexAdapter<R> {
    pub fn with_runner(executable: impl Into<PathBuf>, runner: R) -> Self {
        Self {
            executable: executable.into(),
            runner,
        }
    }

    pub fn discover(&self) -> Result<DiscoveryReport, CodexAdapterError> {
        if !self.executable.is_file() {
            return Err(CodexAdapterError::BinaryMissing);
        }
        Ok(DiscoveryReport {
            adapter_kind: ADAPTER_KIND.into(),
            executable: self.executable.clone(),
            evidence_class: EvidenceClass::LocallyMeasured,
        })
    }

    pub fn version(&self) -> Result<VersionReport, CodexAdapterError> {
        self.discover()?;
        let output = self
            .runner
            .run(&self.executable, &["--version"])
            .map_err(CodexAdapterError::Spawn)?;
        if !output.success {
            return Err(CodexAdapterError::CommandFailed);
        }
        let stdout = std::str::from_utf8(&output.stdout)
            .map_err(|_| CodexAdapterError::NonUtf8Output)?
            .trim();
        let version = stdout
            .strip_prefix("codex-cli ")
            .filter(|value| valid_version(value))
            .ok_or(CodexAdapterError::ProtocolDrift)?;
        Ok(VersionReport {
            adapter_kind: ADAPTER_KIND.into(),
            version: version.into(),
            evidence_class: EvidenceClass::ProviderReported,
        })
    }

    pub fn doctor(&self) -> Result<DoctorReport, CodexAdapterError> {
        self.discover()?;
        let output = self
            .runner
            .run(&self.executable, &["doctor", "--json"])
            .map_err(CodexAdapterError::Spawn)?;
        let stdout =
            std::str::from_utf8(&output.stdout).map_err(|_| CodexAdapterError::NonUtf8Output)?;
        let wire: DoctorWire =
            serde_json::from_str(stdout).map_err(|_| CodexAdapterError::ProtocolDrift)?;
        if wire.schema_version == 0
            || !valid_version(&wire.codex_version)
            || wire.overall_status.trim().is_empty()
        {
            return Err(CodexAdapterError::ProtocolDrift);
        }
        Ok(DoctorReport {
            adapter_kind: ADAPTER_KIND.into(),
            schema_version: wire.schema_version,
            codex_version: wire.codex_version,
            overall_status: wire.overall_status,
            check_count: wire.checks.len(),
            evidence_class: EvidenceClass::ProviderReported,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DoctorWire {
    schema_version: u64,
    codex_version: String,
    overall_status: String,
    checks: Vec<serde_json::Value>,
}

fn valid_version(value: &str) -> bool {
    !value.is_empty()
        && value.split('.').all(|part| {
            !part.is_empty() && part.chars().all(|character| character.is_ascii_digit())
        })
}

pub fn find_on_path(path: Option<&OsStr>) -> Option<PathBuf> {
    let path = path?;
    std::env::split_paths(path).find_map(|directory| {
        executable_names().into_iter().find_map(|name| {
            let candidate = directory.join(name);
            candidate.is_file().then_some(candidate)
        })
    })
}

#[cfg(windows)]
fn executable_names() -> [&'static str; 2] {
    ["codex.exe", "codex.cmd"]
}

#[cfg(not(windows))]
fn executable_names() -> [&'static str; 1] {
    ["codex"]
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, fs};

    use super::*;

    struct FixtureRunner {
        outputs: RefCell<Vec<CommandOutput>>,
        calls: RefCell<Vec<Vec<String>>>,
    }

    impl FixtureRunner {
        fn new(outputs: Vec<CommandOutput>) -> Self {
            Self {
                outputs: RefCell::new(outputs.into_iter().rev().collect()),
                calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl CommandRunner for FixtureRunner {
        fn run(&self, _executable: &Path, arguments: &[&str]) -> io::Result<CommandOutput> {
            self.calls
                .borrow_mut()
                .push(arguments.iter().map(|value| (*value).into()).collect());
            Ok(self.outputs.borrow_mut().pop().unwrap())
        }
    }

    fn executable() -> (tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory
            .path()
            .join(if cfg!(windows) { "codex.exe" } else { "codex" });
        fs::write(&executable, b"fixture").unwrap();
        (directory, executable)
    }

    fn output(stdout: &str) -> CommandOutput {
        CommandOutput {
            success: true,
            stdout: stdout.as_bytes().to_vec(),
            stderr: Vec::new(),
        }
    }

    #[test]
    fn missing_binary_fails_closed_without_spawning() {
        let runner = FixtureRunner::new(Vec::new());
        let adapter = CodexAdapter::with_runner("missing-codex", runner);
        assert!(matches!(
            adapter.discover(),
            Err(CodexAdapterError::BinaryMissing)
        ));
        assert!(adapter.runner.calls.borrow().is_empty());
    }

    #[test]
    fn version_uses_only_the_read_only_version_flag() {
        let (_directory, executable) = executable();
        let runner = FixtureRunner::new(vec![output("codex-cli 0.152.1\n")]);
        let adapter = CodexAdapter::with_runner(executable, runner);
        let report = adapter.version().unwrap();
        assert_eq!(report.version, "0.152.1");
        assert_eq!(adapter.runner.calls.into_inner(), vec![vec!["--version"]]);
    }

    #[test]
    fn unexpected_version_output_is_protocol_drift() {
        let (_directory, executable) = executable();
        let runner = FixtureRunner::new(vec![output("Codex latest")]);
        let adapter = CodexAdapter::with_runner(executable, runner);
        assert!(matches!(
            adapter.version(),
            Err(CodexAdapterError::ProtocolDrift)
        ));
    }

    #[test]
    fn doctor_retains_only_allowlisted_summary_fields() {
        let (_directory, executable) = executable();
        let runner = FixtureRunner::new(vec![output(
            r#"{"schemaVersion":1,"codexVersion":"0.152.1","overallStatus":"warn","checks":[{"secret":"must-not-survive"}]}"#,
        )]);
        let adapter = CodexAdapter::with_runner(executable, runner);
        let report = adapter.doctor().unwrap();
        let serialized = serde_json::to_string(&report).unwrap();
        assert_eq!(report.check_count, 1);
        assert!(!serialized.contains("must-not-survive"));
        assert_eq!(
            adapter.runner.calls.into_inner(),
            vec![vec!["doctor", "--json"]]
        );
    }

    #[test]
    fn path_discovery_is_explicit_and_does_not_execute_candidates() {
        let (directory, executable) = executable();
        let path = std::env::join_paths([directory.path()]).unwrap();
        assert_eq!(find_on_path(Some(&path)), Some(executable));
        assert_eq!(find_on_path(None), None);
    }
}
