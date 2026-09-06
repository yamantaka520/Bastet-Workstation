//! Fail-closed discovery and catalog boundary for the Agy CLI reference adapter.

use std::{
    io,
    path::{Path, PathBuf},
    process::Command,
};

use bastet_core::{AdapterCapabilities, AdapterOperation, EvidenceClass};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const ADAPTER_KIND: &str = "agy_cli";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub success: bool,
    pub stdout: Vec<u8>,
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
pub struct ModelDescriptor {
    pub id: String,
    pub display_name: String,
}

#[derive(Debug, Error)]
pub enum AgyAdapterError {
    #[error("Agy CLI executable was not found at the configured path")]
    BinaryMissing,
    #[error("Agy CLI command could not be started: {0}")]
    Spawn(#[source] io::Error),
    #[error("Agy CLI command failed")]
    CommandFailed,
    #[error("Agy CLI returned non-UTF-8 output")]
    NonUtf8Output,
    #[error("Agy CLI output did not match the expected protocol")]
    ProtocolDrift,
}

#[derive(Debug)]
pub struct AgyAdapter<R = SystemCommandRunner> {
    executable: PathBuf,
    runner: R,
}

impl AgyAdapter<SystemCommandRunner> {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self::with_runner(executable, SystemCommandRunner)
    }
}

impl<R: CommandRunner> AgyAdapter<R> {
    pub fn with_runner(executable: impl Into<PathBuf>, runner: R) -> Self {
        Self {
            executable: executable.into(),
            runner,
        }
    }

    pub fn discover(&self) -> Result<DiscoveryReport, AgyAdapterError> {
        if !self.executable.is_file() {
            return Err(AgyAdapterError::BinaryMissing);
        }
        Ok(DiscoveryReport {
            adapter_kind: ADAPTER_KIND.into(),
            executable: self.executable.clone(),
            evidence_class: EvidenceClass::LocallyMeasured,
        })
    }

    pub fn version(&self) -> Result<VersionReport, AgyAdapterError> {
        self.discover()?;
        let output = self.run(&["--version"])?;
        let version = parse_single_line(&output.stdout)?;
        if !valid_version(version) {
            return Err(AgyAdapterError::ProtocolDrift);
        }
        Ok(VersionReport {
            adapter_kind: ADAPTER_KIND.into(),
            version: version.into(),
            evidence_class: EvidenceClass::ProviderReported,
        })
    }

    pub fn list_models(&self) -> Result<Vec<ModelDescriptor>, AgyAdapterError> {
        self.discover()?;
        let output = self.run(&["models"])?;
        let stdout =
            std::str::from_utf8(&output.stdout).map_err(|_| AgyAdapterError::NonUtf8Output)?;
        let mut models = Vec::new();
        for line in stdout
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            if line == "Fetching available models..." {
                continue;
            }
            let Some((id, display_name)) = line.split_once('\t') else {
                return Err(AgyAdapterError::ProtocolDrift);
            };
            if !valid_model_id(id) || display_name.trim().is_empty() {
                return Err(AgyAdapterError::ProtocolDrift);
            }
            models.push(ModelDescriptor {
                id: id.into(),
                display_name: display_name.trim().into(),
            });
        }
        if models.is_empty() {
            return Err(AgyAdapterError::ProtocolDrift);
        }
        Ok(models)
    }

    pub fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            operations: vec![
                AdapterOperation::Discover,
                AdapterOperation::Version,
                AdapterOperation::ListModels,
            ],
            reasoning_controls: vec!["low".into(), "medium".into(), "high".into()],
            supports_read_only: false,
            supports_write: false,
            supports_resume: false,
            supports_structured_events: false,
        }
    }

    fn run(&self, arguments: &[&str]) -> Result<CommandOutput, AgyAdapterError> {
        let output = self
            .runner
            .run(&self.executable, arguments)
            .map_err(AgyAdapterError::Spawn)?;
        if !output.success {
            return Err(AgyAdapterError::CommandFailed);
        }
        Ok(output)
    }
}

fn parse_single_line(bytes: &[u8]) -> Result<&str, AgyAdapterError> {
    let text = std::str::from_utf8(bytes).map_err(|_| AgyAdapterError::NonUtf8Output)?;
    let mut lines = text.lines();
    let line = lines.next().map(str::trim).filter(|line| !line.is_empty());
    if lines.any(|line| !line.trim().is_empty()) {
        return Err(AgyAdapterError::ProtocolDrift);
    }
    line.ok_or(AgyAdapterError::ProtocolDrift)
}

fn valid_version(value: &str) -> bool {
    let parts = value.split('.').collect::<Vec<_>>();
    parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

fn valid_model_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/'))
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, fs};

    use super::*;

    struct FixtureRunner {
        outputs: std::sync::Mutex<VecDeque<CommandOutput>>,
    }

    impl FixtureRunner {
        fn new(outputs: impl IntoIterator<Item = CommandOutput>) -> Self {
            Self {
                outputs: std::sync::Mutex::new(outputs.into_iter().collect()),
            }
        }
    }

    impl CommandRunner for FixtureRunner {
        fn run(&self, _executable: &Path, _arguments: &[&str]) -> io::Result<CommandOutput> {
            Ok(self.outputs.lock().unwrap().pop_front().unwrap())
        }
    }

    fn executable() -> (tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("agy");
        fs::write(&executable, []).unwrap();
        (directory, executable)
    }

    fn output(text: &str) -> CommandOutput {
        CommandOutput {
            success: true,
            stdout: text.as_bytes().to_vec(),
        }
    }

    #[test]
    fn missing_binary_fails_closed_without_spawning() {
        let adapter = AgyAdapter::with_runner("missing-agy", FixtureRunner::new([]));
        assert!(matches!(
            adapter.discover(),
            Err(AgyAdapterError::BinaryMissing)
        ));
    }

    #[test]
    fn version_accepts_only_a_single_numeric_triplet() {
        let (_directory, path) = executable();
        let adapter = AgyAdapter::with_runner(path, FixtureRunner::new([output("1.1.25\n")]));
        assert_eq!(adapter.version().unwrap().version, "1.1.25");

        let (_directory, path) = executable();
        let adapter = AgyAdapter::with_runner(path, FixtureRunner::new([output("v1.1.25\n")]));
        assert!(matches!(
            adapter.version(),
            Err(AgyAdapterError::ProtocolDrift)
        ));
    }

    #[test]
    fn model_catalog_is_allowlisted_and_non_empty() {
        let (_directory, path) = executable();
        let adapter = AgyAdapter::with_runner(
            path,
            FixtureRunner::new([output(
                "Fetching available models...\ngemini-3.8-flash-high\tGemini 3.8 Flash (High)\n",
            )]),
        );
        assert_eq!(
            adapter.list_models().unwrap(),
            vec![ModelDescriptor {
                id: "gemini-3.8-flash-high".into(),
                display_name: "Gemini 3.8 Flash (High)".into(),
            }]
        );
    }

    #[test]
    fn malformed_or_empty_catalog_fails_closed() {
        for text in ["", "secret output", "bad id!\tDisplay\n", "valid-id\t\n"] {
            let (_directory, path) = executable();
            let adapter = AgyAdapter::with_runner(path, FixtureRunner::new([output(text)]));
            assert!(matches!(
                adapter.list_models(),
                Err(AgyAdapterError::ProtocolDrift)
            ));
        }
    }

    #[test]
    fn capabilities_do_not_claim_execution_before_stream_contract_exists() {
        let capabilities = AgyAdapter::new("agy").capabilities();
        assert!(capabilities
            .operations
            .contains(&AdapterOperation::ListModels));
        assert!(!capabilities.operations.contains(&AdapterOperation::Start));
        assert!(!capabilities.supports_read_only);
        assert!(!capabilities.supports_write);
        assert!(!capabilities.supports_resume);
        assert!(!capabilities.supports_structured_events);
    }
}
