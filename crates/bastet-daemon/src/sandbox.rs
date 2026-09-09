//! OS-enforced process sandbox launch plans. Unsupported enforcement fails closed.

use std::path::{Path, PathBuf};
use std::process::Command;

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxPlatform {
    MacosSeatbelt,
    LinuxBubblewrap,
    WindowsAppContainer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxProfile {
    pub workspace_root: PathBuf,
    pub allow_workspace_write: bool,
    pub allow_network: bool,
}

#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("workspace root must be absolute")]
    NonAbsoluteWorkspace,
    #[error("workspace root contains unsupported profile characters")]
    UnsafeWorkspace,
    #[error("required sandbox enforcer is unavailable: {0}")]
    EnforcerUnavailable(&'static str),
}

impl SandboxProfile {
    pub fn launch_command(
        &self,
        platform: SandboxPlatform,
        executable: &Path,
        arguments: &[String],
    ) -> Result<Command, SandboxError> {
        self.validate()?;
        let mut command = match platform {
            SandboxPlatform::MacosSeatbelt => {
                if self.workspace_root.to_string_lossy().contains('\\') {
                    return Err(SandboxError::UnsafeWorkspace);
                }
                require_executable(Path::new("/usr/bin/sandbox-exec"), "sandbox-exec")?;
                let mut command = Command::new("/usr/bin/sandbox-exec");
                command.args(["-p", &self.macos_profile(), "--"]);
                command.arg(executable);
                command
            }
            SandboxPlatform::LinuxBubblewrap => {
                require_executable(Path::new("/usr/bin/bwrap"), "bubblewrap")?;
                let mut command = Command::new("/usr/bin/bwrap");
                command.args(["--die-with-parent", "--new-session", "--ro-bind", "/", "/"]);
                if self.allow_workspace_write {
                    command
                        .arg("--bind")
                        .arg(&self.workspace_root)
                        .arg(&self.workspace_root);
                }
                if !self.allow_network {
                    command.arg("--unshare-net");
                }
                command.arg("--").arg(executable);
                command
            }
            SandboxPlatform::WindowsAppContainer => {
                return Err(SandboxError::EnforcerUnavailable(
                    "Windows AppContainer broker",
                ));
            }
        };
        command.args(arguments).current_dir(&self.workspace_root);
        Ok(command)
    }

    fn validate(&self) -> Result<(), SandboxError> {
        if !self.workspace_root.is_absolute() {
            return Err(SandboxError::NonAbsoluteWorkspace);
        }
        let root = self.workspace_root.to_string_lossy();
        if root.contains(['\n', '\r', '"']) {
            return Err(SandboxError::UnsafeWorkspace);
        }
        Ok(())
    }

    fn macos_profile(&self) -> String {
        let mut profile = String::from(
            "(version 1) (deny default) (allow process*) (allow sysctl-read) (allow mach-lookup) (allow file-read*)",
        );
        if self.allow_workspace_write {
            profile.push_str(&format!(
                " (allow file-write* (subpath \"{}\"))",
                self.workspace_root.display()
            ));
        }
        if self.allow_network {
            profile.push_str(" (allow network*)");
        }
        profile
    }
}

fn require_executable(path: &Path, name: &'static str) -> Result<(), SandboxError> {
    if path.is_file() {
        Ok(())
    } else {
        Err(SandboxError::EnforcerUnavailable(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "macos")]
    use tempfile::tempdir;

    #[test]
    fn platform_plans_fail_closed_and_never_use_prompt_level_controls() {
        let mac_profile = SandboxProfile {
            workspace_root: PathBuf::from("/workspace"),
            allow_workspace_write: true,
            allow_network: false,
        };
        let mac = mac_profile.macos_profile();
        assert!(mac.contains("deny default"));
        assert!(mac.contains("(subpath \"/workspace\")"));
        assert!(!mac.contains("allow network"));
        assert!(matches!(
            SandboxProfile {
                workspace_root: std::env::current_dir().unwrap(),
                allow_workspace_write: true,
                allow_network: false,
            }
            .launch_command(
                SandboxPlatform::WindowsAppContainer,
                Path::new("agent.exe"),
                &[]
            ),
            Err(SandboxError::EnforcerUnavailable(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_seatbelt_enforces_read_only_workspace() {
        let directory = tempdir().unwrap();
        let outside = tempdir().unwrap();
        // Seatbelt resolves filesystem aliases. Use the same canonical root in
        // the allowed-write control and the denied-write probe.
        let workspace_root = directory.path().canonicalize().unwrap();
        let target = workspace_root.join("write-probe");
        let mut profile = SandboxProfile {
            workspace_root,
            allow_workspace_write: false,
            allow_network: false,
        };
        let denied = write_probe(&profile, &target);
        assert!(!denied.status.success());
        assert!(!target.exists());

        // Without this positive control, a missing/broken sandbox enforcer or
        // unusable fixture path could falsely satisfy the denial assertion.
        profile.allow_workspace_write = true;
        let allowed = write_probe(&profile, &target);
        assert!(
            allowed.status.success(),
            "sandbox write control did not succeed"
        );
        assert!(target.is_file());

        let outside_target = outside
            .path()
            .canonicalize()
            .unwrap()
            .join("must-not-exist");
        let outside_denied = write_probe(&profile, &outside_target);
        assert!(!outside_denied.status.success());
        assert!(!outside_target.exists());
    }

    #[cfg(target_os = "macos")]
    fn write_probe(profile: &SandboxProfile, target: &Path) -> std::process::Output {
        let output = profile
            .launch_command(
                SandboxPlatform::MacosSeatbelt,
                Path::new("/bin/sh"),
                &[
                    "-c".into(),
                    "printf 'bastet-probe-started\\n'; exec /usr/bin/touch \"$1\"".into(),
                    "bastet-sandbox-probe".into(),
                    target.display().to_string(),
                ],
            )
            .unwrap()
            .output()
            .unwrap();
        assert_eq!(
            output.stdout, b"bastet-probe-started\n",
            "sandbox must actually start the probe, not merely return an error"
        );
        output
    }
}
