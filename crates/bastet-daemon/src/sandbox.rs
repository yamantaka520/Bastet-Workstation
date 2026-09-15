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
    /// Explicit runtime/data mounts. No host-wide read permission is implied.
    pub read_only_roots: Vec<PathBuf>,
    pub allow_workspace_write: bool,
    /// Coarse network switch, not destination enforcement. Production must not
    /// equate this with authorization for one provider endpoint.
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

/// Bridges an already selected scope to adapter process construction. Policy
/// derivation and approval remain the daemon coordinator's responsibility.
pub struct SandboxLauncher {
    pub platform: SandboxPlatform,
    pub profile: SandboxProfile,
}

impl bastet_core::AdapterProcessLauncher for SandboxLauncher {
    fn command(&self, executable: &Path) -> std::io::Result<Command> {
        self.profile
            .launch_command(self.platform, executable, &[])
            // Never expose native paths or fall back to an unsandboxed child.
            .map_err(|_| std::io::Error::other("sandbox command rejected"))
    }
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
                if self
                    .read_paths()
                    .any(|path| path.to_string_lossy().contains('\\'))
                {
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
                // Bubblewrap starts with an empty filesystem namespace. Never
                // mount the host root, even read-only: that exposes secrets.
                crate::sandbox_linux::launch_command(
                    &self.workspace_root,
                    &self.read_only_roots,
                    self.allow_workspace_write,
                    self.allow_network,
                    executable,
                )
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
        if self.read_paths().any(|path| !path.is_absolute()) {
            return Err(SandboxError::NonAbsoluteWorkspace);
        }
        if self.read_paths().any(|path| {
            path.parent().is_none()
                || path
                    .canonicalize()
                    .ok()
                    .is_some_and(|resolved| resolved.parent().is_none())
                || path
                    .components()
                    .any(|part| matches!(part, std::path::Component::ParentDir))
                || path
                    .to_str()
                    .is_none_or(|root| root.contains(['\0', '\n', '\r', '"']))
        }) {
            return Err(SandboxError::UnsafeWorkspace);
        }
        Ok(())
    }

    fn read_paths(&self) -> impl Iterator<Item = &Path> {
        std::iter::once(self.workspace_root.as_path())
            .chain(self.read_only_roots.iter().map(PathBuf::as_path))
    }

    fn macos_profile(&self) -> String {
        let mut profile = String::from(
            "(version 1) (deny default) (allow process*) (allow sysctl-read) (allow mach-lookup)",
        );
        for root in self.read_paths() {
            profile.push_str(&format!(
                " (allow file-read* (subpath \"{}\"))",
                root.display()
            ));
            // Runtime loaders open ancestor directories for openat traversal.
            // Literal rules allow those directories, never their descendants.
            for parent in root.ancestors().skip(1) {
                profile.push_str(&format!(
                    " (allow file-read* (literal \"{}\"))",
                    parent.display()
                ));
            }
        }
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

    #[cfg(target_os = "macos")]
    #[test]
    fn adapter_transport_uses_native_wrapper_without_scope_bypass() {
        use bastet_adapter_codex::{AppServerTransport, StdioTransport};
        use std::{os::unix::fs::PermissionsExt, time::Duration};

        let directory = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let inside = root.join("inside");
        let outside_file = outside.path().canonicalize().unwrap().join("outside");
        std::fs::write(&inside, b"inside-fixture").unwrap();
        std::fs::write(&outside_file, b"outside-fixture").unwrap();
        let executable = root.join("fake-stdio.sh");
        // Only generated temporary paths and fixture bytes enter this script.
        // No installed provider, credential, network, or model request is used.
        let script = format!(
            r#"#!/bin/sh
inside=false
outside=false
value=$(/bin/cat '{}') && [ "$value" = inside-fixture ] && inside=true
value=$(/bin/cat '{}') && outside=true
/bin/sleep 3 &
printf '{{"method":"fixture/scope","params":{{"inside":%s,"outside":%s}}}}\n' "$inside" "$outside"
while read -r line; do :; done
"#,
            inside.display(),
            outside_file.display()
        );
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let launcher = SandboxLauncher {
            platform: SandboxPlatform::MacosSeatbelt,
            profile: SandboxProfile {
                workspace_root: root,
                read_only_roots: [
                    "/bin",
                    "/usr/bin",
                    "/usr/lib",
                    "/System/Library",
                    "/System/Volumes/Preboot/Cryptexes/OS",
                    "/System/Cryptexes/OS",
                ]
                .into_iter()
                .map(PathBuf::from)
                .collect(),
                allow_workspace_write: false,
                allow_network: false,
            },
        };
        let mut transport =
            StdioTransport::spawn_with_launcher(&executable, Duration::from_secs(2), &launcher)
                .unwrap();
        let notification = transport
            .poll_notification(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert_eq!(notification.method, "fixture/scope");
        assert_eq!(
            notification.params,
            serde_json::json!({"inside":true,"outside":false})
        );
        let closing = std::time::Instant::now();
        transport.close();
        assert!(closing.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn platform_plans_fail_closed_and_never_use_prompt_level_controls() {
        let mac_profile = SandboxProfile {
            workspace_root: PathBuf::from("/workspace"),
            read_only_roots: vec![],
            allow_workspace_write: true,
            allow_network: false,
        };
        let mac = mac_profile.macos_profile();
        assert!(mac.contains("deny default"));
        assert!(mac.contains("(subpath \"/workspace\")"));
        assert!(!mac.contains("allow network"));
        assert!(!mac.contains("(allow file-read*)"));
        assert!(matches!(
            SandboxProfile {
                workspace_root: std::env::current_dir().unwrap(),
                read_only_roots: vec![],
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
    fn macos_seatbelt_denies_undeclared_reads_including_workspace_symlinks() {
        let directory = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let workspace_root = directory.path().canonicalize().unwrap();
        let inside = workspace_root.join("inside");
        let secret_fixture = outside.path().canonicalize().unwrap().join("outside");
        std::fs::write(&inside, b"inside-fixture").unwrap();
        std::fs::write(&secret_fixture, b"outside-fixture").unwrap();
        let link = workspace_root.join("escape-link");
        std::os::unix::fs::symlink(&secret_fixture, &link).unwrap();
        let mut profile = SandboxProfile {
            workspace_root,
            read_only_roots: [
                "/bin",
                "/usr/bin",
                "/usr/lib",
                "/System/Library",
                "/System/Volumes/Preboot/Cryptexes/OS",
                "/System/Cryptexes/OS",
            ]
            .into_iter()
            .map(PathBuf::from)
            .collect(),
            allow_workspace_write: false,
            allow_network: false,
        };
        let allowed = read_probe(&profile, &inside);
        assert!(allowed.status.success());
        assert_eq!(allowed.stdout, b"bastet-probe-started\ninside-fixture");
        for path in [&secret_fixture, &link] {
            let denied = read_probe(&profile, path);
            assert!(!denied.status.success());
            assert_eq!(denied.stdout, b"bastet-probe-started\n");
        }
        // Same target succeeds only after its explicit scope is granted.
        profile.read_only_roots.push(secret_fixture.clone());
        let allowed = read_probe(&profile, &secret_fixture);
        assert!(allowed.status.success());
        assert_eq!(allowed.stdout, b"bastet-probe-started\noutside-fixture");
    }

    #[test]
    fn broad_relative_and_profile_injection_read_scopes_are_rejected() {
        for root in [
            "/",
            "relative",
            "/runtime/../",
            "/runtime\n",
            "/runtime\"",
            "/runtime\0",
        ] {
            let profile = SandboxProfile {
                workspace_root: std::env::current_dir().unwrap(),
                read_only_roots: vec![PathBuf::from(root)],
                allow_workspace_write: false,
                allow_network: false,
            };
            assert!(profile.validate().is_err(), "{root:?}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_cannot_disguise_a_host_root_scope() {
        let directory = tempfile::tempdir().unwrap();
        let link = directory.path().join("host-root");
        std::os::unix::fs::symlink("/", &link).unwrap();
        let profile = SandboxProfile {
            workspace_root: directory.path().to_path_buf(),
            read_only_roots: vec![link],
            allow_workspace_write: false,
            allow_network: false,
        };
        assert!(profile.validate().is_err());
    }

    #[cfg(target_os = "macos")]
    fn read_probe(profile: &SandboxProfile, target: &Path) -> std::process::Output {
        let output = profile
            .launch_command(
                SandboxPlatform::MacosSeatbelt,
                Path::new("/bin/sh"),
                &[
                    "-c".into(),
                    "printf 'bastet-probe-started\\n'; exec /bin/cat \"$1\"".into(),
                    "bastet-read-probe".into(),
                    target.display().to_string(),
                ],
            )
            .unwrap()
            .output()
            .unwrap();
        assert!(
            output.stdout.starts_with(b"bastet-probe-started\n"),
            "sandbox probe did not start"
        );
        output
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
            read_only_roots: [
                "/bin",
                "/usr/bin",
                "/usr/lib",
                "/System/Library",
                "/System/Volumes/Preboot/Cryptexes/OS",
                "/System/Cryptexes/OS",
            ]
            .into_iter()
            .map(PathBuf::from)
            .collect(),
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
            output.stdout,
            b"bastet-probe-started\n",
            "sandbox must actually start the probe, not merely return an error: {:?} {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }
}
