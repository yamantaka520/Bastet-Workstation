//! Linux Bubblewrap launch-plan construction.
//!
//! This module only builds a command.  Availability checks and policy
//! selection belong to the daemon's owning sandbox boundary.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Build a Bubblewrap command for an already-validated workspace policy.
///
/// The child receives a fresh pid, IPC, and UTS namespace.  Filesystem access
/// is limited to the explicitly supplied read-only roots and the workspace;
/// the workspace is writable only when requested.  In particular, this plan
/// intentionally does not mount the host root, `/proc`, or `/dev`.
pub(super) fn launch_command(
    workspace_root: &Path,
    read_only_roots: &[PathBuf],
    allow_workspace_write: bool,
    allow_network: bool,
    executable: &Path,
) -> Command {
    let mut command = Command::new("/usr/bin/bwrap");
    command.args([
        "--die-with-parent",
        "--new-session",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
    ]);

    for root in read_only_roots {
        command.args(["--ro-bind"]);
        command.arg(root);
        command.arg(root);
    }

    command.args([if allow_workspace_write {
        "--bind"
    } else {
        "--ro-bind"
    }]);
    command.arg(workspace_root);
    command.arg(workspace_root);

    if !allow_network {
        command.arg("--unshare-net");
    }

    command
        .arg("--")
        .arg(executable)
        .current_dir(workspace_root);
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(command: &Command) -> Vec<String> {
        command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn read_only_plan_binds_explicit_roots_and_unshares_network() {
        let workspace = PathBuf::from("/workspace");
        let roots = vec![PathBuf::from("/usr"), PathBuf::from("/tmp/fixtures")];
        let command = launch_command(
            &workspace,
            &roots,
            false,
            false,
            Path::new("/opt/provider/bin/agent"),
        );
        let actual = args(&command);

        assert_eq!(command.get_program(), "/usr/bin/bwrap");
        for flag in [
            "--die-with-parent",
            "--new-session",
            "--unshare-pid",
            "--unshare-ipc",
            "--unshare-uts",
            "--unshare-net",
        ] {
            assert!(actual.iter().any(|arg| arg == flag), "missing {flag}");
        }
        assert_eq!(
            actual
                .windows(3)
                .filter(|window| window[0] == "--ro-bind")
                .count(),
            3
        );
        assert!(!actual.iter().any(|arg| arg == "--bind"));
        assert!(!actual.iter().any(|arg| arg == "/"));
        assert!(!actual.iter().any(|arg| arg == "--proc"));
        assert!(!actual.iter().any(|arg| arg == "--dev"));
    }

    #[test]
    fn writable_network_plan_only_relaxes_workspace_and_network() {
        let workspace = PathBuf::from("/workspace");
        let command = launch_command(
            &workspace,
            &[PathBuf::from("/usr")],
            true,
            true,
            Path::new("agent"),
        );
        let actual = args(&command);

        assert!(actual.iter().any(|arg| arg == "--bind"));
        assert!(!actual.iter().any(|arg| arg == "--unshare-net"));
        assert_eq!(
            actual
                .windows(3)
                .filter(|window| window[0] == "--ro-bind")
                .count(),
            1
        );
        assert!(!actual.iter().any(|arg| arg == "/"));
        assert!(!actual.iter().any(|arg| arg == "--proc"));
        assert!(!actual.iter().any(|arg| arg == "--dev"));
    }
}
