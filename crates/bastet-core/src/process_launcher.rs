//! Trusted process-construction seam; this trait does not grant authority.
//!
//! The daemon supplies an OS-enforced wrapper after validating launch policy.
//! Adapters retain ownership of provider arguments, stdio and the fixed parent
//! environment allowlist. They must never fall back to a direct command after
//! a launcher rejects a request. A command builder must not start a process.

use std::{io, path::Path, process::Command};

pub trait AdapterProcessLauncher {
    /// Return the provider command or a wrapper whose next arguments belong to
    /// the provider. The adapter subsequently clears/rebuilds its environment
    /// and configures stdio, so this is not a secret-injection interface.
    fn command(&self, executable: &Path) -> io::Result<Command>;
}

/// Compatibility launcher for callers that have not selected an OS sandbox.
/// Its use must not be represented as sandbox enforcement.
pub struct DirectAdapterProcessLauncher;

impl AdapterProcessLauncher for DirectAdapterProcessLauncher {
    fn command(&self, executable: &Path) -> io::Result<Command> {
        Ok(Command::new(executable))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_launcher_only_constructs_the_exact_command() {
        let path = Path::new("nonexistent-provider-fixture");
        let command = DirectAdapterProcessLauncher.command(path).unwrap();
        assert_eq!(command.get_program(), path);
        assert_eq!(command.get_args().count(), 0);
    }
}
