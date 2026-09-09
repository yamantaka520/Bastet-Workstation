//! Same-OS-user local IPC. No bearer secrets, DNS, proxy, or TCP fallback.
//!
//! This authenticates an OS account, not arbitrary applications running as that
//! account. Provider sandboxing remains a separate required boundary.

use std::{
    io,
    path::{Path, PathBuf},
};

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub use unix::{bind, connect, ClientStream, EndpointGuard, LocalListener, ServerStream};
#[cfg(windows)]
pub use windows::{bind, connect, ClientStream, EndpointGuard, LocalListener, ServerStream};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub(crate) path: PathBuf,
}

impl Endpoint {
    pub fn for_database(database: &Path) -> io::Result<Self> {
        #[cfg(unix)]
        {
            unix::endpoint_for_database(database)
        }
        #[cfg(windows)]
        {
            windows::endpoint_for_database(database)
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Canonicalize the parent even before SQLite exists, and resolve existing
/// symlinks. This is a canonical-path identity, not an inode/file-ID lock:
/// hard-link aliases and concurrent case aliases are outside its guarantee.
pub(crate) fn database_identity(database: &Path) -> io::Result<PathBuf> {
    let absolute = if database.is_absolute() {
        database.to_path_buf()
    } else {
        std::env::current_dir()?.join(database)
    };
    match std::fs::symlink_metadata(&absolute) {
        Ok(_) => {
            let canonical = absolute.canonicalize()?;
            if !canonical.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "database must be a file",
                ));
            }
            return Ok(canonical);
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let name = absolute
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "database must name a file"))?;
    let parent = absolute.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "database must have a parent")
    })?;
    Ok(parent.canonicalize()?.join(name))
}
