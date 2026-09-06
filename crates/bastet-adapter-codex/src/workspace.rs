use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

const MAX_FILES: usize = 10_000;
const MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum WorkspaceEvidenceError {
    #[error("workspace evidence root must be an absolute directory")]
    InvalidRoot,
    #[error("workspace evidence does not follow symbolic links")]
    SymbolicLink,
    #[error("workspace evidence exceeded its bounded scan limits")]
    LimitExceeded,
    #[error("workspace evidence could not read the bounded root")]
    Io(#[source] io::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSnapshot {
    root: PathBuf,
    files: BTreeMap<PathBuf, [u8; 32]>,
}

impl WorkspaceSnapshot {
    pub fn capture(root: &Path) -> Result<Self, WorkspaceEvidenceError> {
        if !root.is_absolute() || !root.is_dir() {
            return Err(WorkspaceEvidenceError::InvalidRoot);
        }
        let mut files = BTreeMap::new();
        let mut total_bytes = 0;
        capture_directory(root, root, &mut files, &mut total_bytes)?;
        Ok(Self {
            root: root.to_owned(),
            files,
        })
    }

    pub fn write_receipt(&self, after: &Self) -> Result<Option<String>, WorkspaceEvidenceError> {
        if self.root != after.root {
            return Err(WorkspaceEvidenceError::InvalidRoot);
        }
        let changed_files = self
            .files
            .iter()
            .filter(|(path, digest)| after.files.get(*path) != Some(*digest))
            .count()
            + after
                .files
                .keys()
                .filter(|path| !self.files.contains_key(*path))
                .count();
        if changed_files == 0 {
            return Ok(None);
        }
        Ok(Some(
            json!({
                "evidence": "bastet.workspace_snapshot.changed",
                "evidence_class": "locally_measured",
                "changed_files": changed_files
            })
            .to_string(),
        ))
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }
}

fn capture_directory(
    root: &Path,
    directory: &Path,
    files: &mut BTreeMap<PathBuf, [u8; 32]>,
    total_bytes: &mut u64,
) -> Result<(), WorkspaceEvidenceError> {
    let mut entries = fs::read_dir(directory)
        .map_err(WorkspaceEvidenceError::Io)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(WorkspaceEvidenceError::Io)?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let file_type = entry.file_type().map_err(WorkspaceEvidenceError::Io)?;
        if file_type.is_symlink() {
            return Err(WorkspaceEvidenceError::SymbolicLink);
        }
        if file_type.is_dir() {
            capture_directory(root, &entry.path(), files, total_bytes)?;
            continue;
        }
        if !file_type.is_file() || files.len() >= MAX_FILES {
            return Err(WorkspaceEvidenceError::LimitExceeded);
        }
        let metadata = entry.metadata().map_err(WorkspaceEvidenceError::Io)?;
        *total_bytes = total_bytes
            .checked_add(metadata.len())
            .filter(|bytes| *bytes <= MAX_TOTAL_BYTES)
            .ok_or(WorkspaceEvidenceError::LimitExceeded)?;
        let relative = entry
            .path()
            .strip_prefix(root)
            .map_err(|_| WorkspaceEvidenceError::InvalidRoot)?
            .to_owned();
        files.insert(relative, hash_file(&entry.path())?);
    }
    Ok(())
}

fn hash_file(path: &Path) -> Result<[u8; 32], WorkspaceEvidenceError> {
    let mut file = fs::File::open(path).map_err(WorkspaceEvidenceError::Io)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let read = file.read(&mut buffer).map_err(WorkspaceEvidenceError::Io)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_detects_changes_without_exposing_paths_or_content() {
        let root = tempfile::tempdir().unwrap();
        let before = WorkspaceSnapshot::capture(root.path()).unwrap();
        let secret = "WORKSPACE_SECRET_DO_NOT_LOG";
        fs::write(root.path().join("secret.txt"), secret).unwrap();
        let after = WorkspaceSnapshot::capture(root.path()).unwrap();
        let receipt = before.write_receipt(&after).unwrap().unwrap();
        assert!(receipt.contains("locally_measured"));
        assert!(receipt.contains("\"changed_files\":1"));
        assert!(!receipt.contains("secret.txt"));
        assert!(!receipt.contains(secret));
    }

    #[test]
    fn unchanged_workspace_has_no_receipt() {
        let root = tempfile::tempdir().unwrap();
        let before = WorkspaceSnapshot::capture(root.path()).unwrap();
        let after = WorkspaceSnapshot::capture(root.path()).unwrap();
        assert_eq!(before.write_receipt(&after).unwrap(), None);
    }

    #[cfg(unix)]
    #[test]
    fn symbolic_links_fail_closed() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        symlink("outside", root.path().join("link")).unwrap();
        assert!(matches!(
            WorkspaceSnapshot::capture(root.path()),
            Err(WorkspaceEvidenceError::SymbolicLink)
        ));
    }
}
