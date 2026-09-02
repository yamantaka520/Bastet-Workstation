use std::{
    env,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use bastet_client::DaemonClient;

#[derive(Clone)]
pub struct DaemonSupervisor {
    inner: Arc<Inner>,
}

struct Inner {
    child: Mutex<Option<Child>>,
    executable: PathBuf,
    database: PathBuf,
    diagnostics_dir: PathBuf,
    listen: String,
    shutting_down: AtomicBool,
    exit_authorized: AtomicBool,
}

impl DaemonSupervisor {
    pub fn new(data_dir: &Path) -> Result<Self, String> {
        Self::new_with_executable(data_dir, daemon_executable()?)
    }

    fn new_with_executable(data_dir: &Path, executable: PathBuf) -> Result<Self, String> {
        fs::create_dir_all(data_dir).map_err(|error| error.to_string())?;
        let diagnostics_dir = data_dir.join("diagnostics");
        fs::create_dir_all(&diagnostics_dir).map_err(|error| error.to_string())?;
        Ok(Self {
            inner: Arc::new(Inner {
                child: Mutex::new(None),
                executable,
                database: data_dir.join("bastet-workstation.db"),
                diagnostics_dir,
                listen: env::var("BASTET_LISTEN").unwrap_or_else(|_| "127.0.0.1:17841".to_owned()),
                shutting_down: AtomicBool::new(false),
                exit_authorized: AtomicBool::new(false),
            }),
        })
    }

    pub async fn ensure_running(&self, client: &DaemonClient) -> Result<(), String> {
        if self.is_shutting_down() {
            return Ok(());
        }
        if client.snapshot().await.is_ok() {
            return Ok(());
        }
        self.record_previous_exit()?;
        self.spawn()?;
        for _ in 0..30 {
            if client.snapshot().await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err("daemon did not become ready within three seconds; inspect diagnostics".into())
    }

    pub fn begin_shutdown(&self) -> bool {
        self.inner
            .shutting_down
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn cancel_shutdown(&self) {
        self.inner.shutting_down.store(false, Ordering::Release);
    }

    pub fn authorize_exit(&self) {
        self.inner.exit_authorized.store(true, Ordering::Release);
    }

    pub fn exit_authorized(&self) -> bool {
        self.inner.exit_authorized.load(Ordering::Acquire)
    }

    pub fn is_shutting_down(&self) -> bool {
        self.inner.shutting_down.load(Ordering::Acquire)
    }

    pub fn record_previous_exit(&self) -> Result<(), String> {
        let mut child = self
            .inner
            .child
            .lock()
            .map_err(|_| "daemon child lock poisoned")?;
        let Some(process) = child.as_mut() else {
            return Ok(());
        };
        let Some(status) = process.try_wait().map_err(|error| error.to_string())? else {
            return Ok(());
        };
        fs::write(
            self.inner.diagnostics_dir.join("last-exit.txt"),
            format!("daemon exited with {status}\n"),
        )
        .map_err(|error| error.to_string())?;
        *child = None;
        Ok(())
    }

    fn spawn(&self) -> Result<(), String> {
        let mut child = self
            .inner
            .child
            .lock()
            .map_err(|_| "daemon child lock poisoned")?;
        if child.is_some() {
            return Ok(());
        }
        let stdout = diagnostic_log(&self.inner.diagnostics_dir.join("daemon.stdout.log"))?;
        let stderr = diagnostic_log(&self.inner.diagnostics_dir.join("daemon.stderr.log"))?;
        let process = Command::new(&self.inner.executable)
            .env("BASTET_DATABASE", &self.inner.database)
            .env("BASTET_LISTEN", &self.inner.listen)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .map_err(|error| {
                format!(
                    "failed to start daemon {}: {error}",
                    self.inner.executable.display()
                )
            })?;
        *child = Some(process);
        Ok(())
    }
}

fn diagnostic_log(path: &Path) -> Result<File, String> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| error.to_string())
}

fn daemon_executable() -> Result<PathBuf, String> {
    daemon_executable_from(env::var_os("BASTET_DAEMON_BIN"))
}

fn daemon_executable_from(explicit: Option<std::ffi::OsString>) -> Result<PathBuf, String> {
    if let Some(path) = explicit {
        return Ok(PathBuf::from(path));
    }
    let current = env::current_exe().map_err(|error| error.to_string())?;
    let filename = if cfg!(windows) {
        "bastet-daemon.exe"
    } else {
        "bastet-daemon"
    };
    let sibling = current.with_file_name(filename);
    if sibling.is_file() {
        return Ok(sibling);
    }
    Err(format!(
        "daemon executable not found next to desktop binary; set BASTET_DAEMON_BIN (looked for {})",
        sibling.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_daemon_path_has_priority() {
        assert_eq!(
            daemon_executable_from(Some("/tmp/bastet-explicit-daemon".into())).unwrap(),
            PathBuf::from("/tmp/bastet-explicit-daemon")
        );
    }

    #[test]
    fn shutdown_gate_is_single_owner_and_can_recover_after_failure() {
        let directory = tempfile::tempdir().unwrap();
        let supervisor = DaemonSupervisor::new_with_executable(
            directory.path(),
            directory.path().join("missing-daemon"),
        )
        .unwrap();

        assert!(supervisor.begin_shutdown());
        assert!(!supervisor.exit_authorized());
        supervisor.authorize_exit();
        assert!(supervisor.exit_authorized());
        assert!(!supervisor.begin_shutdown());
        assert!(supervisor.is_shutting_down());
        supervisor.cancel_shutdown();
        assert!(!supervisor.is_shutting_down());
        assert!(supervisor.begin_shutdown());
    }
}
