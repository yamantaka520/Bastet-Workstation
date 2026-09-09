//! Explicit compiled-binary lifecycle smoke. This does not run providers.

use std::{
    env,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use bastet_client::{ClientError, DaemonClient};
use serde_json::json;
use tempfile::TempDir;

const READY_TIMEOUT: Duration = Duration::from_secs(10);

struct OwnedDaemon {
    child: Child,
}

impl OwnedDaemon {
    fn spawn(executable: &Path, database: &Path) -> std::io::Result<Self> {
        let child = Command::new(executable)
            .env("BASTET_DATABASE", database)
            .env_remove("BASTET_LISTEN")
            .env_remove("BASTET_DAEMON_URL")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        Ok(Self { child })
    }

    async fn wait_for_exit(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().map_err(|_| "daemon wait failed")? {
                if status.success() {
                    return Ok(());
                }
                return Err("daemon exited unsuccessfully".into());
            }
            if Instant::now() >= deadline {
                return Err("daemon did not exit after graceful shutdown".into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn force_kill_and_reap(&mut self) -> Result<(), String> {
        if self
            .child
            .try_wait()
            .map_err(|_| "daemon wait failed")?
            .is_none()
        {
            self.child.kill().map_err(|_| "owned daemon kill failed")?;
        }
        self.child.wait().map_err(|_| "owned daemon reap failed")?;
        Ok(())
    }
}

impl Drop for OwnedDaemon {
    fn drop(&mut self) {
        let _ = self.force_kill_and_reap();
    }
}

async fn wait_ready(
    client: &DaemonClient,
    daemon: &mut OwnedDaemon,
) -> Result<bastet_protocol::DaemonSnapshot, String> {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        if let Some(status) = daemon.child.try_wait().map_err(|_| "daemon wait failed")? {
            return Err(format!("daemon exited before becoming ready: {status}"));
        }
        if let Ok(snapshot) = client.snapshot().await {
            if snapshot.lifecycle == bastet_protocol::DaemonLifecycle::Ready {
                return Ok(snapshot);
            }
        }
        if Instant::now() >= deadline {
            return Err("daemon did not become ready".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn daemon_path() -> PathBuf {
    PathBuf::from(env::var_os("BASTET_SMOKE_DAEMON").expect("BASTET_SMOKE_DAEMON is required"))
}

#[tokio::test]
#[ignore = "requires an explicitly supplied compiled bastet-daemon executable"]
async fn compiled_daemon_lifecycle_uses_authenticated_local_ipc() -> Result<(), String> {
    let executable = daemon_path();
    if !executable.is_file() {
        return Err("compiled daemon executable is unavailable".into());
    }
    let directory = TempDir::new().map_err(|_| "temporary directory creation failed")?;
    let database = directory.path().join("bastet.db");
    let client = DaemonClient::for_database(&database).map_err(|_| "local client setup failed")?;

    let mut daemon =
        OwnedDaemon::spawn(&executable, &database).map_err(|_| "compiled daemon launch failed")?;
    let initial = wait_ready(&client, &mut daemon).await?;

    let suspended = client
        .suspend(initial.revision, "compiled smoke suspend")
        .await
        .map_err(|_| "suspend request failed")?;
    match client
        .checkpoint(suspended.revision, "must reject while suspended")
        .await
    {
        Err(ClientError::LocalStatus(409)) => {}
        _ => return Err("checkpoint was not rejected while suspended".into()),
    }
    let resumed = client
        .resume(suspended.revision, "compiled smoke resume")
        .await
        .map_err(|_| "resume request failed")?;
    if resumed.event_type != "daemon.resumed" {
        return Err("resume did not return the durable resume event".into());
    }
    let resumed_snapshot = wait_ready(&client, &mut daemon).await?;
    client
        .shutdown(
            resumed_snapshot.revision,
            "compiled smoke graceful shutdown",
        )
        .await
        .map_err(|_| "graceful shutdown request failed")?;
    daemon.wait_for_exit().await?;

    daemon = OwnedDaemon::spawn(&executable, &database).map_err(|_| "daemon restart failed")?;
    let recovered = wait_ready(&client, &mut daemon).await?;
    if recovered.daemon_id != initial.daemon_id {
        return Err("daemon identity changed after graceful restart".into());
    }

    daemon.force_kill_and_reap()?;
    daemon =
        OwnedDaemon::spawn(&executable, &database).map_err(|_| "daemon crash restart failed")?;
    let crash_recovered = wait_ready(&client, &mut daemon).await?;
    if crash_recovered.daemon_id != initial.daemon_id {
        return Err("daemon identity changed after forced restart".into());
    }
    client
        .shutdown(crash_recovered.revision, "compiled smoke complete")
        .await
        .map_err(|_| "final shutdown request failed")?;
    daemon.wait_for_exit().await?;

    println!(
        "BASTET_SMOKE_EVIDENCE={}",
        json!({
            "daemon": executable,
            "initial": initial,
            "suspended_revision": suspended.revision,
            "resume_event": resumed.event_type,
            "recovered": recovered,
            "crash_recovered": crash_recovered,
            "transport": "authenticated_local_ipc",
        })
    );
    Ok(())
}
