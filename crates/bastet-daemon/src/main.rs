use std::{env, path::PathBuf};

use bastet_daemon::{production_router_with_shutdown, Store, StoreError};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database = env::var_os("BASTET_DATABASE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("bastet-workstation.db"));
    reject_legacy_tcp_override(env::var_os("BASTET_LISTEN").is_some())?;
    let endpoint = bastet_local_ipc::Endpoint::for_database(&database)?;
    // Hold the OS-account-scoped endpoint guard through complete HTTP shutdown.
    // A competing process must fail before Store startup recovery can run.
    let (listener, _endpoint_guard) = bastet_local_ipc::bind(&endpoint)?;
    let store = Store::open(database)?;
    store.mark_ready()?;
    println!("bastet-daemon local IPC ready");
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let signal_store = store.clone();
    let mut ctrl_c_available = true;
    #[cfg(unix)]
    let mut terminate_signal =
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(signal) => Some(signal),
            Err(_) => {
                eprintln!("SIGTERM handler unavailable; daemon remains controllable by API");
                None
            }
        };
    axum::serve(
        listener,
        production_router_with_shutdown(store, shutdown_tx),
    )
    .with_graceful_shutdown(async move {
        loop {
            #[cfg(unix)]
            let terminate = async {
                match terminate_signal.as_mut() {
                    Some(signal) => signal.recv().await.is_some(),
                    None => std::future::pending::<bool>().await,
                }
            };
            #[cfg(not(unix))]
            let terminate = std::future::pending::<bool>();
            tokio::select! {
                result = shutdown_rx.changed() => {
                    if result.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                result = tokio::signal::ctrl_c(), if ctrl_c_available => {
                    match result {
                        Ok(()) => {
                            checkpoint_for_signal(&signal_store, "operating system interrupt").await;
                            break;
                        }
                        Err(_) => {
                            eprintln!("interrupt handler unavailable; daemon remains controllable by API");
                            ctrl_c_available = false;
                        }
                    }
                }
                received = terminate => {
                    if received {
                        checkpoint_for_signal(&signal_store, "operating system termination").await;
                        break;
                    }
                    #[cfg(unix)]
                    {
                        eprintln!("SIGTERM handler closed; daemon remains controllable by API");
                        terminate_signal = None;
                    }
                }
            }
        }
    })
    .await?;
    Ok(())
}

fn reject_legacy_tcp_override(present: bool) -> anyhow::Result<()> {
    anyhow::ensure!(
        !present,
        "BASTET_LISTEN is unsupported; daemon requires local IPC"
    );
    Ok(())
}

async fn checkpoint_for_signal(store: &Store, reason: &str) {
    let mut reported_checkpoint_failure = false;
    loop {
        let Ok(snapshot) = store.snapshot() else {
            if !reported_checkpoint_failure {
                eprintln!("shutdown checkpoint unavailable; daemon remains running");
                reported_checkpoint_failure = true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue;
        };
        match store.shutdown(bastet_protocol::CheckpointCommand {
            expected_revision: snapshot.revision,
            reason: reason.into(),
        }) {
            Ok(_) => break,
            Err(StoreError::ProviderRunsActive | StoreError::RevisionConflict { .. }) => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(_) => {
                if !reported_checkpoint_failure {
                    eprintln!("shutdown checkpoint failed; daemon remains running");
                    reported_checkpoint_failure = true;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_listener_has_no_legacy_tcp_override() {
        assert!(reject_legacy_tcp_override(false).is_ok());
        assert!(reject_legacy_tcp_override(true).is_err());
    }
}
