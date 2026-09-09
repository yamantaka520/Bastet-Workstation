use std::{env, net::SocketAddr, path::PathBuf};

use anyhow::Context;
use bastet_daemon::{production_router_with_shutdown, Store, StoreError};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database = env::var_os("BASTET_DATABASE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("bastet-workstation.db"));
    let address = local_listen_address(
        &env::var("BASTET_LISTEN").unwrap_or_else(|_| "127.0.0.1:17841".into()),
    )?;
    // A second process must fail its listener claim before opening the shared
    // store, whose startup recovery would otherwise mark live runs uncertain.
    let listener = tokio::net::TcpListener::bind(address).await?;
    let store = Store::open(database)?;
    store.mark_ready()?;
    println!("bastet-daemon listening on {}", listener.local_addr()?);
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

fn local_listen_address(value: &str) -> anyhow::Result<SocketAddr> {
    let address: SocketAddr = value
        .parse()
        .context("BASTET_LISTEN must be a socket address")?;
    anyhow::ensure!(address.ip().is_loopback(), "BASTET_LISTEN must be loopback");
    Ok(address)
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
    fn listener_is_local_only_even_with_an_explicit_override() {
        for address in ["127.0.0.1:17841", "127.0.0.2:0", "[::1]:17841"] {
            assert!(local_listen_address(address).is_ok());
        }
        for address in ["0.0.0.0:17841", "[::]:17841", "192.0.2.1:17841", "bad"] {
            assert!(local_listen_address(address).is_err());
        }
    }
}
