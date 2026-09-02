use std::{env, net::SocketAddr, path::PathBuf};

use anyhow::Context;
use bastet_daemon::{router_with_shutdown, Store};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database = env::var_os("BASTET_DATABASE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("bastet-workstation.db"));
    let address: SocketAddr = env::var("BASTET_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:17841".into())
        .parse()
        .context("BASTET_LISTEN must be a socket address")?;
    let store = Store::open(database)?;
    store.mark_ready()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("bastet-daemon listening on {}", listener.local_addr()?);
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let signal_store = store.clone();
    axum::serve(listener, router_with_shutdown(store, shutdown_tx))
        .with_graceful_shutdown(async move {
            tokio::select! {
                result = shutdown_rx.changed() => {
                    let _ = result;
                }
                result = tokio::signal::ctrl_c() => {
                    if result.is_ok() {
                        if let Ok(snapshot) = signal_store.snapshot() {
                            let _ = signal_store.shutdown(bastet_protocol::CheckpointCommand {
                                expected_revision: snapshot.revision,
                                reason: "operating system interrupt".into(),
                            });
                        }
                    }
                }
            }
        })
        .await?;
    Ok(())
}
