use std::{env, time::Duration};

use bastet_protocol::{CheckpointCommand, CheckpointReceipt, DaemonSnapshot, PROTOCOL_VERSION};
use thiserror::Error;

#[derive(Clone)]
pub struct DaemonClient {
    base_url: String,
    http: reqwest::Client,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("daemon request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("daemon protocol mismatch: expected {expected}, received {actual}")]
    ProtocolMismatch { expected: u32, actual: u32 },
}

impl DaemonClient {
    pub fn from_env() -> Self {
        Self::new(
            env::var("BASTET_DAEMON_URL").unwrap_or_else(|_| "http://127.0.0.1:17841".to_owned()),
        )
    }

    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(1))
                .timeout(Duration::from_secs(3))
                .build()
                .expect("static HTTP client configuration must be valid"),
        }
    }

    pub async fn snapshot(&self) -> Result<DaemonSnapshot, ClientError> {
        let snapshot = self
            .http
            .get(format!("{}/v1/health", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json::<DaemonSnapshot>()
            .await?;
        require_protocol(snapshot.protocol_version)?;
        Ok(snapshot)
    }

    pub async fn checkpoint(
        &self,
        expected_revision: u64,
        reason: impl Into<String>,
    ) -> Result<CheckpointReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!("{}/v1/checkpoints", self.base_url))
            .json(&CheckpointCommand {
                expected_revision,
                reason: reason.into(),
            })
            .send()
            .await?
            .error_for_status()?
            .json::<CheckpointReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn shutdown(
        &self,
        expected_revision: u64,
        reason: impl Into<String>,
    ) -> Result<CheckpointReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!("{}/v1/shutdown", self.base_url))
            .json(&CheckpointCommand {
                expected_revision,
                reason: reason.into(),
            })
            .send()
            .await?
            .error_for_status()?
            .json::<CheckpointReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }
}

fn require_protocol(actual: u32) -> Result<(), ClientError> {
    if actual == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ClientError::ProtocolMismatch {
            expected: PROTOCOL_VERSION,
            actual,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bastet_daemon::{router_with_shutdown, Store};
    use tempfile::tempdir;

    #[tokio::test]
    async fn reconnects_and_checkpoints_through_real_loopback_api() {
        let directory = tempdir().unwrap();
        let store = Store::open(directory.path().join("bastet.db")).unwrap();
        store.mark_ready().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server_store = store.clone();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(async move {
            axum::serve(listener, router_with_shutdown(server_store, shutdown_tx))
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.changed().await;
                })
                .await
                .unwrap();
        });

        let client = DaemonClient::new(format!("http://{address}"));
        let initial = client.snapshot().await.unwrap();
        let receipt = client
            .checkpoint(initial.revision, "client integration test")
            .await
            .unwrap();
        assert_eq!(receipt.revision, initial.revision + 1);
        assert_eq!(client.snapshot().await.unwrap().revision, receipt.revision);
        assert_eq!(store.events_after(0).unwrap().len(), 2);
        let shutdown = client
            .shutdown(receipt.revision, "client integration shutdown")
            .await
            .unwrap();
        assert_eq!(shutdown.revision, receipt.revision + 1);
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("server must stop after durable shutdown receipt")
            .unwrap();
        assert_eq!(
            store.snapshot().unwrap().lifecycle,
            bastet_protocol::DaemonLifecycle::Stopping
        );
    }
}
