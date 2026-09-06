use std::{env, time::Duration};

use bastet_core::{ApprovalDecision, ApprovalRequest, ApprovalRequestId, IdentityCatalog};
use bastet_protocol::{
    ApprovalList, ApprovalReceipt, ApprovalRecord, CatalogReceipt, CatalogSnapshot,
    CheckpointCommand, CheckpointReceipt, CreateApprovalCommand, DaemonSnapshot,
    DecideApprovalCommand, EventEnvelope, ReplaceCatalogCommand, PROTOCOL_VERSION,
};
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

    pub async fn catalog(&self) -> Result<CatalogSnapshot, ClientError> {
        let snapshot = self
            .http
            .get(format!("{}/v1/catalog", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json::<CatalogSnapshot>()
            .await?;
        require_protocol(snapshot.protocol_version)?;
        Ok(snapshot)
    }

    pub async fn replace_catalog(
        &self,
        expected_revision: u64,
        catalog: IdentityCatalog,
    ) -> Result<CatalogReceipt, ClientError> {
        let receipt = self
            .http
            .put(format!("{}/v1/catalog", self.base_url))
            .json(&ReplaceCatalogCommand {
                expected_revision,
                catalog,
            })
            .send()
            .await?
            .error_for_status()?
            .json::<CatalogReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn create_approval(
        &self,
        request: ApprovalRequest,
    ) -> Result<ApprovalReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!("{}/v1/approvals", self.base_url))
            .json(&CreateApprovalCommand { request })
            .send()
            .await?
            .error_for_status()?
            .json::<ApprovalReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn approval(
        &self,
        request_id: ApprovalRequestId,
    ) -> Result<ApprovalRecord, ClientError> {
        let record = self
            .http
            .get(format!(
                "{}/v1/approvals/{}",
                self.base_url,
                request_id.value()
            ))
            .send()
            .await?
            .error_for_status()?
            .json::<ApprovalRecord>()
            .await?;
        require_protocol(record.protocol_version)?;
        Ok(record)
    }

    pub async fn approvals(&self) -> Result<ApprovalList, ClientError> {
        let records = self
            .http
            .get(format!("{}/v1/approvals", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json::<ApprovalList>()
            .await?;
        require_protocol(records.protocol_version)?;
        Ok(records)
    }

    pub async fn decide_approval(
        &self,
        decision: ApprovalDecision,
    ) -> Result<ApprovalReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!(
                "{}/v1/approvals/{}",
                self.base_url,
                decision.request_id.value()
            ))
            .json(&DecideApprovalCommand { decision })
            .send()
            .await?
            .error_for_status()?
            .json::<ApprovalReceipt>()
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

    pub async fn suspend(
        &self,
        expected_revision: u64,
        reason: impl Into<String>,
    ) -> Result<CheckpointReceipt, ClientError> {
        self.post_checkpoint("/v1/power/suspend", expected_revision, reason)
            .await
    }

    pub async fn resume(
        &self,
        expected_revision: u64,
        reason: impl Into<String>,
    ) -> Result<EventEnvelope, ClientError> {
        let event = self
            .http
            .post(format!("{}/v1/power/resume", self.base_url))
            .json(&CheckpointCommand {
                expected_revision,
                reason: reason.into(),
            })
            .send()
            .await?
            .error_for_status()?
            .json::<EventEnvelope>()
            .await?;
        require_protocol(event.protocol_version)?;
        Ok(event)
    }

    async fn post_checkpoint(
        &self,
        path: &str,
        expected_revision: u64,
        reason: impl Into<String>,
    ) -> Result<CheckpointReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!("{}{path}", self.base_url))
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
        let initial_catalog = client.catalog().await.unwrap();
        assert_eq!(initial_catalog.revision, 0);
        let catalog_receipt = client
            .replace_catalog(initial_catalog.revision, IdentityCatalog::default())
            .await
            .unwrap();
        assert_eq!(catalog_receipt.revision, 1);
        assert_eq!(client.catalog().await.unwrap().revision, 1);
        let stale_catalog = client
            .replace_catalog(0, IdentityCatalog::default())
            .await
            .unwrap_err();
        assert!(matches!(
            stale_catalog,
            ClientError::Request(ref error)
                if error.status() == Some(reqwest::StatusCode::CONFLICT)
        ));
        let receipt = client
            .checkpoint(initial.revision, "client integration test")
            .await
            .unwrap();
        assert_eq!(receipt.revision, initial.revision + 1);
        assert_eq!(client.snapshot().await.unwrap().revision, receipt.revision);
        assert_eq!(store.events_after(0).unwrap().len(), 3);
        let suspended = client
            .suspend(receipt.revision, "integration simulated sleep")
            .await
            .unwrap();
        assert_eq!(
            client.snapshot().await.unwrap().lifecycle,
            bastet_protocol::DaemonLifecycle::Suspended
        );
        let rejected = client
            .checkpoint(suspended.revision, "must be rejected while suspended")
            .await
            .unwrap_err();
        assert!(matches!(
            rejected,
            ClientError::Request(ref error)
                if error.status() == Some(reqwest::StatusCode::CONFLICT)
        ));
        client
            .resume(suspended.revision, "integration simulated wake")
            .await
            .unwrap();
        assert_eq!(
            client.snapshot().await.unwrap().lifecycle,
            bastet_protocol::DaemonLifecycle::Ready
        );
        let shutdown = client
            .shutdown(suspended.revision + 1, "client integration shutdown")
            .await
            .unwrap();
        assert_eq!(shutdown.revision, suspended.revision + 2);
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
