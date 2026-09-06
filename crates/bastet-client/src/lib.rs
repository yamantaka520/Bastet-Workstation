use std::{env, time::Duration};

use bastet_core::{ApprovalDecision, ApprovalRequest, ApprovalRequestId, IdentityCatalog, RunId};
use bastet_protocol::{
    AcceptDecisionBaselineCommand, AcceptDecisionBaselineReceipt, AcceptDocumentCommand,
    ApprovalList, ApprovalReceipt, ApprovalRecord, BeginGraphNodeRunCommand,
    BeginGraphNodeRunReceipt, CancelRunCommand, CancelRunReceipt, CatalogReceipt, CatalogSnapshot,
    CheckpointCommand, CheckpointReceipt, ClaimGraphNodesCommand, ClaimGraphNodesReceipt,
    CompleteGraphNodeCommand, CompleteGraphNodeReceipt, CompleteKnowledgeDeliveryCommand,
    CostReceipt, CreateApprovalCommand, CreateDocumentCommand, CreateGraphExecutionCommand,
    DaemonSnapshot, DecideApprovalCommand, DocumentReceipt, EventEnvelope,
    FinishGraphNodeRunCommand, FinishGraphNodeRunReceipt, GraphExecutionList,
    GraphExecutionReceipt, KnowledgeDeliveryReceipt, M3CatalogSnapshot,
    PrepareKnowledgeDeliveryCommand, PrepareMvpCommand, PrepareMvpReceipt, RecordCostCommand,
    ReplaceCatalogCommand, ReplaceM3CatalogCommand, PROTOCOL_VERSION,
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

    pub async fn m3_catalog(&self) -> Result<M3CatalogSnapshot, ClientError> {
        let snapshot = self
            .http
            .get(format!("{}/v1/m3", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json::<M3CatalogSnapshot>()
            .await?;
        require_protocol(snapshot.protocol_version)?;
        Ok(snapshot)
    }

    pub async fn graph_executions(&self) -> Result<GraphExecutionList, ClientError> {
        let list = self
            .http
            .get(format!("{}/v1/graphs", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json::<GraphExecutionList>()
            .await?;
        require_protocol(list.protocol_version)?;
        Ok(list)
    }

    pub async fn create_graph_execution(
        &self,
        command: CreateGraphExecutionCommand,
    ) -> Result<GraphExecutionReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!("{}/v1/graphs", self.base_url))
            .json(&command)
            .send()
            .await?
            .error_for_status()?
            .json::<GraphExecutionReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn prepare_mvp(
        &self,
        command: PrepareMvpCommand,
    ) -> Result<PrepareMvpReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!("{}/v1/mvp/prepare", self.base_url))
            .json(&command)
            .send()
            .await?
            .error_for_status()?
            .json::<PrepareMvpReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn accept_mvp_decision(
        &self,
        command: AcceptDecisionBaselineCommand,
    ) -> Result<AcceptDecisionBaselineReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!("{}/v1/mvp/accept-decision", self.base_url))
            .json(&command)
            .send()
            .await?
            .error_for_status()?
            .json::<AcceptDecisionBaselineReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn claim_graph_nodes(
        &self,
        execution_id: bastet_core::GraphRunId,
        command: ClaimGraphNodesCommand,
    ) -> Result<ClaimGraphNodesReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!(
                "{}/v1/graphs/{}/claim",
                self.base_url,
                execution_id.value()
            ))
            .json(&command)
            .send()
            .await?
            .error_for_status()?
            .json::<ClaimGraphNodesReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn begin_graph_node_run(
        &self,
        execution_id: bastet_core::GraphRunId,
        command: BeginGraphNodeRunCommand,
    ) -> Result<BeginGraphNodeRunReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!(
                "{}/v1/graphs/{}/runs",
                self.base_url,
                execution_id.value()
            ))
            .json(&command)
            .send()
            .await?
            .error_for_status()?
            .json::<BeginGraphNodeRunReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn finish_graph_node_run(
        &self,
        execution_id: bastet_core::GraphRunId,
        command: FinishGraphNodeRunCommand,
    ) -> Result<FinishGraphNodeRunReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!(
                "{}/v1/graphs/{}/runs/finish",
                self.base_url,
                execution_id.value()
            ))
            .json(&command)
            .send()
            .await?
            .error_for_status()?
            .json::<FinishGraphNodeRunReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn complete_graph_node(
        &self,
        execution_id: bastet_core::GraphRunId,
        command: CompleteGraphNodeCommand,
    ) -> Result<CompleteGraphNodeReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!(
                "{}/v1/graphs/{}/complete",
                self.base_url,
                execution_id.value()
            ))
            .json(&command)
            .send()
            .await?
            .error_for_status()?
            .json::<CompleteGraphNodeReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn create_mvp_document(
        &self,
        command: CreateDocumentCommand,
    ) -> Result<DocumentReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!("{}/v1/mvp/document", self.base_url))
            .json(&command)
            .send()
            .await?
            .error_for_status()?
            .json::<DocumentReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn accept_mvp_document(
        &self,
        command: AcceptDocumentCommand,
    ) -> Result<DocumentReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!("{}/v1/mvp/document/accept", self.base_url))
            .json(&command)
            .send()
            .await?
            .error_for_status()?
            .json::<DocumentReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn prepare_knowledge_delivery(
        &self,
        command: PrepareKnowledgeDeliveryCommand,
    ) -> Result<KnowledgeDeliveryReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!("{}/v1/mvp/knowledge/prepare", self.base_url))
            .json(&command)
            .send()
            .await?
            .error_for_status()?
            .json::<KnowledgeDeliveryReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn complete_knowledge_delivery(
        &self,
        command: CompleteKnowledgeDeliveryCommand,
    ) -> Result<KnowledgeDeliveryReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!("{}/v1/mvp/knowledge/complete", self.base_url))
            .json(&command)
            .send()
            .await?
            .error_for_status()?
            .json::<KnowledgeDeliveryReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn record_cost(
        &self,
        command: RecordCostCommand,
    ) -> Result<CostReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!("{}/v1/mvp/costs", self.base_url))
            .json(&command)
            .send()
            .await?
            .error_for_status()?
            .json::<CostReceipt>()
            .await?;
        require_protocol(receipt.protocol_version)?;
        Ok(receipt)
    }

    pub async fn replace_m3_catalog(
        &self,
        command: ReplaceM3CatalogCommand,
    ) -> Result<CatalogReceipt, ClientError> {
        let receipt = self
            .http
            .put(format!("{}/v1/m3", self.base_url))
            .json(&command)
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

    pub async fn cancel_run(
        &self,
        run_id: RunId,
        expected_catalog_revision: u64,
    ) -> Result<CancelRunReceipt, ClientError> {
        let receipt = self
            .http
            .post(format!(
                "{}/v1/runs/{}/cancel",
                self.base_url,
                run_id.value()
            ))
            .json(&CancelRunCommand {
                run_id,
                expected_catalog_revision,
            })
            .send()
            .await?
            .error_for_status()?
            .json::<CancelRunReceipt>()
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
        let initial_m3 = client.m3_catalog().await.unwrap();
        assert_eq!(initial_m3.revision, 0);
        let m3_receipt = client
            .replace_m3_catalog(ReplaceM3CatalogCommand {
                expected_revision: 0,
                catalog: bastet_core::M3Catalog::default(),
            })
            .await
            .unwrap();
        assert_eq!(m3_receipt.revision, 1);
        assert_eq!(client.m3_catalog().await.unwrap().revision, 1);
        let prepared = client
            .prepare_mvp(PrepareMvpCommand {
                expected_catalog_revision: 1,
                expected_m3_revision: 1,
                project_name: "Client MVP".into(),
                workspace_root: directory.path().to_string_lossy().into_owned(),
                codex_model: "gpt-test".into(),
                agy_model: "agy-test".into(),
            })
            .await
            .unwrap();
        let accepted = client
            .accept_mvp_decision(AcceptDecisionBaselineCommand {
                expected_m3_revision: prepared.m3_revision,
                meeting_id: prepared.meeting_id,
                content: "Two research branches and one explicit document join.".into(),
                accepted_by: "test-user".into(),
                accepted_at: "2026-09-07T00:00:00Z".into(),
            })
            .await
            .unwrap();
        assert_eq!(accepted.m3_revision, prepared.m3_revision + 1);
        let execution_id = accepted.graph_execution_id;
        let graph = client
            .graph_executions()
            .await
            .unwrap()
            .executions
            .remove(0);
        let begun = client
            .begin_graph_node_run(
                execution_id,
                BeginGraphNodeRunCommand {
                    expected_catalog_revision: prepared.catalog_revision,
                    expected_graph_revision: graph.revision,
                    node_id: graph.nodes[0].node_id,
                    owner: "codex-worker".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(begun.adapter_kind, "codex_cli");
        assert!(begun.prompt.contains("Assigned task"));
        let branches = client
            .claim_graph_nodes(
                execution_id,
                ClaimGraphNodesCommand {
                    expected_revision: begun.graph_revision,
                    owner: "research".into(),
                    limit: 2,
                },
            )
            .await
            .unwrap();
        assert_eq!(branches.claimed.len(), 1);
        let mut revision = branches.revision;
        let finished = client
            .finish_graph_node_run(
                execution_id,
                FinishGraphNodeRunCommand {
                    expected_catalog_revision: begun.catalog_revision,
                    expected_graph_revision: revision,
                    expected_m3_revision: accepted.m3_revision,
                    node_id: begun.node_id,
                    run_id: begun.run_id,
                    owner: "codex-worker".into(),
                    terminal_state: bastet_core::NormalizedRunState::Succeeded,
                    provider_session_id: Some("provider-thread-client-test".into()),
                    cost: bastet_core::CostEvidence {
                        evidence_class: bastet_core::EvidenceClass::ProviderReported,
                        currency: None,
                        amount: None,
                        input_tokens: Some(8),
                        output_tokens: Some(2),
                        confidence: 1.0,
                    },
                },
            )
            .await
            .unwrap();
        revision = finished.graph_revision;
        for node_id in branches.claimed {
            revision = client
                .complete_graph_node(
                    execution_id,
                    CompleteGraphNodeCommand {
                        expected_revision: revision,
                        node_id,
                        owner: "research".into(),
                        succeeded: true,
                    },
                )
                .await
                .unwrap()
                .revision;
        }
        let join = client
            .claim_graph_nodes(
                execution_id,
                ClaimGraphNodesCommand {
                    expected_revision: revision,
                    owner: "integrator".into(),
                    limit: 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(join.claimed.len(), 1);
        client
            .complete_graph_node(
                execution_id,
                CompleteGraphNodeCommand {
                    expected_revision: join.revision,
                    node_id: join.claimed[0],
                    owner: "integrator".into(),
                    succeeded: true,
                },
            )
            .await
            .unwrap();
        let document = client
            .create_mvp_document(CreateDocumentCommand {
                expected_m3_revision: finished.m3_revision,
                graph_execution_id: execution_id,
                title: "Client report".into(),
                markdown: "# Client report\n\nJoined evidence.".into(),
            })
            .await
            .unwrap();
        let accepted_document = client
            .accept_mvp_document(AcceptDocumentCommand {
                expected_m3_revision: document.m3_revision,
                artifact_id: document.artifact_id,
                version_id: document.version_id,
                content_hash: document.content_hash,
                accepted_by: "test-user".into(),
                accepted_at: "2026-09-07T00:01:00Z".into(),
            })
            .await
            .unwrap();
        let memory = client
            .prepare_knowledge_delivery(PrepareKnowledgeDeliveryCommand {
                expected_m3_revision: accepted_document.m3_revision,
                project_id: prepared.project_id,
                artifact_version_id: document.version_id,
                target: bastet_core::KnowledgeTarget::AgentMemoryOs,
                preview: "Redacted durable project result.".into(),
            })
            .await
            .unwrap();
        assert_eq!(memory.state, bastet_core::DeliveryState::Prepared);
        let memory = client
            .complete_knowledge_delivery(CompleteKnowledgeDeliveryCommand {
                expected_m3_revision: memory.m3_revision,
                delivery_id: memory.delivery_id,
                destination_receipt: "agent-memory://client-test".into(),
            })
            .await
            .unwrap();
        assert_eq!(memory.state, bastet_core::DeliveryState::Delivered);
        let mind = client
            .prepare_knowledge_delivery(PrepareKnowledgeDeliveryCommand {
                expected_m3_revision: memory.m3_revision,
                project_id: prepared.project_id,
                artifact_version_id: document.version_id,
                target: bastet_core::KnowledgeTarget::BastetMind,
                preview: "Redacted sourced project result.".into(),
            })
            .await
            .unwrap();
        let mind = client
            .complete_knowledge_delivery(CompleteKnowledgeDeliveryCommand {
                expected_m3_revision: mind.m3_revision,
                delivery_id: mind.delivery_id,
                destination_receipt: "bastet-mind://client-test".into(),
            })
            .await
            .unwrap();
        assert_eq!(mind.state, bastet_core::DeliveryState::Delivered);
        let cost = client
            .record_cost(RecordCostCommand {
                expected_m3_revision: mind.m3_revision,
                record: bastet_core::CostLedgerRecord {
                    metadata: bastet_core::EntityMetadata {
                        id: bastet_core::CostRecordId::new(),
                        revision: 0,
                        created_at: "2026-09-07T00:02:00Z".into(),
                        updated_at: "2026-09-07T00:02:00Z".into(),
                        provenance: bastet_core::Provenance {
                            source_kind: "provider_event".into(),
                            source_id: "client-test".into(),
                            recorded_by: "bastet-client-test".into(),
                        },
                        lifecycle: bastet_core::EntityLifecycle::Active,
                    },
                    project_id: prepared.project_id,
                    node_id: begun.node_id,
                    run_id: begun.run_id,
                    provider: begun.adapter_kind,
                    account: "local-default".into(),
                    model: begun.model,
                    currency: None,
                    amount: None,
                    input_tokens: Some(10),
                    output_tokens: Some(2),
                    evidence_class: bastet_core::EvidenceClass::ProviderReported,
                    source: "provider usage event".into(),
                    formula_version: None,
                    confidence: 1.0,
                    reconciliation_state: "observed".into(),
                },
            })
            .await
            .unwrap();
        assert_eq!(cost.m3_revision, mind.m3_revision + 1);
        let receipt = client
            .checkpoint(initial.revision, "client integration test")
            .await
            .unwrap();
        assert_eq!(receipt.revision, initial.revision + 1);
        assert_eq!(client.snapshot().await.unwrap().revision, receipt.revision);
        assert_eq!(store.events_after(0).unwrap().len(), 19);
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
