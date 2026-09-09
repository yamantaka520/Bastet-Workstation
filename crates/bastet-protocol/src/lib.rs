//! Shared wire types. The daemon is authoritative; clients only project this state.

use bastet_core::{
    ApprovalDecision, ApprovalRequest, ApprovalRequestId, GraphExecution, IdentityCatalog,
    M3Catalog, RunId,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonLifecycle {
    Starting,
    Ready,
    Checkpointing,
    Suspended,
    Stopping,
    Recovering,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonSnapshot {
    pub protocol_version: u32,
    pub daemon_id: Uuid,
    pub revision: u64,
    pub lifecycle: DaemonLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub protocol_version: u32,
    pub event_id: Uuid,
    pub sequence: u64,
    pub event_type: String,
    pub occurred_at: String,
    pub payload_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointCommand {
    pub expected_revision: u64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointReceipt {
    pub protocol_version: u32,
    pub checkpoint_id: Uuid,
    pub revision: u64,
    pub event_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub protocol_version: u32,
    pub revision: u64,
    pub catalog: IdentityCatalog,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplaceCatalogCommand {
    pub expected_revision: u64,
    pub catalog: IdentityCatalog,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogReceipt {
    pub protocol_version: u32,
    pub revision: u64,
    pub event_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct M3CatalogSnapshot {
    pub protocol_version: u32,
    pub revision: u64,
    pub catalog: M3Catalog,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplaceM3CatalogCommand {
    pub expected_revision: u64,
    pub catalog: M3Catalog,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphExecutionList {
    pub protocol_version: u32,
    pub executions: Vec<GraphExecution>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateGraphExecutionCommand {
    pub execution: GraphExecution,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphExecutionReceipt {
    pub protocol_version: u32,
    pub execution_id: bastet_core::GraphRunId,
    pub revision: u64,
    pub event_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestartMissingOutputGraphCommand {
    pub expected_graph_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryFailedGraphNodeCommand {
    pub node_id: bastet_core::GraphNodeId,
    pub expected_graph_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepareMvpCommand {
    pub expected_catalog_revision: u64,
    pub expected_m3_revision: u64,
    pub project_name: String,
    pub workspace_root: String,
    pub codex_model: String,
    pub agy_model: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepareMvpReceipt {
    pub protocol_version: u32,
    pub project_id: bastet_core::ProjectId,
    pub meeting_id: bastet_core::MeetingId,
    pub catalog_revision: u64,
    pub m3_revision: u64,
    pub event_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptDecisionBaselineCommand {
    pub expected_m3_revision: u64,
    pub meeting_id: bastet_core::MeetingId,
    pub content: String,
    pub accepted_by: String,
    pub accepted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptDecisionBaselineReceipt {
    pub protocol_version: u32,
    pub baseline_id: bastet_core::DecisionBaselineId,
    pub graph_execution_id: bastet_core::GraphRunId,
    pub m3_revision: u64,
    pub event_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimGraphNodesCommand {
    pub expected_revision: u64,
    pub owner: String,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimGraphNodesReceipt {
    pub protocol_version: u32,
    pub execution_id: bastet_core::GraphRunId,
    pub revision: u64,
    pub claimed: Vec<bastet_core::GraphNodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteGraphNodeCommand {
    pub expected_revision: u64,
    pub node_id: bastet_core::GraphNodeId,
    pub owner: String,
    pub succeeded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteGraphNodeReceipt {
    pub protocol_version: u32,
    pub execution_id: bastet_core::GraphRunId,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecuteReadyGraphCommand {
    pub expected_catalog_revision: u64,
    pub expected_graph_revision: u64,
}

/// Accepted daemon-owned work; provider output remains in the durable ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecuteReadyGraphReceipt {
    pub protocol_version: u32,
    pub execution_id: bastet_core::GraphRunId,
    pub run_ids: Vec<bastet_core::RunId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeginGraphNodeRunCommand {
    pub expected_catalog_revision: u64,
    pub expected_graph_revision: u64,
    pub node_id: bastet_core::GraphNodeId,
    pub owner: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCredentialSelection {
    pub reference_id: bastet_core::CredentialReferenceId,
    pub backend: bastet_core::CredentialBackend,
    pub service: String,
    pub account_label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAccountSelection {
    pub account_id: bastet_core::AccountId,
    pub provider_identity: String,
    pub credential: Option<ProviderCredentialSelection>,
}

/// Immutable selected identity, not evidence of provider authentication or a
/// credential grant. Secrets and arbitrary provider payloads are never included.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderLaunchIdentity {
    pub agent_instance_id: bastet_core::AgentInstanceId,
    pub agent_provider_id: bastet_core::AgentProviderId,
    pub project_id: bastet_core::ProjectId,
    pub model_id: bastet_core::ModelId,
    pub adapter_kind: String,
    pub model: String,
    pub account: Option<ProviderAccountSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeginGraphNodeRunReceipt {
    pub protocol_version: u32,
    pub execution_id: bastet_core::GraphRunId,
    pub node_id: bastet_core::GraphNodeId,
    pub session_id: bastet_core::SessionId,
    pub run_id: bastet_core::RunId,
    /// Absent on old wire receipts; new daemon-owned launches require it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_identity: Option<ProviderLaunchIdentity>,
    pub adapter_kind: String,
    pub model: String,
    pub workspace_root: String,
    pub prompt: String,
    pub catalog_revision: u64,
    pub graph_revision: u64,
    pub event_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FinishGraphNodeRunCommand {
    pub expected_catalog_revision: u64,
    pub expected_graph_revision: u64,
    pub expected_m3_revision: u64,
    pub node_id: bastet_core::GraphNodeId,
    pub run_id: bastet_core::RunId,
    pub owner: String,
    pub terminal_state: bastet_core::NormalizedRunState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<bastet_core::AdapterFailure>,
    pub provider_session_id: Option<String>,
    pub output_markdown: Option<String>,
    pub cost: bastet_core::CostEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinishGraphNodeRunReceipt {
    pub protocol_version: u32,
    pub execution_id: bastet_core::GraphRunId,
    pub node_id: bastet_core::GraphNodeId,
    pub run_id: bastet_core::RunId,
    pub cost_record_id: bastet_core::CostRecordId,
    pub output_content_hash: Option<String>,
    pub catalog_revision: u64,
    pub graph_revision: u64,
    pub m3_revision: u64,
    pub event_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateDocumentCommand {
    pub expected_m3_revision: u64,
    pub graph_execution_id: bastet_core::GraphRunId,
    pub title: String,
    pub markdown: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentReceipt {
    pub protocol_version: u32,
    pub artifact_id: bastet_core::ArtifactId,
    pub version_id: bastet_core::ArtifactVersionId,
    pub content_hash: String,
    pub m3_revision: u64,
    pub event_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptDocumentCommand {
    pub expected_m3_revision: u64,
    pub artifact_id: bastet_core::ArtifactId,
    pub version_id: bastet_core::ArtifactVersionId,
    pub content_hash: String,
    pub accepted_by: String,
    pub accepted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrepareKnowledgeDeliveryCommand {
    pub expected_m3_revision: u64,
    pub project_id: bastet_core::ProjectId,
    pub artifact_version_id: bastet_core::ArtifactVersionId,
    pub target: bastet_core::KnowledgeTarget,
    pub preview: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteKnowledgeDeliveryCommand {
    pub expected_m3_revision: u64,
    pub delivery_id: bastet_core::KnowledgeDeliveryId,
    pub destination_receipt: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeDeliveryReceipt {
    pub protocol_version: u32,
    pub delivery_id: bastet_core::KnowledgeDeliveryId,
    pub state: bastet_core::DeliveryState,
    pub m3_revision: u64,
    pub event_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordCostCommand {
    pub expected_m3_revision: u64,
    pub record: bastet_core::CostLedgerRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CostReceipt {
    pub protocol_version: u32,
    pub cost_record_id: bastet_core::CostRecordId,
    pub m3_revision: u64,
    pub event_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateApprovalCommand {
    pub request: ApprovalRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecideApprovalCommand {
    pub decision: ApprovalDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRecord {
    pub protocol_version: u32,
    pub request: ApprovalRequest,
    pub decision: Option<ApprovalDecision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalList {
    pub protocol_version: u32,
    pub records: Vec<ApprovalRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalReceipt {
    pub protocol_version: u32,
    pub request_id: ApprovalRequestId,
    pub event_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRunCommand {
    pub run_id: RunId,
    pub expected_catalog_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRunReceipt {
    pub protocol_version: u32,
    pub run_id: RunId,
    pub catalog_revision: u64,
    pub event_sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(launch_identity: Option<ProviderLaunchIdentity>) -> BeginGraphNodeRunReceipt {
        BeginGraphNodeRunReceipt {
            protocol_version: PROTOCOL_VERSION,
            execution_id: bastet_core::GraphRunId::from_bytes([1; 16]),
            node_id: bastet_core::GraphNodeId::from_bytes([2; 16]),
            session_id: bastet_core::SessionId::from_bytes([3; 16]),
            run_id: bastet_core::RunId::from_bytes([4; 16]),
            launch_identity,
            adapter_kind: "codex_cli".into(),
            model: "gpt-fixture".into(),
            workspace_root: "/fixture".into(),
            prompt: "bounded fixture prompt".into(),
            catalog_revision: 5,
            graph_revision: 6,
            event_sequence: 7,
        }
    }

    fn launch_identity() -> ProviderLaunchIdentity {
        ProviderLaunchIdentity {
            agent_instance_id: bastet_core::AgentInstanceId::from_bytes([8; 16]),
            agent_provider_id: bastet_core::AgentProviderId::from_bytes([9; 16]),
            project_id: bastet_core::ProjectId::from_bytes([10; 16]),
            model_id: bastet_core::ModelId::from_bytes([11; 16]),
            adapter_kind: "codex_cli".into(),
            model: "gpt-fixture".into(),
            account: Some(ProviderAccountSelection {
                account_id: bastet_core::AccountId::from_bytes([12; 16]),
                provider_identity: "fixture-account".into(),
                credential: Some(ProviderCredentialSelection {
                    reference_id: bastet_core::CredentialReferenceId::from_bytes([13; 16]),
                    backend: bastet_core::CredentialBackend::MacosKeychain,
                    service: "dev.bastet.fixture".into(),
                    account_label: "fixture-label".into(),
                }),
            }),
        }
    }

    #[test]
    fn protocol_serializes_stable_snake_case_states() {
        let json = serde_json::to_string(&DaemonLifecycle::Checkpointing).unwrap();
        assert_eq!(json, "\"checkpointing\"");
        assert_eq!(
            serde_json::to_string(&DaemonLifecycle::Suspended).unwrap(),
            "\"suspended\""
        );
    }

    #[test]
    fn legacy_begin_run_receipt_without_launch_identity_decodes_to_none() {
        let encoded = serde_json::to_value(receipt(None)).unwrap();
        assert!(encoded.get("launch_identity").is_none());
        let decoded = serde_json::from_value::<BeginGraphNodeRunReceipt>(encoded).unwrap();
        assert_eq!(decoded.launch_identity, None);
    }

    #[test]
    fn new_launch_identity_is_ignored_by_a_minimal_old_receipt_fixture() {
        #[derive(serde::Deserialize)]
        struct OldBeginGraphNodeRunReceipt {
            protocol_version: u32,
            execution_id: bastet_core::GraphRunId,
            node_id: bastet_core::GraphNodeId,
            session_id: bastet_core::SessionId,
            run_id: bastet_core::RunId,
            adapter_kind: String,
            model: String,
            workspace_root: String,
            prompt: String,
            catalog_revision: u64,
            graph_revision: u64,
            event_sequence: u64,
        }

        let expected = receipt(Some(launch_identity()));
        let old = serde_json::from_value::<OldBeginGraphNodeRunReceipt>(
            serde_json::to_value(&expected).unwrap(),
        )
        .unwrap();
        assert_eq!(old.protocol_version, expected.protocol_version);
        assert_eq!(old.execution_id, expected.execution_id);
        assert_eq!(old.node_id, expected.node_id);
        assert_eq!(old.session_id, expected.session_id);
        assert_eq!(old.run_id, expected.run_id);
        assert_eq!(old.adapter_kind, expected.adapter_kind);
        assert_eq!(old.model, expected.model);
        assert_eq!(old.workspace_root, expected.workspace_root);
        assert_eq!(old.prompt, expected.prompt);
        assert_eq!(old.catalog_revision, expected.catalog_revision);
        assert_eq!(old.graph_revision, expected.graph_revision);
        assert_eq!(old.event_sequence, expected.event_sequence);
    }

    #[test]
    fn launch_identity_round_trips_typed_optional_account_and_credential_metadata() {
        let expected = launch_identity();
        let encoded = serde_json::to_value(&expected).unwrap();
        let decoded = serde_json::from_value::<ProviderLaunchIdentity>(encoded.clone()).unwrap();
        assert_eq!(decoded, expected);
        let rendered = encoded.to_string().to_ascii_lowercase();
        for forbidden in [
            "secret",
            "token",
            "api_key",
            "access_key",
            "private_key",
            "password",
        ] {
            assert!(!rendered.contains(forbidden));
        }
        assert!(encoded["account"]["credential"].is_object());
        assert!(encoded["account"]["credential"]
            .get("reference_id")
            .is_some());
        assert!(encoded["account"]["credential"].get("backend").is_some());
        assert!(encoded["account"]["credential"].get("service").is_some());
        assert!(encoded["account"]["credential"]
            .get("account_label")
            .is_some());
    }

    #[test]
    fn launch_identity_rejects_unknown_secret_bearing_field() {
        let mut encoded = serde_json::to_value(launch_identity()).unwrap();
        encoded["api_key"] = serde_json::Value::String("fixture".into());
        assert!(serde_json::from_value::<ProviderLaunchIdentity>(encoded).is_err());
    }
}
