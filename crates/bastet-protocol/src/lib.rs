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
pub struct PrepareMvpCommand {
    pub expected_catalog_revision: u64,
    pub expected_m3_revision: u64,
    pub project_name: String,
    pub workspace_root: String,
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
pub struct BeginGraphNodeRunCommand {
    pub expected_catalog_revision: u64,
    pub expected_graph_revision: u64,
    pub node_id: bastet_core::GraphNodeId,
    pub owner: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeginGraphNodeRunReceipt {
    pub protocol_version: u32,
    pub execution_id: bastet_core::GraphRunId,
    pub node_id: bastet_core::GraphNodeId,
    pub session_id: bastet_core::SessionId,
    pub run_id: bastet_core::RunId,
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
    pub succeeded: bool,
    pub provider_session_id: Option<String>,
    pub cost: bastet_core::CostEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinishGraphNodeRunReceipt {
    pub protocol_version: u32,
    pub execution_id: bastet_core::GraphRunId,
    pub node_id: bastet_core::GraphNodeId,
    pub run_id: bastet_core::RunId,
    pub cost_record_id: bastet_core::CostRecordId,
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

    #[test]
    fn protocol_serializes_stable_snake_case_states() {
        let json = serde_json::to_string(&DaemonLifecycle::Checkpointing).unwrap();
        assert_eq!(json, "\"checkpointing\"");
        assert_eq!(
            serde_json::to_string(&DaemonLifecycle::Suspended).unwrap(),
            "\"suspended\""
        );
    }
}
