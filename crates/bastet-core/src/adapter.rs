use serde::{Deserialize, Serialize};

use crate::{AccountId, AgentInstanceId, ModelId, RunId, SessionId};

pub const AGENT_ADAPTER_CONTRACT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterOperation {
    Discover,
    Install,
    Update,
    Version,
    Doctor,
    Authenticate,
    ListModels,
    Start,
    Attach,
    Prompt,
    Steer,
    FollowUp,
    Status,
    Explain,
    Wait,
    Cancel,
    Terminate,
    ExportSession,
    ExportResult,
    ExportUsage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterCapabilities {
    pub operations: Vec<AdapterOperation>,
    pub reasoning_controls: Vec<String>,
    pub supports_read_only: bool,
    pub supports_write: bool,
    pub supports_resume: bool,
    pub supports_structured_events: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceClass {
    ProviderReported,
    AgentReported,
    LocallyMeasured,
    Estimated,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalizedRunState {
    Starting,
    Running,
    AwaitingApproval,
    Blocked,
    Cancelling,
    Cancelled,
    Failed,
    Succeeded,
    Recovering,
    Uncertain,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizedAdapterEvent {
    pub contract_version: u32,
    pub run_id: RunId,
    pub sequence: u64,
    pub state: NormalizedRunState,
    pub event_type: String,
    pub occurred_at: String,
    pub evidence_class: EvidenceClass,
    pub provider_event_id: Option<String>,
    pub redacted_payload_json: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterFailureKind {
    BinaryMissing,
    Authentication,
    Quota,
    PermissionDenied,
    Timeout,
    Cancelled,
    Crashed,
    ProtocolDrift,
    MalformedOutput,
    Unsupported,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterFailure {
    pub kind: AdapterFailureKind,
    pub message_key: String,
    pub retryable: bool,
    pub provider_code: Option<String>,
    pub redacted_detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartRunRequest {
    pub agent_instance_id: AgentInstanceId,
    pub account_id: Option<AccountId>,
    pub model_id: ModelId,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub prompt: String,
    pub read_only: bool,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostEvidence {
    pub evidence_class: EvidenceClass,
    pub currency: Option<String>,
    pub amount: Option<f64>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub confidence: f32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_states_are_stable_snake_case() {
        assert_eq!(
            serde_json::to_string(&NormalizedRunState::AwaitingApproval).unwrap(),
            "\"awaiting_approval\""
        );
        assert_eq!(
            serde_json::to_string(&AdapterFailureKind::ProtocolDrift).unwrap(),
            "\"protocol_drift\""
        );
    }

    #[test]
    fn evidence_class_does_not_conflate_estimates_with_provider_facts() {
        assert_ne!(EvidenceClass::Estimated, EvidenceClass::ProviderReported);
        assert_eq!(
            serde_json::to_string(&EvidenceClass::LocallyMeasured).unwrap(),
            "\"locally_measured\""
        );
    }

    #[test]
    fn contract_version_is_explicit() {
        assert_eq!(AGENT_ADAPTER_CONTRACT_VERSION, 1);
    }
}
