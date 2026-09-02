//! Shared wire types. The daemon is authoritative; clients only project this state.

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
