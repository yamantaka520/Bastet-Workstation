use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    AgentInstanceId, ApprovalRequestId, CredentialReferenceId, PolicyError, PolicyLayer, ProjectId,
    RoleId, RunId, ScopedPolicy,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalRisk {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalScope {
    pub project_id: ProjectId,
    pub run_id: Option<RunId>,
    pub filesystem_roots: Vec<String>,
    pub data_scopes: Vec<String>,
    pub network_destinations: Vec<String>,
    pub credential_reference_ids: Vec<CredentialReferenceId>,
    pub destination: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalAction {
    pub agent_instance_id: AgentInstanceId,
    pub role_id: Option<RoleId>,
    pub action_key: String,
    pub reason_key: String,
    pub consequence_key: String,
    pub risk: ApprovalRisk,
    pub scope: ApprovalScope,
    pub requested_policy: ScopedPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub id: ApprovalRequestId,
    pub request_hash: String,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    pub action: ApprovalAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecisionKind {
    Approve,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDecision {
    pub request_id: ApprovalRequestId,
    pub request_hash: String,
    pub kind: ApprovalDecisionKind,
    pub decided_at_ms: u64,
    pub actor: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ApprovalError {
    #[error("approval request contains an empty required field: {0}")]
    EmptyField(&'static str),
    #[error("approval request expiry must be after creation")]
    InvalidExpiry,
    #[error("approval request must use a single-run policy layer")]
    InvalidPolicyLayer,
    #[error("approval request exceeds its parent policy ceiling: {0}")]
    Policy(#[from] PolicyError),
    #[error("high-risk approval cannot become persistent")]
    PersistentHighRisk,
    #[error("approval decision does not match the immutable request")]
    HashMismatch,
    #[error("approval request has expired")]
    Expired,
    #[error("approval decision actor is empty")]
    EmptyActor,
    #[error("approval request references missing {0}")]
    MissingReference(&'static str),
}

impl ApprovalRequest {
    pub fn create(
        id: ApprovalRequestId,
        created_at_ms: u64,
        expires_at_ms: u64,
        action: ApprovalAction,
        parent_policy: &ScopedPolicy,
    ) -> Result<Self, ApprovalError> {
        validate_action(&action, parent_policy)?;
        if expires_at_ms <= created_at_ms {
            return Err(ApprovalError::InvalidExpiry);
        }
        let request_hash = canonical_hash(id, created_at_ms, expires_at_ms, &action);
        Ok(Self {
            id,
            request_hash,
            created_at_ms,
            expires_at_ms,
            action,
        })
    }

    pub fn verify_decision(&self, decision: &ApprovalDecision) -> Result<(), ApprovalError> {
        if decision.actor.trim().is_empty() {
            return Err(ApprovalError::EmptyActor);
        }
        if decision.request_id != self.id || decision.request_hash != self.request_hash {
            return Err(ApprovalError::HashMismatch);
        }
        if decision.decided_at_ms > self.expires_at_ms {
            return Err(ApprovalError::Expired);
        }
        Ok(())
    }

    pub fn validate_unchanged(&self) -> Result<(), ApprovalError> {
        let expected = canonical_hash(
            self.id,
            self.created_at_ms,
            self.expires_at_ms,
            &self.action,
        );
        if expected == self.request_hash {
            Ok(())
        } else {
            Err(ApprovalError::HashMismatch)
        }
    }

    pub fn validate_against(&self, catalog: &crate::IdentityCatalog) -> Result<(), ApprovalError> {
        self.validate_unchanged()?;
        if !catalog
            .agent_instances
            .iter()
            .any(|item| item.metadata.id == self.action.agent_instance_id)
        {
            return Err(ApprovalError::MissingReference("agent_instance_id"));
        }
        let project = catalog
            .projects
            .iter()
            .find(|item| item.metadata.id == self.action.scope.project_id)
            .ok_or(ApprovalError::MissingReference("project_id"))?;
        let parent_policy = if let Some(role_id) = self.action.role_id {
            &catalog
                .roles
                .iter()
                .find(|item| item.metadata.id == role_id)
                .ok_or(ApprovalError::MissingReference("role_id"))?
                .policy
        } else {
            &project.policy
        };
        if let Some(run_id) = self.action.scope.run_id {
            if !catalog.runs.iter().any(|item| item.metadata.id == run_id) {
                return Err(ApprovalError::MissingReference("run_id"));
            }
        }
        for reference_id in &self.action.scope.credential_reference_ids {
            if !catalog
                .credential_references
                .iter()
                .any(|item| item.metadata.id == *reference_id)
            {
                return Err(ApprovalError::MissingReference("credential_reference_id"));
            }
        }
        validate_action(&self.action, parent_policy)
    }
}

fn validate_action(action: &ApprovalAction, parent: &ScopedPolicy) -> Result<(), ApprovalError> {
    for (name, value) in [
        ("action_key", action.action_key.as_str()),
        ("reason_key", action.reason_key.as_str()),
        ("consequence_key", action.consequence_key.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(ApprovalError::EmptyField(name));
        }
    }
    if action.requested_policy.layer != PolicyLayer::SingleRun {
        return Err(ApprovalError::InvalidPolicyLayer);
    }
    parent.restrict(action.requested_policy.clone())?;
    if matches!(action.risk, ApprovalRisk::High | ApprovalRisk::Critical)
        && action.requested_policy.ceiling.persistent_approval
    {
        return Err(ApprovalError::PersistentHighRisk);
    }
    Ok(())
}

fn canonical_hash(
    id: ApprovalRequestId,
    created_at_ms: u64,
    expires_at_ms: u64,
    action: &ApprovalAction,
) -> String {
    #[derive(Serialize)]
    struct HashInput<'a> {
        schema: &'static str,
        id: ApprovalRequestId,
        created_at_ms: u64,
        expires_at_ms: u64,
        action: &'a ApprovalAction,
    }
    let encoded = serde_json::to_vec(&HashInput {
        schema: "bastet.approval-request.v1",
        id,
        created_at_ms,
        expires_at_ms,
        action,
    })
    .expect("typed approval request must serialize");
    let digest = Sha256::digest(encoded);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PermissionLevel, PolicyCeiling};

    fn policy(layer: PolicyLayer, level: PermissionLevel, persistent: bool) -> ScopedPolicy {
        ScopedPolicy {
            layer,
            ceiling: PolicyCeiling {
                filesystem: level,
                network: level,
                process: level,
                device: level,
                credential: level,
                persistent_approval: persistent,
            },
        }
    }

    fn action() -> ApprovalAction {
        ApprovalAction {
            agent_instance_id: AgentInstanceId::from_bytes([1; 16]),
            role_id: Some(RoleId::from_bytes([2; 16])),
            action_key: "agent.write_file".into(),
            reason_key: "approval.reason.generate_report".into(),
            consequence_key: "approval.consequence.modifies_workspace".into(),
            risk: ApprovalRisk::Medium,
            scope: ApprovalScope {
                project_id: ProjectId::from_bytes([3; 16]),
                run_id: Some(RunId::from_bytes([4; 16])),
                filesystem_roots: vec!["/workspace".into()],
                data_scopes: vec!["project.documents".into()],
                network_destinations: Vec::new(),
                credential_reference_ids: Vec::new(),
                destination: None,
            },
            requested_policy: policy(PolicyLayer::SingleRun, PermissionLevel::Observe, false),
        }
    }

    fn request() -> ApprovalRequest {
        ApprovalRequest::create(
            ApprovalRequestId::from_bytes([5; 16]),
            100,
            200,
            action(),
            &policy(PolicyLayer::RoleOrAgent, PermissionLevel::Use, true),
        )
        .unwrap()
    }

    #[test]
    fn canonical_hash_is_stable_and_any_change_is_rejected() {
        let original = request();
        assert_eq!(original, request());
        original.validate_unchanged().unwrap();
        let mut changed = original.clone();
        changed.action.scope.destination = Some("external.example".into());
        assert_eq!(
            changed.validate_unchanged(),
            Err(ApprovalError::HashMismatch)
        );
    }

    #[test]
    fn decision_requires_exact_hash_actor_and_unexpired_time() {
        let request = request();
        request
            .verify_decision(&ApprovalDecision {
                request_id: request.id,
                request_hash: request.request_hash.clone(),
                kind: ApprovalDecisionKind::Approve,
                decided_at_ms: 150,
                actor: "local-user".into(),
            })
            .unwrap();
        let mut expired = ApprovalDecision {
            request_id: request.id,
            request_hash: request.request_hash.clone(),
            kind: ApprovalDecisionKind::Approve,
            decided_at_ms: 201,
            actor: "local-user".into(),
        };
        assert_eq!(
            request.verify_decision(&expired),
            Err(ApprovalError::Expired)
        );
        expired.decided_at_ms = 150;
        expired.request_hash = "0".repeat(64);
        assert_eq!(
            request.verify_decision(&expired),
            Err(ApprovalError::HashMismatch)
        );
        expired.request_hash.clone_from(&request.request_hash);
        expired.actor.clear();
        assert_eq!(
            request.verify_decision(&expired),
            Err(ApprovalError::EmptyActor)
        );
    }

    #[test]
    fn policy_ceiling_expiry_and_persistent_high_risk_fail_closed() {
        let parent = policy(PolicyLayer::RoleOrAgent, PermissionLevel::Observe, false);
        let mut expanded = action();
        expanded.requested_policy = policy(PolicyLayer::SingleRun, PermissionLevel::Use, false);
        assert!(matches!(
            ApprovalRequest::create(ApprovalRequestId::new(), 100, 200, expanded, &parent),
            Err(ApprovalError::Policy(_))
        ));
        assert_eq!(
            ApprovalRequest::create(ApprovalRequestId::new(), 100, 100, action(), &parent),
            Err(ApprovalError::InvalidExpiry)
        );
        let mut dangerous = action();
        dangerous.risk = ApprovalRisk::Critical;
        dangerous.requested_policy = policy(PolicyLayer::SingleRun, PermissionLevel::Observe, true);
        assert_eq!(
            ApprovalRequest::create(
                ApprovalRequestId::new(),
                100,
                200,
                dangerous,
                &policy(PolicyLayer::RoleOrAgent, PermissionLevel::Use, true)
            ),
            Err(ApprovalError::PersistentHighRisk)
        );
    }

    #[test]
    fn denial_is_bound_to_the_same_immutable_request() {
        let request = request();
        request
            .verify_decision(&ApprovalDecision {
                request_id: request.id,
                request_hash: request.request_hash.clone(),
                kind: ApprovalDecisionKind::Deny,
                decided_at_ms: 150,
                actor: "local-user".into(),
            })
            .unwrap();
    }
}
