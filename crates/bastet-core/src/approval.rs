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
    #[error("approval credential scope requires an account for the acting agent")]
    CredentialAccountRequired,
    #[error("approval credential scope requires a credential reference on the acting account")]
    AccountCredentialRequired,
    #[error("approval credential scope does not match the acting account")]
    CredentialAccountMismatch,
    #[error("approval run scope belongs to a different agent")]
    RunAgentMismatch,
    #[error("approval run scope belongs to a different project")]
    RunProjectMismatch,
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
        let agent = catalog
            .agent_instances
            .iter()
            .find(|item| item.metadata.id == self.action.agent_instance_id)
            .ok_or(ApprovalError::MissingReference("agent_instance_id"))?;
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
            let run = catalog
                .runs
                .iter()
                .find(|item| item.metadata.id == run_id)
                .ok_or(ApprovalError::MissingReference("run_id"))?;
            let session = catalog
                .sessions
                .iter()
                .find(|item| item.metadata.id == run.session_id)
                .ok_or(ApprovalError::MissingReference("session_id"))?;
            if session.agent_instance_id != self.action.agent_instance_id {
                return Err(ApprovalError::RunAgentMismatch);
            }
            if session.project_id != self.action.scope.project_id {
                return Err(ApprovalError::RunProjectMismatch);
            }
        }
        if !self.action.scope.credential_reference_ids.is_empty() {
            for reference_id in &self.action.scope.credential_reference_ids {
                if !catalog
                    .credential_references
                    .iter()
                    .any(|item| item.metadata.id == *reference_id)
                {
                    return Err(ApprovalError::MissingReference("credential_reference_id"));
                }
            }
            let account_id = agent
                .account_id
                .ok_or(ApprovalError::CredentialAccountRequired)?;
            let account = catalog
                .accounts
                .iter()
                .find(|item| item.metadata.id == account_id)
                .ok_or(ApprovalError::MissingReference("account_id"))?;
            let account_reference = account
                .credential_reference_id
                .ok_or(ApprovalError::AccountCredentialRequired)?;
            for reference_id in &self.action.scope.credential_reference_ids {
                if *reference_id != account_reference {
                    return Err(ApprovalError::CredentialAccountMismatch);
                }
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
    use crate::{
        Account, AccountId, AgentInstance, AgentProvider, AgentProviderId, CredentialBackend,
        CredentialReference, EntityLifecycle, EntityMetadata, IdentityCatalog, Model, ModelId,
        ModelProvider, ModelProviderId, NormalizedRunState, PermissionLevel, PolicyCeiling,
        Project, Provenance, Run, Session, SessionId,
    };

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

    fn metadata<I>(id: I) -> EntityMetadata<I> {
        EntityMetadata {
            id,
            revision: 0,
            created_at: "2026-09-09T00:00:00Z".into(),
            updated_at: "2026-09-09T00:00:00Z".into(),
            provenance: Provenance {
                source_kind: "test".into(),
                source_id: "approval".into(),
                recorded_by: "test".into(),
            },
            lifecycle: EntityLifecycle::Active,
        }
    }

    fn bound_catalog() -> IdentityCatalog {
        let own_credential = CredentialReferenceId::from_bytes([10; 16]);
        let other_credential = CredentialReferenceId::from_bytes([11; 16]);
        let account_id = AccountId::from_bytes([12; 16]);
        let other_account_id = AccountId::from_bytes([17; 16]);
        let agent_id = AgentInstanceId::from_bytes([1; 16]);
        let other_agent_id = AgentInstanceId::from_bytes([6; 16]);
        let project_id = ProjectId::from_bytes([3; 16]);
        let other_project_id = ProjectId::from_bytes([13; 16]);
        let session_id = SessionId::from_bytes([7; 16]);
        let other_agent_session_id = SessionId::from_bytes([8; 16]);
        let other_project_session_id = SessionId::from_bytes([9; 16]);
        let agent_provider_id = AgentProviderId::from_bytes([2; 16]);
        let model_provider_id = ModelProviderId::from_bytes([18; 16]);
        let model_id = ModelId::from_bytes([16; 16]);
        let project_policy = policy(PolicyLayer::Project, PermissionLevel::Use, true);
        let catalog = IdentityCatalog {
            credential_references: vec![
                CredentialReference {
                    metadata: metadata(own_credential),
                    backend: CredentialBackend::MacosKeychain,
                    service: "test.own".into(),
                    account_label: "own".into(),
                },
                CredentialReference {
                    metadata: metadata(other_credential),
                    backend: CredentialBackend::MacosKeychain,
                    service: "test.other".into(),
                    account_label: "other".into(),
                },
            ],
            agent_providers: vec![AgentProvider {
                metadata: metadata(agent_provider_id),
                adapter_kind: "test_adapter".into(),
                display_name: "Test adapter".into(),
            }],
            model_providers: vec![ModelProvider {
                metadata: metadata(model_provider_id),
                provider_key: "test-model-provider".into(),
                display_name: "Test model provider".into(),
            }],
            accounts: vec![
                Account {
                    metadata: metadata(account_id),
                    agent_provider_id,
                    provider_identity: "own-account".into(),
                    credential_reference_id: Some(own_credential),
                },
                Account {
                    metadata: metadata(other_account_id),
                    agent_provider_id,
                    provider_identity: "other-account".into(),
                    credential_reference_id: Some(other_credential),
                },
            ],
            models: vec![Model {
                metadata: metadata(model_id),
                model_provider_id,
                provider_model_id: "test-model".into(),
                reasoning_controls: Vec::new(),
            }],
            agent_instances: vec![
                AgentInstance {
                    metadata: metadata(agent_id),
                    agent_provider_id,
                    account_id: Some(account_id),
                    default_model_id: None,
                },
                AgentInstance {
                    metadata: metadata(other_agent_id),
                    agent_provider_id,
                    account_id: Some(other_account_id),
                    default_model_id: None,
                },
            ],
            projects: vec![
                Project {
                    metadata: metadata(project_id),
                    name: "own project".into(),
                    workspace_root: "/own".into(),
                    policy: project_policy.clone(),
                },
                Project {
                    metadata: metadata(other_project_id),
                    name: "other project".into(),
                    workspace_root: "/other".into(),
                    policy: project_policy,
                },
            ],
            sessions: vec![
                Session {
                    metadata: metadata(session_id),
                    agent_instance_id: agent_id,
                    project_id,
                    provider_session_id: None,
                },
                Session {
                    metadata: metadata(other_agent_session_id),
                    agent_instance_id: other_agent_id,
                    project_id,
                    provider_session_id: None,
                },
                Session {
                    metadata: metadata(other_project_session_id),
                    agent_instance_id: agent_id,
                    project_id: other_project_id,
                    provider_session_id: None,
                },
            ],
            runs: vec![
                Run {
                    metadata: metadata(RunId::from_bytes([4; 16])),
                    session_id,
                    model_id,
                    state: NormalizedRunState::Starting,
                    started_at: None,
                    finished_at: None,
                },
                Run {
                    metadata: metadata(RunId::from_bytes([14; 16])),
                    session_id: other_agent_session_id,
                    model_id,
                    state: NormalizedRunState::Starting,
                    started_at: None,
                    finished_at: None,
                },
                Run {
                    metadata: metadata(RunId::from_bytes([15; 16])),
                    session_id: other_project_session_id,
                    model_id,
                    state: NormalizedRunState::Starting,
                    started_at: None,
                    finished_at: None,
                },
            ],
            ..IdentityCatalog::default()
        };
        catalog.validate().unwrap();
        catalog
    }

    fn bound_request() -> ApprovalRequest {
        let mut action = action();
        action.role_id = None;
        action.scope.credential_reference_ids = vec![CredentialReferenceId::from_bytes([10; 16])];
        ApprovalRequest::create(
            ApprovalRequestId::from_bytes([5; 16]),
            100,
            200,
            action,
            &policy(PolicyLayer::Project, PermissionLevel::Use, true),
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

    #[test]
    fn matching_own_credential_and_run_scope_succeeds() {
        bound_request().validate_against(&bound_catalog()).unwrap();
    }

    #[test]
    fn other_account_credential_is_rejected() {
        let mut request = bound_request();
        request.action.scope.credential_reference_ids =
            vec![CredentialReferenceId::from_bytes([11; 16])];
        request.request_hash = canonical_hash(
            request.id,
            request.created_at_ms,
            request.expires_at_ms,
            &request.action,
        );
        assert_eq!(
            request.validate_against(&bound_catalog()),
            Err(ApprovalError::CredentialAccountMismatch)
        );
    }

    #[test]
    fn credential_scope_without_an_account_is_rejected() {
        let request = bound_request();
        let mut catalog = bound_catalog();
        catalog.agent_instances[0].account_id = None;
        catalog.validate().unwrap();
        assert_eq!(
            request.validate_against(&catalog),
            Err(ApprovalError::CredentialAccountRequired)
        );
    }

    #[test]
    fn credential_scope_without_an_account_reference_is_rejected() {
        let request = bound_request();
        let mut catalog = bound_catalog();
        catalog.accounts[0].credential_reference_id = None;
        catalog.validate().unwrap();
        assert_eq!(
            request.validate_against(&catalog),
            Err(ApprovalError::AccountCredentialRequired)
        );
    }

    #[test]
    fn run_for_another_agent_is_rejected() {
        let mut request = bound_request();
        request.action.scope.run_id = Some(RunId::from_bytes([14; 16]));
        request.request_hash = canonical_hash(
            request.id,
            request.created_at_ms,
            request.expires_at_ms,
            &request.action,
        );
        assert_eq!(
            request.validate_against(&bound_catalog()),
            Err(ApprovalError::RunAgentMismatch)
        );
    }

    #[test]
    fn run_for_another_project_is_rejected() {
        let mut request = bound_request();
        request.action.scope.run_id = Some(RunId::from_bytes([15; 16]));
        request.request_hash = canonical_hash(
            request.id,
            request.created_at_ms,
            request.expires_at_ms,
            &request.action,
        );
        assert_eq!(
            request.validate_against(&bound_catalog()),
            Err(ApprovalError::RunProjectMismatch)
        );
    }

    #[test]
    fn matching_run_scope_succeeds_without_a_credential_scope() {
        let mut request = bound_request();
        request.action.scope.credential_reference_ids.clear();
        request.request_hash = canonical_hash(
            request.id,
            request.created_at_ms,
            request.expires_at_ms,
            &request.action,
        );
        request.validate_against(&bound_catalog()).unwrap();
    }
}
