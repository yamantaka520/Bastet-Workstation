use std::collections::HashSet;

use thiserror::Error;
use uuid::Uuid;

use crate::{
    Account, AgentInstance, AgentProvider, CredentialReference, EntityMetadata, Model,
    ModelProvider, NormalizedRunState, PolicyLayer, Project, Role, Run, Session,
};

#[derive(Debug, Clone, Default)]
pub struct IdentityCatalog {
    pub credential_references: Vec<CredentialReference>,
    pub agent_providers: Vec<AgentProvider>,
    pub model_providers: Vec<ModelProvider>,
    pub accounts: Vec<Account>,
    pub models: Vec<Model>,
    pub agent_instances: Vec<AgentInstance>,
    pub projects: Vec<Project>,
    pub roles: Vec<Role>,
    pub sessions: Vec<Session>,
    pub runs: Vec<Run>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CatalogError {
    #[error("duplicate {entity} id {id}")]
    DuplicateId { entity: &'static str, id: Uuid },
    #[error("{entity} references missing {field} {id}")]
    MissingReference {
        entity: &'static str,
        field: &'static str,
        id: Uuid,
    },
    #[error("account and AgentInstance use different AgentProvider ids")]
    AgentProviderMismatch,
    #[error("{entity} has an empty required field: {field}")]
    EmptyField {
        entity: &'static str,
        field: &'static str,
    },
    #[error("{entity} must use the {expected:?} policy layer")]
    InvalidPolicyLayer {
        entity: &'static str,
        expected: PolicyLayer,
    },
    #[error("run state {state:?} has invalid start/finish timestamps")]
    InvalidRunTiming { state: NormalizedRunState },
}

impl IdentityCatalog {
    pub fn validate(&self) -> Result<(), CatalogError> {
        unique(
            "CredentialReference",
            self.credential_references
                .iter()
                .map(|item| item.metadata.id.value()),
        )?;
        unique(
            "AgentProvider",
            self.agent_providers
                .iter()
                .map(|item| item.metadata.id.value()),
        )?;
        unique(
            "ModelProvider",
            self.model_providers
                .iter()
                .map(|item| item.metadata.id.value()),
        )?;
        unique(
            "Account",
            self.accounts.iter().map(|item| item.metadata.id.value()),
        )?;
        unique(
            "Model",
            self.models.iter().map(|item| item.metadata.id.value()),
        )?;
        unique(
            "AgentInstance",
            self.agent_instances
                .iter()
                .map(|item| item.metadata.id.value()),
        )?;
        unique(
            "Project",
            self.projects.iter().map(|item| item.metadata.id.value()),
        )?;
        unique(
            "Role",
            self.roles.iter().map(|item| item.metadata.id.value()),
        )?;
        unique(
            "Session",
            self.sessions.iter().map(|item| item.metadata.id.value()),
        )?;
        unique("Run", self.runs.iter().map(|item| item.metadata.id.value()))?;

        self.validate_required_fields()?;

        for account in &self.accounts {
            require(
                self.agent_providers
                    .iter()
                    .any(|item| item.metadata.id == account.agent_provider_id),
                "Account",
                "agent_provider_id",
                account.agent_provider_id.value(),
            )?;
            if let Some(reference_id) = account.credential_reference_id {
                require(
                    self.credential_references
                        .iter()
                        .any(|item| item.metadata.id == reference_id),
                    "Account",
                    "credential_reference_id",
                    reference_id.value(),
                )?;
            }
        }
        for model in &self.models {
            require(
                self.model_providers
                    .iter()
                    .any(|item| item.metadata.id == model.model_provider_id),
                "Model",
                "model_provider_id",
                model.model_provider_id.value(),
            )?;
        }
        for instance in &self.agent_instances {
            require(
                self.agent_providers
                    .iter()
                    .any(|item| item.metadata.id == instance.agent_provider_id),
                "AgentInstance",
                "agent_provider_id",
                instance.agent_provider_id.value(),
            )?;
            if let Some(account_id) = instance.account_id {
                let account = self
                    .accounts
                    .iter()
                    .find(|item| item.metadata.id == account_id)
                    .ok_or(CatalogError::MissingReference {
                        entity: "AgentInstance",
                        field: "account_id",
                        id: account_id.value(),
                    })?;
                if account.agent_provider_id != instance.agent_provider_id {
                    return Err(CatalogError::AgentProviderMismatch);
                }
            }
            if let Some(model_id) = instance.default_model_id {
                require(
                    self.models.iter().any(|item| item.metadata.id == model_id),
                    "AgentInstance",
                    "default_model_id",
                    model_id.value(),
                )?;
            }
        }
        for session in &self.sessions {
            require(
                self.agent_instances
                    .iter()
                    .any(|item| item.metadata.id == session.agent_instance_id),
                "Session",
                "agent_instance_id",
                session.agent_instance_id.value(),
            )?;
            require(
                self.projects
                    .iter()
                    .any(|item| item.metadata.id == session.project_id),
                "Session",
                "project_id",
                session.project_id.value(),
            )?;
        }
        for run in &self.runs {
            require(
                self.sessions
                    .iter()
                    .any(|item| item.metadata.id == run.session_id),
                "Run",
                "session_id",
                run.session_id.value(),
            )?;
            require(
                self.models
                    .iter()
                    .any(|item| item.metadata.id == run.model_id),
                "Run",
                "model_id",
                run.model_id.value(),
            )?;
            validate_run_timing(run)?;
        }
        Ok(())
    }

    fn validate_required_fields(&self) -> Result<(), CatalogError> {
        for provider in &self.agent_providers {
            validate_metadata("AgentProvider", &provider.metadata)?;
            nonempty("AgentProvider", "adapter_kind", &provider.adapter_kind)?;
            nonempty("AgentProvider", "display_name", &provider.display_name)?;
        }
        for provider in &self.model_providers {
            validate_metadata("ModelProvider", &provider.metadata)?;
            nonempty("ModelProvider", "provider_key", &provider.provider_key)?;
        }
        for account in &self.accounts {
            validate_metadata("Account", &account.metadata)?;
            nonempty("Account", "provider_identity", &account.provider_identity)?;
        }
        for model in &self.models {
            validate_metadata("Model", &model.metadata)?;
            nonempty("Model", "provider_model_id", &model.provider_model_id)?;
        }
        for instance in &self.agent_instances {
            validate_metadata("AgentInstance", &instance.metadata)?;
        }
        for project in &self.projects {
            validate_metadata("Project", &project.metadata)?;
            nonempty("Project", "name", &project.name)?;
            nonempty("Project", "workspace_root", &project.workspace_root)?;
            if project.policy.layer != PolicyLayer::Project {
                return Err(CatalogError::InvalidPolicyLayer {
                    entity: "Project",
                    expected: PolicyLayer::Project,
                });
            }
        }
        for role in &self.roles {
            validate_metadata("Role", &role.metadata)?;
            nonempty("Role", "name", &role.name)?;
            if role.policy.layer != PolicyLayer::RoleOrAgent {
                return Err(CatalogError::InvalidPolicyLayer {
                    entity: "Role",
                    expected: PolicyLayer::RoleOrAgent,
                });
            }
        }
        for session in &self.sessions {
            validate_metadata("Session", &session.metadata)?;
            if let Some(provider_session_id) = &session.provider_session_id {
                nonempty("Session", "provider_session_id", provider_session_id)?;
            }
        }
        for run in &self.runs {
            validate_metadata("Run", &run.metadata)?;
        }
        for reference in &self.credential_references {
            validate_metadata("CredentialReference", &reference.metadata)?;
            nonempty("CredentialReference", "service", &reference.service)?;
            nonempty(
                "CredentialReference",
                "account_label",
                &reference.account_label,
            )?;
        }
        Ok(())
    }
}

fn validate_run_timing(run: &Run) -> Result<(), CatalogError> {
    let terminal = matches!(
        run.state,
        NormalizedRunState::Cancelled | NormalizedRunState::Failed | NormalizedRunState::Succeeded
    );
    let active = !matches!(run.state, NormalizedRunState::Starting);
    if (active && run.started_at.is_none()) || (terminal && run.finished_at.is_none()) {
        return Err(CatalogError::InvalidRunTiming { state: run.state });
    }
    Ok(())
}

fn unique(entity: &'static str, ids: impl IntoIterator<Item = Uuid>) -> Result<(), CatalogError> {
    let mut seen = HashSet::new();
    for id in ids {
        if !seen.insert(id) {
            return Err(CatalogError::DuplicateId { entity, id });
        }
    }
    Ok(())
}

fn require(
    exists: bool,
    entity: &'static str,
    field: &'static str,
    id: Uuid,
) -> Result<(), CatalogError> {
    if exists {
        Ok(())
    } else {
        Err(CatalogError::MissingReference { entity, field, id })
    }
}

fn nonempty(entity: &'static str, field: &'static str, value: &str) -> Result<(), CatalogError> {
    if value.trim().is_empty() {
        Err(CatalogError::EmptyField { entity, field })
    } else {
        Ok(())
    }
}

fn validate_metadata<I>(
    entity: &'static str,
    metadata: &EntityMetadata<I>,
) -> Result<(), CatalogError> {
    nonempty(entity, "created_at", &metadata.created_at)?;
    nonempty(entity, "updated_at", &metadata.updated_at)?;
    nonempty(
        entity,
        "provenance.source_kind",
        &metadata.provenance.source_kind,
    )?;
    nonempty(
        entity,
        "provenance.source_id",
        &metadata.provenance.source_id,
    )?;
    nonempty(
        entity,
        "provenance.recorded_by",
        &metadata.provenance.recorded_by,
    )
}

#[cfg(test)]
mod tests {
    use crate::{
        AccountId, AgentInstanceId, AgentProviderId, CredentialBackend, CredentialReferenceId,
        EntityLifecycle, EntityMetadata, ModelId, ModelProviderId, NormalizedRunState,
        PermissionLevel, PolicyCeiling, PolicyLayer, ProjectId, Provenance, RoleId, RunId,
        ScopedPolicy, SessionId,
    };

    use super::*;

    fn metadata<I>(id: I) -> EntityMetadata<I> {
        EntityMetadata {
            id,
            revision: 0,
            created_at: "2026-09-03T00:00:00Z".into(),
            updated_at: "2026-09-03T00:00:00Z".into(),
            provenance: Provenance {
                source_kind: "test_fixture".into(),
                source_id: "catalog".into(),
                recorded_by: "bastet-core".into(),
            },
            lifecycle: EntityLifecycle::Active,
        }
    }

    fn policy(layer: PolicyLayer) -> ScopedPolicy {
        ScopedPolicy {
            layer,
            ceiling: PolicyCeiling {
                filesystem: PermissionLevel::Observe,
                network: PermissionLevel::Deny,
                process: PermissionLevel::Observe,
                device: PermissionLevel::Deny,
                credential: PermissionLevel::Deny,
                persistent_approval: false,
            },
        }
    }

    fn fixture() -> IdentityCatalog {
        let credential_id = CredentialReferenceId::new();
        let agent_provider_id = AgentProviderId::new();
        let model_provider_id = ModelProviderId::new();
        let account_id = AccountId::new();
        let model_id = ModelId::new();
        let agent_instance_id = AgentInstanceId::new();
        let project_id = ProjectId::new();
        let session_id = SessionId::new();
        IdentityCatalog {
            credential_references: vec![CredentialReference {
                metadata: metadata(credential_id),
                backend: CredentialBackend::MacosKeychain,
                service: "dev.bastet.workstation.codex".into(),
                account_label: "default".into(),
            }],
            agent_providers: vec![AgentProvider {
                metadata: metadata(agent_provider_id),
                adapter_kind: "codex_cli".into(),
                display_name: "Codex CLI".into(),
            }],
            model_providers: vec![ModelProvider {
                metadata: metadata(model_provider_id),
                provider_key: "openai".into(),
                display_name: "OpenAI".into(),
            }],
            accounts: vec![Account {
                metadata: metadata(account_id),
                agent_provider_id,
                provider_identity: "codex-default".into(),
                credential_reference_id: Some(credential_id),
            }],
            models: vec![Model {
                metadata: metadata(model_id),
                model_provider_id,
                provider_model_id: "gpt-fixture".into(),
                reasoning_controls: vec!["low".into(), "high".into()],
            }],
            agent_instances: vec![AgentInstance {
                metadata: metadata(agent_instance_id),
                agent_provider_id,
                account_id: Some(account_id),
                default_model_id: Some(model_id),
            }],
            projects: vec![Project {
                metadata: metadata(project_id),
                name: "Fixture".into(),
                workspace_root: "/fixture".into(),
                policy: policy(PolicyLayer::Project),
            }],
            roles: vec![Role {
                metadata: metadata(RoleId::new()),
                name: "Researcher".into(),
                responsibilities: vec!["read".into()],
                policy: policy(PolicyLayer::RoleOrAgent),
            }],
            sessions: vec![Session {
                metadata: metadata(session_id),
                agent_instance_id,
                project_id,
                provider_session_id: Some("provider-session-fixture".into()),
            }],
            runs: vec![Run {
                metadata: metadata(RunId::new()),
                session_id,
                model_id,
                state: NormalizedRunState::Starting,
                started_at: None,
                finished_at: None,
            }],
        }
    }

    #[test]
    fn complete_catalog_is_valid() {
        fixture().validate().unwrap();
    }

    #[test]
    fn duplicate_ids_are_rejected() {
        let mut catalog = fixture();
        catalog
            .agent_providers
            .push(catalog.agent_providers[0].clone());
        assert!(matches!(
            catalog.validate(),
            Err(CatalogError::DuplicateId {
                entity: "AgentProvider",
                ..
            })
        ));
    }

    #[test]
    fn missing_credential_reference_is_rejected() {
        let mut catalog = fixture();
        catalog.credential_references.clear();
        assert!(matches!(
            catalog.validate(),
            Err(CatalogError::MissingReference {
                entity: "Account",
                field: "credential_reference_id",
                ..
            })
        ));
    }

    #[test]
    fn account_must_belong_to_instance_provider() {
        let mut catalog = fixture();
        catalog.agent_instances[0].agent_provider_id = AgentProviderId::new();
        catalog.agent_providers.push(AgentProvider {
            metadata: metadata(catalog.agent_instances[0].agent_provider_id),
            adapter_kind: "agy_cli".into(),
            display_name: "Agy CLI".into(),
        });
        assert_eq!(
            catalog.validate().unwrap_err(),
            CatalogError::AgentProviderMismatch
        );
    }

    #[test]
    fn empty_provenance_is_rejected() {
        let mut catalog = fixture();
        catalog.models[0].metadata.provenance.source_id.clear();
        assert_eq!(
            catalog.validate().unwrap_err(),
            CatalogError::EmptyField {
                entity: "Model",
                field: "provenance.source_id"
            }
        );
    }

    #[test]
    fn project_must_use_project_policy_layer() {
        let mut catalog = fixture();
        catalog.projects[0].policy.layer = PolicyLayer::SingleRun;
        assert_eq!(
            catalog.validate().unwrap_err(),
            CatalogError::InvalidPolicyLayer {
                entity: "Project",
                expected: PolicyLayer::Project
            }
        );
    }

    #[test]
    fn terminal_run_requires_start_and_finish_timestamps() {
        let mut catalog = fixture();
        catalog.runs[0].state = NormalizedRunState::Succeeded;
        assert_eq!(
            catalog.validate().unwrap_err(),
            CatalogError::InvalidRunTiming {
                state: NormalizedRunState::Succeeded
            }
        );
        catalog.runs[0].started_at = Some("2026-09-03T00:00:01Z".into());
        catalog.runs[0].finished_at = Some("2026-09-03T00:00:02Z".into());
        catalog.validate().unwrap();
    }
}
