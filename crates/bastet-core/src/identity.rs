use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{NormalizedRunState, ScopedPolicy};

macro_rules! stable_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            pub fn value(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

stable_id!(AgentProviderId);
stable_id!(ModelProviderId);
stable_id!(AccountId);
stable_id!(ModelId);
stable_id!(AgentInstanceId);
stable_id!(ProjectId);
stable_id!(RoleId);
stable_id!(SessionId);
stable_id!(RunId);
stable_id!(CredentialReferenceId);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityLifecycle {
    Active,
    Disabled,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    pub source_kind: String,
    pub source_id: String,
    pub recorded_by: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityMetadata<I> {
    pub id: I,
    pub revision: u64,
    pub created_at: String,
    pub updated_at: String,
    pub provenance: Provenance,
    pub lifecycle: EntityLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialBackend {
    MacosKeychain,
    WindowsCredentialManager,
    LinuxSecretService,
}

/// Metadata that locates a secret in an OS credential store. There is
/// intentionally no password, token, key, or arbitrary payload field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialReference {
    pub metadata: EntityMetadata<CredentialReferenceId>,
    pub backend: CredentialBackend,
    pub service: String,
    pub account_label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentProvider {
    pub metadata: EntityMetadata<AgentProviderId>,
    pub adapter_kind: String,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelProvider {
    pub metadata: EntityMetadata<ModelProviderId>,
    pub provider_key: String,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Account {
    pub metadata: EntityMetadata<AccountId>,
    pub agent_provider_id: AgentProviderId,
    pub provider_identity: String,
    pub credential_reference_id: Option<CredentialReferenceId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Model {
    pub metadata: EntityMetadata<ModelId>,
    pub model_provider_id: ModelProviderId,
    pub provider_model_id: String,
    pub reasoning_controls: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentInstance {
    pub metadata: EntityMetadata<AgentInstanceId>,
    pub agent_provider_id: AgentProviderId,
    pub account_id: Option<AccountId>,
    pub default_model_id: Option<ModelId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project {
    pub metadata: EntityMetadata<ProjectId>,
    pub name: String,
    pub workspace_root: String,
    pub policy: ScopedPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Role {
    pub metadata: EntityMetadata<RoleId>,
    pub name: String,
    pub responsibilities: Vec<String>,
    pub policy: ScopedPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub metadata: EntityMetadata<SessionId>,
    pub agent_instance_id: AgentInstanceId,
    pub project_id: ProjectId,
    pub provider_session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    pub metadata: EntityMetadata<RunId>,
    pub session_id: SessionId,
    pub model_id: ModelId,
    pub state: NormalizedRunState,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata<I>(id: I) -> EntityMetadata<I> {
        EntityMetadata {
            id,
            revision: 0,
            created_at: "2026-09-03T00:00:00Z".into(),
            updated_at: "2026-09-03T00:00:00Z".into(),
            provenance: Provenance {
                source_kind: "user".into(),
                source_id: "fixture".into(),
                recorded_by: "test".into(),
            },
            lifecycle: EntityLifecycle::Active,
        }
    }

    #[test]
    fn typed_ids_do_not_share_values_accidentally() {
        assert_ne!(AgentProviderId::new().value(), AccountId::new().value());
    }

    #[test]
    fn credential_reference_serialization_has_no_secret_field() {
        let reference = CredentialReference {
            metadata: metadata(CredentialReferenceId::new()),
            backend: CredentialBackend::MacosKeychain,
            service: "dev.bastet.workstation.codex".into(),
            account_label: "default".into(),
        };
        let json = serde_json::to_value(reference).unwrap();
        assert!(json.get("secret").is_none());
        assert!(json.get("token").is_none());
        assert!(json.get("password").is_none());
        assert_eq!(json["backend"], "macos_keychain");
    }

    #[test]
    fn stable_ids_round_trip_as_opaque_values() {
        let id = AgentInstanceId::new();
        let encoded = serde_json::to_string(&id).unwrap();
        assert_eq!(
            serde_json::from_str::<AgentInstanceId>(&encoded).unwrap(),
            id
        );
    }
}
