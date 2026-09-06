use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    AgentInstanceId, EntityMetadata, PetAssignmentId, PetProfileId, ProjectId, RoleId, RoomId,
};

pub const REQUIRED_PET_STATES: [&str; 8] = [
    "idle",
    "thinking",
    "working",
    "waiting",
    "blocked",
    "approval_required",
    "succeeded",
    "failed",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PetStateAsset {
    pub state_key: String,
    pub asset_ref: String,
    pub accessible_label_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PetProfile {
    pub metadata: EntityMetadata<PetProfileId>,
    pub name: String,
    pub version: u32,
    pub states: Vec<PetStateAsset>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PetAssignment {
    pub metadata: EntityMetadata<PetAssignmentId>,
    pub project_id: ProjectId,
    pub role_id: RoleId,
    pub agent_instance_id: AgentInstanceId,
    pub pet_profile_id: PetProfileId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Room {
    pub metadata: EntityMetadata<RoomId>,
    pub project_id: ProjectId,
    pub name: String,
    pub capacity: u16,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfficeCatalog {
    pub pet_profiles: Vec<PetProfile>,
    pub pet_assignments: Vec<PetAssignment>,
    pub rooms: Vec<Room>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OfficeError {
    #[error("duplicate {entity} id {id}")]
    DuplicateId { entity: &'static str, id: Uuid },
    #[error("{entity} has an empty required field: {field}")]
    EmptyField {
        entity: &'static str,
        field: &'static str,
    },
    #[error("PetProfile version must be positive")]
    InvalidVersion,
    #[error("Room capacity must be positive")]
    InvalidCapacity,
    #[error("PetProfile is missing required state {0}")]
    MissingPetState(&'static str),
    #[error("PetProfile contains duplicate state {0}")]
    DuplicatePetState(String),
    #[error("PetAssignment references a missing PetProfile")]
    MissingPetProfile,
    #[error("PetAssignment references a missing Project, Role, or AgentInstance")]
    MissingIdentityReference,
    #[error("Room references a missing Project")]
    MissingProject,
    #[error("a project role can have only one active PetAssignment")]
    DuplicateRoleAssignment,
}

impl OfficeCatalog {
    pub fn validate(&self, identity: &crate::IdentityCatalog) -> Result<(), OfficeError> {
        unique(
            "PetProfile",
            self.pet_profiles
                .iter()
                .map(|item| item.metadata.id.value()),
        )?;
        unique(
            "PetAssignment",
            self.pet_assignments
                .iter()
                .map(|item| item.metadata.id.value()),
        )?;
        unique(
            "Room",
            self.rooms.iter().map(|item| item.metadata.id.value()),
        )?;

        for profile in &self.pet_profiles {
            nonempty("PetProfile", "name", &profile.name)?;
            if profile.version == 0 {
                return Err(OfficeError::InvalidVersion);
            }
            let mut states = HashSet::new();
            for state in &profile.states {
                nonempty("PetStateAsset", "state_key", &state.state_key)?;
                nonempty("PetStateAsset", "asset_ref", &state.asset_ref)?;
                nonempty(
                    "PetStateAsset",
                    "accessible_label_key",
                    &state.accessible_label_key,
                )?;
                if !states.insert(state.state_key.as_str()) {
                    return Err(OfficeError::DuplicatePetState(state.state_key.clone()));
                }
            }
            for required in REQUIRED_PET_STATES {
                if !states.contains(required) {
                    return Err(OfficeError::MissingPetState(required));
                }
            }
        }

        let mut assigned_roles = HashSet::new();
        for assignment in &self.pet_assignments {
            if !self
                .pet_profiles
                .iter()
                .any(|profile| profile.metadata.id == assignment.pet_profile_id)
            {
                return Err(OfficeError::MissingPetProfile);
            }
            if !identity
                .projects
                .iter()
                .any(|project| project.metadata.id == assignment.project_id)
                || !identity
                    .roles
                    .iter()
                    .any(|role| role.metadata.id == assignment.role_id)
                || !identity
                    .agent_instances
                    .iter()
                    .any(|agent| agent.metadata.id == assignment.agent_instance_id)
            {
                return Err(OfficeError::MissingIdentityReference);
            }
            if !assigned_roles.insert((assignment.project_id, assignment.role_id)) {
                return Err(OfficeError::DuplicateRoleAssignment);
            }
        }
        for room in &self.rooms {
            nonempty("Room", "name", &room.name)?;
            if room.capacity == 0 {
                return Err(OfficeError::InvalidCapacity);
            }
            if !identity
                .projects
                .iter()
                .any(|project| project.metadata.id == room.project_id)
            {
                return Err(OfficeError::MissingProject);
            }
        }
        Ok(())
    }
}

fn unique(entity: &'static str, ids: impl IntoIterator<Item = Uuid>) -> Result<(), OfficeError> {
    let mut seen = HashSet::new();
    for id in ids {
        if !seen.insert(id) {
            return Err(OfficeError::DuplicateId { entity, id });
        }
    }
    Ok(())
}

fn nonempty(entity: &'static str, field: &'static str, value: &str) -> Result<(), OfficeError> {
    if value.trim().is_empty() {
        Err(OfficeError::EmptyField { entity, field })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EntityLifecycle, Provenance};

    fn metadata(id: PetProfileId) -> EntityMetadata<PetProfileId> {
        EntityMetadata {
            id,
            revision: 0,
            created_at: "2026-09-06T00:00:00Z".into(),
            updated_at: "2026-09-06T00:00:00Z".into(),
            provenance: Provenance {
                source_kind: "first_party".into(),
                source_id: "builtin-cat-v1".into(),
                recorded_by: "bastet-core".into(),
            },
            lifecycle: EntityLifecycle::Active,
        }
    }

    fn profile() -> PetProfile {
        PetProfile {
            metadata: metadata(PetProfileId::from_bytes([31; 16])),
            name: "Bastet Cat".into(),
            version: 1,
            states: REQUIRED_PET_STATES
                .into_iter()
                .map(|state| PetStateAsset {
                    state_key: state.into(),
                    asset_ref: format!("builtin://bastet-cat/{state}.svg"),
                    accessible_label_key: format!("pet.state.{state}"),
                })
                .collect(),
        }
    }

    #[test]
    fn complete_first_party_pet_profile_is_valid_without_identity_bindings() {
        let office = OfficeCatalog {
            pet_profiles: vec![profile()],
            ..OfficeCatalog::default()
        };
        office.validate(&crate::IdentityCatalog::default()).unwrap();
    }

    #[test]
    fn missing_or_duplicate_pet_states_fail_closed() {
        let mut missing = profile();
        missing.states.pop();
        assert_eq!(
            OfficeCatalog {
                pet_profiles: vec![missing],
                ..OfficeCatalog::default()
            }
            .validate(&crate::IdentityCatalog::default()),
            Err(OfficeError::MissingPetState("failed"))
        );

        let mut duplicate = profile();
        duplicate.states.push(duplicate.states[0].clone());
        assert!(matches!(
            OfficeCatalog {
                pet_profiles: vec![duplicate],
                ..OfficeCatalog::default()
            }
            .validate(&crate::IdentityCatalog::default()),
            Err(OfficeError::DuplicatePetState(_))
        ));
    }
}
