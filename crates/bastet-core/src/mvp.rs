use std::path::Path;

use thiserror::Error;

use crate::*;

#[derive(Debug, Clone, PartialEq)]
pub struct MvpDraft {
    pub identity: IdentityCatalog,
    pub m3: M3Catalog,
    pub project_id: ProjectId,
    pub meeting_id: MeetingId,
    pub research_role_ids: [RoleId; 2],
    pub integrator_role_id: RoleId,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MvpError {
    #[error("MVP project name and absolute workspace directory are required")]
    InvalidProject,
    #[error("MVP draft is invalid")]
    InvalidDraft,
    #[error("MVP meeting is not awaiting a decision")]
    NotAwaitingDecision,
}

impl MvpDraft {
    pub fn prepare(
        project_name: &str,
        workspace_root: &Path,
        codex_model: &str,
        agy_model: &str,
    ) -> Result<Self, MvpError> {
        if project_name.trim().is_empty()
            || !workspace_root.is_absolute()
            || !workspace_root.is_dir()
            || codex_model.trim().is_empty()
            || agy_model.trim().is_empty()
        {
            return Err(MvpError::InvalidProject);
        }
        let project_id = ProjectId::new();
        let meeting_id = MeetingId::new();
        let room_id = RoomId::new();
        let research_role_ids = [RoleId::new(), RoleId::new()];
        let integrator_role_id = RoleId::new();
        let codex_provider_id = AgentProviderId::new();
        let agy_provider_id = AgentProviderId::new();
        let openai_provider_id = ModelProviderId::new();
        let agy_model_provider_id = ModelProviderId::new();
        let codex_model_id = ModelId::new();
        let agy_model_id = ModelId::new();
        let codex_agent_id = AgentInstanceId::new();
        let agy_agent_id = AgentInstanceId::new();
        let pet_profile = builtin_pet_profile();
        let project_policy = policy(PolicyLayer::Project, PermissionLevel::Use);
        let role_policy = policy(PolicyLayer::RoleOrAgent, PermissionLevel::Use);

        let identity = IdentityCatalog {
            agent_providers: vec![
                AgentProvider {
                    metadata: metadata(codex_provider_id),
                    adapter_kind: "codex_cli".into(),
                    display_name: "Codex CLI".into(),
                },
                AgentProvider {
                    metadata: metadata(agy_provider_id),
                    adapter_kind: "agy_cli".into(),
                    display_name: "Agy CLI".into(),
                },
            ],
            model_providers: vec![
                ModelProvider {
                    metadata: metadata(openai_provider_id),
                    provider_key: "openai".into(),
                    display_name: "OpenAI".into(),
                },
                ModelProvider {
                    metadata: metadata(agy_model_provider_id),
                    provider_key: "agy".into(),
                    display_name: "Agy".into(),
                },
            ],
            models: vec![
                Model {
                    metadata: metadata(codex_model_id),
                    model_provider_id: openai_provider_id,
                    provider_model_id: codex_model.trim().into(),
                    reasoning_controls: vec!["low".into(), "medium".into(), "high".into()],
                },
                Model {
                    metadata: metadata(agy_model_id),
                    model_provider_id: agy_model_provider_id,
                    provider_model_id: agy_model.trim().into(),
                    reasoning_controls: vec!["low".into(), "medium".into(), "high".into()],
                },
            ],
            agent_instances: vec![
                AgentInstance {
                    metadata: metadata(codex_agent_id),
                    agent_provider_id: codex_provider_id,
                    account_id: None,
                    default_model_id: Some(codex_model_id),
                },
                AgentInstance {
                    metadata: metadata(agy_agent_id),
                    agent_provider_id: agy_provider_id,
                    account_id: None,
                    default_model_id: Some(agy_model_id),
                },
            ],
            projects: vec![Project {
                metadata: metadata(project_id),
                name: project_name.trim().into(),
                workspace_root: workspace_root.to_string_lossy().into_owned(),
                policy: project_policy,
            }],
            roles: vec![
                Role {
                    metadata: metadata(research_role_ids[0]),
                    name: "Research A".into(),
                    responsibilities: vec!["independent research branch A".into()],
                    policy: role_policy.clone(),
                },
                Role {
                    metadata: metadata(research_role_ids[1]),
                    name: "Research B".into(),
                    responsibilities: vec!["independent research branch B".into()],
                    policy: role_policy.clone(),
                },
                Role {
                    metadata: metadata(integrator_role_id),
                    name: "Document Integrator".into(),
                    responsibilities: vec!["join evidence into a versioned document".into()],
                    policy: role_policy,
                },
            ],
            ..IdentityCatalog::default()
        };
        let office = OfficeCatalog {
            pet_profiles: vec![pet_profile.clone()],
            pet_assignments: vec![
                assignment(
                    project_id,
                    research_role_ids[0],
                    codex_agent_id,
                    pet_profile.metadata.id,
                ),
                assignment(
                    project_id,
                    research_role_ids[1],
                    agy_agent_id,
                    pet_profile.metadata.id,
                ),
                assignment(
                    project_id,
                    integrator_role_id,
                    codex_agent_id,
                    pet_profile.metadata.id,
                ),
            ],
            rooms: vec![Room {
                metadata: metadata(room_id),
                project_id,
                name: "Project Room".into(),
                capacity: 12,
            }],
        };
        let m3 = M3Catalog {
            office,
            meetings: MeetingCatalog {
                meetings: vec![ProjectMeeting {
                    metadata: metadata(meeting_id),
                    project_id,
                    room_id,
                    participant_role_ids: vec![research_role_ids[0], research_role_ids[1], integrator_role_id],
                    max_rounds: 3,
                    rounds: vec![MeetingRound { index: 1, summary: "Two independent research branches followed by one explicit document join.".into() }],
                    state: MeetingState::AwaitingDecision,
                }],
                decision_baselines: vec![],
            },
            deliverables: DeliverableCatalog::default(),
        };
        let draft = Self {
            identity,
            m3,
            project_id,
            meeting_id,
            research_role_ids,
            integrator_role_id,
        };
        draft.validate()?;
        Ok(draft)
    }

    pub fn accept_decision(
        &mut self,
        content: String,
        actor: &str,
        accepted_at: &str,
    ) -> Result<GraphExecution, MvpError> {
        self.validate()?;
        accept_mvp_decision(
            &mut self.m3,
            &self.identity,
            self.meeting_id,
            content,
            actor,
            accepted_at,
        )
    }

    pub fn validate(&self) -> Result<(), MvpError> {
        self.identity
            .validate()
            .map_err(|_| MvpError::InvalidDraft)?;
        self.m3
            .office
            .validate(&self.identity)
            .map_err(|_| MvpError::InvalidDraft)?;
        self.m3
            .meetings
            .validate(&self.identity, &self.m3.office)
            .map_err(|_| MvpError::InvalidDraft)?;
        Ok(())
    }
}

pub fn accept_mvp_decision(
    m3: &mut M3Catalog,
    identity: &IdentityCatalog,
    meeting_id: MeetingId,
    content: String,
    actor: &str,
    accepted_at: &str,
) -> Result<GraphExecution, MvpError> {
    let meeting = m3
        .meetings
        .meetings
        .iter_mut()
        .find(|meeting| meeting.metadata.id == meeting_id)
        .ok_or(MvpError::InvalidDraft)?;
    if meeting.state != MeetingState::AwaitingDecision {
        return Err(MvpError::NotAwaitingDecision);
    }
    let [left_role, right_role, integrator_role] = meeting.participant_role_ids.as_slice() else {
        return Err(MvpError::InvalidDraft);
    };
    if [left_role, right_role, integrator_role]
        .into_iter()
        .any(|role_id| {
            !identity
                .roles
                .iter()
                .any(|role| role.metadata.id == *role_id)
        })
    {
        return Err(MvpError::InvalidDraft);
    }
    let baseline = DecisionBaseline::create(
        metadata(DecisionBaselineId::new()),
        meeting_id,
        content,
        actor.into(),
        accepted_at.into(),
    )
    .map_err(|_| MvpError::InvalidDraft)?;
    let baseline_id = baseline.metadata.id;
    meeting.state = MeetingState::Accepted;
    m3.meetings.decision_baselines.push(baseline);
    let left = GraphNodeId::new();
    let right = GraphNodeId::new();
    GraphExecution::start(
        GraphRunId::new(),
        WorkflowGraph {
            decision_baseline_id: baseline_id,
            nodes: vec![
                GraphNode {
                    id: left,
                    kind: GraphNodeKind::Research,
                    role_id: *left_role,
                    title: "Independent research A".into(),
                    needs: vec![],
                },
                GraphNode {
                    id: right,
                    kind: GraphNodeKind::Research,
                    role_id: *right_role,
                    title: "Independent research B".into(),
                    needs: vec![],
                },
                GraphNode {
                    id: GraphNodeId::new(),
                    kind: GraphNodeKind::Join,
                    role_id: *integrator_role,
                    title: "Join research into document".into(),
                    needs: vec![left, right],
                },
            ],
        },
    )
    .map_err(|_| MvpError::InvalidDraft)
}

pub fn builtin_pet_profile() -> PetProfile {
    let mut pet_metadata = metadata(PetProfileId::from_bytes([0xBA; 16]));
    pet_metadata.provenance = Provenance {
        source_kind: "first_party".into(),
        source_id: "bastet-cat-v1".into(),
        recorded_by: "bastet-workstation".into(),
    };
    PetProfile {
        metadata: pet_metadata,
        name: "Bastet Cat".into(),
        version: 1,
        states: REQUIRED_PET_STATES
            .into_iter()
            .map(|state| PetStateAsset {
                state_key: state.into(),
                asset_ref: format!("builtin://bastet-cat/{state}"),
                accessible_label_key: format!("pet.state.{state}"),
            })
            .collect(),
    }
}

fn assignment(
    project_id: ProjectId,
    role_id: RoleId,
    agent_instance_id: AgentInstanceId,
    pet_profile_id: PetProfileId,
) -> PetAssignment {
    PetAssignment {
        metadata: metadata(PetAssignmentId::new()),
        project_id,
        role_id,
        agent_instance_id,
        pet_profile_id,
    }
}

fn policy(layer: PolicyLayer, level: PermissionLevel) -> ScopedPolicy {
    ScopedPolicy {
        layer,
        ceiling: PolicyCeiling {
            filesystem: level,
            network: level,
            process: level,
            device: PermissionLevel::Deny,
            credential: PermissionLevel::Deny,
            persistent_approval: false,
        },
    }
}

fn metadata<I>(id: I) -> EntityMetadata<I> {
    EntityMetadata {
        id,
        revision: 0,
        created_at: "locally-created".into(),
        updated_at: "locally-created".into(),
        provenance: Provenance {
            source_kind: "local_user".into(),
            source_id: "mvp_vertical_slice".into(),
            recorded_by: "bastet-workstation".into(),
        },
        lifecycle: EntityLifecycle::Active,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mvp_draft_requires_human_decision_before_graph_exists() {
        let root = tempfile::tempdir().unwrap();
        let mut draft = MvpDraft::prepare("MVP", root.path(), "gpt-test", "agy-test").unwrap();
        assert!(draft.m3.meetings.decision_baselines.is_empty());
        let execution = draft
            .accept_decision(
                "Research both perspectives and produce one sourced report.".into(),
                "local-user",
                "2026-09-07T00:00:00Z",
            )
            .unwrap();
        assert_eq!(
            execution
                .graph
                .nodes
                .iter()
                .filter(|node| node.kind == GraphNodeKind::Research)
                .count(),
            2
        );
        assert_eq!(
            execution
                .graph
                .nodes
                .iter()
                .filter(|node| node.kind == GraphNodeKind::Join)
                .count(),
            1
        );
        M3State {
            catalog: draft.m3,
            graph_executions: vec![execution],
        }
        .validate(&draft.identity)
        .unwrap();
    }
}
