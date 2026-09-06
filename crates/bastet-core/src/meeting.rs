use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    DecisionBaselineId, EntityMetadata, IdentityCatalog, MeetingId, OfficeCatalog, ProjectId,
    RoleId, RoomId,
};

pub const MAX_MEETING_ROUNDS: u8 = 5;
pub const MAX_MEETING_PARTICIPANTS: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeetingState {
    Draft,
    InProgress,
    AwaitingDecision,
    Accepted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeetingRound {
    pub index: u8,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectMeeting {
    pub metadata: EntityMetadata<MeetingId>,
    pub project_id: ProjectId,
    pub room_id: RoomId,
    pub participant_role_ids: Vec<RoleId>,
    pub max_rounds: u8,
    pub rounds: Vec<MeetingRound>,
    pub state: MeetingState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionBaseline {
    pub metadata: EntityMetadata<DecisionBaselineId>,
    pub meeting_id: MeetingId,
    pub content: String,
    pub content_hash: String,
    pub accepted_by: String,
    pub accepted_at: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeetingCatalog {
    pub meetings: Vec<ProjectMeeting>,
    pub decision_baselines: Vec<DecisionBaseline>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MeetingError {
    #[error("meeting references a missing or mismatched project room")]
    InvalidRoom,
    #[error("meeting has invalid participants")]
    InvalidParticipants,
    #[error("meeting round limit must be between one and five")]
    InvalidRoundLimit,
    #[error("meeting rounds are out of order, empty, or exceed the limit")]
    InvalidRounds,
    #[error("DecisionBaseline references a missing meeting")]
    MissingMeeting,
    #[error("DecisionBaseline requires a meeting awaiting a human decision")]
    MeetingNotAwaitingDecision,
    #[error("DecisionBaseline requires content and explicit human acceptance")]
    MissingAcceptance,
    #[error("DecisionBaseline content hash does not match immutable content")]
    HashMismatch,
    #[error("meeting may have only one accepted DecisionBaseline")]
    DuplicateBaseline,
}

impl DecisionBaseline {
    pub fn create(
        metadata: EntityMetadata<DecisionBaselineId>,
        meeting_id: MeetingId,
        content: String,
        accepted_by: String,
        accepted_at: String,
    ) -> Result<Self, MeetingError> {
        if content.trim().is_empty()
            || accepted_by.trim().is_empty()
            || accepted_at.trim().is_empty()
        {
            return Err(MeetingError::MissingAcceptance);
        }
        let content_hash = hash_content(&content);
        Ok(Self {
            metadata,
            meeting_id,
            content,
            content_hash,
            accepted_by,
            accepted_at,
        })
    }

    pub fn validate_unchanged(&self) -> Result<(), MeetingError> {
        if self.content.trim().is_empty()
            || self.accepted_by.trim().is_empty()
            || self.accepted_at.trim().is_empty()
        {
            return Err(MeetingError::MissingAcceptance);
        }
        if self.content_hash != hash_content(&self.content) {
            return Err(MeetingError::HashMismatch);
        }
        Ok(())
    }
}

impl MeetingCatalog {
    pub fn validate(
        &self,
        identity: &IdentityCatalog,
        office: &OfficeCatalog,
    ) -> Result<(), MeetingError> {
        for meeting in &self.meetings {
            let room_matches = office.rooms.iter().any(|room| {
                room.metadata.id == meeting.room_id && room.project_id == meeting.project_id
            });
            if !room_matches {
                return Err(MeetingError::InvalidRoom);
            }
            if meeting.participant_role_ids.is_empty()
                || meeting.participant_role_ids.len() > MAX_MEETING_PARTICIPANTS
                || meeting
                    .participant_role_ids
                    .iter()
                    .collect::<HashSet<_>>()
                    .len()
                    != meeting.participant_role_ids.len()
                || meeting.participant_role_ids.iter().any(|role_id| {
                    !identity
                        .roles
                        .iter()
                        .any(|role| role.metadata.id == *role_id)
                })
            {
                return Err(MeetingError::InvalidParticipants);
            }
            if meeting.max_rounds == 0 || meeting.max_rounds > MAX_MEETING_ROUNDS {
                return Err(MeetingError::InvalidRoundLimit);
            }
            if meeting.rounds.len() > usize::from(meeting.max_rounds)
                || meeting.rounds.iter().enumerate().any(|(index, round)| {
                    round.index != u8::try_from(index + 1).unwrap_or(u8::MAX)
                        || round.summary.trim().is_empty()
                })
            {
                return Err(MeetingError::InvalidRounds);
            }
        }

        let mut baseline_meetings = HashSet::new();
        for baseline in &self.decision_baselines {
            baseline.validate_unchanged()?;
            let meeting = self
                .meetings
                .iter()
                .find(|meeting| meeting.metadata.id == baseline.meeting_id)
                .ok_or(MeetingError::MissingMeeting)?;
            if meeting.state != MeetingState::AwaitingDecision {
                return Err(MeetingError::MeetingNotAwaitingDecision);
            }
            if !baseline_meetings.insert(baseline.meeting_id) {
                return Err(MeetingError::DuplicateBaseline);
            }
        }
        Ok(())
    }
}

fn hash_content(content: &str) -> String {
    format!("sha256:{:x}", Sha256::digest(content.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EntityLifecycle, Provenance};

    fn metadata<I>(id: I) -> EntityMetadata<I> {
        EntityMetadata {
            id,
            revision: 0,
            created_at: "2026-09-06T00:00:00Z".into(),
            updated_at: "2026-09-06T00:00:00Z".into(),
            provenance: Provenance {
                source_kind: "test_fixture".into(),
                source_id: "m3.2".into(),
                recorded_by: "bastet-core".into(),
            },
            lifecycle: EntityLifecycle::Active,
        }
    }

    #[test]
    fn decision_baseline_hash_detects_any_post_acceptance_change() {
        let mut baseline = DecisionBaseline::create(
            metadata(DecisionBaselineId::from_bytes([41; 16])),
            MeetingId::from_bytes([42; 16]),
            "Research A and B, then join into report v1.".into(),
            "local-user".into(),
            "2026-09-06T00:01:00Z".into(),
        )
        .unwrap();
        baseline.validate_unchanged().unwrap();
        baseline.content.push_str(" changed");
        assert_eq!(
            baseline.validate_unchanged(),
            Err(MeetingError::HashMismatch)
        );
    }

    #[test]
    fn decision_baseline_requires_explicit_human_acceptance_fields() {
        assert_eq!(
            DecisionBaseline::create(
                metadata(DecisionBaselineId::from_bytes([43; 16])),
                MeetingId::from_bytes([44; 16]),
                "decision".into(),
                "".into(),
                "2026-09-06T00:01:00Z".into(),
            ),
            Err(MeetingError::MissingAcceptance)
        );
    }
}
