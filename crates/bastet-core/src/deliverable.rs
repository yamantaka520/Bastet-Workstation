use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    ArtifactId, ArtifactVersionId, CostRecordId, EntityMetadata, EvidenceClass, GraphNodeId,
    KnowledgeDeliveryId, ProjectId, RunId,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentArtifact {
    pub metadata: EntityMetadata<ArtifactId>,
    pub project_id: ProjectId,
    pub title: String,
    pub versions: Vec<DocumentVersion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentVersion {
    pub id: ArtifactVersionId,
    pub version: u32,
    pub parent_id: Option<ArtifactVersionId>,
    pub markdown: String,
    pub content_hash: String,
    pub source_node_ids: Vec<GraphNodeId>,
    pub accepted_by: Option<String>,
    pub accepted_at: Option<String>,
}

impl DocumentVersion {
    pub fn create(
        id: ArtifactVersionId,
        version: u32,
        parent_id: Option<ArtifactVersionId>,
        markdown: String,
        source_node_ids: Vec<GraphNodeId>,
    ) -> Result<Self, DeliverableError> {
        if version == 0 || markdown.trim().is_empty() {
            return Err(DeliverableError::InvalidDocumentVersion);
        }
        Ok(Self {
            id,
            version,
            parent_id,
            content_hash: hash(&markdown),
            markdown,
            source_node_ids,
            accepted_by: None,
            accepted_at: None,
        })
    }

    pub fn accept(&mut self, actor: &str, accepted_at: &str) -> Result<(), DeliverableError> {
        self.validate_unchanged()?;
        if self.accepted_by.is_some() || actor.trim().is_empty() || accepted_at.trim().is_empty() {
            return Err(DeliverableError::InvalidAcceptance);
        }
        self.accepted_by = Some(actor.into());
        self.accepted_at = Some(accepted_at.into());
        Ok(())
    }

    pub fn validate_unchanged(&self) -> Result<(), DeliverableError> {
        if self.version == 0
            || self.markdown.trim().is_empty()
            || self.content_hash != hash(&self.markdown)
            || self.accepted_by.is_some() != self.accepted_at.is_some()
        {
            return Err(DeliverableError::InvalidDocumentVersion);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostLedgerRecord {
    pub metadata: EntityMetadata<CostRecordId>,
    pub project_id: ProjectId,
    pub node_id: GraphNodeId,
    pub run_id: RunId,
    pub provider: String,
    pub account: String,
    pub model: String,
    pub currency: Option<String>,
    pub amount: Option<f64>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub evidence_class: EvidenceClass,
    pub source: String,
    pub formula_version: Option<String>,
    pub confidence: f32,
    pub reconciliation_state: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeTarget {
    AgentMemoryOs,
    BastetMind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    Prepared,
    Delivered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeDelivery {
    pub metadata: EntityMetadata<KnowledgeDeliveryId>,
    pub project_id: ProjectId,
    pub artifact_version_id: ArtifactVersionId,
    pub target: KnowledgeTarget,
    pub preview: String,
    pub redacted: bool,
    pub state: DeliveryState,
    pub destination_receipt: Option<String>,
}

impl KnowledgeDelivery {
    pub fn prepare(
        metadata: EntityMetadata<KnowledgeDeliveryId>,
        project_id: ProjectId,
        artifact_version_id: ArtifactVersionId,
        target: KnowledgeTarget,
        preview: String,
    ) -> Result<Self, DeliverableError> {
        if preview.trim().is_empty() {
            return Err(DeliverableError::InvalidKnowledgeDelivery);
        }
        Ok(Self {
            metadata,
            project_id,
            artifact_version_id,
            target,
            preview,
            redacted: true,
            state: DeliveryState::Prepared,
            destination_receipt: None,
        })
    }

    pub fn record_delivered(&mut self, receipt: String) -> Result<(), DeliverableError> {
        if self.state != DeliveryState::Prepared || receipt.trim().is_empty() {
            return Err(DeliverableError::InvalidKnowledgeDelivery);
        }
        self.state = DeliveryState::Delivered;
        self.destination_receipt = Some(receipt);
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DeliverableCatalog {
    pub documents: Vec<DocumentArtifact>,
    pub costs: Vec<CostLedgerRecord>,
    pub knowledge_deliveries: Vec<KnowledgeDelivery>,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeliverableError {
    #[error("document version is invalid or its content changed")]
    InvalidDocumentVersion,
    #[error("document acceptance must be explicit and immutable")]
    InvalidAcceptance,
    #[error("document version chain is invalid")]
    InvalidVersionChain,
    #[error("joined document must cite exactly two distinct research nodes")]
    InvalidJoinReceipt,
    #[error("cost evidence is invalid")]
    InvalidCost,
    #[error("knowledge delivery preview, redaction, or receipt is invalid")]
    InvalidKnowledgeDelivery,
    #[error("knowledge delivery references an unaccepted artifact version")]
    UnacceptedArtifact,
}

impl DeliverableCatalog {
    pub fn validate(&self) -> Result<(), DeliverableError> {
        let mut accepted_versions = HashSet::new();
        for document in &self.documents {
            if document.title.trim().is_empty() || document.versions.is_empty() {
                return Err(DeliverableError::InvalidDocumentVersion);
            }
            let mut previous = None;
            for (index, version) in document.versions.iter().enumerate() {
                version.validate_unchanged()?;
                if version.version != u32::try_from(index + 1).unwrap_or(u32::MAX)
                    || version.parent_id != previous
                {
                    return Err(DeliverableError::InvalidVersionChain);
                }
                if version
                    .source_node_ids
                    .iter()
                    .copied()
                    .collect::<HashSet<_>>()
                    .len()
                    != 2
                {
                    return Err(DeliverableError::InvalidJoinReceipt);
                }
                if version.accepted_by.is_some() {
                    accepted_versions.insert(version.id);
                }
                previous = Some(version.id);
            }
        }
        for cost in &self.costs {
            if cost.provider.trim().is_empty()
                || cost.account.trim().is_empty()
                || cost.model.trim().is_empty()
                || cost.source.trim().is_empty()
                || cost.reconciliation_state.trim().is_empty()
                || !(0.0..=1.0).contains(&cost.confidence)
                || cost
                    .amount
                    .is_some_and(|amount| !amount.is_finite() || amount < 0.0)
                || cost.evidence_class == EvidenceClass::Unknown
                    && (cost.amount.is_some()
                        || cost.input_tokens.is_some()
                        || cost.output_tokens.is_some())
            {
                return Err(DeliverableError::InvalidCost);
            }
        }
        for delivery in &self.knowledge_deliveries {
            if delivery.preview.trim().is_empty()
                || !delivery.redacted
                || match delivery.state {
                    DeliveryState::Prepared => delivery.destination_receipt.is_some(),
                    DeliveryState::Delivered => delivery
                        .destination_receipt
                        .as_deref()
                        .is_none_or(|receipt| receipt.trim().is_empty()),
                }
            {
                return Err(DeliverableError::InvalidKnowledgeDelivery);
            }
            if !accepted_versions.contains(&delivery.artifact_version_id) {
                return Err(DeliverableError::UnacceptedArtifact);
            }
        }
        Ok(())
    }
}

fn hash(content: &str) -> String {
    format!("sha256:{:x}", Sha256::digest(content.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_document_detects_mutation_and_keeps_two_source_join_receipt() {
        let mut version = DocumentVersion::create(
            ArtifactVersionId::from_bytes([71; 16]),
            1,
            None,
            "# Report\n\nEvidence.".into(),
            vec![
                GraphNodeId::from_bytes([72; 16]),
                GraphNodeId::from_bytes([73; 16]),
            ],
        )
        .unwrap();
        version
            .accept("local-user", "2026-09-06T00:00:00Z")
            .unwrap();
        version.validate_unchanged().unwrap();
        version.markdown.push_str(" changed");
        assert_eq!(
            version.validate_unchanged(),
            Err(DeliverableError::InvalidDocumentVersion)
        );
    }

    #[test]
    fn unknown_cost_cannot_carry_invented_usage_or_amount() {
        let json = serde_json::json!({
            "evidence_class": "unknown", "currency": "USD", "amount": 1.0,
            "input_tokens": null, "output_tokens": null, "confidence": 0.0
        });
        let evidence: crate::CostEvidence = serde_json::from_value(json).unwrap();
        assert_eq!(evidence.evidence_class, EvidenceClass::Unknown);
    }

    #[test]
    fn knowledge_delivery_requires_preview_then_external_receipt() {
        let metadata = EntityMetadata {
            id: KnowledgeDeliveryId::from_bytes([74; 16]),
            revision: 0,
            created_at: "now".into(),
            updated_at: "now".into(),
            provenance: crate::Provenance {
                source_kind: "local_user".into(),
                source_id: "delivery".into(),
                recorded_by: "test".into(),
            },
            lifecycle: crate::EntityLifecycle::Active,
        };
        let mut delivery = KnowledgeDelivery::prepare(
            metadata,
            ProjectId::from_bytes([75; 16]),
            ArtifactVersionId::from_bytes([76; 16]),
            KnowledgeTarget::AgentMemoryOs,
            "Redacted decision summary".into(),
        )
        .unwrap();
        assert_eq!(delivery.state, DeliveryState::Prepared);
        assert!(delivery.record_delivered("".into()).is_err());
        delivery
            .record_delivered("memory:receipt-1".into())
            .unwrap();
        assert_eq!(delivery.state, DeliveryState::Delivered);
    }
}
