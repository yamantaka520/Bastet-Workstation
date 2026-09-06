use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    DeliverableCatalog, DeliverableError, GraphError, GraphExecution, IdentityCatalog,
    MeetingCatalog, MeetingError, OfficeCatalog, OfficeError,
};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct M3Catalog {
    pub office: OfficeCatalog,
    pub meetings: MeetingCatalog,
    pub deliverables: DeliverableCatalog,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct M3State {
    pub catalog: M3Catalog,
    pub graph_executions: Vec<GraphExecution>,
}

#[derive(Debug, Error, PartialEq)]
pub enum M3Error {
    #[error(transparent)]
    Office(#[from] OfficeError),
    #[error(transparent)]
    Meeting(#[from] MeetingError),
    #[error(transparent)]
    Graph(#[from] GraphError),
    #[error(transparent)]
    Deliverable(#[from] DeliverableError),
    #[error("graph execution references an unaccepted DecisionBaseline")]
    MissingDecisionBaseline,
    #[error("graph node references a missing Role")]
    MissingRole,
    #[error("deliverable references a missing Project, Run, graph node, or artifact")]
    MissingDeliverableReference,
    #[error("M3 state contains duplicate graph execution ids")]
    DuplicateGraphExecution,
}

impl M3State {
    pub fn validate(&self, identity: &IdentityCatalog) -> Result<(), M3Error> {
        identity
            .validate()
            .map_err(|_| M3Error::MissingDeliverableReference)?;
        self.catalog.office.validate(identity)?;
        self.catalog
            .meetings
            .validate(identity, &self.catalog.office)?;
        self.catalog.deliverables.validate()?;

        let graph_ids = self
            .graph_executions
            .iter()
            .map(|execution| execution.id)
            .collect::<HashSet<_>>();
        if graph_ids.len() != self.graph_executions.len() {
            return Err(M3Error::DuplicateGraphExecution);
        }
        let baselines = self
            .catalog
            .meetings
            .decision_baselines
            .iter()
            .map(|baseline| baseline.metadata.id)
            .collect::<HashSet<_>>();
        let mut graph_nodes = HashSet::new();
        for execution in &self.graph_executions {
            execution.validate()?;
            if !baselines.contains(&execution.graph.decision_baseline_id) {
                return Err(M3Error::MissingDecisionBaseline);
            }
            for node in &execution.graph.nodes {
                if !identity
                    .roles
                    .iter()
                    .any(|role| role.metadata.id == node.role_id)
                {
                    return Err(M3Error::MissingRole);
                }
                graph_nodes.insert(node.id);
            }
        }

        let project_ids = identity
            .projects
            .iter()
            .map(|project| project.metadata.id)
            .collect::<HashSet<_>>();
        let run_ids = identity
            .runs
            .iter()
            .map(|run| run.metadata.id)
            .collect::<HashSet<_>>();
        if self
            .graph_executions
            .iter()
            .flat_map(|execution| &execution.outputs)
            .any(|output| !run_ids.contains(&output.run_id))
        {
            return Err(M3Error::MissingDeliverableReference);
        }
        let artifact_versions = self
            .catalog
            .deliverables
            .documents
            .iter()
            .flat_map(|document| document.versions.iter().map(|version| version.id))
            .collect::<HashSet<_>>();
        let has_invalid_source_receipt = self
            .catalog
            .deliverables
            .documents
            .iter()
            .flat_map(|document| &document.versions)
            .any(|version| {
                let (Some(execution_id), Some(join_node_id), Some(join_hash)) = (
                    version.source_execution_id,
                    version.source_join_node_id,
                    version.source_join_output_hash.as_deref(),
                ) else {
                    return version.source_execution_id.is_some()
                        || version.source_join_node_id.is_some()
                        || version.source_join_output_hash.is_some();
                };
                let Some(execution) = self
                    .graph_executions
                    .iter()
                    .find(|execution| execution.id == execution_id)
                else {
                    return true;
                };
                let Some(join_node) = execution.graph.nodes.iter().find(|node| {
                    node.id == join_node_id && node.kind == crate::GraphNodeKind::Join
                }) else {
                    return true;
                };
                let source_ids = version
                    .source_node_ids
                    .iter()
                    .copied()
                    .collect::<HashSet<_>>();
                source_ids != join_node.needs.iter().copied().collect::<HashSet<_>>()
                    || join_node.needs.iter().any(|node_id| {
                        execution.output(*node_id).is_none()
                            || execution
                                .graph
                                .nodes
                                .iter()
                                .find(|node| node.id == *node_id)
                                .is_none_or(|node| node.kind != crate::GraphNodeKind::Research)
                    })
                    || execution
                        .output(join_node_id)
                        .is_none_or(|output| output.content_hash != join_hash)
            });
        if self.catalog.deliverables.documents.iter().any(|document| {
            !project_ids.contains(&document.project_id)
                || document
                    .versions
                    .iter()
                    .flat_map(|version| &version.source_node_ids)
                    .any(|node| !graph_nodes.contains(node))
        }) || has_invalid_source_receipt
            || self.catalog.deliverables.costs.iter().any(|cost| {
                !project_ids.contains(&cost.project_id)
                    || !run_ids.contains(&cost.run_id)
                    || !graph_nodes.contains(&cost.node_id)
            })
            || self
                .catalog
                .deliverables
                .knowledge_deliveries
                .iter()
                .any(|delivery| {
                    !project_ids.contains(&delivery.project_id)
                        || !artifact_versions.contains(&delivery.artifact_version_id)
                })
        {
            return Err(M3Error::MissingDeliverableReference);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_m3_state_is_a_valid_pre_configuration_baseline() {
        M3State::default()
            .validate(&IdentityCatalog::default())
            .unwrap();
    }
}
