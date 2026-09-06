use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    DeliverableCatalog, DeliverableError, GraphError, GraphExecution, IdentityCatalog,
    MeetingCatalog, MeetingError, OfficeCatalog, OfficeError,
};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct M3State {
    pub office: OfficeCatalog,
    pub meetings: MeetingCatalog,
    pub graph_executions: Vec<GraphExecution>,
    pub deliverables: DeliverableCatalog,
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
        self.office.validate(identity)?;
        self.meetings.validate(identity, &self.office)?;
        self.deliverables.validate()?;

        let graph_ids = self
            .graph_executions
            .iter()
            .map(|execution| execution.id)
            .collect::<HashSet<_>>();
        if graph_ids.len() != self.graph_executions.len() {
            return Err(M3Error::DuplicateGraphExecution);
        }
        let baselines = self
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
        let artifact_versions = self
            .deliverables
            .documents
            .iter()
            .flat_map(|document| document.versions.iter().map(|version| version.id))
            .collect::<HashSet<_>>();
        if self.deliverables.documents.iter().any(|document| {
            !project_ids.contains(&document.project_id)
                || document
                    .versions
                    .iter()
                    .flat_map(|version| &version.source_node_ids)
                    .any(|node| !graph_nodes.contains(node))
        }) || self.deliverables.costs.iter().any(|cost| {
            !project_ids.contains(&cost.project_id)
                || !run_ids.contains(&cost.run_id)
                || !graph_nodes.contains(&cost.node_id)
        }) || self
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
