use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{DecisionBaselineId, GraphNodeId, GraphRunId, RoleId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphNodeKind {
    Research,
    Join,
    Document,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphNode {
    pub id: GraphNodeId,
    pub kind: GraphNodeKind,
    pub role_id: RoleId,
    pub title: String,
    pub needs: Vec<GraphNodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowGraph {
    pub decision_baseline_id: DecisionBaselineId,
    pub nodes: Vec<GraphNode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraphNodeState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Blocked,
    Uncertain,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphNodeExecution {
    pub node_id: GraphNodeId,
    pub state: GraphNodeState,
    pub owner: Option<String>,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphExecution {
    pub id: GraphRunId,
    pub graph: WorkflowGraph,
    pub nodes: Vec<GraphNodeExecution>,
    pub revision: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GraphError {
    #[error("graph contains duplicate node ids")]
    DuplicateNode,
    #[error("graph node has an empty title")]
    EmptyTitle,
    #[error("graph references an unknown or self dependency")]
    InvalidDependency,
    #[error("graph contains a dependency cycle")]
    Cycle,
    #[error(
        "research vertical slice requires exactly two research branches and one explicit join"
    )]
    InvalidResearchJoin,
    #[error("graph execution owner or state is invalid")]
    InvalidExecution,
}

impl WorkflowGraph {
    pub fn validate(&self) -> Result<(), GraphError> {
        let ids = self
            .nodes
            .iter()
            .map(|node| node.id)
            .collect::<HashSet<_>>();
        if ids.len() != self.nodes.len() {
            return Err(GraphError::DuplicateNode);
        }
        for node in &self.nodes {
            if node.title.trim().is_empty() {
                return Err(GraphError::EmptyTitle);
            }
            if node
                .needs
                .iter()
                .any(|dependency| *dependency == node.id || !ids.contains(dependency))
            {
                return Err(GraphError::InvalidDependency);
            }
        }
        let mut visiting = HashSet::new();
        let mut visited = HashSet::new();
        for node in &self.nodes {
            visit(node.id, self, &mut visiting, &mut visited)?;
        }
        self.validate_research_join()
    }

    fn validate_research_join(&self) -> Result<(), GraphError> {
        let research = self
            .nodes
            .iter()
            .filter(|node| node.kind == GraphNodeKind::Research)
            .collect::<Vec<_>>();
        let joins = self
            .nodes
            .iter()
            .filter(|node| node.kind == GraphNodeKind::Join)
            .collect::<Vec<_>>();
        if research.len() != 2 || joins.len() != 1 {
            return Err(GraphError::InvalidResearchJoin);
        }
        let research_ids = research.iter().map(|node| node.id).collect::<HashSet<_>>();
        let join_needs = joins[0].needs.iter().copied().collect::<HashSet<_>>();
        if join_needs != research_ids || research.iter().any(|node| !node.needs.is_empty()) {
            return Err(GraphError::InvalidResearchJoin);
        }
        Ok(())
    }
}

impl GraphExecution {
    pub fn start(id: GraphRunId, graph: WorkflowGraph) -> Result<Self, GraphError> {
        graph.validate()?;
        let nodes = graph
            .nodes
            .iter()
            .map(|node| GraphNodeExecution {
                node_id: node.id,
                state: GraphNodeState::Pending,
                owner: None,
                revision: 0,
            })
            .collect();
        Ok(Self {
            id,
            graph,
            nodes,
            revision: 0,
        })
    }

    pub fn validate(&self) -> Result<(), GraphError> {
        self.graph.validate()?;
        if self.nodes.len() != self.graph.nodes.len()
            || self
                .nodes
                .iter()
                .map(|node| node.node_id)
                .collect::<HashSet<_>>()
                .len()
                != self.nodes.len()
            || self.nodes.iter().any(|execution| {
                !self
                    .graph
                    .nodes
                    .iter()
                    .any(|node| node.id == execution.node_id)
                    || match execution.state {
                        GraphNodeState::Running
                        | GraphNodeState::Succeeded
                        | GraphNodeState::Failed => execution.owner.is_none(),
                        GraphNodeState::Pending
                        | GraphNodeState::Blocked
                        | GraphNodeState::Uncertain => execution.owner.is_some(),
                    }
            })
        {
            return Err(GraphError::InvalidExecution);
        }
        Ok(())
    }

    pub fn reconcile_after_restart(&mut self) -> usize {
        let mut changed = 0;
        for node in &mut self.nodes {
            if node.state == GraphNodeState::Running {
                node.state = GraphNodeState::Uncertain;
                node.owner = None;
                node.revision += 1;
                changed += 1;
            }
        }
        if changed > 0 {
            self.revision += 1;
        }
        changed
    }

    pub fn claim_ready(
        &mut self,
        owner: &str,
        limit: usize,
    ) -> Result<Vec<GraphNodeId>, GraphError> {
        if owner.trim().is_empty() || limit == 0 {
            return Err(GraphError::InvalidExecution);
        }
        let states = self
            .nodes
            .iter()
            .map(|node| (node.node_id, node.state))
            .collect::<HashMap<_, _>>();
        let mut claimed = Vec::new();
        for execution in &mut self.nodes {
            if claimed.len() == limit || execution.state != GraphNodeState::Pending {
                continue;
            }
            let node = self
                .graph
                .nodes
                .iter()
                .find(|node| node.id == execution.node_id)
                .ok_or(GraphError::InvalidExecution)?;
            if node
                .needs
                .iter()
                .all(|dependency| states.get(dependency) == Some(&GraphNodeState::Succeeded))
            {
                execution.state = GraphNodeState::Running;
                execution.owner = Some(owner.into());
                execution.revision += 1;
                claimed.push(execution.node_id);
            }
        }
        if !claimed.is_empty() {
            self.revision += 1;
        }
        Ok(claimed)
    }

    pub fn complete(
        &mut self,
        node_id: GraphNodeId,
        owner: &str,
        succeeded: bool,
    ) -> Result<(), GraphError> {
        let execution = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .ok_or(GraphError::InvalidExecution)?;
        if execution.state != GraphNodeState::Running || execution.owner.as_deref() != Some(owner) {
            return Err(GraphError::InvalidExecution);
        }
        execution.state = if succeeded {
            GraphNodeState::Succeeded
        } else {
            GraphNodeState::Failed
        };
        execution.revision += 1;
        self.revision += 1;
        if !succeeded {
            self.block_dependents(node_id);
        }
        Ok(())
    }

    fn block_dependents(&mut self, failed: GraphNodeId) {
        let mut blocked = HashSet::from([failed]);
        loop {
            let mut changed = false;
            for node in &self.graph.nodes {
                if node
                    .needs
                    .iter()
                    .any(|dependency| blocked.contains(dependency))
                    && blocked.insert(node.id)
                {
                    if let Some(execution) = self
                        .nodes
                        .iter_mut()
                        .find(|execution| execution.node_id == node.id)
                    {
                        if execution.state == GraphNodeState::Pending {
                            execution.state = GraphNodeState::Blocked;
                            execution.revision += 1;
                        }
                    }
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }
}

fn visit(
    id: GraphNodeId,
    graph: &WorkflowGraph,
    visiting: &mut HashSet<GraphNodeId>,
    visited: &mut HashSet<GraphNodeId>,
) -> Result<(), GraphError> {
    if visited.contains(&id) {
        return Ok(());
    }
    if !visiting.insert(id) {
        return Err(GraphError::Cycle);
    }
    let node = graph
        .nodes
        .iter()
        .find(|node| node.id == id)
        .ok_or(GraphError::InvalidDependency)?;
    for dependency in &node.needs {
        visit(*dependency, graph, visiting, visited)?;
    }
    visiting.remove(&id);
    visited.insert(id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph() -> WorkflowGraph {
        let left = GraphNodeId::from_bytes([51; 16]);
        let right = GraphNodeId::from_bytes([52; 16]);
        WorkflowGraph {
            decision_baseline_id: DecisionBaselineId::from_bytes([50; 16]),
            nodes: vec![
                GraphNode {
                    id: left,
                    kind: GraphNodeKind::Research,
                    role_id: RoleId::from_bytes([1; 16]),
                    title: "Research A".into(),
                    needs: vec![],
                },
                GraphNode {
                    id: right,
                    kind: GraphNodeKind::Research,
                    role_id: RoleId::from_bytes([2; 16]),
                    title: "Research B".into(),
                    needs: vec![],
                },
                GraphNode {
                    id: GraphNodeId::from_bytes([53; 16]),
                    kind: GraphNodeKind::Join,
                    role_id: RoleId::from_bytes([3; 16]),
                    title: "Join evidence".into(),
                    needs: vec![left, right],
                },
            ],
        }
    }

    #[test]
    fn two_research_branches_claim_concurrently_before_explicit_join() {
        let mut execution =
            GraphExecution::start(GraphRunId::from_bytes([54; 16]), graph()).unwrap();
        let branches = execution.claim_ready("worker", 2).unwrap();
        assert_eq!(branches.len(), 2);
        assert!(execution.claim_ready("join", 1).unwrap().is_empty());
        for branch in branches {
            execution.complete(branch, "worker", true).unwrap();
        }
        assert_eq!(
            execution.claim_ready("join", 1).unwrap(),
            vec![GraphNodeId::from_bytes([53; 16])]
        );
    }

    #[test]
    fn failed_branch_blocks_join_and_wrong_owner_cannot_complete() {
        let mut execution =
            GraphExecution::start(GraphRunId::from_bytes([55; 16]), graph()).unwrap();
        let branches = execution.claim_ready("worker", 2).unwrap();
        assert_eq!(
            execution.complete(branches[0], "other", true),
            Err(GraphError::InvalidExecution)
        );
        execution.complete(branches[0], "worker", false).unwrap();
        assert_eq!(execution.nodes[2].state, GraphNodeState::Blocked);
    }

    #[test]
    fn graph_rejects_cycle_and_implicit_join() {
        let mut invalid = graph();
        let join_id = invalid.nodes[2].id;
        invalid.nodes[0].needs.push(join_id);
        assert_eq!(invalid.validate(), Err(GraphError::Cycle));
        let mut implicit = graph();
        implicit.nodes[2].needs.pop();
        assert_eq!(implicit.validate(), Err(GraphError::InvalidResearchJoin));
    }
}
