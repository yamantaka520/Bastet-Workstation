use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{DecisionBaselineId, GraphNodeId, GraphRunId, RoleId, RunId};

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

pub const MAX_NODE_OUTPUT_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphNodeOutput {
    pub node_id: GraphNodeId,
    pub run_id: RunId,
    pub content_hash: String,
    pub markdown: String,
}

impl GraphNodeOutput {
    pub fn create(
        node_id: GraphNodeId,
        run_id: RunId,
        markdown: String,
    ) -> Result<Self, GraphError> {
        if markdown.trim().is_empty() || markdown.len() > MAX_NODE_OUTPUT_BYTES {
            return Err(GraphError::InvalidOutput);
        }
        Ok(Self {
            node_id,
            run_id,
            content_hash: output_hash(&markdown),
            markdown,
        })
    }

    pub fn validate(&self) -> Result<(), GraphError> {
        if self.markdown.trim().is_empty()
            || self.markdown.len() > MAX_NODE_OUTPUT_BYTES
            || self.content_hash != output_hash(&self.markdown)
        {
            return Err(GraphError::InvalidOutput);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphExecution {
    pub id: GraphRunId,
    pub graph: WorkflowGraph,
    pub nodes: Vec<GraphNodeExecution>,
    #[serde(default)]
    pub outputs: Vec<GraphNodeOutput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restarted_from_execution_id: Option<GraphRunId>,
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
    #[error("graph node output is missing, invalid, too large, or duplicated")]
    InvalidOutput,
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
            outputs: Vec::new(),
            restarted_from_execution_id: None,
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
        let output_nodes = self
            .outputs
            .iter()
            .map(|output| output.node_id)
            .collect::<HashSet<_>>();
        let output_runs = self
            .outputs
            .iter()
            .map(|output| output.run_id)
            .collect::<HashSet<_>>();
        if output_nodes.len() != self.outputs.len()
            || output_runs.len() != self.outputs.len()
            || self.outputs.iter().any(|output| {
                output.validate().is_err()
                    || !self
                        .graph
                        .nodes
                        .iter()
                        .any(|node| node.id == output.node_id)
                    || self
                        .nodes
                        .iter()
                        .find(|node| node.node_id == output.node_id)
                        .is_none_or(|node| {
                            !matches!(
                                node.state,
                                GraphNodeState::Succeeded | GraphNodeState::Failed
                            )
                        })
            })
        {
            return Err(GraphError::InvalidOutput);
        }
        Ok(())
    }

    pub fn restart_as(&self, id: GraphRunId) -> Result<Self, GraphError> {
        self.validate()?;
        if self
            .nodes
            .iter()
            .any(|node| node.state != GraphNodeState::Succeeded)
            || self.outputs.len() == self.nodes.len()
        {
            return Err(GraphError::InvalidOutput);
        }
        let mut restarted = Self::start(id, self.graph.clone())?;
        restarted.restarted_from_execution_id = Some(self.id);
        Ok(restarted)
    }

    pub fn output(&self, node_id: GraphNodeId) -> Option<&GraphNodeOutput> {
        self.outputs.iter().find(|output| output.node_id == node_id)
    }

    pub fn record_output(&mut self, output: GraphNodeOutput) -> Result<(), GraphError> {
        output.validate()?;
        if self
            .outputs
            .iter()
            .any(|existing| existing.node_id == output.node_id || existing.run_id == output.run_id)
            || self
                .nodes
                .iter()
                .find(|node| node.node_id == output.node_id)
                .is_none_or(|node| {
                    !matches!(
                        node.state,
                        GraphNodeState::Succeeded | GraphNodeState::Failed
                    )
                })
        {
            return Err(GraphError::InvalidOutput);
        }
        self.outputs.push(output);
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

    pub fn claim_node(&mut self, node_id: GraphNodeId, owner: &str) -> Result<(), GraphError> {
        if owner.trim().is_empty() {
            return Err(GraphError::InvalidExecution);
        }
        let states = self
            .nodes
            .iter()
            .map(|node| (node.node_id, node.state))
            .collect::<HashMap<_, _>>();
        let definition = self
            .graph
            .nodes
            .iter()
            .find(|node| node.id == node_id)
            .ok_or(GraphError::InvalidExecution)?;
        if !definition
            .needs
            .iter()
            .all(|dependency| states.get(dependency) == Some(&GraphNodeState::Succeeded))
        {
            return Err(GraphError::InvalidExecution);
        }
        let execution = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .ok_or(GraphError::InvalidExecution)?;
        if execution.state != GraphNodeState::Pending {
            return Err(GraphError::InvalidExecution);
        }
        execution.state = GraphNodeState::Running;
        execution.owner = Some(owner.into());
        execution.revision += 1;
        self.revision += 1;
        Ok(())
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

    pub fn finish_terminal(
        &mut self,
        node_id: GraphNodeId,
        owner: &str,
        state: GraphNodeState,
    ) -> Result<(), GraphError> {
        if !matches!(
            state,
            GraphNodeState::Succeeded
                | GraphNodeState::Failed
                | GraphNodeState::Blocked
                | GraphNodeState::Uncertain
        ) {
            return Err(GraphError::InvalidExecution);
        }
        let execution = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .ok_or(GraphError::InvalidExecution)?;
        if execution.state != GraphNodeState::Running || execution.owner.as_deref() != Some(owner) {
            return Err(GraphError::InvalidExecution);
        }
        execution.state = state;
        execution.owner = if matches!(state, GraphNodeState::Succeeded | GraphNodeState::Failed) {
            Some(owner.into())
        } else {
            None
        };
        execution.revision += 1;
        self.revision += 1;
        if matches!(state, GraphNodeState::Failed | GraphNodeState::Blocked) {
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

fn output_hash(content: &str) -> String {
    format!("sha256:{:x}", Sha256::digest(content.as_bytes()))
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
    fn uncertain_provider_result_releases_owner_without_unlocking_join() {
        let mut execution = GraphExecution::start(GraphRunId::new(), graph()).unwrap();
        let node_id = execution.claim_ready("worker", 1).unwrap()[0];
        execution
            .finish_terminal(node_id, "worker", GraphNodeState::Uncertain)
            .unwrap();
        let node = execution
            .nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .unwrap();
        assert_eq!(node.state, GraphNodeState::Uncertain);
        assert_eq!(node.owner, None);
        let still_ready = execution.claim_ready("other-research", 1).unwrap();
        assert_eq!(still_ready.len(), 1);
        assert_eq!(
            execution
                .graph
                .nodes
                .iter()
                .find(|node| node.id == still_ready[0])
                .unwrap()
                .kind,
            GraphNodeKind::Research
        );
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

    #[test]
    fn output_is_hash_bound_and_restart_preserves_original() {
        let mut execution = GraphExecution::start(GraphRunId::new(), graph()).unwrap();
        let original_id = execution.id;
        let branches = execution.claim_ready("worker", 2).unwrap();
        for (index, node_id) in branches.into_iter().enumerate() {
            execution.complete(node_id, "worker", true).unwrap();
            execution
                .record_output(
                    GraphNodeOutput::create(
                        node_id,
                        RunId::new(),
                        format!("research output {index}"),
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let join = execution.claim_ready("join", 1).unwrap()[0];
        execution.complete(join, "join", true).unwrap();
        assert_eq!(execution.validate(), Ok(()));

        let restarted = execution.restart_as(GraphRunId::new()).unwrap();
        assert_eq!(restarted.restarted_from_execution_id, Some(original_id));
        assert!(restarted.outputs.is_empty());
        assert!(restarted
            .nodes
            .iter()
            .all(|node| node.state == GraphNodeState::Pending));

        let mut tampered = execution;
        tampered.outputs[0].markdown.push_str(" tampered");
        assert_eq!(tampered.validate(), Err(GraphError::InvalidOutput));
    }
}
