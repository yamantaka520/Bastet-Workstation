//! Durable selected-account launch approval coordination.
//!
//! This ledger records approval-gated launch intent only. It never reads a
//! credential, consumes a grant, or dispatches/cancels a provider process.

use super::*;
use bastet_core::{
    ApprovalAction, ApprovalDecision, ApprovalDecisionKind, ApprovalRequest, ApprovalRisk,
    ApprovalScope, CredentialGrantBinding, EntityLifecycle, NormalizedRunState, PermissionLevel,
    PolicyCeiling, PolicyLayer, Run, ScopedPolicy,
};
use rusqlite::Transaction;

const APPROVAL_TTL_MS: u64 = 5 * 60 * 1_000;
const STAGED_AWAITING: &str = "awaiting_approval";
const STAGED_READY: &str = "ready";
const STAGED_CANCELLED: &str = "cancelled";

#[derive(Debug, PartialEq, Eq)]
struct AttemptState {
    run_id: String,
    attempt: u64,
    finished_at: Option<String>,
    terminal_state: Option<String>,
    failure_json: Option<String>,
}

/// Creates the daemon-owned credential approval for an already-persisted launch
/// plan. The caller owns the surrounding transaction and has persisted the
/// catalog run, graph staging state, attempt ledger, and immutable plan first.
pub(super) fn stage(
    transaction: &Transaction<'_>,
    plan: &provider_launch::ProviderLaunchPlan,
) -> Result<(), StoreError> {
    let catalog = load_catalog(transaction)?;
    let run = catalog
        .runs
        .iter()
        .find(|run| run.metadata.id == plan.run_id)
        .ok_or(StoreError::RunNotFound)?;
    let action = approval_action(plan)?;
    let role_policy = current_role_policy(&catalog, plan)?;
    let now = credential_grants::now_ms()?;
    let expires_at_ms = now
        .checked_add(APPROVAL_TTL_MS)
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or(StoreError::CredentialGrantRejected)?;
    let request = ApprovalRequest::create(
        ApprovalRequestId::new(),
        now,
        expires_at_ms,
        action,
        &role_policy,
    )?;
    request.validate_against(&catalog)?;

    transaction.execute(
        "INSERT INTO approval_requests(
            request_id, request_hash, request_json, expires_at_ms, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            request.id.value().to_string(),
            request.request_hash,
            serde_json::to_string(&request)?,
            request.expires_at_ms,
            timestamp(),
        ],
    )?;
    transaction.execute(
        "INSERT INTO staged_provider_launches(
            run_id, request_id, state, staged_at_ms)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            plan.run_id.value().to_string(),
            request.id.value().to_string(),
            STAGED_AWAITING,
            now,
        ],
    )?;

    // Validate only after both rows exist. Besides giving restart validation the
    // same source of truth, this makes every mismatch abort the outer begin txn.
    validate_staged_context(transaction, run, false, true)?;
    insert_event(
        transaction,
        "approval.requested",
        &serde_json::json!({"request_id": request.id, "risk": request.action.risk}).to_string(),
    )?;
    insert_event(
        transaction,
        "provider_launch.staged",
        &serde_json::json!({
            "execution_id": plan.execution_id,
            "node_id": plan.node_id,
            "run_id": plan.run_id,
            "request_id": request.id,
            "plan_hash": plan.hash()?,
        })
        .to_string(),
    )?;
    Ok(())
}

/// Applies a decision to a staged launch when the request belongs to this
/// ledger. Ordinary approvals are deliberately a no-op here.
pub(super) fn on_decision(
    transaction: &Transaction<'_>,
    request: &ApprovalRequest,
    kind: ApprovalDecisionKind,
) -> Result<(), StoreError> {
    let row: Option<(String, String)> = transaction
        .query_row(
            "SELECT run_id, state FROM staged_provider_launches WHERE request_id=?1",
            [request.id.value().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((run_id, state)) = row else {
        return Ok(());
    };
    if state != STAGED_AWAITING {
        return Err(StoreError::ApprovalConflict);
    }
    let run_id = parse_run_id(&run_id)?;
    let catalog = load_catalog(transaction)?;
    let run = catalog
        .runs
        .iter()
        .find(|run| run.metadata.id == run_id)
        .ok_or(StoreError::RunNotFound)?;
    match kind {
        ApprovalDecisionKind::Approve => {
            validate_staged_context(transaction, run, false, true)?;
            validate_request_for_plan(
                &provider_launch::ProviderLaunchPlan::load(transaction, run_id)?,
                request,
            )?;
            let updated = transaction.execute(
                "UPDATE staged_provider_launches SET state=?1
                 WHERE request_id=?2 AND state=?3",
                params![
                    STAGED_READY,
                    request.id.value().to_string(),
                    STAGED_AWAITING
                ],
            )?;
            if updated != 1 {
                return Err(StoreError::ApprovalConflict);
            }
            insert_event(
                transaction,
                "provider_launch.ready",
                &serde_json::json!({"run_id": run_id, "request_id": request.id}).to_string(),
            )?;
        }
        ApprovalDecisionKind::Deny => {
            validate_request_for_plan(
                &provider_launch::ProviderLaunchPlan::load(transaction, run_id)?,
                request,
            )?;
            cancel_for_request(transaction, request.id, "approval_denied")?;
        }
    }
    Ok(())
}

/// Terminates an unlaunched staged run. A request not owned by this ledger is a
/// no-op so ordinary approvals and non-account launches remain independent.
pub(super) fn cancel_for_request(
    transaction: &Transaction<'_>,
    request_id: ApprovalRequestId,
    reason: &str,
) -> Result<(), StoreError> {
    if reason.trim().is_empty() || reason.len() > 200 {
        return Err(StoreError::InvalidRunState(
            "invalid staged launch cancellation reason".into(),
        ));
    }
    let row: Option<(String, String, u64)> = transaction
        .query_row(
            "SELECT run_id, state, staged_at_ms
             FROM staged_provider_launches WHERE request_id=?1",
            [request_id.value().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((run_id, state, staged_at_ms)) = row else {
        return Ok(());
    };
    if state == STAGED_CANCELLED {
        return Ok(());
    }
    if state != STAGED_AWAITING && state != STAGED_READY {
        return Err(StoreError::InvalidRunState(
            "invalid staged launch ledger state".into(),
        ));
    }
    let run_id = parse_run_id(&run_id)?;
    let (catalog_revision, mut catalog) = load_catalog_row(transaction)?;
    let run_index = catalog
        .runs
        .iter()
        .position(|run| run.metadata.id == run_id)
        .ok_or(StoreError::RunNotFound)?;
    validate_staged_context(transaction, &catalog.runs[run_index], false, false)?;
    let plan = provider_launch::ProviderLaunchPlan::load(transaction, run_id)?;

    let (graph_revision, graph_json): (u64, String) = transaction
        .query_row(
            "SELECT revision, execution_json FROM graph_executions WHERE execution_id=?1",
            [plan.execution_id.value().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or(StoreError::GraphNotFound)?;
    let mut execution: GraphExecution = serde_json::from_str(&graph_json)?;
    if graph_revision != execution.revision {
        return Err(StoreError::InvalidGraph(GraphError::InvalidExecution));
    }
    execution.cancel_staged_node(plan.node_id, &plan.owner, plan.run_id)?;
    execution.validate()?;

    let now_ms = credential_grants::now_ms()?;
    // Cancellation must remain possible after a wall-clock rollback. Ledger
    // time stays monotonic without pretending that provider execution began.
    let finished_at_ms = now_ms.max(staged_at_ms);
    let finished_at = finished_at_ms.to_string();
    let run = &mut catalog.runs[run_index];
    run.state = NormalizedRunState::Cancelled;
    run.finished_at = Some(finished_at.clone());
    run.metadata.revision = run
        .metadata
        .revision
        .checked_add(1)
        .ok_or(StoreError::RevisionOverflow)?;
    run.metadata.updated_at = finished_at.clone();
    catalog.validate()?;

    let m3: M3Catalog = serde_json::from_str(&transaction.query_row::<String, _, _>(
        "SELECT catalog_json FROM m3_catalog WHERE singleton=1",
        [],
        |row| row.get(0),
    )?)?;
    let mut executions = load_graph_executions(transaction)?;
    let persisted = executions
        .iter_mut()
        .find(|candidate| candidate.id == execution.id)
        .ok_or(StoreError::GraphNotFound)?;
    *persisted = execution.clone();
    M3State {
        catalog: m3,
        graph_executions: executions,
    }
    .validate(&catalog)
    .map_err(|error| StoreError::InvalidM3(error.to_string()))?;

    let next_catalog_revision = catalog_revision
        .checked_add(1)
        .ok_or(StoreError::RevisionOverflow)?;
    transaction.execute(
        "UPDATE identity_catalog
         SET revision=?1, catalog_json=?2, updated_at=?3 WHERE singleton=1",
        params![
            next_catalog_revision,
            serde_json::to_string(&catalog)?,
            finished_at,
        ],
    )?;
    transaction.execute(
        "UPDATE graph_executions
         SET revision=?1, execution_json=?2, updated_at=?3 WHERE execution_id=?4",
        params![
            execution.revision,
            serde_json::to_string(&execution)?,
            finished_at,
            execution.id.value().to_string(),
        ],
    )?;
    let finished = transaction.execute(
        "UPDATE graph_node_runs
         SET finished_at=?1, terminal_state=?2, failure_json=NULL
         WHERE run_id=?3 AND finished_at IS NULL AND terminal_state IS NULL",
        params![
            finished_at,
            serde_json::to_string(&NormalizedRunState::Cancelled)?,
            run_id.value().to_string(),
        ],
    )?;
    if finished != 1 {
        return Err(StoreError::InvalidRunState(
            "staged run attempt is already terminal".into(),
        ));
    }
    let cancelled = transaction.execute(
        "UPDATE staged_provider_launches
         SET state=?1, finished_at_ms=?2, cancel_reason=?3
         WHERE request_id=?4 AND state IN (?5, ?6)",
        params![
            STAGED_CANCELLED,
            finished_at_ms,
            reason,
            request_id.value().to_string(),
            STAGED_AWAITING,
            STAGED_READY,
        ],
    )?;
    if cancelled != 1 {
        return Err(StoreError::InvalidRunState(
            "staged launch cancellation lost its ledger claim".into(),
        ));
    }

    // Expiry and explicit run cancellation make an issued, unspent grant
    // permanently unusable. This is metadata revocation only; no secret is read.
    let revoked = transaction.execute(
        "UPDATE credential_grants
         SET revoked_at_ms=MAX(issued_at_ms, ?1),
             revoked_by='daemon:staged_launch_cancelled'
         WHERE request_id=?2 AND consumed_at_ms IS NULL AND revoked_at_ms IS NULL",
        params![finished_at_ms, request_id.value().to_string()],
    )?;
    if revoked == 1 {
        insert_event(
            transaction,
            "credential.grant_revoked",
            &serde_json::json!({"request_id": request_id}).to_string(),
        )?;
    }
    insert_event(
        transaction,
        "provider_launch.cancelled",
        &serde_json::json!({
            "execution_id": execution.id,
            "node_id": plan.node_id,
            "run_id": plan.run_id,
            "request_id": request_id,
            "catalog_revision": next_catalog_revision,
            "graph_revision": execution.revision,
        })
        .to_string(),
    )?;
    Ok(())
}

/// Cancels expired approval-gated launch intents. The caller supplies time so a
/// whole pass is deterministic and can run inside an existing store transaction.
pub(super) fn expire(transaction: &Transaction<'_>, now: u64) -> Result<(), StoreError> {
    if now > i64::MAX as u64 {
        return Err(StoreError::CredentialGrantRejected);
    }
    let request_ids = {
        let mut statement = transaction.prepare(
            "SELECT staged.request_id
             FROM staged_provider_launches staged
             JOIN approval_requests approval USING(request_id)
             WHERE approval.expires_at_ms <= ?1
               AND ((staged.state=?2 AND approval.decision_json IS NULL)
                 OR (staged.state=?3 AND approval.decision_json IS NOT NULL))
             ORDER BY staged.staged_at_ms, staged.request_id",
        )?;
        let rows = statement
            .query_map(params![now, STAGED_AWAITING, STAGED_READY], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows
    };
    for request_id in request_ids {
        cancel_for_request(
            transaction,
            parse_approval_request_id(&request_id)?,
            "approval_expired",
        )?;
    }
    Ok(())
}

/// Recovery proof for a never-dispatched `AwaitingApproval` catalog run. Missing
/// or inconsistent approval/plan/attempt data is rejected rather than inferred.
pub(super) fn validate_staged_run(
    transaction: &Transaction<'_>,
    run: &Run,
) -> Result<(), StoreError> {
    validate_staged_context(transaction, run, true, false)
}

fn validate_staged_context(
    transaction: &Transaction<'_>,
    run: &Run,
    validate_decision_state: bool,
    require_live_authority: bool,
) -> Result<(), StoreError> {
    if run.state != NormalizedRunState::AwaitingApproval
        || run.started_at.is_some()
        || run.finished_at.is_some()
    {
        return Err(StoreError::InvalidRunState(
            "staged run has invalid catalog timing".into(),
        ));
    }
    let row: Option<(String, String, Option<u64>, Option<String>)> = transaction
        .query_row(
            "SELECT request_id, state, finished_at_ms, cancel_reason
             FROM staged_provider_launches WHERE run_id=?1",
            [run.metadata.id.value().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let (request_id, staged_state, finished_at_ms, cancel_reason) = row
        .ok_or_else(|| StoreError::InvalidRunState("staged launch ledger is unavailable".into()))?;
    if !matches!(staged_state.as_str(), STAGED_AWAITING | STAGED_READY)
        || finished_at_ms.is_some()
        || cancel_reason.is_some()
    {
        return Err(StoreError::InvalidRunState(
            "staged launch ledger is terminal or corrupt".into(),
        ));
    }
    let request_id = parse_approval_request_id(&request_id)?;
    let plan = provider_launch::ProviderLaunchPlan::load(transaction, run.metadata.id)?;
    if plan.run_id != run.metadata.id
        || plan.session_id != run.session_id
        || plan.identity.model_id != run.model_id
    {
        return Err(StoreError::InvalidRunState(
            "staged run differs from its launch plan".into(),
        ));
    }

    let catalog = load_catalog(transaction)?;
    let persisted_run = catalog
        .runs
        .iter()
        .find(|candidate| candidate.metadata.id == run.metadata.id)
        .ok_or(StoreError::RunNotFound)?;
    if persisted_run != run {
        return Err(StoreError::InvalidRunState(
            "staged run differs from the current catalog".into(),
        ));
    }
    let session = catalog
        .sessions
        .iter()
        .find(|session| session.metadata.id == run.session_id)
        .ok_or(StoreError::RunNotFound)?;
    if session.agent_instance_id != plan.identity.agent_instance_id
        || session.project_id != plan.identity.project_id
    {
        return Err(StoreError::InvalidRunState(
            "staged session differs from its launch identity".into(),
        ));
    }

    let (stored_graph_revision, graph_json): (u64, String) = transaction
        .query_row(
            "SELECT revision, execution_json FROM graph_executions WHERE execution_id=?1",
            [plan.execution_id.value().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or(StoreError::GraphNotFound)?;
    let execution: GraphExecution = serde_json::from_str(&graph_json)?;
    execution.validate()?;
    if stored_graph_revision != execution.revision {
        return Err(StoreError::InvalidGraph(GraphError::InvalidExecution));
    }
    let definition = execution
        .graph
        .nodes
        .iter()
        .find(|node| node.id == plan.node_id)
        .ok_or(StoreError::RunNotFound)?;
    let node = execution
        .nodes
        .iter()
        .find(|node| node.node_id == plan.node_id)
        .ok_or(StoreError::RunNotFound)?;
    if definition.role_id != plan.role_id
        || node.state != GraphNodeState::AwaitingApproval
        || node.owner.as_deref() != Some(plan.owner.as_str())
        || node.run_id != Some(plan.run_id)
    {
        return Err(StoreError::InvalidRunState(
            "staged graph node differs from its launch plan".into(),
        ));
    }

    let latest: Option<AttemptState> = transaction
        .query_row(
            "SELECT run_id, attempt, finished_at, terminal_state, failure_json
             FROM graph_node_runs
             WHERE execution_id=?1 AND node_id=?2
             ORDER BY attempt DESC LIMIT 1",
            params![
                plan.execution_id.value().to_string(),
                plan.node_id.value().to_string()
            ],
            |row| {
                Ok(AttemptState {
                    run_id: row.get(0)?,
                    attempt: row.get(1)?,
                    finished_at: row.get(2)?,
                    terminal_state: row.get(3)?,
                    failure_json: row.get(4)?,
                })
            },
        )
        .optional()?;
    if latest
        != Some(AttemptState {
            run_id: plan.run_id.value().to_string(),
            attempt: plan.attempt,
            finished_at: None,
            terminal_state: None,
            failure_json: None,
        })
    {
        return Err(StoreError::InvalidRunState(
            "staged run is not the current unfinished attempt".into(),
        ));
    }

    let approval_row: Option<(String, Option<String>)> = transaction
        .query_row(
            "SELECT request_json, decision_json FROM approval_requests WHERE request_id=?1",
            [request_id.value().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (request_json, decision_json) = approval_row.ok_or(StoreError::ApprovalNotFound)?;
    let request: ApprovalRequest = serde_json::from_str(&request_json)?;
    if request.id != request_id {
        return Err(StoreError::CredentialGrantRejected);
    }
    validate_request_for_plan(&plan, &request)?;
    if require_live_authority {
        request.validate_against(&catalog)?;
        current_role_policy(&catalog, &plan)?;
        let m3: M3Catalog = serde_json::from_str(&transaction.query_row::<String, _, _>(
            "SELECT catalog_json FROM m3_catalog WHERE singleton=1",
            [],
            |row| row.get(0),
        )?)?;
        let binding = resolve_provider_run_binding(&catalog, &m3, &execution, plan.node_id)?;
        if binding.launch_identity != plan.identity
            || binding.workspace_root != plan.workspace_root
            || binding.prompt != plan.prompt
        {
            return Err(StoreError::InvalidRunState(
                "current provider selection differs from the staged launch plan".into(),
            ));
        }
    }
    if validate_decision_state {
        validate_decision_and_grant_state(
            transaction,
            &request,
            staged_state.as_str(),
            decision_json.as_deref(),
        )?;
    }
    Ok(())
}

fn validate_decision_and_grant_state(
    transaction: &Transaction<'_>,
    request: &ApprovalRequest,
    staged_state: &str,
    decision_json: Option<&str>,
) -> Result<(), StoreError> {
    let grant: Option<(u64, Option<u64>, Option<u64>)> = transaction
        .query_row(
            "SELECT issued_at_ms, consumed_at_ms, revoked_at_ms
             FROM credential_grants WHERE request_id=?1",
            [request.id.value().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    match (staged_state, decision_json, grant) {
        (STAGED_AWAITING, None, None) => Ok(()),
        (STAGED_READY, Some(json), Some((issued_at_ms, None, None))) => {
            let decision: ApprovalDecision = serde_json::from_str(json)?;
            request.verify_decision(&decision)?;
            if decision.kind == ApprovalDecisionKind::Approve
                && issued_at_ms == decision.decided_at_ms
            {
                Ok(())
            } else {
                Err(StoreError::CredentialGrantRejected)
            }
        }
        _ => Err(StoreError::CredentialGrantRejected),
    }
}

fn validate_request_for_plan(
    plan: &provider_launch::ProviderLaunchPlan,
    request: &ApprovalRequest,
) -> Result<(), StoreError> {
    let action = approval_action(plan)?;
    if request.expires_at_ms.checked_sub(request.created_at_ms) != Some(APPROVAL_TTL_MS) {
        return Err(StoreError::CredentialGrantRejected);
    }
    request.validate_unchanged()?;
    if request.action != action {
        return Err(StoreError::CredentialGrantRejected);
    }
    Ok(())
}

fn current_role_policy(
    catalog: &IdentityCatalog,
    plan: &provider_launch::ProviderLaunchPlan,
) -> Result<ScopedPolicy, StoreError> {
    plan.validate_current_policy(catalog)?;
    plan.validate_online_cli_requirements()?;
    let role = catalog
        .roles
        .iter()
        .find(|role| role.metadata.id == plan.role_id)
        .filter(|role| role.metadata.lifecycle == EntityLifecycle::Active)
        .ok_or(StoreError::CredentialGrantRejected)?;
    Ok(role.policy.clone())
}

fn approval_action(
    plan: &provider_launch::ProviderLaunchPlan,
) -> Result<ApprovalAction, StoreError> {
    let account = plan
        .identity
        .account
        .as_ref()
        .ok_or(StoreError::CredentialGrantRejected)?;
    let credential = account
        .credential
        .as_ref()
        .ok_or(StoreError::CredentialGrantRejected)?;
    let policy = ScopedPolicy {
        layer: PolicyLayer::SingleRun,
        ceiling: PolicyCeiling {
            filesystem: PermissionLevel::Deny,
            network: PermissionLevel::Deny,
            process: PermissionLevel::Deny,
            device: PermissionLevel::Deny,
            credential: PermissionLevel::Use,
            persistent_approval: false,
        },
    };
    Ok(ApprovalAction {
        agent_instance_id: plan.identity.agent_instance_id,
        role_id: Some(plan.role_id),
        action_key: "credential.use".into(),
        reason_key: "provider.authentication".into(),
        consequence_key: "credential.single_run".into(),
        risk: ApprovalRisk::High,
        scope: ApprovalScope {
            project_id: plan.identity.project_id,
            run_id: Some(plan.run_id),
            filesystem_roots: vec![plan.workspace_root.clone()],
            data_scopes: vec![format!("provider_launch_plan:{}", plan.hash()?)],
            network_destinations: Vec::new(),
            credential_reference_ids: vec![credential.reference_id],
            destination: None,
            credential_binding: Some(CredentialGrantBinding {
                agent_provider_id: plan.identity.agent_provider_id,
                account_id: account.account_id,
                adapter_kind: plan.identity.adapter_kind.clone(),
                provider_identity: account.provider_identity.clone(),
                credential_reference_id: credential.reference_id,
                backend: credential.backend.clone(),
                service: credential.service.clone(),
                account_label: credential.account_label.clone(),
                capability_key: "provider.authenticate".into(),
            }),
        },
        requested_policy: policy,
    })
}

fn load_catalog(transaction: &Transaction<'_>) -> Result<IdentityCatalog, StoreError> {
    Ok(load_catalog_row(transaction)?.1)
}

fn load_catalog_row(transaction: &Transaction<'_>) -> Result<(u64, IdentityCatalog), StoreError> {
    let (revision, json): (u64, String) = transaction.query_row(
        "SELECT revision, catalog_json FROM identity_catalog WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let catalog: IdentityCatalog = serde_json::from_str(&json)?;
    catalog.validate()?;
    Ok((revision, catalog))
}

fn parse_run_id(value: &str) -> Result<RunId, StoreError> {
    let id = Uuid::parse_str(value).map_err(|_| {
        StoreError::InvalidRunState("staged launch contains an invalid run id".into())
    })?;
    Ok(RunId::from_bytes(*id.as_bytes()))
}

fn parse_approval_request_id(value: &str) -> Result<ApprovalRequestId, StoreError> {
    let id = Uuid::parse_str(value).map_err(|_| StoreError::CredentialGrantRejected)?;
    Ok(ApprovalRequestId::from_bytes(*id.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{attach_selected_account, begin_fixture_node, provider_execution_fixture};
    use bastet_core::ApprovalDecision;
    use bastet_protocol::{
        BeginGraphNodeRunCommand, DecideApprovalCommand, ReplaceCatalogCommand,
        RevokeCredentialGrantCommand,
    };

    fn staged_fixture() -> (
        tempfile::TempDir,
        tempfile::TempDir,
        Store,
        GraphRunId,
        BeginGraphNodeRunReceipt,
        ApprovalRequest,
    ) {
        let (directory, workspace, store, execution_id) = provider_execution_fixture();
        let graph = store.graph_execution(execution_id).unwrap();
        let node_id = graph.nodes[0].node_id;
        attach_selected_account(&store, execution_id, node_id);
        let mut snapshot = store.catalog().unwrap();
        snapshot.catalog.projects[0].policy.ceiling.credential = PermissionLevel::Use;
        for role in &mut snapshot.catalog.roles {
            role.policy.ceiling.credential = PermissionLevel::Use;
        }
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: snapshot.revision,
                catalog: snapshot.catalog,
            })
            .unwrap();
        let catalog = store.catalog().unwrap();
        let graph = store.graph_execution(execution_id).unwrap();
        let binding = resolve_provider_run_binding(
            &catalog.catalog,
            &store.m3_catalog().unwrap().catalog,
            &graph,
            node_id,
        )
        .unwrap();
        let receipt = store
            .begin_provider_graph_node_run(
                execution_id,
                BeginGraphNodeRunCommand {
                    expected_catalog_revision: catalog.revision,
                    expected_graph_revision: graph.revision,
                    node_id,
                    owner: provider_executor::owner(node_id),
                },
                &binding,
            )
            .unwrap();
        let request = store.approvals().unwrap().records[0].request.clone();
        (directory, workspace, store, execution_id, receipt, request)
    }

    fn assert_no_provider_evidence(store: &Store, execution_id: GraphRunId) {
        assert!(store
            .m3_catalog()
            .unwrap()
            .catalog
            .deliverables
            .costs
            .is_empty());
        assert!(store
            .graph_execution(execution_id)
            .unwrap()
            .outputs
            .is_empty());
    }

    #[test]
    fn staged_request_and_ready_grant_survive_restart_without_dispatch() {
        let (directory, _workspace, store, execution_id, receipt, request) = staged_fixture();
        assert_eq!(
            request.expires_at_ms - request.created_at_ms,
            APPROVAL_TTL_MS
        );
        assert_eq!(
            request.action.requested_policy.ceiling.credential,
            PermissionLevel::Use
        );
        assert_eq!(
            request.action.requested_policy.ceiling.filesystem,
            PermissionLevel::Deny
        );
        store
            .decide_approval(DecideApprovalCommand {
                decision: ApprovalDecision {
                    request_id: request.id,
                    request_hash: request.request_hash.clone(),
                    kind: ApprovalDecisionKind::Approve,
                    decided_at_ms: 0,
                    actor: "fixture-human".into(),
                },
                credential_scope_acknowledged: true,
            })
            .unwrap();
        assert!(store
            .credential_grant(request.id)
            .unwrap()
            .consumed_at_ms
            .is_none());
        drop(store);

        let restored = Store::open(directory.path().join("provider-execution.db")).unwrap();
        let run = restored
            .catalog()
            .unwrap()
            .catalog
            .runs
            .into_iter()
            .find(|run| run.metadata.id == receipt.run_id)
            .unwrap();
        assert_eq!(run.state, NormalizedRunState::AwaitingApproval);
        assert!(run.started_at.is_none());
        let state: String = restored
            .connection()
            .unwrap()
            .query_row(
                "SELECT state FROM staged_provider_launches WHERE run_id=?1",
                [receipt.run_id.value().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, STAGED_READY);
        assert_no_provider_evidence(&restored, execution_id);
    }

    #[test]
    fn live_authority_drift_rejects_approve_but_cannot_block_deny() {
        for field in ["credential", "network", "filesystem"] {
            let (_directory, _workspace, store, execution_id, receipt, request) = staged_fixture();
            let mut snapshot = store.catalog().unwrap();
            let role_id = request.action.role_id.unwrap();
            let ceiling = &mut snapshot
                .catalog
                .roles
                .iter_mut()
                .find(|role| role.metadata.id == role_id)
                .unwrap()
                .policy
                .ceiling;
            match field {
                "credential" => ceiling.credential = PermissionLevel::Deny,
                "network" => ceiling.network = PermissionLevel::Deny,
                _ => ceiling.filesystem = PermissionLevel::Deny,
            }
            store
                .replace_catalog(ReplaceCatalogCommand {
                    expected_revision: snapshot.revision,
                    catalog: snapshot.catalog,
                })
                .unwrap();

            assert!(store
                .decide_approval(DecideApprovalCommand {
                    decision: ApprovalDecision {
                        request_id: request.id,
                        request_hash: request.request_hash.clone(),
                        kind: ApprovalDecisionKind::Approve,
                        decided_at_ms: 0,
                        actor: "fixture-human".into(),
                    },
                    credential_scope_acknowledged: true,
                })
                .is_err());
            store
                .decide_approval(DecideApprovalCommand {
                    decision: ApprovalDecision {
                        request_id: request.id,
                        request_hash: request.request_hash,
                        kind: ApprovalDecisionKind::Deny,
                        decided_at_ms: 0,
                        actor: "fixture-human".into(),
                    },
                    credential_scope_acknowledged: false,
                })
                .unwrap();
            let run = store
                .catalog()
                .unwrap()
                .catalog
                .runs
                .into_iter()
                .find(|run| run.metadata.id == receipt.run_id)
                .unwrap();
            assert_eq!(run.state, NormalizedRunState::Cancelled);
            assert!(run.started_at.is_none());
            assert_no_provider_evidence(&store, execution_id);
        }
    }

    #[test]
    fn legacy_policyless_staging_survives_restart_and_can_be_denied_not_approved() {
        let (directory, _workspace, store, execution_id, receipt, request) = staged_fixture();
        let catalog = store.catalog().unwrap().catalog;
        let connection = store.connection().unwrap();
        let mut plan =
            provider_launch::ProviderLaunchPlan::load(&connection, receipt.run_id).unwrap();
        plan.version = 1;
        plan.policy = None;
        let role = catalog
            .roles
            .iter()
            .find(|role| role.metadata.id == plan.role_id)
            .unwrap();
        let legacy_request = ApprovalRequest::create(
            request.id,
            request.created_at_ms,
            request.expires_at_ms,
            approval_action(&plan).unwrap(),
            &role.policy,
        )
        .unwrap();
        // Build an internally consistent historical fixture; live plan edits
        // remain forbidden by the production immutable-table trigger.
        connection
            .execute_batch("DROP TRIGGER provider_launch_plans_immutable;")
            .unwrap();
        connection
            .execute(
                "UPDATE provider_launch_plans SET plan_json=?1,plan_hash=?2 WHERE run_id=?3",
                params![
                    serde_json::to_string(&plan).unwrap(),
                    plan.hash().unwrap(),
                    receipt.run_id.value().to_string()
                ],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE approval_requests SET request_json=?1,request_hash=?2 WHERE request_id=?3",
                params![
                    serde_json::to_string(&legacy_request).unwrap(),
                    legacy_request.request_hash,
                    request.id.value().to_string()
                ],
            )
            .unwrap();
        drop(connection);
        drop(store);
        let restored = Store::open(directory.path().join("provider-execution.db")).unwrap();
        assert_eq!(
            restored.approval(request.id).unwrap().staged_launch_state,
            Some(bastet_protocol::StagedLaunchState::AwaitingApproval)
        );
        assert!(restored
            .decide_approval(DecideApprovalCommand {
                decision: ApprovalDecision {
                    request_id: request.id,
                    request_hash: legacy_request.request_hash.clone(),
                    kind: ApprovalDecisionKind::Approve,
                    decided_at_ms: 0,
                    actor: "fixture-human".into()
                },
                credential_scope_acknowledged: true
            })
            .is_err());
        restored
            .decide_approval(DecideApprovalCommand {
                decision: ApprovalDecision {
                    request_id: request.id,
                    request_hash: legacy_request.request_hash,
                    kind: ApprovalDecisionKind::Deny,
                    decided_at_ms: 0,
                    actor: "fixture-human".into(),
                },
                credential_scope_acknowledged: false,
            })
            .unwrap();
        assert_no_provider_evidence(&restored, execution_id);
        assert_eq!(
            restored.approval(request.id).unwrap().staged_launch_state,
            Some(bastet_protocol::StagedLaunchState::Cancelled)
        );
    }

    #[test]
    fn approved_grant_expiry_revokes_and_cancels_exact_unlaunched_run() {
        let (_directory, _workspace, store, execution_id, receipt, request) = staged_fixture();
        store
            .decide_approval(DecideApprovalCommand {
                decision: ApprovalDecision {
                    request_id: request.id,
                    request_hash: request.request_hash.clone(),
                    kind: ApprovalDecisionKind::Approve,
                    decided_at_ms: 0,
                    actor: "fixture-human".into(),
                },
                credential_scope_acknowledged: true,
            })
            .unwrap();
        {
            let mut connection = store.connection().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            expire(&transaction, request.expires_at_ms).unwrap();
            transaction.commit().unwrap();
        }
        let grant = store.credential_grant(request.id).unwrap();
        assert!(grant.consumed_at_ms.is_none());
        assert!(grant.revoked_at_ms.is_some());
        let run = store
            .catalog()
            .unwrap()
            .catalog
            .runs
            .into_iter()
            .find(|run| run.metadata.id == receipt.run_id)
            .unwrap();
        assert_eq!(run.state, NormalizedRunState::Cancelled);
        assert_no_provider_evidence(&store, execution_id);
    }

    #[test]
    fn revoking_ready_grant_cancels_its_exact_staged_run() {
        let (_directory, _workspace, store, execution_id, receipt, request) = staged_fixture();
        store
            .decide_approval(DecideApprovalCommand {
                decision: ApprovalDecision {
                    request_id: request.id,
                    request_hash: request.request_hash.clone(),
                    kind: ApprovalDecisionKind::Approve,
                    decided_at_ms: 0,
                    actor: "fixture-human".into(),
                },
                credential_scope_acknowledged: true,
            })
            .unwrap();
        let grant = store
            .revoke_credential_grant(
                request.id,
                RevokeCredentialGrantCommand {
                    actor: "fixture-human".into(),
                },
            )
            .unwrap();
        assert!(grant.revoked_at_ms.is_some());
        let run = store
            .catalog()
            .unwrap()
            .catalog
            .runs
            .into_iter()
            .find(|run| run.metadata.id == receipt.run_id)
            .unwrap();
        assert_eq!(run.state, NormalizedRunState::Cancelled);
        let other_staged: u64 = store
            .connection()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM staged_provider_launches
                 WHERE run_id<>?1 AND state='cancelled'",
                [receipt.run_id.value().to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(other_staged, 0);
        assert_no_provider_evidence(&store, execution_id);
    }

    #[test]
    fn expiry_cancels_after_account_drift_and_restart_without_provider_evidence() {
        let (directory, _workspace, store, execution_id, receipt, request) = staged_fixture();
        let mut snapshot = store.catalog().unwrap();
        let account_id = request
            .action
            .scope
            .credential_binding
            .as_ref()
            .unwrap()
            .account_id;
        snapshot
            .catalog
            .accounts
            .iter_mut()
            .find(|account| account.metadata.id == account_id)
            .unwrap()
            .provider_identity = "changed-while-awaiting".into();
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: snapshot.revision,
                catalog: snapshot.catalog,
            })
            .unwrap();
        drop(store);
        let store = Store::open(directory.path().join("provider-execution.db")).unwrap();
        {
            let mut connection = store.connection().unwrap();
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            expire(&transaction, request.expires_at_ms).unwrap();
            transaction.commit().unwrap();
        }
        let run = store
            .catalog()
            .unwrap()
            .catalog
            .runs
            .into_iter()
            .find(|run| run.metadata.id == receipt.run_id)
            .unwrap();
        assert_eq!(run.state, NormalizedRunState::Cancelled);
        assert!(store.approval(request.id).unwrap().decision.is_none());
        assert_no_provider_evidence(&store, execution_id);
    }

    #[test]
    fn staged_event_failure_rolls_back_request_run_graph_plan_and_ledger() {
        let (_directory, _workspace, store, execution_id) = provider_execution_fixture();
        let graph = store.graph_execution(execution_id).unwrap();
        let node_id = graph.nodes[0].node_id;
        attach_selected_account(&store, execution_id, node_id);
        let mut snapshot = store.catalog().unwrap();
        snapshot.catalog.projects[0].policy.ceiling.credential = PermissionLevel::Use;
        snapshot.catalog.roles[0].policy.ceiling.credential = PermissionLevel::Use;
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: snapshot.revision,
                catalog: snapshot.catalog,
            })
            .unwrap();
        let before_catalog = store.catalog().unwrap();
        let before_graph = store.graph_execution(execution_id).unwrap();
        let binding = resolve_provider_run_binding(
            &before_catalog.catalog,
            &store.m3_catalog().unwrap().catalog,
            &before_graph,
            node_id,
        )
        .unwrap();
        store
            .connection()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER fixture_reject_staged_event
                 BEFORE INSERT ON event_journal
                 WHEN NEW.event_type='provider_launch.staged' BEGIN
                 SELECT RAISE(ABORT, 'fixture staged event failure'); END;",
            )
            .unwrap();
        assert!(store
            .begin_provider_graph_node_run(
                execution_id,
                BeginGraphNodeRunCommand {
                    expected_catalog_revision: before_catalog.revision,
                    expected_graph_revision: before_graph.revision,
                    node_id,
                    owner: provider_executor::owner(node_id),
                },
                &binding,
            )
            .is_err());
        assert_eq!(store.catalog().unwrap(), before_catalog);
        assert_eq!(store.graph_execution(execution_id).unwrap(), before_graph);
        for table in [
            "approval_requests",
            "staged_provider_launches",
            "provider_launch_plans",
            "graph_node_runs",
        ] {
            let count: u64 = store
                .connection()
                .unwrap()
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "{table} must roll back");
        }
        assert_no_provider_evidence(&store, execution_id);
    }

    #[test]
    fn schema_eleven_upgrade_never_infers_staged_intent() {
        let (directory, _workspace, store, execution_id) = provider_execution_fixture();
        let receipt = begin_fixture_node(&store, execution_id);
        store
            .connection()
            .unwrap()
            .execute_batch(
                "DROP TABLE provider_dispatch_claims; DROP TABLE staged_provider_launches;
                 DELETE FROM schema_migrations WHERE version>=12;",
            )
            .unwrap();
        drop(store);

        let upgraded = Store::open(directory.path().join("provider-execution.db")).unwrap();
        assert_eq!(upgraded.schema_version().unwrap(), SCHEMA_VERSION);
        assert!(provider_launch::ProviderLaunchPlan::load(
            &upgraded.connection().unwrap(),
            receipt.run_id
        )
        .is_ok());
        let count: u64 = upgraded
            .connection()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM staged_provider_launches", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }
}
