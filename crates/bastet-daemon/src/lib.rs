//! Durable state primitives for the Bastet Workstation local daemon.

pub mod sandbox;

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use bastet_core::{
    ApprovalError, ApprovalRequestId, CatalogError, GraphError, GraphExecution, GraphNodeId,
    GraphRunId, IdentityCatalog, M3Catalog, RunId,
};
use bastet_protocol::{
    ApprovalList, ApprovalReceipt, ApprovalRecord, CancelRunReceipt, CatalogReceipt,
    CatalogSnapshot, CheckpointCommand, CheckpointReceipt, CreateApprovalCommand,
    CreateGraphExecutionCommand, DaemonLifecycle, DaemonSnapshot, DecideApprovalCommand,
    EventEnvelope, GraphExecutionList, GraphExecutionReceipt, M3CatalogSnapshot,
    ReplaceCatalogCommand, ReplaceM3CatalogCommand, PROTOCOL_VERSION,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

const SCHEMA_VERSION: u32 = 5;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("state revision conflict: expected {expected}, actual {actual}")]
    RevisionConflict { expected: u64, actual: u64 },
    #[error("database schema version {actual} is newer than supported version {supported}")]
    UnsupportedSchemaVersion { actual: u32, supported: u32 },
    #[error("invalid daemon lifecycle: expected {expected}, actual {actual}")]
    InvalidLifecycle {
        expected: &'static str,
        actual: String,
    },
    #[error("store mutex was poisoned")]
    Poisoned,
    #[error("identity catalog is invalid: {0}")]
    InvalidCatalog(#[from] CatalogError),
    #[error("identity catalog serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("approval is invalid: {0}")]
    InvalidApproval(#[from] ApprovalError),
    #[error("approval request was not found")]
    ApprovalNotFound,
    #[error("approval request already exists or has already been decided")]
    ApprovalConflict,
    #[error("state revision overflow")]
    RevisionOverflow,
    #[error("run was not found")]
    RunNotFound,
    #[error("run cannot be cancelled from state {0}")]
    InvalidRunState(String),
    #[error("provider run controller rejected cancellation")]
    RunControlRejected,
    #[error("graph execution is invalid: {0}")]
    InvalidGraph(#[from] GraphError),
    #[error("M3 catalog is invalid: {0}")]
    InvalidM3(String),
    #[error("graph execution was not found")]
    GraphNotFound,
    #[error("graph execution already exists")]
    GraphConflict,
}

#[derive(Clone)]
pub struct Store {
    connection: Arc<Mutex<Connection>>,
}

#[derive(Clone)]
struct AppState {
    store: Store,
    shutdown: Option<watch::Sender<bool>>,
    run_controller: Arc<dyn RunController>,
}

pub trait RunController: Send + Sync + 'static {
    fn cancel(&self, run_id: RunId) -> Result<(), RunControlError>;
}

#[derive(Debug, Error)]
#[error("provider run controller rejected cancellation")]
pub struct RunControlError;

#[derive(Clone, Default)]
pub struct RunControllerRegistry {
    controllers: Arc<RwLock<HashMap<RunId, Arc<dyn RunController>>>>,
}

impl RunControllerRegistry {
    pub fn register(
        &self,
        run_id: RunId,
        controller: Arc<dyn RunController>,
    ) -> Result<(), RunControlError> {
        let mut controllers = self.controllers.write().map_err(|_| RunControlError)?;
        if controllers.contains_key(&run_id) {
            return Err(RunControlError);
        }
        controllers.insert(run_id, controller);
        Ok(())
    }

    pub fn unregister(&self, run_id: RunId) -> Result<bool, RunControlError> {
        Ok(self
            .controllers
            .write()
            .map_err(|_| RunControlError)?
            .remove(&run_id)
            .is_some())
    }
}

impl RunController for RunControllerRegistry {
    fn cancel(&self, run_id: RunId) -> Result<(), RunControlError> {
        let controllers = self.controllers.read().map_err(|_| RunControlError)?;
        controllers
            .get(&run_id)
            .ok_or(RunControlError)?
            .cancel(run_id)
    }
}

struct UnavailableRunController;
impl RunController for UnavailableRunController {
    fn cancel(&self, _run_id: RunId) -> Result<(), RunControlError> {
        Err(RunControlError)
    }
}

#[derive(Deserialize)]
struct EventsQuery {
    #[serde(default)]
    after_sequence: u64,
}

pub fn router(store: Store) -> Router {
    build_router(store, None)
}

pub fn router_with_shutdown(store: Store, shutdown_signal: watch::Sender<bool>) -> Router {
    build_router(store, Some(shutdown_signal))
}

fn build_router(store: Store, shutdown_signal: Option<watch::Sender<bool>>) -> Router {
    build_router_with_controller(store, shutdown_signal, Arc::new(UnavailableRunController))
}

pub fn router_with_run_controller(store: Store, run_controller: Arc<dyn RunController>) -> Router {
    build_router_with_controller(store, None, run_controller)
}

pub fn router_with_shutdown_and_run_controller(
    store: Store,
    shutdown_signal: watch::Sender<bool>,
    run_controller: Arc<dyn RunController>,
) -> Router {
    build_router_with_controller(store, Some(shutdown_signal), run_controller)
}

fn build_router_with_controller(
    store: Store,
    shutdown_signal: Option<watch::Sender<bool>>,
    run_controller: Arc<dyn RunController>,
) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/events", get(events))
        .route("/v1/catalog", get(catalog).put(replace_catalog))
        .route("/v1/m3", get(m3_catalog).put(replace_m3_catalog))
        .route(
            "/v1/graphs",
            get(graph_executions).post(create_graph_execution),
        )
        .route("/v1/approvals", get(approvals).post(create_approval))
        .route(
            "/v1/approvals/{request_id}",
            get(approval).post(decide_approval),
        )
        .route("/v1/checkpoints", post(checkpoint))
        .route("/v1/power/suspend", post(suspend))
        .route("/v1/power/resume", post(resume))
        .route("/v1/shutdown", post(shutdown))
        .route("/v1/runs/{run_id}/cancel", post(cancel_run))
        .with_state(AppState {
            store,
            shutdown: shutdown_signal,
            run_controller,
        })
}

async fn cancel_run(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<Uuid>,
    Json(command): Json<bastet_protocol::CancelRunCommand>,
) -> Result<Json<CancelRunReceipt>, ApiError> {
    let path_id = RunId::from_bytes(*run_id.as_bytes());
    if path_id != command.run_id {
        return Err(StoreError::RunNotFound.into());
    }
    state
        .run_controller
        .cancel(path_id)
        .map_err(|_| StoreError::RunControlRejected)?;
    Ok(Json(state.store.record_provider_cancel_accepted(
        path_id,
        command.expected_catalog_revision,
    )?))
}

async fn health(State(state): State<AppState>) -> Result<Json<DaemonSnapshot>, ApiError> {
    Ok(Json(state.store.snapshot()?))
}

async fn events(
    State(state): State<AppState>,
    Query(query): Query<EventsQuery>,
) -> Result<Json<Vec<EventEnvelope>>, ApiError> {
    Ok(Json(state.store.events_after(query.after_sequence)?))
}

async fn checkpoint(
    State(state): State<AppState>,
    Json(command): Json<CheckpointCommand>,
) -> Result<Json<CheckpointReceipt>, ApiError> {
    Ok(Json(state.store.checkpoint(command)?))
}

async fn catalog(State(state): State<AppState>) -> Result<Json<CatalogSnapshot>, ApiError> {
    Ok(Json(state.store.catalog()?))
}

async fn replace_catalog(
    State(state): State<AppState>,
    Json(command): Json<ReplaceCatalogCommand>,
) -> Result<Json<CatalogReceipt>, ApiError> {
    Ok(Json(state.store.replace_catalog(command)?))
}

async fn m3_catalog(State(state): State<AppState>) -> Result<Json<M3CatalogSnapshot>, ApiError> {
    Ok(Json(state.store.m3_catalog()?))
}

async fn replace_m3_catalog(
    State(state): State<AppState>,
    Json(command): Json<ReplaceM3CatalogCommand>,
) -> Result<Json<CatalogReceipt>, ApiError> {
    Ok(Json(state.store.replace_m3_catalog(command)?))
}

async fn graph_executions(
    State(state): State<AppState>,
) -> Result<Json<GraphExecutionList>, ApiError> {
    Ok(Json(GraphExecutionList {
        protocol_version: PROTOCOL_VERSION,
        executions: state.store.graph_executions()?,
    }))
}

async fn create_graph_execution(
    State(state): State<AppState>,
    Json(command): Json<CreateGraphExecutionCommand>,
) -> Result<Json<GraphExecutionReceipt>, ApiError> {
    Ok(Json(
        state.store.create_graph_execution(&command.execution)?,
    ))
}

async fn create_approval(
    State(state): State<AppState>,
    Json(command): Json<CreateApprovalCommand>,
) -> Result<Json<ApprovalReceipt>, ApiError> {
    Ok(Json(state.store.create_approval(command)?))
}

async fn approvals(State(state): State<AppState>) -> Result<Json<ApprovalList>, ApiError> {
    Ok(Json(state.store.approvals()?))
}

async fn approval(
    State(state): State<AppState>,
    AxumPath(request_id): AxumPath<Uuid>,
) -> Result<Json<ApprovalRecord>, ApiError> {
    Ok(Json(state.store.approval(
        ApprovalRequestId::from_bytes(*request_id.as_bytes()),
    )?))
}

async fn decide_approval(
    State(state): State<AppState>,
    AxumPath(request_id): AxumPath<Uuid>,
    Json(command): Json<DecideApprovalCommand>,
) -> Result<Json<ApprovalReceipt>, ApiError> {
    if command.decision.request_id.value() != request_id {
        return Err(StoreError::InvalidApproval(ApprovalError::HashMismatch).into());
    }
    Ok(Json(state.store.decide_approval(command)?))
}

async fn shutdown(
    State(state): State<AppState>,
    Json(command): Json<CheckpointCommand>,
) -> Result<Json<CheckpointReceipt>, ApiError> {
    let receipt = state.store.shutdown(command)?;
    if let Some(signal) = state.shutdown {
        let _ = signal.send(true);
    }
    Ok(Json(receipt))
}

async fn suspend(
    State(state): State<AppState>,
    Json(command): Json<CheckpointCommand>,
) -> Result<Json<CheckpointReceipt>, ApiError> {
    Ok(Json(state.store.suspend(command)?))
}

async fn resume(
    State(state): State<AppState>,
    Json(command): Json<CheckpointCommand>,
) -> Result<Json<EventEnvelope>, ApiError> {
    Ok(Json(state.store.resume(command)?))
}

struct ApiError(StoreError);

impl From<StoreError> for ApiError {
    fn from(value: StoreError) -> Self {
        Self(value)
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let status = match self.0 {
            StoreError::RevisionConflict { .. }
            | StoreError::InvalidLifecycle { .. }
            | StoreError::ApprovalConflict
            | StoreError::GraphConflict
            | StoreError::InvalidRunState(_) => StatusCode::CONFLICT,
            StoreError::ApprovalNotFound | StoreError::RunNotFound | StoreError::GraphNotFound => {
                StatusCode::NOT_FOUND
            }
            StoreError::RunControlRejected => StatusCode::SERVICE_UNAVAILABLE,
            StoreError::InvalidCatalog(_)
            | StoreError::InvalidApproval(_)
            | StoreError::InvalidGraph(_)
            | StoreError::InvalidM3(_)
            | StoreError::Serialization(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(serde_json::json!({"error": self.0.to_string()})),
        )
            .into_response()
    }
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let mut connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        apply_migrations(&mut connection)?;
        let initialized = connection.execute(
            "INSERT OR IGNORE INTO daemon_state(singleton, daemon_id, revision, lifecycle)
             VALUES (1, ?1, 0, 'starting')",
            [Uuid::new_v4().to_string()],
        )?;
        if initialized == 0 {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current: u64 = transaction.query_row(
                "SELECT revision FROM daemon_state WHERE singleton = 1",
                [],
                |row| row.get(0),
            )?;
            transaction.execute(
                "UPDATE daemon_state SET revision = ?1, lifecycle = 'recovering'
                 WHERE singleton = 1",
                [current + 1],
            )?;
            insert_event(&transaction, "daemon.recovery_started", "{}")?;
            reconcile_catalog_for_recovery(&transaction)?;
            reconcile_graphs_for_recovery(&transaction)?;
            transaction.commit()?;
        }
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub fn journal_mode(&self) -> Result<String, StoreError> {
        Ok(self
            .connection()?
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))?)
    }

    pub fn schema_version(&self) -> Result<u32, StoreError> {
        Ok(self.connection()?.query_row(
            "SELECT MAX(version) FROM schema_migrations",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn snapshot(&self) -> Result<DaemonSnapshot, StoreError> {
        let connection = self.connection()?;
        let (daemon_id, revision, lifecycle): (String, u64, String) = connection.query_row(
            "SELECT daemon_id, revision, lifecycle FROM daemon_state WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        Ok(DaemonSnapshot {
            protocol_version: PROTOCOL_VERSION,
            daemon_id: Uuid::parse_str(&daemon_id).expect("stored daemon UUID must be valid"),
            revision,
            lifecycle: parse_lifecycle(&lifecycle),
        })
    }

    pub fn catalog(&self) -> Result<CatalogSnapshot, StoreError> {
        let connection = self.connection()?;
        let (revision, catalog_json): (u64, String) = connection.query_row(
            "SELECT revision, catalog_json FROM identity_catalog WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let catalog: IdentityCatalog = serde_json::from_str(&catalog_json)?;
        catalog.validate()?;
        Ok(CatalogSnapshot {
            protocol_version: PROTOCOL_VERSION,
            revision,
            catalog,
        })
    }

    pub fn replace_catalog(
        &self,
        command: ReplaceCatalogCommand,
    ) -> Result<CatalogReceipt, StoreError> {
        command.catalog.validate()?;
        let catalog_json = serde_json::to_string(&command.catalog)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let actual: u64 = transaction.query_row(
            "SELECT revision FROM identity_catalog WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        if actual != command.expected_revision {
            return Err(StoreError::RevisionConflict {
                expected: command.expected_revision,
                actual,
            });
        }
        let revision = actual + 1;
        transaction.execute(
            "UPDATE identity_catalog SET revision = ?1, catalog_json = ?2, updated_at = ?3
             WHERE singleton = 1",
            params![revision, catalog_json, timestamp()],
        )?;
        let event = insert_event(
            &transaction,
            "catalog.replaced",
            &serde_json::json!({
                "revision": revision,
                "agent_providers": command.catalog.agent_providers.len(),
                "projects": command.catalog.projects.len(),
                "sessions": command.catalog.sessions.len(),
                "runs": command.catalog.runs.len()
            })
            .to_string(),
        )?;
        transaction.commit()?;
        Ok(CatalogReceipt {
            protocol_version: PROTOCOL_VERSION,
            revision,
            event_sequence: event.sequence,
        })
    }

    pub fn m3_catalog(&self) -> Result<M3CatalogSnapshot, StoreError> {
        let connection = self.connection()?;
        let (revision, catalog_json): (u64, String) = connection.query_row(
            "SELECT revision, catalog_json FROM m3_catalog WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let catalog: M3Catalog = serde_json::from_str(&catalog_json)?;
        let identity_json: String = connection.query_row(
            "SELECT catalog_json FROM identity_catalog WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        let identity: IdentityCatalog = serde_json::from_str(&identity_json)?;
        validate_m3_catalog(&catalog, &identity)?;
        Ok(M3CatalogSnapshot {
            protocol_version: PROTOCOL_VERSION,
            revision,
            catalog,
        })
    }

    pub fn replace_m3_catalog(
        &self,
        command: ReplaceM3CatalogCommand,
    ) -> Result<CatalogReceipt, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let identity_json: String = transaction.query_row(
            "SELECT catalog_json FROM identity_catalog WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        let identity: IdentityCatalog = serde_json::from_str(&identity_json)?;
        validate_m3_catalog(&command.catalog, &identity)?;
        let actual: u64 = transaction.query_row(
            "SELECT revision FROM m3_catalog WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        if actual != command.expected_revision {
            return Err(StoreError::RevisionConflict {
                expected: command.expected_revision,
                actual,
            });
        }
        let revision = actual.checked_add(1).ok_or(StoreError::RevisionOverflow)?;
        transaction.execute(
            "UPDATE m3_catalog SET revision = ?1, catalog_json = ?2, updated_at = ?3
             WHERE singleton = 1",
            params![
                revision,
                serde_json::to_string(&command.catalog)?,
                timestamp()
            ],
        )?;
        let event = insert_event(
            &transaction,
            "m3.catalog_replaced",
            &serde_json::json!({
                "revision": revision,
                "pet_profiles": command.catalog.office.pet_profiles.len(),
                "meetings": command.catalog.meetings.meetings.len(),
                "documents": command.catalog.deliverables.documents.len()
            })
            .to_string(),
        )?;
        transaction.commit()?;
        Ok(CatalogReceipt {
            protocol_version: PROTOCOL_VERSION,
            revision,
            event_sequence: event.sequence,
        })
    }

    /// Persists cancellation only after the daemon-owned provider controller accepted interrupt.
    pub fn record_provider_cancel_accepted(
        &self,
        run_id: RunId,
        expected_catalog_revision: u64,
    ) -> Result<CancelRunReceipt, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (actual, catalog_json): (u64, String) = transaction.query_row(
            "SELECT revision, catalog_json FROM identity_catalog WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if actual != expected_catalog_revision {
            return Err(StoreError::RevisionConflict {
                expected: expected_catalog_revision,
                actual,
            });
        }
        let mut catalog: IdentityCatalog = serde_json::from_str(&catalog_json)?;
        let run = catalog
            .runs
            .iter_mut()
            .find(|run| run.metadata.id == run_id)
            .ok_or(StoreError::RunNotFound)?;
        if !matches!(
            run.state,
            bastet_core::NormalizedRunState::Starting
                | bastet_core::NormalizedRunState::Running
                | bastet_core::NormalizedRunState::Recovering
        ) {
            return Err(StoreError::InvalidRunState(
                format!("{:?}", run.state).to_lowercase(),
            ));
        }
        run.state = bastet_core::NormalizedRunState::Cancelling;
        run.metadata.revision = run
            .metadata
            .revision
            .checked_add(1)
            .ok_or(StoreError::RevisionOverflow)?;
        run.metadata.updated_at = timestamp();
        catalog.validate()?;
        let revision = actual.checked_add(1).ok_or(StoreError::RevisionOverflow)?;
        transaction.execute("UPDATE identity_catalog SET revision=?1, catalog_json=?2, updated_at=?3 WHERE singleton=1", params![revision, serde_json::to_string(&catalog)?, timestamp()])?;
        let event = insert_event(
            &transaction,
            "run.cancel_accepted",
            &serde_json::json!({"run_id": run_id}).to_string(),
        )?;
        transaction.commit()?;
        Ok(CancelRunReceipt {
            protocol_version: PROTOCOL_VERSION,
            run_id,
            catalog_revision: revision,
            event_sequence: event.sequence,
        })
    }

    pub fn create_graph_execution(
        &self,
        execution: &GraphExecution,
    ) -> Result<GraphExecutionReceipt, StoreError> {
        execution.validate()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO graph_executions(execution_id, revision, execution_json, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                execution.id.value().to_string(),
                execution.revision,
                serde_json::to_string(execution)?,
                timestamp()
            ],
        )?;
        if inserted == 0 {
            return Err(StoreError::GraphConflict);
        }
        let event = insert_event(
            &transaction,
            "graph.execution_created",
            &serde_json::json!({"execution_id": execution.id, "nodes": execution.nodes.len()})
                .to_string(),
        )?;
        transaction.commit()?;
        Ok(GraphExecutionReceipt {
            protocol_version: PROTOCOL_VERSION,
            execution_id: execution.id,
            revision: execution.revision,
            event_sequence: event.sequence,
        })
    }

    pub fn graph_execution(&self, id: GraphRunId) -> Result<GraphExecution, StoreError> {
        let connection = self.connection()?;
        let json: Option<String> = connection
            .query_row(
                "SELECT execution_json FROM graph_executions WHERE execution_id = ?1",
                [id.value().to_string()],
                |row| row.get(0),
            )
            .optional()?;
        let execution: GraphExecution =
            serde_json::from_str(&json.ok_or(StoreError::GraphNotFound)?)?;
        execution.validate()?;
        Ok(execution)
    }

    pub fn graph_executions(&self) -> Result<Vec<GraphExecution>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT execution_json FROM graph_executions ORDER BY updated_at, execution_id",
        )?;
        let executions = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .map(|row| {
                let execution: GraphExecution = serde_json::from_str(&row?)?;
                execution.validate()?;
                Ok(execution)
            })
            .collect();
        executions
    }

    pub fn claim_graph_nodes(
        &self,
        id: GraphRunId,
        expected_revision: u64,
        owner: &str,
        limit: usize,
    ) -> Result<Vec<GraphNodeId>, StoreError> {
        self.update_graph(id, expected_revision, "graph.nodes_claimed", |execution| {
            execution.claim_ready(owner, limit)
        })
    }

    pub fn complete_graph_node(
        &self,
        id: GraphRunId,
        expected_revision: u64,
        node_id: GraphNodeId,
        owner: &str,
        succeeded: bool,
    ) -> Result<(), StoreError> {
        self.update_graph(id, expected_revision, "graph.node_completed", |execution| {
            execution.complete(node_id, owner, succeeded)
        })
    }

    fn update_graph<T>(
        &self,
        id: GraphRunId,
        expected_revision: u64,
        event_type: &str,
        update: impl FnOnce(&mut GraphExecution) -> Result<T, GraphError>,
    ) -> Result<T, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<(u64, String)> = transaction
            .query_row(
                "SELECT revision, execution_json FROM graph_executions WHERE execution_id = ?1",
                [id.value().to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let (actual, json) = row.ok_or(StoreError::GraphNotFound)?;
        if actual != expected_revision {
            return Err(StoreError::RevisionConflict {
                expected: expected_revision,
                actual,
            });
        }
        let mut execution: GraphExecution = serde_json::from_str(&json)?;
        execution.validate()?;
        let result = update(&mut execution)?;
        execution.validate()?;
        transaction.execute(
            "UPDATE graph_executions SET revision = ?1, execution_json = ?2, updated_at = ?3
             WHERE execution_id = ?4",
            params![
                execution.revision,
                serde_json::to_string(&execution)?,
                timestamp(),
                id.value().to_string()
            ],
        )?;
        insert_event(
            &transaction,
            event_type,
            &serde_json::json!({"execution_id": id, "revision": execution.revision}).to_string(),
        )?;
        transaction.commit()?;
        Ok(result)
    }

    pub fn create_approval(
        &self,
        command: CreateApprovalCommand,
    ) -> Result<ApprovalReceipt, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let catalog_json: String = transaction.query_row(
            "SELECT catalog_json FROM identity_catalog WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        let catalog: IdentityCatalog = serde_json::from_str(&catalog_json)?;
        catalog.validate()?;
        command.request.validate_against(&catalog)?;
        let request_json = serde_json::to_string(&command.request)?;
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO approval_requests(request_id, request_hash, request_json, expires_at_ms, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                command.request.id.value().to_string(),
                command.request.request_hash,
                request_json,
                command.request.expires_at_ms,
                timestamp()
            ],
        )?;
        if inserted == 0 {
            return Err(StoreError::ApprovalConflict);
        }
        let event = insert_event(
            &transaction,
            "approval.requested",
            &serde_json::json!({"request_id": command.request.id, "risk": command.request.action.risk}).to_string(),
        )?;
        transaction.commit()?;
        Ok(ApprovalReceipt {
            protocol_version: PROTOCOL_VERSION,
            request_id: command.request.id,
            event_sequence: event.sequence,
        })
    }

    pub fn approval(&self, request_id: ApprovalRequestId) -> Result<ApprovalRecord, StoreError> {
        let connection = self.connection()?;
        let result: Option<(String, Option<String>)> = connection
            .query_row(
                "SELECT request_json, decision_json FROM approval_requests WHERE request_id = ?1",
                [request_id.value().to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let (request_json, decision_json) = result.ok_or(StoreError::ApprovalNotFound)?;
        let request = serde_json::from_str(&request_json)?;
        let decision = decision_json
            .map(|value| serde_json::from_str(&value))
            .transpose()?;
        Ok(ApprovalRecord {
            protocol_version: PROTOCOL_VERSION,
            request,
            decision,
        })
    }

    pub fn approvals(&self) -> Result<ApprovalList, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT request_json, decision_json FROM approval_requests ORDER BY rowid DESC",
        )?;
        let records = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })?
            .map(|row| {
                let (request_json, decision_json) = row?;
                Ok(ApprovalRecord {
                    protocol_version: PROTOCOL_VERSION,
                    request: serde_json::from_str(&request_json)?,
                    decision: decision_json
                        .map(|value| serde_json::from_str(&value))
                        .transpose()?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        Ok(ApprovalList {
            protocol_version: PROTOCOL_VERSION,
            records,
        })
    }

    pub fn decide_approval(
        &self,
        command: DecideApprovalCommand,
    ) -> Result<ApprovalReceipt, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<(String, Option<String>)> = transaction
            .query_row(
                "SELECT request_json, decision_json FROM approval_requests WHERE request_id = ?1",
                [command.decision.request_id.value().to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let (request_json, existing_decision) = row.ok_or(StoreError::ApprovalNotFound)?;
        if existing_decision.is_some() {
            return Err(StoreError::ApprovalConflict);
        }
        let request: bastet_core::ApprovalRequest = serde_json::from_str(&request_json)?;
        request.validate_unchanged()?;
        request.verify_decision(&command.decision)?;
        transaction.execute(
            "UPDATE approval_requests SET decision_json = ?1, decided_at = ?2 WHERE request_id = ?3 AND decision_json IS NULL",
            params![
                serde_json::to_string(&command.decision)?,
                timestamp(),
                command.decision.request_id.value().to_string()
            ],
        )?;
        let event = insert_event(
            &transaction,
            match command.decision.kind {
                bastet_core::ApprovalDecisionKind::Approve => "approval.approved",
                bastet_core::ApprovalDecisionKind::Deny => "approval.denied",
            },
            &serde_json::json!({"request_id": command.decision.request_id, "actor": command.decision.actor}).to_string(),
        )?;
        transaction.commit()?;
        Ok(ApprovalReceipt {
            protocol_version: PROTOCOL_VERSION,
            request_id: command.decision.request_id,
            event_sequence: event.sequence,
        })
    }

    pub fn mark_ready(&self) -> Result<EventEnvelope, StoreError> {
        self.transition(DaemonLifecycle::Ready, "daemon.ready", "{}")
    }

    pub fn checkpoint(&self, command: CheckpointCommand) -> Result<CheckpointReceipt, StoreError> {
        self.persist_checkpoint(
            command,
            Some("ready"),
            DaemonLifecycle::Ready,
            "daemon.checkpointed",
        )
    }

    pub fn shutdown(&self, command: CheckpointCommand) -> Result<CheckpointReceipt, StoreError> {
        self.persist_checkpoint(
            command,
            None,
            DaemonLifecycle::Stopping,
            "daemon.shutdown_requested",
        )
    }

    pub fn suspend(&self, command: CheckpointCommand) -> Result<CheckpointReceipt, StoreError> {
        self.persist_checkpoint(
            command,
            Some("ready"),
            DaemonLifecycle::Suspended,
            "daemon.suspended",
        )
    }

    pub fn resume(&self, command: CheckpointCommand) -> Result<EventEnvelope, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (actual, lifecycle): (u64, String) = transaction.query_row(
            "SELECT revision, lifecycle FROM daemon_state WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if actual != command.expected_revision {
            return Err(StoreError::RevisionConflict {
                expected: command.expected_revision,
                actual,
            });
        }
        if lifecycle != "suspended" {
            return Err(StoreError::InvalidLifecycle {
                expected: "suspended",
                actual: lifecycle,
            });
        }
        transaction.execute(
            "UPDATE daemon_state SET revision = ?1, lifecycle = 'ready' WHERE singleton = 1",
            [actual + 1],
        )?;
        let event = insert_event(
            &transaction,
            "daemon.resumed",
            &serde_json::json!({"reason": command.reason}).to_string(),
        )?;
        transaction.commit()?;
        Ok(event)
    }

    fn persist_checkpoint(
        &self,
        command: CheckpointCommand,
        required_lifecycle: Option<&'static str>,
        final_lifecycle: DaemonLifecycle,
        event_type: &str,
    ) -> Result<CheckpointReceipt, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (actual, lifecycle): (u64, String) = transaction.query_row(
            "SELECT revision, lifecycle FROM daemon_state WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if actual != command.expected_revision {
            return Err(StoreError::RevisionConflict {
                expected: command.expected_revision,
                actual,
            });
        }
        if let Some(expected) = required_lifecycle {
            if lifecycle != expected {
                return Err(StoreError::InvalidLifecycle {
                    expected,
                    actual: lifecycle,
                });
            }
        }
        let revision = actual + 1;
        transaction.execute(
            "UPDATE daemon_state SET revision = ?1, lifecycle = 'checkpointing' WHERE singleton = 1",
            [revision],
        )?;
        let event = insert_event(
            &transaction,
            event_type,
            &serde_json::json!({"reason": command.reason}).to_string(),
        )?;
        let checkpoint_id = Uuid::new_v4();
        transaction.execute(
            "INSERT INTO checkpoints(checkpoint_id, revision, event_sequence, reason, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                checkpoint_id.to_string(),
                revision,
                event.sequence,
                command.reason,
                timestamp()
            ],
        )?;
        transaction.execute(
            "UPDATE daemon_state SET lifecycle = ?1 WHERE singleton = 1",
            [lifecycle_name(&final_lifecycle)],
        )?;
        transaction.commit()?;
        Ok(CheckpointReceipt {
            protocol_version: PROTOCOL_VERSION,
            checkpoint_id,
            revision,
            event_sequence: event.sequence,
        })
    }

    pub fn events_after(&self, sequence: u64) -> Result<Vec<EventEnvelope>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT event_id, sequence, event_type, occurred_at, payload_json
             FROM event_journal WHERE sequence > ?1 ORDER BY sequence",
        )?;
        let events = statement
            .query_map([sequence], |row| {
                let event_id: String = row.get(0)?;
                Ok(EventEnvelope {
                    protocol_version: PROTOCOL_VERSION,
                    event_id: Uuid::parse_str(&event_id).expect("stored event UUID must be valid"),
                    sequence: row.get(1)?,
                    event_type: row.get(2)?,
                    occurred_at: row.get(3)?,
                    payload_json: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(events)
    }

    /// Creates a consistent SQLite backup while the live database remains open.
    pub fn backup_to(&self, path: impl AsRef<Path>) -> Result<(), StoreError> {
        let source = self.connection()?;
        let mut destination = Connection::open(path)?;
        let backup = rusqlite::backup::Backup::new(&source, &mut destination)?;
        backup.run_to_completion(64, Duration::from_millis(1), None)?;
        Ok(())
    }

    fn transition(
        &self,
        lifecycle: DaemonLifecycle,
        event_type: &str,
        payload: &str,
    ) -> Result<EventEnvelope, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current: u64 = transaction.query_row(
            "SELECT revision FROM daemon_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        transaction.execute(
            "UPDATE daemon_state SET revision = ?1, lifecycle = ?2 WHERE singleton = 1",
            params![current + 1, lifecycle_name(&lifecycle)],
        )?;
        let event = insert_event(&transaction, event_type, payload)?;
        transaction.commit()?;
        Ok(event)
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, StoreError> {
        self.connection.lock().map_err(|_| StoreError::Poisoned)
    }
}

fn apply_migrations(connection: &mut Connection) -> Result<(), StoreError> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);",
    )?;
    let current: u32 = connection.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |row| row.get(0),
    )?;
    if current > SCHEMA_VERSION {
        return Err(StoreError::UnsupportedSchemaVersion {
            actual: current,
            supported: SCHEMA_VERSION,
        });
    }
    if current < 1 {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS daemon_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                daemon_id TEXT NOT NULL, revision INTEGER NOT NULL, lifecycle TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS event_journal (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE, protocol_version INTEGER NOT NULL,
                event_type TEXT NOT NULL, occurred_at TEXT NOT NULL, payload_json TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS checkpoints (
                checkpoint_id TEXT PRIMARY KEY, revision INTEGER NOT NULL,
                event_sequence INTEGER NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL);",
        )?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (1, ?1)",
            [timestamp()],
        )?;
        transaction.commit()?;
    }
    if current < 2 {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let empty_catalog = serde_json::to_string(&IdentityCatalog::default())?;
        transaction.execute_batch(
            "CREATE TABLE identity_catalog (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                revision INTEGER NOT NULL,
                catalog_json TEXT NOT NULL,
                updated_at TEXT NOT NULL);",
        )?;
        transaction.execute(
            "INSERT INTO identity_catalog(singleton, revision, catalog_json, updated_at)
             VALUES (1, 0, ?1, ?2)",
            params![empty_catalog, timestamp()],
        )?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (2, ?1)",
            [timestamp()],
        )?;
        transaction.commit()?;
    }
    if current < 3 {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "CREATE TABLE approval_requests (
                request_id TEXT PRIMARY KEY,
                request_hash TEXT NOT NULL,
                request_json TEXT NOT NULL,
                expires_at_ms INTEGER NOT NULL,
                decision_json TEXT,
                created_at TEXT NOT NULL,
                decided_at TEXT);
             CREATE UNIQUE INDEX approval_request_hash ON approval_requests(request_hash);",
        )?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (3, ?1)",
            [timestamp()],
        )?;
        transaction.commit()?;
    }
    if current < 4 {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "CREATE TABLE graph_executions (
                execution_id TEXT PRIMARY KEY,
                revision INTEGER NOT NULL,
                execution_json TEXT NOT NULL,
                updated_at TEXT NOT NULL);",
        )?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (4, ?1)",
            [timestamp()],
        )?;
        transaction.commit()?;
    }
    if current < 5 {
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "CREATE TABLE m3_catalog (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                revision INTEGER NOT NULL,
                catalog_json TEXT NOT NULL,
                updated_at TEXT NOT NULL);",
        )?;
        transaction.execute(
            "INSERT INTO m3_catalog(singleton, revision, catalog_json, updated_at)
             VALUES (1, 0, ?1, ?2)",
            params![serde_json::to_string(&M3Catalog::default())?, timestamp()],
        )?;
        transaction.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (5, ?1)",
            [timestamp()],
        )?;
        transaction.commit()?;
    }
    Ok(())
}

fn validate_m3_catalog(catalog: &M3Catalog, identity: &IdentityCatalog) -> Result<(), StoreError> {
    catalog
        .office
        .validate(identity)
        .map_err(|error| StoreError::InvalidM3(error.to_string()))?;
    catalog
        .meetings
        .validate(identity, &catalog.office)
        .map_err(|error| StoreError::InvalidM3(error.to_string()))?;
    catalog
        .deliverables
        .validate()
        .map_err(|error| StoreError::InvalidM3(error.to_string()))?;
    Ok(())
}

fn reconcile_graphs_for_recovery(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), StoreError> {
    let rows = {
        let mut statement = transaction.prepare(
            "SELECT execution_id, execution_json FROM graph_executions ORDER BY execution_id",
        )?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows
    };
    for (id, json) in rows {
        let mut execution: GraphExecution = serde_json::from_str(&json)?;
        execution.validate()?;
        let changed = execution.reconcile_after_restart();
        if changed == 0 {
            continue;
        }
        transaction.execute(
            "UPDATE graph_executions SET revision = ?1, execution_json = ?2, updated_at = ?3
             WHERE execution_id = ?4",
            params![
                execution.revision,
                serde_json::to_string(&execution)?,
                timestamp(),
                id
            ],
        )?;
        insert_event(
            transaction,
            "graph.running_nodes_marked_uncertain",
            &serde_json::json!({"execution_id": execution.id, "nodes": changed}).to_string(),
        )?;
    }
    Ok(())
}

fn reconcile_catalog_for_recovery(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), StoreError> {
    let (catalog_revision, catalog_json): (u64, String) = transaction.query_row(
        "SELECT revision, catalog_json FROM identity_catalog WHERE singleton = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let mut catalog: IdentityCatalog = serde_json::from_str(&catalog_json)?;
    catalog.validate()?;
    let updated_at = timestamp();
    let mut changed = 0_u64;
    for run in &mut catalog.runs {
        if matches!(
            run.state,
            bastet_core::NormalizedRunState::Running
                | bastet_core::NormalizedRunState::Cancelling
                | bastet_core::NormalizedRunState::Recovering
        ) {
            run.state = bastet_core::NormalizedRunState::Uncertain;
            run.metadata.revision = run
                .metadata
                .revision
                .checked_add(1)
                .ok_or(StoreError::RevisionOverflow)?;
            run.metadata.updated_at.clone_from(&updated_at);
            changed = changed.checked_add(1).ok_or(StoreError::RevisionOverflow)?;
        }
    }
    if changed == 0 {
        return Ok(());
    }
    catalog.validate()?;
    let revision = catalog_revision
        .checked_add(1)
        .ok_or(StoreError::RevisionOverflow)?;
    transaction.execute(
        "UPDATE identity_catalog SET revision = ?1, catalog_json = ?2, updated_at = ?3
         WHERE singleton = 1",
        params![revision, serde_json::to_string(&catalog)?, updated_at],
    )?;
    insert_event(
        transaction,
        "catalog.runs_marked_uncertain",
        &serde_json::json!({"revision": revision, "runs": changed}).to_string(),
    )?;
    Ok(())
}

pub fn has_checkpoint_for_revision(store: &Store, revision: u64) -> Result<bool, StoreError> {
    Ok(store
        .connection()?
        .query_row(
            "SELECT 1 FROM checkpoints WHERE revision = ?1 LIMIT 1",
            [revision],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false))
}

fn insert_event(
    transaction: &rusqlite::Transaction<'_>,
    event_type: &str,
    payload: &str,
) -> Result<EventEnvelope, rusqlite::Error> {
    let event_id = Uuid::new_v4();
    let occurred_at = timestamp();
    transaction.execute(
        "INSERT INTO event_journal(event_id, protocol_version, event_type, occurred_at, payload_json)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![event_id.to_string(), PROTOCOL_VERSION, event_type, occurred_at, payload])?;
    Ok(EventEnvelope {
        protocol_version: PROTOCOL_VERSION,
        event_id,
        sequence: transaction.last_insert_rowid() as u64,
        event_type: event_type.to_owned(),
        occurred_at,
        payload_json: payload.to_owned(),
    })
}

fn lifecycle_name(value: &DaemonLifecycle) -> &'static str {
    match value {
        DaemonLifecycle::Starting => "starting",
        DaemonLifecycle::Ready => "ready",
        DaemonLifecycle::Checkpointing => "checkpointing",
        DaemonLifecycle::Suspended => "suspended",
        DaemonLifecycle::Stopping => "stopping",
        DaemonLifecycle::Recovering => "recovering",
    }
}

fn parse_lifecycle(value: &str) -> DaemonLifecycle {
    match value {
        "ready" => DaemonLifecycle::Ready,
        "checkpointing" => DaemonLifecycle::Checkpointing,
        "suspended" => DaemonLifecycle::Suspended,
        "stopping" => DaemonLifecycle::Stopping,
        "recovering" => DaemonLifecycle::Recovering,
        _ => DaemonLifecycle::Starting,
    }
}

fn timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_millis()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use bastet_core::{
        Account, AccountId, AgentInstance, AgentInstanceId, AgentProvider, AgentProviderId,
        ApprovalAction, ApprovalDecision, ApprovalDecisionKind, ApprovalRequest, ApprovalRequestId,
        ApprovalRisk, ApprovalScope, CredentialBackend, CredentialReference, CredentialReferenceId,
        EntityLifecycle, EntityMetadata, Model, ModelId, ModelProvider, ModelProviderId,
        PermissionLevel, PolicyCeiling, PolicyLayer, Project, ProjectId, Provenance, Role, RoleId,
        Run, RunId, ScopedPolicy, Session, SessionId,
    };
    use tempfile::tempdir;
    use tower::ServiceExt;

    struct AcceptingRunController;

    impl RunController for AcceptingRunController {
        fn cancel(&self, _run_id: RunId) -> Result<(), RunControlError> {
            Ok(())
        }
    }

    #[test]
    fn run_controller_registry_routes_exactly_one_owned_run() {
        let registry = RunControllerRegistry::default();
        let owned = RunId::from_bytes([21; 16]);
        let unknown = RunId::from_bytes([22; 16]);
        registry
            .register(owned, Arc::new(AcceptingRunController))
            .unwrap();
        assert!(registry
            .register(owned, Arc::new(AcceptingRunController))
            .is_err());
        assert!(registry.cancel(owned).is_ok());
        assert!(registry.cancel(unknown).is_err());
        assert!(registry.unregister(owned).unwrap());
        assert!(registry.cancel(owned).is_err());
    }

    fn metadata<I>(id: I) -> EntityMetadata<I> {
        EntityMetadata {
            id,
            revision: 0,
            created_at: "2026-09-06T00:00:00Z".into(),
            updated_at: "2026-09-06T00:00:00Z".into(),
            provenance: Provenance {
                source_kind: "test_fixture".into(),
                source_id: "m2.5".into(),
                recorded_by: "bastet-daemon".into(),
            },
            lifecycle: EntityLifecycle::Active,
        }
    }

    fn catalog_fixture() -> IdentityCatalog {
        let credential_id = CredentialReferenceId::from_bytes([1; 16]);
        let agent_provider_id = AgentProviderId::from_bytes([2; 16]);
        let model_provider_id = ModelProviderId::from_bytes([3; 16]);
        let account_id = AccountId::from_bytes([4; 16]);
        let model_id = ModelId::from_bytes([5; 16]);
        let observe = PolicyCeiling {
            filesystem: PermissionLevel::Observe,
            network: PermissionLevel::Deny,
            process: PermissionLevel::Observe,
            device: PermissionLevel::Deny,
            credential: PermissionLevel::Deny,
            persistent_approval: false,
        };
        let agent_instance_id = AgentInstanceId::from_bytes([6; 16]);
        let project_id = ProjectId::from_bytes([7; 16]);
        let session_id = SessionId::from_bytes([9; 16]);
        IdentityCatalog {
            credential_references: vec![CredentialReference {
                metadata: metadata(credential_id),
                backend: CredentialBackend::MacosKeychain,
                service: "dev.bastet.workstation.agy".into(),
                account_label: "opaque-default".into(),
            }],
            agent_providers: vec![AgentProvider {
                metadata: metadata(agent_provider_id),
                adapter_kind: "agy_cli".into(),
                display_name: "Agy CLI".into(),
            }],
            model_providers: vec![ModelProvider {
                metadata: metadata(model_provider_id),
                provider_key: "google".into(),
                display_name: "Google".into(),
            }],
            accounts: vec![Account {
                metadata: metadata(account_id),
                agent_provider_id,
                provider_identity: "agy-default".into(),
                credential_reference_id: Some(credential_id),
            }],
            models: vec![Model {
                metadata: metadata(model_id),
                model_provider_id,
                provider_model_id: "gemini-fixture".into(),
                reasoning_controls: vec!["low".into()],
            }],
            agent_instances: vec![AgentInstance {
                metadata: metadata(agent_instance_id),
                agent_provider_id,
                account_id: Some(account_id),
                default_model_id: Some(model_id),
            }],
            projects: vec![Project {
                metadata: metadata(project_id),
                name: "Fixture".into(),
                workspace_root: "/fixture".into(),
                policy: ScopedPolicy {
                    layer: PolicyLayer::Project,
                    ceiling: observe.clone(),
                },
            }],
            roles: vec![Role {
                metadata: metadata(RoleId::from_bytes([8; 16])),
                name: "Researcher".into(),
                responsibilities: vec!["research".into()],
                policy: ScopedPolicy {
                    layer: PolicyLayer::RoleOrAgent,
                    ceiling: observe,
                },
            }],
            sessions: vec![Session {
                metadata: metadata(session_id),
                agent_instance_id,
                project_id,
                provider_session_id: Some("provider-session-fixture".into()),
            }],
            runs: vec![Run {
                metadata: metadata(RunId::from_bytes([10; 16])),
                session_id,
                model_id,
                state: bastet_core::NormalizedRunState::Starting,
                started_at: None,
                finished_at: None,
            }],
        }
    }

    fn approval_fixture() -> ApprovalRequest {
        ApprovalRequest::create(
            ApprovalRequestId::from_bytes([11; 16]),
            100,
            200,
            ApprovalAction {
                agent_instance_id: AgentInstanceId::from_bytes([6; 16]),
                role_id: Some(RoleId::from_bytes([8; 16])),
                action_key: "agent.write_file".into(),
                reason_key: "approval.reason.report".into(),
                consequence_key: "approval.consequence.workspace_change".into(),
                risk: ApprovalRisk::Medium,
                scope: ApprovalScope {
                    project_id: ProjectId::from_bytes([7; 16]),
                    run_id: Some(RunId::from_bytes([10; 16])),
                    filesystem_roots: vec!["/fixture".into()],
                    data_scopes: vec![],
                    network_destinations: vec![],
                    credential_reference_ids: vec![],
                    destination: None,
                },
                requested_policy: ScopedPolicy {
                    layer: PolicyLayer::SingleRun,
                    ceiling: PolicyCeiling {
                        filesystem: PermissionLevel::Observe,
                        network: PermissionLevel::Deny,
                        process: PermissionLevel::Observe,
                        device: PermissionLevel::Deny,
                        credential: PermissionLevel::Deny,
                        persistent_approval: false,
                    },
                },
            },
            &catalog_fixture().roles[0].policy,
        )
        .unwrap()
    }

    fn graph_fixture() -> GraphExecution {
        let left = GraphNodeId::from_bytes([61; 16]);
        let right = GraphNodeId::from_bytes([62; 16]);
        GraphExecution::start(
            GraphRunId::from_bytes([60; 16]),
            bastet_core::WorkflowGraph {
                decision_baseline_id: bastet_core::DecisionBaselineId::from_bytes([59; 16]),
                nodes: vec![
                    bastet_core::GraphNode {
                        id: left,
                        kind: bastet_core::GraphNodeKind::Research,
                        role_id: RoleId::from_bytes([8; 16]),
                        title: "Research A".into(),
                        needs: vec![],
                    },
                    bastet_core::GraphNode {
                        id: right,
                        kind: bastet_core::GraphNodeKind::Research,
                        role_id: RoleId::from_bytes([8; 16]),
                        title: "Research B".into(),
                        needs: vec![],
                    },
                    bastet_core::GraphNode {
                        id: GraphNodeId::from_bytes([63; 16]),
                        kind: bastet_core::GraphNodeKind::Join,
                        role_id: RoleId::from_bytes([8; 16]),
                        title: "Join".into(),
                        needs: vec![left, right],
                    },
                ],
            },
        )
        .unwrap()
    }

    #[test]
    fn enables_wal_and_applies_forward_migration() {
        let directory = tempdir().unwrap();
        let store = Store::open(directory.path().join("bastet.db")).unwrap();
        assert_eq!(store.journal_mode().unwrap().to_lowercase(), "wal");
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
    }

    #[test]
    fn upgrades_v0_fixture_without_replacing_existing_identity() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("v0.db");
        let daemon_id = Uuid::new_v4();
        let fixture = Connection::open(&path).unwrap();
        fixture
            .execute_batch(
                "CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
                 CREATE TABLE daemon_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    daemon_id TEXT NOT NULL, revision INTEGER NOT NULL, lifecycle TEXT NOT NULL);
                 CREATE TABLE event_journal (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE, protocol_version INTEGER NOT NULL,
                    event_type TEXT NOT NULL, occurred_at TEXT NOT NULL, payload_json TEXT NOT NULL);",
            )
            .unwrap();
        fixture
            .execute(
                "INSERT INTO daemon_state(singleton, daemon_id, revision, lifecycle)
                 VALUES (1, ?1, 41, 'ready')",
                [daemon_id.to_string()],
            )
            .unwrap();
        drop(fixture);

        let upgraded = Store::open(&path).unwrap();
        let snapshot = upgraded.snapshot().unwrap();
        assert_eq!(upgraded.schema_version().unwrap(), SCHEMA_VERSION);
        assert_eq!(snapshot.daemon_id, daemon_id);
        assert_eq!(snapshot.revision, 42);
        assert_eq!(snapshot.lifecycle, DaemonLifecycle::Recovering);
        upgraded.mark_ready().unwrap();
        upgraded
            .checkpoint(CheckpointCommand {
                expected_revision: 43,
                reason: "post-upgrade fixture".into(),
            })
            .unwrap();
    }

    #[test]
    fn refuses_database_from_a_newer_schema() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("future.db");
        let fixture = Connection::open(&path).unwrap();
        fixture
            .execute_batch(
                "CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
                 INSERT INTO schema_migrations(version, applied_at) VALUES (99, 'future');",
            )
            .unwrap();
        drop(fixture);

        assert!(matches!(
            Store::open(&path),
            Err(StoreError::UnsupportedSchemaVersion {
                actual: 99,
                supported: SCHEMA_VERSION
            })
        ));
    }

    #[test]
    fn upgrades_previous_v1_fixture_with_empty_valid_catalog() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("v1.db");
        let daemon_id = Uuid::new_v4();
        let fixture = Connection::open(&path).unwrap();
        fixture
            .execute_batch(
                "CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
                 INSERT INTO schema_migrations(version, applied_at) VALUES (1, 'fixture');
                 CREATE TABLE daemon_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    daemon_id TEXT NOT NULL, revision INTEGER NOT NULL, lifecycle TEXT NOT NULL);
                 CREATE TABLE event_journal (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE, protocol_version INTEGER NOT NULL,
                    event_type TEXT NOT NULL, occurred_at TEXT NOT NULL, payload_json TEXT NOT NULL);
                 CREATE TABLE checkpoints (
                    checkpoint_id TEXT PRIMARY KEY, revision INTEGER NOT NULL,
                    event_sequence INTEGER NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL);",
            )
            .unwrap();
        fixture
            .execute(
                "INSERT INTO daemon_state(singleton, daemon_id, revision, lifecycle)
                 VALUES (1, ?1, 9, 'ready')",
                [daemon_id.to_string()],
            )
            .unwrap();
        drop(fixture);

        let upgraded = Store::open(&path).unwrap();
        assert_eq!(upgraded.schema_version().unwrap(), SCHEMA_VERSION);
        assert_eq!(upgraded.snapshot().unwrap().daemon_id, daemon_id);
        assert_eq!(upgraded.catalog().unwrap().revision, 0);
        assert_eq!(
            upgraded.catalog().unwrap().catalog,
            IdentityCatalog::default()
        );
    }

    #[test]
    fn catalog_is_validated_durable_and_revision_guarded() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("catalog.db");
        let store = Store::open(&path).unwrap();
        let catalog = catalog_fixture();
        let receipt = store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: 0,
                catalog: catalog.clone(),
            })
            .unwrap();
        assert_eq!(receipt.revision, 1);
        assert_eq!(store.catalog().unwrap().catalog, catalog);
        assert!(matches!(
            store.replace_catalog(ReplaceCatalogCommand {
                expected_revision: 0,
                catalog: catalog_fixture(),
            }),
            Err(StoreError::RevisionConflict {
                expected: 0,
                actual: 1
            })
        ));
        drop(store);

        let reopened = Store::open(&path).unwrap();
        let persisted = reopened.catalog().unwrap();
        assert_eq!(persisted.revision, 1);
        assert_eq!(persisted.catalog, catalog);
        let events = reopened.events_after(0).unwrap();
        let replaced = events
            .iter()
            .find(|event| event.event_type == "catalog.replaced")
            .unwrap();
        assert!(!replaced.payload_json.contains("opaque-default"));
        assert!(!replaced.payload_json.contains("dev.bastet.workstation.agy"));
    }

    #[test]
    fn active_run_is_marked_uncertain_atomically_on_restart() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("active-run.db");
        let store = Store::open(&path).unwrap();
        let mut catalog = catalog_fixture();
        catalog.runs[0].state = bastet_core::NormalizedRunState::Running;
        catalog.runs[0].started_at = Some("2026-09-06T00:00:01Z".into());
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: 0,
                catalog,
            })
            .unwrap();
        drop(store);

        let reopened = Store::open(&path).unwrap();
        let recovered = reopened.catalog().unwrap();
        assert_eq!(recovered.revision, 2);
        assert_eq!(
            recovered.catalog.runs[0].state,
            bastet_core::NormalizedRunState::Uncertain
        );
        assert_eq!(recovered.catalog.runs[0].metadata.revision, 1);
        let events = reopened.events_after(0).unwrap();
        let reconcile = events
            .iter()
            .find(|event| event.event_type == "catalog.runs_marked_uncertain")
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&reconcile.payload_json).unwrap(),
            serde_json::json!({"revision": 2, "runs": 1})
        );
    }

    #[test]
    fn invalid_catalog_is_rejected_before_revision_changes() {
        let directory = tempdir().unwrap();
        let store = Store::open(directory.path().join("invalid.db")).unwrap();
        let mut catalog = catalog_fixture();
        catalog.credential_references.clear();
        assert!(matches!(
            store.replace_catalog(ReplaceCatalogCommand {
                expected_revision: 0,
                catalog,
            }),
            Err(StoreError::InvalidCatalog(_))
        ));
        assert_eq!(store.catalog().unwrap().revision, 0);
    }

    #[test]
    fn journals_state_before_returning_ready() {
        let directory = tempdir().unwrap();
        let store = Store::open(directory.path().join("bastet.db")).unwrap();
        let event = store.mark_ready().unwrap();
        assert_eq!(store.snapshot().unwrap().lifecycle, DaemonLifecycle::Ready);
        assert_eq!(store.events_after(0).unwrap(), vec![event]);
    }

    #[test]
    fn checkpoint_is_durable_and_revision_guarded() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("bastet.db");
        let store = Store::open(&path).unwrap();
        store.mark_ready().unwrap();
        let receipt = store
            .checkpoint(CheckpointCommand {
                expected_revision: 1,
                reason: "test shutdown".into(),
            })
            .unwrap();
        assert!(has_checkpoint_for_revision(&store, receipt.revision).unwrap());
        drop(store);
        let reopened = Store::open(&path).unwrap();
        assert!(has_checkpoint_for_revision(&reopened, receipt.revision).unwrap());
        assert_eq!(
            reopened.snapshot().unwrap().lifecycle,
            DaemonLifecycle::Recovering
        );
        assert_eq!(reopened.events_after(0).unwrap().len(), 3);
        assert!(matches!(
            reopened.checkpoint(CheckpointCommand {
                expected_revision: 1,
                reason: "stale retry".into()
            }),
            Err(StoreError::RevisionConflict { .. })
        ));
        reopened.mark_ready().unwrap();
        assert_eq!(
            reopened.snapshot().unwrap().lifecycle,
            DaemonLifecycle::Ready
        );
        assert_eq!(reopened.events_after(0).unwrap().len(), 4);
    }

    #[test]
    fn shutdown_is_durable_and_leaves_store_stopping() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("bastet.db");
        let store = Store::open(&path).unwrap();
        store.mark_ready().unwrap();
        let receipt = store
            .shutdown(CheckpointCommand {
                expected_revision: 1,
                reason: "test graceful shutdown".into(),
            })
            .unwrap();

        assert_eq!(
            store.snapshot().unwrap().lifecycle,
            DaemonLifecycle::Stopping
        );
        assert!(has_checkpoint_for_revision(&store, receipt.revision).unwrap());
        let events = store.events_after(0).unwrap();
        assert_eq!(
            events.last().unwrap().event_type,
            "daemon.shutdown_requested"
        );
    }

    #[test]
    fn suspend_checkpoints_before_resume_advances_state() {
        let directory = tempdir().unwrap();
        let store = Store::open(directory.path().join("bastet.db")).unwrap();
        store.mark_ready().unwrap();
        let receipt = store
            .suspend(CheckpointCommand {
                expected_revision: 1,
                reason: "simulated system sleep".into(),
            })
            .unwrap();
        assert!(has_checkpoint_for_revision(&store, receipt.revision).unwrap());
        assert_eq!(
            store.snapshot().unwrap().lifecycle,
            DaemonLifecycle::Suspended
        );
        assert!(matches!(
            store.checkpoint(CheckpointCommand {
                expected_revision: receipt.revision,
                reason: "must not admit work while suspended".into(),
            }),
            Err(StoreError::InvalidLifecycle {
                expected: "ready",
                ..
            })
        ));

        let resumed = store
            .resume(CheckpointCommand {
                expected_revision: receipt.revision,
                reason: "simulated system wake".into(),
            })
            .unwrap();
        let snapshot = store.snapshot().unwrap();
        assert_eq!(snapshot.revision, receipt.revision + 1);
        assert_eq!(snapshot.lifecycle, DaemonLifecycle::Ready);
        assert_eq!(resumed.event_type, "daemon.resumed");
    }

    #[test]
    fn approval_is_immutable_durable_and_decided_once() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("approvals.db");
        let store = Store::open(&path).unwrap();
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: 0,
                catalog: catalog_fixture(),
            })
            .unwrap();
        let request = approval_fixture();
        store
            .create_approval(CreateApprovalCommand {
                request: request.clone(),
            })
            .unwrap();
        assert!(matches!(
            store.create_approval(CreateApprovalCommand {
                request: request.clone()
            }),
            Err(StoreError::ApprovalConflict)
        ));
        let decision = ApprovalDecision {
            request_id: request.id,
            request_hash: request.request_hash.clone(),
            kind: ApprovalDecisionKind::Deny,
            decided_at_ms: 150,
            actor: "local-user".into(),
        };
        store
            .decide_approval(DecideApprovalCommand {
                decision: decision.clone(),
            })
            .unwrap();
        assert!(matches!(
            store.decide_approval(DecideApprovalCommand {
                decision: decision.clone()
            }),
            Err(StoreError::ApprovalConflict)
        ));
        drop(store);

        let reopened = Store::open(&path).unwrap();
        let record = reopened.approval(request.id).unwrap();
        assert_eq!(record.request, request);
        assert_eq!(record.decision, Some(decision));
        let events = reopened.events_after(0).unwrap();
        assert!(events
            .iter()
            .any(|event| event.event_type == "approval.requested"));
        assert!(events
            .iter()
            .any(|event| event.event_type == "approval.denied"));
        assert!(!events
            .iter()
            .any(|event| event.payload_json.contains("/fixture")));
    }

    #[test]
    fn provider_accepted_cancel_is_revision_guarded_and_durable() {
        let directory = tempdir().unwrap();
        let store = Store::open(directory.path().join("cancel.db")).unwrap();
        let mut catalog = catalog_fixture();
        catalog.runs[0].state = bastet_core::NormalizedRunState::Running;
        catalog.runs[0].started_at = Some("2026-09-06T00:00:01Z".into());
        let run_id = catalog.runs[0].metadata.id;
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: 0,
                catalog,
            })
            .unwrap();
        let receipt = store.record_provider_cancel_accepted(run_id, 1).unwrap();
        assert_eq!(receipt.catalog_revision, 2);
        assert_eq!(
            store.catalog().unwrap().catalog.runs[0].state,
            bastet_core::NormalizedRunState::Cancelling
        );
        assert!(matches!(
            store.record_provider_cancel_accepted(run_id, 1),
            Err(StoreError::RevisionConflict { .. })
        ));
        assert_eq!(
            store.events_after(0).unwrap().last().unwrap().event_type,
            "run.cancel_accepted"
        );
    }

    #[test]
    fn graph_claim_is_revision_guarded_and_running_nodes_become_uncertain_on_restart() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("graph.db");
        let store = Store::open(&path).unwrap();
        let execution = graph_fixture();
        let id = execution.id;
        store.create_graph_execution(&execution).unwrap();
        assert!(matches!(
            store.create_graph_execution(&execution),
            Err(StoreError::GraphConflict)
        ));
        let claimed = store.claim_graph_nodes(id, 0, "worker", 2).unwrap();
        assert_eq!(claimed.len(), 2);
        assert!(matches!(
            store.claim_graph_nodes(id, 0, "worker", 2),
            Err(StoreError::RevisionConflict { .. })
        ));
        drop(store);

        let reopened = Store::open(&path).unwrap();
        let recovered = reopened.graph_execution(id).unwrap();
        assert_eq!(recovered.revision, 2);
        assert!(recovered
            .nodes
            .iter()
            .take(2)
            .all(|node| node.state == bastet_core::GraphNodeState::Uncertain));
        assert!(reopened.events_after(0).unwrap().iter().any(|event| {
            event.event_type == "graph.running_nodes_marked_uncertain"
                && !event.payload_json.contains("worker")
        }));
    }

    #[test]
    fn m3_catalog_is_revision_guarded_and_durable() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("m3.db");
        let store = Store::open(&path).unwrap();
        let catalog = M3Catalog::default();
        let receipt = store
            .replace_m3_catalog(ReplaceM3CatalogCommand {
                expected_revision: 0,
                catalog: catalog.clone(),
            })
            .unwrap();
        assert_eq!(receipt.revision, 1);
        assert!(matches!(
            store.replace_m3_catalog(ReplaceM3CatalogCommand {
                expected_revision: 0,
                catalog: catalog.clone(),
            }),
            Err(StoreError::RevisionConflict { .. })
        ));
        drop(store);

        let reopened = Store::open(&path).unwrap();
        let snapshot = reopened.m3_catalog().unwrap();
        assert_eq!(snapshot.revision, 1);
        assert_eq!(snapshot.catalog, catalog);
        assert!(reopened
            .events_after(0)
            .unwrap()
            .iter()
            .any(|event| event.event_type == "m3.catalog_replaced"));
    }

    #[tokio::test]
    async fn cancel_route_persists_only_after_controller_acceptance() {
        let directory = tempdir().unwrap();
        let store = Store::open(directory.path().join("cancel-route.db")).unwrap();
        let mut catalog = catalog_fixture();
        catalog.runs[0].state = bastet_core::NormalizedRunState::Running;
        catalog.runs[0].started_at = Some("2026-09-06T00:00:01Z".into());
        let run_id = catalog.runs[0].metadata.id;
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: 0,
                catalog,
            })
            .unwrap();
        let command = bastet_protocol::CancelRunCommand {
            run_id,
            expected_catalog_revision: 1,
        };
        let request = Request::builder()
            .method("POST")
            .uri(format!("/v1/runs/{}/cancel", run_id.value()))
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&command).unwrap()))
            .unwrap();

        let response = router_with_run_controller(store.clone(), Arc::new(AcceptingRunController))
            .oneshot(request)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            store.catalog().unwrap().catalog.runs[0].state,
            bastet_core::NormalizedRunState::Cancelling
        );
    }

    #[tokio::test]
    async fn cancel_route_fails_closed_without_provider_controller() {
        let directory = tempdir().unwrap();
        let store = Store::open(directory.path().join("cancel-rejected.db")).unwrap();
        let mut catalog = catalog_fixture();
        catalog.runs[0].state = bastet_core::NormalizedRunState::Running;
        catalog.runs[0].started_at = Some("2026-09-06T00:00:01Z".into());
        let run_id = catalog.runs[0].metadata.id;
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: 0,
                catalog,
            })
            .unwrap();
        let command = bastet_protocol::CancelRunCommand {
            run_id,
            expected_catalog_revision: 1,
        };
        let request = Request::builder()
            .method("POST")
            .uri(format!("/v1/runs/{}/cancel", run_id.value()))
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&command).unwrap()))
            .unwrap();

        let response = router(store.clone()).oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            store.catalog().unwrap().catalog.runs[0].state,
            bastet_core::NormalizedRunState::Running
        );
    }

    #[test]
    fn approval_rejects_changed_content_unknown_references_and_expiry() {
        let directory = tempdir().unwrap();
        let store = Store::open(directory.path().join("approvals.db")).unwrap();
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: 0,
                catalog: catalog_fixture(),
            })
            .unwrap();
        let mut changed = approval_fixture();
        changed.action.scope.destination = Some("external.example".into());
        assert!(matches!(
            store.create_approval(CreateApprovalCommand { request: changed }),
            Err(StoreError::InvalidApproval(ApprovalError::HashMismatch))
        ));
        let mut missing = approval_fixture();
        missing.action.agent_instance_id = AgentInstanceId::new();
        missing = ApprovalRequest::create(
            missing.id,
            missing.created_at_ms,
            missing.expires_at_ms,
            missing.action,
            &catalog_fixture().roles[0].policy,
        )
        .unwrap();
        assert!(matches!(
            store.create_approval(CreateApprovalCommand { request: missing }),
            Err(StoreError::InvalidApproval(
                ApprovalError::MissingReference("agent_instance_id")
            ))
        ));

        let request = approval_fixture();
        store
            .create_approval(CreateApprovalCommand {
                request: request.clone(),
            })
            .unwrap();
        assert!(matches!(
            store.decide_approval(DecideApprovalCommand {
                decision: ApprovalDecision {
                    request_id: request.id,
                    request_hash: request.request_hash,
                    kind: ApprovalDecisionKind::Approve,
                    decided_at_ms: 201,
                    actor: "local-user".into(),
                }
            }),
            Err(StoreError::InvalidApproval(ApprovalError::Expired))
        ));
        assert!(store.approval(request.id).unwrap().decision.is_none());
    }

    #[test]
    fn online_backup_reopens_with_state_journal_and_checkpoint() {
        let directory = tempdir().unwrap();
        let live_path = directory.path().join("live.db");
        let backup_path = directory.path().join("backup.db");
        let store = Store::open(&live_path).unwrap();
        store.mark_ready().unwrap();
        let catalog = catalog_fixture();
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: 0,
                catalog: catalog.clone(),
            })
            .unwrap();
        let receipt = store
            .checkpoint(CheckpointCommand {
                expected_revision: 1,
                reason: "backup fixture".into(),
            })
            .unwrap();

        store.backup_to(&backup_path).unwrap();
        let restored = Store::open(&backup_path).unwrap();

        assert_eq!(restored.schema_version().unwrap(), SCHEMA_VERSION);
        assert_eq!(restored.catalog().unwrap().catalog, catalog);
        assert_eq!(restored.catalog().unwrap().revision, 1);
        assert!(has_checkpoint_for_revision(&restored, receipt.revision).unwrap());
        assert_eq!(restored.snapshot().unwrap().revision, receipt.revision + 1);
        let events = restored.events_after(0).unwrap();
        assert!(events
            .iter()
            .any(|event| event.event_type == "daemon.checkpointed"));
        assert_eq!(events.last().unwrap().event_type, "daemon.recovery_started");
    }
}
