//! Durable state primitives for the Bastet Workstation local daemon.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use bastet_protocol::{
    CheckpointCommand, CheckpointReceipt, DaemonLifecycle, DaemonSnapshot, EventEnvelope,
    PROTOCOL_VERSION,
};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

const SCHEMA_VERSION: u32 = 1;

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
}

#[derive(Clone)]
pub struct Store {
    connection: Arc<Mutex<Connection>>,
}

#[derive(Clone)]
struct AppState {
    store: Store,
    shutdown: Option<watch::Sender<bool>>,
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
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/events", get(events))
        .route("/v1/checkpoints", post(checkpoint))
        .route("/v1/power/suspend", post(suspend))
        .route("/v1/power/resume", post(resume))
        .route("/v1/shutdown", post(shutdown))
        .with_state(AppState {
            store,
            shutdown: shutdown_signal,
        })
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
        let status = if matches!(
            self.0,
            StoreError::RevisionConflict { .. } | StoreError::InvalidLifecycle { .. }
        ) {
            StatusCode::CONFLICT
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
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
    use tempfile::tempdir;

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
        assert_eq!(upgraded.schema_version().unwrap(), 1);
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
                supported: 1
            })
        ));
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
    fn online_backup_reopens_with_state_journal_and_checkpoint() {
        let directory = tempdir().unwrap();
        let live_path = directory.path().join("live.db");
        let backup_path = directory.path().join("backup.db");
        let store = Store::open(&live_path).unwrap();
        store.mark_ready().unwrap();
        let receipt = store
            .checkpoint(CheckpointCommand {
                expected_revision: 1,
                reason: "backup fixture".into(),
            })
            .unwrap();

        store.backup_to(&backup_path).unwrap();
        let restored = Store::open(&backup_path).unwrap();

        assert_eq!(restored.schema_version().unwrap(), SCHEMA_VERSION);
        assert!(has_checkpoint_for_revision(&restored, receipt.revision).unwrap());
        assert_eq!(restored.snapshot().unwrap().revision, receipt.revision + 1);
        let events = restored.events_after(0).unwrap();
        assert_eq!(events[1].event_type, "daemon.checkpointed");
        assert_eq!(events.last().unwrap().event_type, "daemon.recovery_started");
    }
}
