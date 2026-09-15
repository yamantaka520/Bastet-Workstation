//! Trusted broker boundary. No HTTP endpoint exposes claims or secret material.
//! Production dispatch remains disabled until provider credential injection is
//! implemented; callers must establish that support before consuming a grant.
#![cfg_attr(not(test), allow(dead_code))]

use super::*;
use bastet_core::NormalizedRunState;
use native_credentials::{CredentialReader, SecretBytes};

pub(super) struct PreparedLaunch {
    pub receipt: BeginGraphNodeRunReceipt,
    pub credential: SecretBytes,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum BrokerError {
    #[error("launch authorization rejected")]
    Authorization,
    #[error("credential lookup failed")]
    Credential,
    #[error("credential failure recovery unavailable")]
    Recovery,
}

// Private constructor and no Clone: each successful transaction creates one
// in-process opportunity to perform the native lookup, not a bearer token.
struct DispatchClaim {
    receipt: BeginGraphNodeRunReceipt,
    selection: bastet_protocol::ProviderCredentialSelection,
}

impl Store {
    pub(super) fn prepare_staged_credentials(
        &self,
        request_id: ApprovalRequestId,
        reader: &dyn CredentialReader,
    ) -> Result<PreparedLaunch, BrokerError> {
        let claim = self
            .claim_staged_dispatch(request_id, reader)
            .map_err(|_| BrokerError::Authorization)?;
        // The database mutex is released and consumption is durable before an
        // OS keychain prompt or lookup is possible. Never refund on error.
        match reader.read(&claim.selection) {
            Ok(credential) => Ok(PreparedLaunch {
                receipt: claim.receipt,
                credential,
            }),
            Err(_) => {
                self.fail_dispatch_claim(claim.receipt.run_id)
                    .map_err(|_| BrokerError::Recovery)?;
                Err(BrokerError::Credential)
            }
        }
    }

    fn claim_staged_dispatch(
        &self,
        request_id: ApprovalRequestId,
        reader: &dyn CredentialReader,
    ) -> Result<DispatchClaim, StoreError> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let lifecycle: String = tx.query_row(
            "SELECT lifecycle FROM daemon_state WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        if lifecycle != "ready" {
            return Err(StoreError::CredentialGrantRejected);
        }
        let record = credential_grants::load(&tx, request_id)?;
        let now = credential_grants::now_ms()?;
        credential_grants::validate_time(&record.request, now)?;
        if record.issued_at_ms > now
            || record.consumed_at_ms.is_some()
            || record.revoked_at_ms.is_some()
        {
            return Err(StoreError::CredentialGrantRejected);
        }
        let run_id = record
            .request
            .action
            .scope
            .run_id
            .ok_or(StoreError::CredentialGrantRejected)?;
        let state: Option<String> = tx
            .query_row(
                "SELECT state FROM staged_provider_launches WHERE request_id=?1 AND run_id=?2",
                params![request_id.value().to_string(), run_id.value().to_string()],
                |row| row.get(0),
            )
            .optional()?;
        if state.as_deref() != Some("ready") {
            return Err(StoreError::CredentialGrantRejected);
        }
        let (revision, json): (u64, String) = tx.query_row(
            "SELECT revision,catalog_json FROM identity_catalog WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let mut catalog: IdentityCatalog = serde_json::from_str(&json)?;
        let run = catalog
            .runs
            .iter_mut()
            .find(|run| run.metadata.id == run_id)
            .ok_or(StoreError::RunNotFound)?;
        staged_launches::validate_staged_run(&tx, run)?;
        credential_grants::validate_current_catalog(&tx, &record.request)?;
        let plan = provider_launch::ProviderLaunchPlan::load(&tx, run_id)?;
        let selection = plan
            .identity
            .account
            .as_ref()
            .and_then(|account| account.credential.clone())
            .ok_or(StoreError::CredentialGrantRejected)?;
        // Validate the exact immutable plan selection under the claim
        // transaction, before any irreversible mutation or native-store read.
        reader
            .validate(&selection)
            .map_err(|_| StoreError::CredentialGrantRejected)?;
        let mut graph: GraphExecution = serde_json::from_str(&tx.query_row::<String, _, _>(
            "SELECT execution_json FROM graph_executions WHERE execution_id=?1",
            [plan.execution_id.value().to_string()],
            |row| row.get(0),
        )?)?;
        graph.activate_staged_node(plan.node_id, &plan.owner, run_id)?;
        graph.validate()?;
        let started_at = now.to_string();
        run.state = NormalizedRunState::Starting;
        run.started_at = Some(started_at.clone());
        run.metadata.revision = run
            .metadata
            .revision
            .checked_add(1)
            .ok_or(StoreError::RevisionOverflow)?;
        run.metadata.updated_at = started_at.clone();
        catalog.validate()?;
        let revision = revision
            .checked_add(1)
            .ok_or(StoreError::RevisionOverflow)?;
        if tx.execute("UPDATE credential_grants SET consumed_at_ms=?1 WHERE request_id=?2 AND consumed_at_ms IS NULL AND revoked_at_ms IS NULL",
            params![now,request_id.value().to_string()])? != 1 { return Err(StoreError::CredentialGrantRejected); }
        if tx.execute("UPDATE staged_provider_launches SET state='dispatching' WHERE request_id=?1 AND state='ready'", [request_id.value().to_string()])? != 1 {
            return Err(StoreError::CredentialGrantRejected);
        }
        tx.execute("INSERT INTO provider_dispatch_claims(run_id,request_id,claimed_at_ms) VALUES (?1,?2,?3)", params![run_id.value().to_string(),request_id.value().to_string(),now])?;
        tx.execute("UPDATE identity_catalog SET revision=?1,catalog_json=?2,updated_at=?3 WHERE singleton=1", params![revision,serde_json::to_string(&catalog)?,started_at])?;
        tx.execute("UPDATE graph_executions SET revision=?1,execution_json=?2,updated_at=?3 WHERE execution_id=?4", params![graph.revision,serde_json::to_string(&graph)?,started_at,plan.execution_id.value().to_string()])?;
        insert_event(
            &tx,
            "credential.grant_consumed",
            &serde_json::json!({"request_id":request_id,"run_id":run_id}).to_string(),
        )?;
        let event = insert_event(
            &tx,
            "provider_launch.dispatch_claimed",
            &serde_json::json!({"request_id":request_id,"run_id":run_id}).to_string(),
        )?;
        tx.commit()?;
        Ok(DispatchClaim {
            selection,
            receipt: BeginGraphNodeRunReceipt {
                protocol_version: PROTOCOL_VERSION,
                execution_id: plan.execution_id,
                node_id: plan.node_id,
                session_id: plan.session_id,
                run_id,
                adapter_kind: plan.identity.adapter_kind.clone(),
                model: plan.identity.model.clone(),
                launch_identity: Some(plan.identity),
                workspace_root: plan.workspace_root,
                prompt: plan.prompt,
                catalog_revision: revision,
                graph_revision: graph.revision,
                event_sequence: event.sequence,
            },
        })
    }

    fn fail_dispatch_claim(&self, run_id: RunId) -> Result<(), StoreError> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let plan = provider_launch::ProviderLaunchPlan::load(&tx, run_id)?;
        let (revision, json): (u64, String) = tx.query_row(
            "SELECT revision,catalog_json FROM identity_catalog WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let mut catalog: IdentityCatalog = serde_json::from_str(&json)?;
        let run = catalog
            .runs
            .iter_mut()
            .find(|run| run.metadata.id == run_id)
            .ok_or(StoreError::RunNotFound)?;
        if run.state != NormalizedRunState::Starting {
            return Err(StoreError::CredentialGrantRejected);
        }
        let mut graph: GraphExecution = serde_json::from_str(&tx.query_row::<String, _, _>(
            "SELECT execution_json FROM graph_executions WHERE execution_id=?1",
            [plan.execution_id.value().to_string()],
            |row| row.get(0),
        )?)?;
        if graph
            .nodes
            .iter()
            .find(|node| node.node_id == plan.node_id)
            .and_then(|node| node.run_id)
            != Some(run_id)
        {
            return Err(StoreError::CredentialGrantRejected);
        }
        graph.finish_terminal(plan.node_id, &plan.owner, GraphNodeState::Uncertain, None)?;
        run.state = NormalizedRunState::Uncertain;
        run.metadata.revision = run
            .metadata
            .revision
            .checked_add(1)
            .ok_or(StoreError::RevisionOverflow)?;
        run.metadata.updated_at = timestamp();
        catalog.validate()?;
        if tx.execute("UPDATE staged_provider_launches SET state='uncertain' WHERE run_id=?1 AND state='dispatching'", [run_id.value().to_string()])? != 1 { return Err(StoreError::CredentialGrantRejected); }
        tx.execute("UPDATE identity_catalog SET revision=?1,catalog_json=?2,updated_at=?3 WHERE singleton=1", params![revision.checked_add(1).ok_or(StoreError::RevisionOverflow)?,serde_json::to_string(&catalog)?,timestamp()])?;
        tx.execute("UPDATE graph_executions SET revision=?1,execution_json=?2,updated_at=?3 WHERE execution_id=?4", params![graph.revision,serde_json::to_string(&graph)?,timestamp(),plan.execution_id.value().to_string()])?;
        insert_event(
            &tx,
            "provider_launch.credential_lookup_failed",
            &serde_json::json!({"run_id":run_id}).to_string(),
        )?;
        tx.commit()?;
        Ok(())
    }
}

pub(super) fn reconcile(tx: &rusqlite::Transaction<'_>) -> Result<(), StoreError> {
    let changed = tx.execute(
        "UPDATE staged_provider_launches SET state='uncertain' WHERE state='dispatching'",
        [],
    )?;
    if changed > 0 {
        insert_event(
            tx,
            "provider_launch.claims_marked_uncertain",
            &serde_json::json!({"count":changed}).to_string(),
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use native_credentials::CredentialReadError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn fixture() -> (
        tempfile::TempDir,
        tempfile::TempDir,
        Store,
        ApprovalRequestId,
    ) {
        let (directory, workspace, store, execution_id) =
            crate::tests::provider_execution_fixture();
        let graph = store.graph_execution(execution_id).unwrap();
        let node_id = graph.nodes[0].node_id;
        crate::tests::attach_selected_account(&store, execution_id, node_id);
        let mut catalog = store.catalog().unwrap();
        for project in &mut catalog.catalog.projects {
            project.policy.ceiling.credential = bastet_core::PermissionLevel::Use;
        }
        for role in &mut catalog.catalog.roles {
            role.policy.ceiling.credential = bastet_core::PermissionLevel::Use;
        }
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: catalog.revision,
                catalog: catalog.catalog,
            })
            .unwrap();
        let catalog = store.catalog().unwrap();
        let binding = resolve_provider_run_binding(
            &catalog.catalog,
            &store.m3_catalog().unwrap().catalog,
            &graph,
            node_id,
        )
        .unwrap();
        store
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
        store
            .decide_approval(DecideApprovalCommand {
                decision: bastet_core::ApprovalDecision {
                    request_id: request.id,
                    request_hash: request.request_hash,
                    kind: bastet_core::ApprovalDecisionKind::Approve,
                    decided_at_ms: 0,
                    actor: "fixture-human".into(),
                },
                credential_scope_acknowledged: true,
            })
            .unwrap();
        (directory, workspace, store, request.id)
    }

    struct Reader {
        store: Store,
        id: ApprovalRequestId,
        calls: AtomicUsize,
        fail: bool,
    }

    #[test]
    fn schema_twelve_upgrade_preserves_ready_intent_without_consumption() {
        let (directory, _workspace, store, id) = fixture();
        store
            .connection()
            .unwrap()
            .execute_batch(
                "DROP TABLE provider_dispatch_claims;
             ALTER TABLE staged_provider_launches RENAME TO current_staging;
             CREATE TABLE staged_provider_launches (
                run_id TEXT PRIMARY KEY NOT NULL, request_id TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL CHECK(state IN ('awaiting_approval','ready','cancelled')),
                staged_at_ms INTEGER NOT NULL CHECK(staged_at_ms>=0),
                finished_at_ms INTEGER, cancel_reason TEXT,
                CHECK((state='cancelled')=(finished_at_ms IS NOT NULL)),
                CHECK((state='cancelled')=(cancel_reason IS NOT NULL)),
                CHECK(finished_at_ms IS NULL OR finished_at_ms>=staged_at_ms));
             INSERT INTO staged_provider_launches SELECT * FROM current_staging;
             DROP TABLE current_staging;
             DELETE FROM schema_migrations WHERE version=13;",
            )
            .unwrap();
        drop(store);
        let upgraded = Store::open(directory.path().join("provider-execution.db")).unwrap();
        assert_eq!(upgraded.schema_version().unwrap(), SCHEMA_VERSION);
        assert_eq!(
            upgraded.approval(id).unwrap().staged_launch_state,
            Some(bastet_protocol::StagedLaunchState::Ready)
        );
        assert!(upgraded
            .credential_grant(id)
            .unwrap()
            .consumed_at_ms
            .is_none());
        let claims: u64 = upgraded
            .connection()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM provider_dispatch_claims", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(claims, 0);
    }

    #[test]
    fn restart_after_claim_before_lookup_never_replays_or_refunds() {
        let (directory, _workspace, store, id) = fixture();
        let reader = Reader {
            store: store.clone(),
            id,
            calls: AtomicUsize::new(0),
            fail: false,
        };
        let claim = store.claim_staged_dispatch(id, &reader).unwrap();
        let run_id = claim.receipt.run_id;
        drop(claim);
        drop(store);
        let restored = Store::open(directory.path().join("provider-execution.db")).unwrap();
        restored.mark_ready().unwrap();
        let reader = Reader {
            store: restored.clone(),
            id,
            calls: AtomicUsize::new(0),
            fail: false,
        };
        assert!(restored.prepare_staged_credentials(id, &reader).is_err());
        assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            restored
                .catalog()
                .unwrap()
                .catalog
                .runs
                .iter()
                .find(|run| run.metadata.id == run_id)
                .unwrap()
                .state,
            NormalizedRunState::Uncertain
        );
        assert_eq!(
            restored.approvals().unwrap().records[0].staged_launch_state,
            Some(bastet_protocol::StagedLaunchState::Uncertain)
        );
    }
    impl CredentialReader for Reader {
        fn validate(
            &self,
            _selection: &bastet_protocol::ProviderCredentialSelection,
        ) -> Result<(), CredentialReadError> {
            Ok(())
        }

        fn read(
            &self,
            selection: &bastet_protocol::ProviderCredentialSelection,
        ) -> Result<SecretBytes, CredentialReadError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            // Reacquiring the store proves the native boundary is outside its
            // mutex and sees committed authority, never an open transaction.
            let grant = self.store.credential_grant(self.id).unwrap();
            assert!(grant.consumed_at_ms.is_some());
            assert_eq!(
                selection.reference_id,
                grant
                    .request
                    .action
                    .scope
                    .credential_binding
                    .as_ref()
                    .unwrap()
                    .credential_reference_id
            );
            assert_eq!(
                selection.service,
                grant
                    .request
                    .action
                    .scope
                    .credential_binding
                    .as_ref()
                    .unwrap()
                    .service
            );
            if self.fail {
                Err(CredentialReadError::NotFound)
            } else {
                SecretBytes::new(b"synthetic-credential-never-native".to_vec())
            }
        }
    }

    struct RejectingReader {
        validate_calls: AtomicUsize,
        read_calls: AtomicUsize,
    }

    impl CredentialReader for RejectingReader {
        fn validate(
            &self,
            _selection: &bastet_protocol::ProviderCredentialSelection,
        ) -> Result<(), CredentialReadError> {
            self.validate_calls.fetch_add(1, Ordering::SeqCst);
            Err(CredentialReadError::UnsupportedBackend)
        }

        fn read(
            &self,
            _selection: &bastet_protocol::ProviderCredentialSelection,
        ) -> Result<SecretBytes, CredentialReadError> {
            self.read_calls.fetch_add(1, Ordering::SeqCst);
            Err(CredentialReadError::UnsupportedBackend)
        }
    }

    #[test]
    fn reader_preflight_rejection_leaves_ready_grant_unspent_without_claim_or_lookup() {
        let (_directory, _workspace, store, id) = fixture();
        let catalog_before = store.catalog().unwrap();
        let events_before = store.events_after(0).unwrap();
        let reader = RejectingReader {
            validate_calls: AtomicUsize::new(0),
            read_calls: AtomicUsize::new(0),
        };

        assert!(matches!(
            store.prepare_staged_credentials(id, &reader),
            Err(BrokerError::Authorization)
        ));
        assert_eq!(reader.validate_calls.load(Ordering::SeqCst), 1);
        assert_eq!(reader.read_calls.load(Ordering::SeqCst), 0);
        assert!(store.credential_grant(id).unwrap().consumed_at_ms.is_none());
        assert_eq!(store.catalog().unwrap(), catalog_before);
        assert_eq!(store.events_after(0).unwrap(), events_before);
        assert_eq!(
            store.approval(id).unwrap().staged_launch_state,
            Some(bastet_protocol::StagedLaunchState::Ready)
        );
        let claims: u64 = store
            .connection()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM provider_dispatch_claims", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(claims, 0);
    }

    #[test]
    fn policy_drift_after_approval_blocks_consumption_and_lookup() {
        let (_directory, _workspace, store, id) = fixture();
        let mut catalog = store.catalog().unwrap();
        let role_id = store.approval(id).unwrap().request.action.role_id.unwrap();
        catalog
            .catalog
            .roles
            .iter_mut()
            .find(|role| role.metadata.id == role_id)
            .unwrap()
            .policy
            .ceiling
            .network = bastet_core::PermissionLevel::Deny;
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: catalog.revision,
                catalog: catalog.catalog,
            })
            .unwrap();
        let before = store.events_after(0).unwrap();
        let reader = Reader {
            store: store.clone(),
            id,
            calls: AtomicUsize::new(0),
            fail: false,
        };
        assert!(matches!(
            store.prepare_staged_credentials(id, &reader),
            Err(BrokerError::Authorization)
        ));
        assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
        assert!(store.credential_grant(id).unwrap().consumed_at_ms.is_none());
        assert_eq!(store.events_after(0).unwrap(), before);
    }

    #[test]
    fn consumption_commits_before_one_exact_lookup_and_never_leaks_into_journal() {
        let (_directory, _workspace, store, id) = fixture();
        let reader = Reader {
            store: store.clone(),
            id,
            calls: AtomicUsize::new(0),
            fail: false,
        };
        let prepared = store.prepare_staged_credentials(id, &reader).unwrap();
        prepared
            .credential
            .with_bytes(|bytes| assert_eq!(bytes, b"synthetic-credential-never-native"));
        store.preflight_provider_launch(&prepared.receipt).unwrap();
        assert!(store.prepare_staged_credentials(id, &reader).is_err());
        assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
        assert!(!serde_json::to_string(&store.events_after(0).unwrap())
            .unwrap()
            .contains("synthetic-credential-never-native"));
        assert!(store
            .m3_catalog()
            .unwrap()
            .catalog
            .deliverables
            .costs
            .is_empty());
    }

    #[test]
    fn lookup_failure_spends_grant_and_resolves_intent_without_provider_evidence() {
        let (_directory, _workspace, store, id) = fixture();
        let reader = Reader {
            store: store.clone(),
            id,
            calls: AtomicUsize::new(0),
            fail: true,
        };
        assert!(matches!(
            store.prepare_staged_credentials(id, &reader),
            Err(BrokerError::Credential)
        ));
        assert!(store.credential_grant(id).unwrap().consumed_at_ms.is_some());
        assert_eq!(
            store.approval(id).unwrap().staged_launch_state,
            Some(bastet_protocol::StagedLaunchState::Uncertain)
        );
        assert!(store.prepare_staged_credentials(id, &reader).is_err());
        assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
        assert!(store
            .m3_catalog()
            .unwrap()
            .catalog
            .deliverables
            .costs
            .is_empty());
        store
            .shutdown(CheckpointCommand {
                expected_revision: store.snapshot().unwrap().revision,
                reason: "failed native read has no worker".into(),
            })
            .unwrap();
    }

    #[test]
    fn simultaneous_claims_allow_only_one_reader_and_restart_does_not_replay() {
        let (directory, _workspace, store, id) = fixture();
        let reader = Arc::new(Reader {
            store: store.clone(),
            id,
            calls: AtomicUsize::new(0),
            fail: false,
        });
        let mut threads = Vec::new();
        for _ in 0..2 {
            let store = store.clone();
            let reader = reader.clone();
            threads.push(std::thread::spawn(move || {
                store
                    .prepare_staged_credentials(id, reader.as_ref())
                    .is_ok()
            }));
        }
        assert_eq!(
            threads
                .into_iter()
                .map(|thread| usize::from(thread.join().unwrap()))
                .sum::<usize>(),
            1
        );
        assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
        drop(reader);
        drop(store);
        let restored = Store::open(directory.path().join("provider-execution.db")).unwrap();
        restored.mark_ready().unwrap();
        let reader = Reader {
            store: restored.clone(),
            id,
            calls: AtomicUsize::new(0),
            fail: false,
        };
        assert!(restored.prepare_staged_credentials(id, &reader).is_err());
        assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            restored.approval(id).unwrap().staged_launch_state,
            Some(bastet_protocol::StagedLaunchState::Uncertain)
        );
        assert!(restored
            .credential_grant(id)
            .unwrap()
            .consumed_at_ms
            .is_some());
    }

    #[test]
    fn journal_failure_rolls_back_claim_and_prevents_native_read() {
        let (_directory, _workspace, store, id) = fixture();
        let before = store.catalog().unwrap();
        store.connection().unwrap().execute_batch("CREATE TRIGGER reject_dispatch_event BEFORE INSERT ON event_journal WHEN NEW.event_type='provider_launch.dispatch_claimed' BEGIN SELECT RAISE(ABORT,'fixture failure'); END;").unwrap();
        let reader = Reader {
            store: store.clone(),
            id,
            calls: AtomicUsize::new(0),
            fail: false,
        };
        assert!(store.prepare_staged_credentials(id, &reader).is_err());
        assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
        assert_eq!(store.catalog().unwrap(), before);
        assert!(store.credential_grant(id).unwrap().consumed_at_ms.is_none());
        assert_eq!(
            store.approval(id).unwrap().staged_launch_state,
            Some(bastet_protocol::StagedLaunchState::Ready)
        );
        assert_eq!(
            store
                .connection()
                .unwrap()
                .query_row("SELECT COUNT(*) FROM provider_dispatch_claims", [], |row| {
                    row.get::<_, u64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn changed_authority_prevents_claim_and_native_read() {
        let (_directory, _workspace, store, id) = fixture();
        let mut catalog = store.catalog().unwrap();
        catalog.catalog.accounts[0].provider_identity = "different-account".into();
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: catalog.revision,
                catalog: catalog.catalog,
            })
            .unwrap();
        let reader = Reader {
            store: store.clone(),
            id,
            calls: AtomicUsize::new(0),
            fail: false,
        };
        assert!(store.prepare_staged_credentials(id, &reader).is_err());
        assert_eq!(reader.calls.load(Ordering::SeqCst), 0);
        assert!(store.credential_grant(id).unwrap().consumed_at_ms.is_none());
    }
}
