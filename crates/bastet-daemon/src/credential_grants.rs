//! Durable metadata-only authorization. Native secret access must happen only
//! after successful consumption inside a trusted broker, never from an HTTP caller.
//! Consumption is intentionally not exposed as an HTTP route. This module does
//! not authenticate a provider or enable selected-account execution by itself.

use super::*;
use bastet_core::{ApprovalAction, ApprovalDecision, ApprovalDecisionKind, ApprovalRequest};
use bastet_protocol::{CredentialGrantRecord, RevokeCredentialGrantCommand};
use rusqlite::Transaction;

pub(super) fn now_ms() -> Result<u64, StoreError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|value| u64::try_from(value.as_millis()).ok())
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or(StoreError::CredentialGrantRejected)
}

pub(super) fn validate_time(request: &ApprovalRequest, now: u64) -> Result<(), StoreError> {
    if now < request.created_at_ms
        || now >= request.expires_at_ms
        || request.expires_at_ms > i64::MAX as u64
    {
        return Err(StoreError::CredentialGrantRejected);
    }
    Ok(())
}

pub(super) fn validate_current_catalog(
    transaction: &Transaction<'_>,
    request: &ApprovalRequest,
) -> Result<(), StoreError> {
    let json: String = transaction.query_row(
        "SELECT catalog_json FROM identity_catalog WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?;
    let catalog: IdentityCatalog = serde_json::from_str(&json)?;
    catalog.validate()?;
    request.validate_against(&catalog)?;
    let run = catalog
        .runs
        .iter()
        .find(|run| Some(run.metadata.id) == request.action.scope.run_id)
        .ok_or(StoreError::CredentialGrantRejected)?;
    if run.state != bastet_core::NormalizedRunState::Starting || run.finished_at.is_some() {
        return Err(StoreError::CredentialGrantRejected);
    }
    let role_id = request
        .action
        .role_id
        .ok_or(StoreError::CredentialGrantRejected)?;
    let m3: M3Catalog = serde_json::from_str(&transaction.query_row::<String, _, _>(
        "SELECT catalog_json FROM m3_catalog WHERE singleton = 1",
        [],
        |row| row.get(0),
    )?)?;
    validate_m3_catalog(&m3, &catalog)?;
    let assignments: Vec<_> = m3
        .office
        .pet_assignments
        .iter()
        .filter(|assignment| {
            assignment.metadata.lifecycle == EntityLifecycle::Active
                && assignment.project_id == request.action.scope.project_id
                && assignment.agent_instance_id == request.action.agent_instance_id
        })
        .collect();
    if !assignments
        .iter()
        .any(|assignment| assignment.role_id == role_id)
    {
        return Err(StoreError::CredentialGrantRejected);
    }
    let graph_row: Option<(String, String, Option<String>)> = transaction
        .query_row(
            "SELECT execution_id, node_id, finished_at FROM graph_node_runs WHERE run_id = ?1",
            [run.metadata.id.value().to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    if let Some((execution_id, node_id, finished_at)) = graph_row {
        if finished_at.is_some() {
            return Err(StoreError::CredentialGrantRejected);
        }
        let graph: GraphExecution = serde_json::from_str(&transaction.query_row::<String, _, _>(
            "SELECT execution_json FROM graph_executions WHERE execution_id = ?1",
            [execution_id],
            |row| row.get(0),
        )?)?;
        graph.validate()?;
        let definition = graph
            .graph
            .nodes
            .iter()
            .find(|node| node.id.value().to_string() == node_id)
            .ok_or(StoreError::CredentialGrantRejected)?;
        let current = graph
            .nodes
            .iter()
            .find(|node| node.node_id == definition.id)
            .ok_or(StoreError::CredentialGrantRejected)?;
        if definition.role_id != role_id
            || current.run_id != Some(run.metadata.id)
            || current.state != GraphNodeState::Running
        {
            return Err(StoreError::CredentialGrantRejected);
        }
    } else if assignments.len() != 1 {
        // A non-graph run has no persisted role slot. Until it gains one,
        // multiple available roles are ambiguous rather than caller-selectable.
        return Err(StoreError::CredentialGrantRejected);
    }
    Ok(())
}

pub(super) fn issue(
    transaction: &Transaction<'_>,
    request: &ApprovalRequest,
    decision: &ApprovalDecision,
) -> Result<(), StoreError> {
    transaction.execute(
        "INSERT INTO credential_grants(request_id, issued_at_ms) VALUES (?1, ?2)",
        params![request.id.value().to_string(), decision.decided_at_ms],
    )?;
    insert_event(
        transaction,
        "credential.grant_issued",
        &serde_json::json!({"request_id": request.id, "run_id": request.action.scope.run_id})
            .to_string(),
    )?;
    Ok(())
}

fn load(
    connection: &Connection,
    id: ApprovalRequestId,
) -> Result<CredentialGrantRecord, StoreError> {
    let row = connection
        .query_row(
            "SELECT a.request_json, a.decision_json, g.issued_at_ms,
                g.consumed_at_ms, g.revoked_at_ms, g.revoked_by
         FROM credential_grants g JOIN approval_requests a USING(request_id)
         WHERE g.request_id = ?1",
            [id.value().to_string()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, u64>(2)?,
                    row.get::<_, Option<u64>>(3)?,
                    row.get::<_, Option<u64>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .optional()?
        .ok_or(StoreError::CredentialGrantNotFound)?;
    let record = CredentialGrantRecord {
        protocol_version: PROTOCOL_VERSION,
        request: serde_json::from_str(&row.0)?,
        decision: serde_json::from_str(&row.1.ok_or(StoreError::CredentialGrantRejected)?)?,
        issued_at_ms: row.2,
        consumed_at_ms: row.3,
        revoked_at_ms: row.4,
        revoked_by: row.5,
    };
    record.request.validate_unchanged()?;
    record.request.verify_decision(&record.decision)?;
    if record.request.id != id
        || record.decision.kind != ApprovalDecisionKind::Approve
        || record.request.action.scope.credential_binding.is_none()
        || record.issued_at_ms != record.decision.decided_at_ms
    {
        return Err(StoreError::CredentialGrantRejected);
    }
    Ok(record)
}

impl Store {
    /// Read-only audit metadata; presence is not a promise of current usability.
    pub fn credential_grant(
        &self,
        id: ApprovalRequestId,
    ) -> Result<CredentialGrantRecord, StoreError> {
        let connection = self.connection()?;
        load(&connection, id)
    }

    /// Permanently spends an exact grant before any native credential read.
    /// The trusted caller must construct `action` from its execution context,
    /// not copy a client-supplied approval and treat that as authenticated intent.
    /// The returned record contains no secret and is not a reusable bearer token.
    /// A failed subsequent credential lookup does not refund the grant.
    #[cfg_attr(not(test), allow(dead_code))] // Native broker integration remains disabled.
    pub(crate) fn consume_credential_grant(
        &self,
        id: ApprovalRequestId,
        action: &ApprovalAction,
    ) -> Result<CredentialGrantRecord, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut record = load(&transaction, id)?;
        let now = now_ms()?;
        validate_time(&record.request, now)?;
        if &record.request.action != action
            || now < record.issued_at_ms
            || record.consumed_at_ms.is_some()
            || record.revoked_at_ms.is_some()
        {
            return Err(StoreError::CredentialGrantRejected);
        }
        let lifecycle: String = transaction.query_row(
            "SELECT lifecycle FROM daemon_state WHERE singleton = 1",
            [],
            |row| row.get(0),
        )?;
        if lifecycle != "ready" {
            return Err(StoreError::CredentialGrantRejected);
        }
        validate_current_catalog(&transaction, &record.request)?;
        let changed = transaction.execute(
            "UPDATE credential_grants SET consumed_at_ms = ?1
             WHERE request_id = ?2 AND consumed_at_ms IS NULL AND revoked_at_ms IS NULL",
            params![now, id.value().to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::CredentialGrantRejected);
        }
        insert_event(
            &transaction,
            "credential.grant_consumed",
            &serde_json::json!({"request_id": id, "run_id": action.scope.run_id}).to_string(),
        )?;
        transaction.commit()?;
        record.consumed_at_ms = Some(now);
        Ok(record)
    }

    /// Revocation is terminal and does not imply revocation of a provider token.
    pub fn revoke_credential_grant(
        &self,
        id: ApprovalRequestId,
        command: RevokeCredentialGrantCommand,
    ) -> Result<CredentialGrantRecord, StoreError> {
        if command.actor.trim().is_empty() || command.actor.len() > 200 {
            return Err(StoreError::CredentialGrantRejected);
        }
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut record = load(&transaction, id)?;
        let now = now_ms()?;
        validate_time(&record.request, now)?;
        if now < record.issued_at_ms
            || record.consumed_at_ms.is_some()
            || record.revoked_at_ms.is_some()
        {
            return Err(StoreError::CredentialGrantRejected);
        }
        let changed = transaction.execute(
            "UPDATE credential_grants SET revoked_at_ms = ?1, revoked_by = ?2
             WHERE request_id = ?3 AND consumed_at_ms IS NULL AND revoked_at_ms IS NULL",
            params![now, command.actor, id.value().to_string()],
        )?;
        if changed != 1 {
            return Err(StoreError::CredentialGrantRejected);
        }
        // Actor is retained with the grant, not copied into the general journal.
        insert_event(
            &transaction,
            "credential.grant_revoked",
            &serde_json::json!({"request_id": id}).to_string(),
        )?;
        transaction.commit()?;
        record.revoked_at_ms = Some(now);
        record.revoked_by = Some(command.actor);
        Ok(record)
    }
}

pub(super) async fn get_grant(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
) -> Result<Json<CredentialGrantRecord>, ApiError> {
    Ok(Json(state.store.credential_grant(
        ApprovalRequestId::from_bytes(*id.as_bytes()),
    )?))
}

pub(super) async fn revoke_grant(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<Uuid>,
    Json(command): Json<RevokeCredentialGrantCommand>,
) -> Result<Json<CredentialGrantRecord>, ApiError> {
    Ok(Json(state.store.revoke_credential_grant(
        ApprovalRequestId::from_bytes(*id.as_bytes()),
        command,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bastet_core::{
        ApprovalRisk, ApprovalScope, CredentialGrantBinding, PermissionLevel, PolicyLayer,
        ScopedPolicy,
    };
    use tempfile::tempdir;

    fn setup() -> (tempfile::TempDir, Store, ApprovalRequest) {
        let directory = tempdir().unwrap();
        let store = Store::open(directory.path().join("grants.db")).unwrap();
        let mut catalog = crate::tests::catalog_fixture();
        catalog.projects[0].policy.ceiling.credential = PermissionLevel::Use;
        catalog.roles[0].policy.ceiling.credential = PermissionLevel::Use;
        let now = now_ms().unwrap();
        let reference = &catalog.credential_references[0];
        let account = &catalog.accounts[0];
        let provider = &catalog.agent_providers[0];
        let request = ApprovalRequest::create(
            ApprovalRequestId::new(),
            now - 10,
            now + 60_000,
            ApprovalAction {
                agent_instance_id: catalog.agent_instances[0].metadata.id,
                role_id: Some(catalog.roles[0].metadata.id),
                action_key: "credential.use".into(),
                reason_key: "provider.authentication".into(),
                consequence_key: "credential.single_run".into(),
                risk: ApprovalRisk::High,
                scope: ApprovalScope {
                    project_id: catalog.projects[0].metadata.id,
                    run_id: Some(catalog.runs[0].metadata.id),
                    filesystem_roots: vec!["/fixture".into()],
                    data_scopes: vec!["synthetic".into()],
                    network_destinations: vec!["provider.example".into()],
                    credential_reference_ids: vec![reference.metadata.id],
                    credential_binding: Some(CredentialGrantBinding {
                        agent_provider_id: provider.metadata.id,
                        account_id: account.metadata.id,
                        adapter_kind: provider.adapter_kind.clone(),
                        provider_identity: account.provider_identity.clone(),
                        credential_reference_id: reference.metadata.id,
                        backend: reference.backend.clone(),
                        service: reference.service.clone(),
                        account_label: reference.account_label.clone(),
                        capability_key: "provider.authenticate".into(),
                    }),
                    destination: None,
                },
                requested_policy: ScopedPolicy {
                    layer: PolicyLayer::SingleRun,
                    ceiling: catalog.roles[0].policy.ceiling.clone(),
                },
            },
            &catalog.roles[0].policy,
        )
        .unwrap();
        let profile = bastet_core::builtin_pet_profile();
        let mut m3 = M3Catalog::default();
        m3.office.pet_assignments.push(bastet_core::PetAssignment {
            metadata: entity_metadata(bastet_core::PetAssignmentId::new(), "credential-fixture"),
            project_id: catalog.projects[0].metadata.id,
            role_id: catalog.roles[0].metadata.id,
            agent_instance_id: catalog.agent_instances[0].metadata.id,
            pet_profile_id: profile.metadata.id,
        });
        m3.office.pet_profiles.push(profile);
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: 0,
                catalog,
            })
            .unwrap();
        store
            .replace_m3_catalog(ReplaceM3CatalogCommand {
                expected_revision: 0,
                catalog: m3,
            })
            .unwrap();
        store.mark_ready().unwrap();
        store
            .create_approval(CreateApprovalCommand {
                request: request.clone(),
            })
            .unwrap();
        (directory, store, request)
    }

    fn decide(
        store: &Store,
        request: &ApprovalRequest,
        kind: ApprovalDecisionKind,
    ) -> Result<ApprovalReceipt, StoreError> {
        store.decide_approval(DecideApprovalCommand {
            credential_scope_acknowledged: true,
            decision: ApprovalDecision {
                request_id: request.id,
                request_hash: request.request_hash.clone(),
                kind,
                decided_at_ms: now_ms().unwrap(),
                actor: "local-fixture-human".into(),
            },
        })
    }

    #[test]
    fn grant_is_issued_with_decision_consumed_once_and_preserved_by_backup() {
        let (directory, store, request) = setup();
        assert!(matches!(
            store.credential_grant(request.id),
            Err(StoreError::CredentialGrantNotFound)
        ));
        decide(&store, &request, ApprovalDecisionKind::Approve).unwrap();
        let issued = store.credential_grant(request.id).unwrap();
        assert_eq!(issued.request, request);
        assert!(issued.consumed_at_ms.is_none());
        assert_eq!(issued.issued_at_ms, issued.decision.decided_at_ms);
        let spent = store
            .consume_credential_grant(request.id, &request.action)
            .unwrap();
        assert!(spent.consumed_at_ms.is_some());
        let events = store.events_after(0).unwrap();
        assert!(matches!(
            store.consume_credential_grant(request.id, &request.action),
            Err(StoreError::CredentialGrantRejected)
        ));
        assert!(store
            .revoke_credential_grant(
                request.id,
                RevokeCredentialGrantCommand {
                    actor: "human".into()
                }
            )
            .is_err());
        assert_eq!(store.events_after(0).unwrap(), events);
        let backup = directory.path().join("backup.db");
        store.backup_to(&backup).unwrap();
        let reopened = Store::open(&backup).unwrap();
        assert_eq!(reopened.credential_grant(request.id).unwrap(), spent);
        assert!(reopened
            .consume_credential_grant(request.id, &request.action)
            .is_err());
        let grant_events: Vec<_> = events
            .iter()
            .filter(|event| event.event_type.starts_with("credential."))
            .collect();
        assert_eq!(grant_events.len(), 2);
        for event in grant_events {
            let payload: serde_json::Value = serde_json::from_str(&event.payload_json).unwrap();
            assert!(payload
                .as_object()
                .unwrap()
                .keys()
                .all(|key| key == "request_id" || key == "run_id"));
        }
    }

    #[test]
    fn denial_and_revocation_never_authorize_consumption() {
        let (_, store, request) = setup();
        decide(&store, &request, ApprovalDecisionKind::Deny).unwrap();
        assert!(matches!(
            store.credential_grant(request.id),
            Err(StoreError::CredentialGrantNotFound)
        ));
        assert!(store
            .consume_credential_grant(request.id, &request.action)
            .is_err());

        let (_directory, store, request) = setup();
        decide(&store, &request, ApprovalDecisionKind::Approve).unwrap();
        let revoked = store
            .revoke_credential_grant(
                request.id,
                RevokeCredentialGrantCommand {
                    actor: "fixture-revoker".into(),
                },
            )
            .unwrap();
        assert_eq!(revoked.revoked_by.as_deref(), Some("fixture-revoker"));
        assert!(revoked.revoked_at_ms.is_some());
        assert!(store
            .consume_credential_grant(request.id, &request.action)
            .is_err());
        assert!(store
            .revoke_credential_grant(
                request.id,
                RevokeCredentialGrantCommand {
                    actor: "again".into()
                }
            )
            .is_err());
    }

    #[test]
    fn action_scope_changes_and_catalog_drift_do_not_spend_grant() {
        let (_directory, store, request) = setup();
        decide(&store, &request, ApprovalDecisionKind::Approve).unwrap();
        let events = store.events_after(0).unwrap();
        for field in 0..6 {
            let mut action = request.action.clone();
            match field {
                0 => action.scope.run_id = Some(RunId::new()),
                1 => action
                    .scope
                    .network_destinations
                    .push("different.example".into()),
                2 => action.scope.filesystem_roots.push("/outside".into()),
                3 => {
                    action
                        .scope
                        .credential_binding
                        .as_mut()
                        .unwrap()
                        .capability_key = "different".into()
                }
                4 => {
                    action
                        .scope
                        .credential_binding
                        .as_mut()
                        .unwrap()
                        .account_label = "different".into()
                }
                _ => action.requested_policy.ceiling.filesystem = PermissionLevel::Use,
            }
            assert!(store.consume_credential_grant(request.id, &action).is_err());
        }
        assert_eq!(store.events_after(0).unwrap(), events);
        let mut snapshot = store.catalog().unwrap();
        snapshot.catalog.credential_references[0].account_label = "locator-changed".into();
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: snapshot.revision,
                catalog: snapshot.catalog,
            })
            .unwrap();
        assert!(store
            .consume_credential_grant(request.id, &request.action)
            .is_err());
        assert!(store
            .credential_grant(request.id)
            .unwrap()
            .consumed_at_ms
            .is_none());
    }

    #[test]
    fn approval_rechecks_catalog_and_uses_server_time_not_backdated_client_time() {
        let (_directory, store, request) = setup();
        let mut snapshot = store.catalog().unwrap();
        snapshot.catalog.accounts[0].provider_identity = "changed".into();
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: snapshot.revision,
                catalog: snapshot.catalog,
            })
            .unwrap();
        let events = store.events_after(0).unwrap();
        assert!(decide(&store, &request, ApprovalDecisionKind::Approve).is_err());
        assert!(store.approval(request.id).unwrap().decision.is_none());
        assert!(store.credential_grant(request.id).is_err());
        assert_eq!(store.events_after(0).unwrap(), events);

        let (_directory, store, request) = setup();
        // Retain a self-consistent request whose interval has actually expired;
        // a forged in-interval client timestamp must not resurrect it.
        let expired = ApprovalRequest::create(
            request.id,
            1,
            10,
            request.action.clone(),
            &store.catalog().unwrap().catalog.roles[0].policy,
        )
        .unwrap();
        store.connection().unwrap().execute(
            "UPDATE approval_requests SET request_hash = ?1, request_json = ?2, expires_at_ms = 10 WHERE request_id = ?3",
            params![expired.request_hash, serde_json::to_string(&expired).unwrap(), request.id.value().to_string()],
        ).unwrap();
        assert!(store
            .decide_approval(DecideApprovalCommand {
                credential_scope_acknowledged: true,
                decision: ApprovalDecision {
                    request_id: expired.id,
                    request_hash: expired.request_hash,
                    kind: ApprovalDecisionKind::Approve,
                    decided_at_ms: 5,
                    actor: "backdated".into(),
                }
            })
            .is_err());
        assert!(store.credential_grant(request.id).is_err());
    }

    #[test]
    fn concurrent_consumers_have_exactly_one_winner() {
        let (_directory, store, request) = setup();
        decide(&store, &request, ApprovalDecisionKind::Approve).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let workers: Vec<_> = (0..2)
            .map(|_| {
                let store = store.clone();
                let request = request.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    store
                        .consume_credential_grant(request.id, &request.action)
                        .is_ok()
                })
            })
            .collect();
        barrier.wait();
        let winners = workers
            .into_iter()
            .map(|worker| usize::from(worker.join().unwrap()))
            .sum::<usize>();
        assert_eq!(winners, 1);
        assert_eq!(
            store
                .events_after(0)
                .unwrap()
                .iter()
                .filter(|event| event.event_type == "credential.grant_consumed")
                .count(),
            1
        );
    }

    #[test]
    fn grant_expiry_is_exclusive_and_clock_rollback_fails_closed() {
        let (_directory, _store, request) = setup();
        assert!(validate_time(&request, request.created_at_ms - 1).is_err());
        assert!(validate_time(&request, request.created_at_ms).is_ok());
        assert!(validate_time(&request, request.expires_at_ms - 1).is_ok());
        assert!(validate_time(&request, request.expires_at_ms).is_err());
    }

    #[test]
    fn issuing_and_consuming_roll_back_if_their_journal_write_fails() {
        let (_directory, store, request) = setup();
        store
            .connection()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER fail_grant_journal BEFORE INSERT ON event_journal
             WHEN NEW.event_type = 'credential.grant_issued'
             BEGIN SELECT RAISE(ABORT, 'fixture journal failure'); END;",
            )
            .unwrap();
        let before = store.events_after(0).unwrap();
        assert!(decide(&store, &request, ApprovalDecisionKind::Approve).is_err());
        assert!(store.approval(request.id).unwrap().decision.is_none());
        assert!(store.credential_grant(request.id).is_err());
        assert_eq!(store.events_after(0).unwrap(), before);
        store
            .connection()
            .unwrap()
            .execute_batch("DROP TRIGGER fail_grant_journal;")
            .unwrap();
        decide(&store, &request, ApprovalDecisionKind::Approve).unwrap();
        store
            .connection()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER fail_grant_journal BEFORE INSERT ON event_journal
             WHEN NEW.event_type = 'credential.grant_consumed'
             BEGIN SELECT RAISE(ABORT, 'fixture journal failure'); END;",
            )
            .unwrap();
        let before = store.events_after(0).unwrap();
        assert!(store
            .consume_credential_grant(request.id, &request.action)
            .is_err());
        assert!(store
            .credential_grant(request.id)
            .unwrap()
            .consumed_at_ms
            .is_none());
        assert_eq!(store.events_after(0).unwrap(), before);
    }

    #[test]
    fn upgrades_v9_preserving_approvals_without_minting_retroactive_grants() {
        let (directory, store, request) = setup();
        // The historical v9 schema knew no grant binding. Keep an ordinary
        // approval and prove migration does not treat it as credential consent.
        let mut action = request.action.clone();
        action.action_key = "agent.observe".into();
        action.scope.credential_binding = None;
        let legacy = ApprovalRequest::create(
            request.id,
            request.created_at_ms,
            request.expires_at_ms,
            action,
            &store.catalog().unwrap().catalog.roles[0].policy,
        )
        .unwrap();
        let decision = ApprovalDecision {
            request_id: legacy.id,
            request_hash: legacy.request_hash.clone(),
            kind: ApprovalDecisionKind::Approve,
            decided_at_ms: now_ms().unwrap(),
            actor: "legacy".into(),
        };
        {
            let connection = store.connection().unwrap();
            connection.execute("UPDATE approval_requests SET request_hash = ?1, request_json = ?2, decision_json = ?3 WHERE request_id = ?4",
                params![legacy.request_hash, serde_json::to_string(&legacy).unwrap(), serde_json::to_string(&decision).unwrap(), legacy.id.value().to_string()]).unwrap();
            connection.execute_batch("DROP TABLE credential_grants; DELETE FROM schema_migrations WHERE version = 10;").unwrap();
        }
        drop(store);
        let upgraded = Store::open(directory.path().join("grants.db")).unwrap();
        assert_eq!(upgraded.schema_version().unwrap(), 10);
        assert_eq!(upgraded.approval(legacy.id).unwrap().request, legacy);
        assert_eq!(
            upgraded.approval(legacy.id).unwrap().decision,
            Some(decision)
        );
        assert!(matches!(
            upgraded.credential_grant(legacy.id),
            Err(StoreError::CredentialGrantNotFound)
        ));
    }

    #[test]
    fn expired_issued_grant_and_stopping_daemon_cannot_be_consumed() {
        let (_directory, store, request) = setup();
        decide(&store, &request, ApprovalDecisionKind::Approve).unwrap();
        let expired = ApprovalRequest::create(
            request.id,
            1,
            10,
            request.action.clone(),
            &store.catalog().unwrap().catalog.roles[0].policy,
        )
        .unwrap();
        let decision = ApprovalDecision {
            request_id: expired.id,
            request_hash: expired.request_hash.clone(),
            kind: ApprovalDecisionKind::Approve,
            decided_at_ms: 5,
            actor: "fixture".into(),
        };
        {
            let connection = store.connection().unwrap();
            connection.execute("UPDATE approval_requests SET request_hash = ?1, request_json = ?2, decision_json = ?3, expires_at_ms = 10 WHERE request_id = ?4",
                params![expired.request_hash, serde_json::to_string(&expired).unwrap(), serde_json::to_string(&decision).unwrap(), expired.id.value().to_string()]).unwrap();
            connection
                .execute(
                    "UPDATE credential_grants SET issued_at_ms = 5 WHERE request_id = ?1",
                    [request.id.value().to_string()],
                )
                .unwrap();
        }
        assert!(store
            .consume_credential_grant(expired.id, &expired.action)
            .is_err());
        assert!(store
            .credential_grant(expired.id)
            .unwrap()
            .consumed_at_ms
            .is_none());

        let (_directory, store, request) = setup();
        decide(&store, &request, ApprovalDecisionKind::Approve).unwrap();
        store
            .shutdown(CheckpointCommand {
                expected_revision: store.snapshot().unwrap().revision,
                reason: "fixture".into(),
            })
            .unwrap();
        assert!(store
            .consume_credential_grant(request.id, &request.action)
            .is_err());
        assert!(store
            .credential_grant(request.id)
            .unwrap()
            .consumed_at_ms
            .is_none());
    }

    #[test]
    fn client_clock_skew_does_not_control_credential_decision_time() {
        for client_time in [0, u64::MAX] {
            let (_directory, store, request) = setup();
            let before = now_ms().unwrap();
            store
                .decide_approval(DecideApprovalCommand {
                    credential_scope_acknowledged: true,
                    decision: ApprovalDecision {
                        request_id: request.id,
                        request_hash: request.request_hash.clone(),
                        kind: ApprovalDecisionKind::Approve,
                        decided_at_ms: client_time,
                        actor: "skewed-client".into(),
                    },
                })
                .unwrap();
            let record = store.credential_grant(request.id).unwrap();
            assert!((before..=now_ms().unwrap()).contains(&record.decision.decided_at_ms));
            assert_eq!(record.issued_at_ms, record.decision.decided_at_ms);
        }
    }

    #[test]
    fn changed_or_ambiguous_role_assignment_cannot_authorize_credential_use() {
        let (_directory, store, request) = setup();
        decide(&store, &request, ApprovalDecisionKind::Approve).unwrap();
        let mut snapshot = store.m3_catalog().unwrap();
        snapshot.catalog.office.pet_assignments[0]
            .metadata
            .lifecycle = EntityLifecycle::Disabled;
        store
            .replace_m3_catalog(ReplaceM3CatalogCommand {
                expected_revision: snapshot.revision,
                catalog: snapshot.catalog,
            })
            .unwrap();
        assert!(store
            .consume_credential_grant(request.id, &request.action)
            .is_err());
        assert!(store
            .credential_grant(request.id)
            .unwrap()
            .consumed_at_ms
            .is_none());

        let (_directory, store, request) = setup();
        let mut identity = store.catalog().unwrap();
        let mut other_role = identity.catalog.roles[0].clone();
        other_role.metadata.id = bastet_core::RoleId::new();
        identity.catalog.roles.push(other_role.clone());
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: identity.revision,
                catalog: identity.catalog,
            })
            .unwrap();
        let mut m3 = store.m3_catalog().unwrap();
        let mut other_assignment = m3.catalog.office.pet_assignments[0].clone();
        other_assignment.metadata.id = bastet_core::PetAssignmentId::new();
        other_assignment.role_id = other_role.metadata.id;
        m3.catalog.office.pet_assignments.push(other_assignment);
        store
            .replace_m3_catalog(ReplaceM3CatalogCommand {
                expected_revision: m3.revision,
                catalog: m3.catalog,
            })
            .unwrap();
        assert!(decide(&store, &request, ApprovalDecisionKind::Approve).is_err());
        assert!(store.approval(request.id).unwrap().decision.is_none());
    }

    #[tokio::test]
    async fn routes_issue_inspect_revoke_but_never_expose_consumption() {
        use axum::{
            body::{to_bytes, Body},
            http::Request,
        };
        use tower::ServiceExt;
        let (_directory, store, request) = setup();
        let app = crate::router(store.clone());
        let command = DecideApprovalCommand {
            credential_scope_acknowledged: true,
            decision: ApprovalDecision {
                request_id: request.id,
                request_hash: request.request_hash.clone(),
                kind: ApprovalDecisionKind::Approve,
                decided_at_ms: now_ms().unwrap(),
                actor: "fixture-http".into(),
            },
        };
        let legacy_command = serde_json::json!({"decision": command.decision});
        let before = store.events_after(0).unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::post(format!("/v1/approvals/{}", request.id.value()))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&legacy_command).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(store.approval(request.id).unwrap().decision.is_none());
        assert_eq!(store.events_after(0).unwrap(), before);
        let response = app
            .clone()
            .oneshot(
                Request::post(format!("/v1/approvals/{}", request.id.value()))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&command).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let path = format!("/v1/credential-grants/{}", request.id.value());
        let response = app
            .clone()
            .oneshot(Request::get(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let record: CredentialGrantRecord =
            serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
        assert_eq!(record.request, request);
        let response = app
            .clone()
            .oneshot(
                Request::post(format!("{path}/consume"))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&request.action).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let response = app
            .clone()
            .oneshot(
                Request::post(&path)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"actor":"fixture-revoker","secret":"must-be-rejected"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(store
            .credential_grant(request.id)
            .unwrap()
            .revoked_at_ms
            .is_none());
        let response = app
            .oneshot(
                Request::post(&path)
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"actor":"fixture-revoker"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(store
            .credential_grant(request.id)
            .unwrap()
            .revoked_at_ms
            .is_some());
        assert!(store
            .consume_credential_grant(request.id, &request.action)
            .is_err());
    }

    #[test]
    fn graph_grant_requires_the_current_attempt_and_exact_node_role() {
        for corrupt_attempt in [false, true] {
            let (_directory, _workspace, store, execution_id) =
                crate::tests::provider_execution_fixture();
            let graph = store.graph_execution(execution_id).unwrap();
            let node_id = graph.nodes[0].node_id;
            let role_id = graph
                .graph
                .nodes
                .iter()
                .find(|node| node.id == node_id)
                .unwrap()
                .role_id;
            crate::tests::attach_selected_account(&store, execution_id, node_id);
            let mut catalog = store.catalog().unwrap();
            for project in &mut catalog.catalog.projects {
                project.policy.ceiling.credential = PermissionLevel::Use;
            }
            for role in &mut catalog.catalog.roles {
                role.policy.ceiling.credential = PermissionLevel::Use;
            }
            store
                .replace_catalog(ReplaceCatalogCommand {
                    expected_revision: catalog.revision,
                    catalog: catalog.catalog,
                })
                .unwrap();
            // Stage only via the real begin boundary; no provider worker is launched.
            let receipt = crate::tests::begin_fixture_node(&store, execution_id);
            let identity = receipt.launch_identity.as_ref().unwrap();
            let account = identity.account.as_ref().unwrap();
            let credential = account.credential.as_ref().unwrap();
            let catalog = store.catalog().unwrap().catalog;
            let role = catalog
                .roles
                .iter()
                .find(|role| role.metadata.id == role_id)
                .unwrap();
            let now = now_ms().unwrap();
            let request = ApprovalRequest::create(
                ApprovalRequestId::new(),
                now - 1,
                now + 60_000,
                ApprovalAction {
                    agent_instance_id: identity.agent_instance_id,
                    role_id: Some(role_id),
                    action_key: "credential.use".into(),
                    reason_key: "provider.authentication".into(),
                    consequence_key: "credential.single_run".into(),
                    risk: ApprovalRisk::High,
                    scope: ApprovalScope {
                        project_id: identity.project_id,
                        run_id: Some(receipt.run_id),
                        filesystem_roots: vec![receipt.workspace_root.clone()],
                        data_scopes: vec![],
                        network_destinations: vec![],
                        credential_reference_ids: vec![credential.reference_id],
                        destination: None,
                        credential_binding: Some(CredentialGrantBinding {
                            agent_provider_id: identity.agent_provider_id,
                            account_id: account.account_id,
                            adapter_kind: identity.adapter_kind.clone(),
                            provider_identity: account.provider_identity.clone(),
                            credential_reference_id: credential.reference_id,
                            backend: credential.backend.clone(),
                            service: credential.service.clone(),
                            account_label: credential.account_label.clone(),
                            capability_key: "provider.authenticate".into(),
                        }),
                    },
                    requested_policy: ScopedPolicy {
                        layer: PolicyLayer::SingleRun,
                        ceiling: role.policy.ceiling.clone(),
                    },
                },
                &role.policy,
            )
            .unwrap();
            store
                .create_approval(CreateApprovalCommand {
                    request: request.clone(),
                })
                .unwrap();
            decide(&store, &request, ApprovalDecisionKind::Approve).unwrap();
            if corrupt_attempt {
                // A historical/misdirected ledger row must not authorize the current slot.
                store
                    .connection()
                    .unwrap()
                    .execute(
                        "UPDATE graph_node_runs SET node_id = ?1 WHERE run_id = ?2",
                        params![
                            graph.nodes[1].node_id.value().to_string(),
                            receipt.run_id.value().to_string()
                        ],
                    )
                    .unwrap();
                assert!(store
                    .consume_credential_grant(request.id, &request.action)
                    .is_err());
                assert!(store
                    .credential_grant(request.id)
                    .unwrap()
                    .consumed_at_ms
                    .is_none());
            } else {
                store
                    .consume_credential_grant(request.id, &request.action)
                    .unwrap();
            }
        }
    }
}
