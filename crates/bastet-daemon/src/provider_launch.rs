//! Exact, immutable launch inputs. This is intent, not a credential grant or
//! evidence that a worker was dispatched. Legacy attempts have no inferred plan.

use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProviderLaunchPlan {
    pub version: u32,
    pub execution_id: GraphRunId,
    pub node_id: GraphNodeId,
    pub session_id: bastet_core::SessionId,
    pub run_id: RunId,
    pub attempt: u64,
    pub role_id: bastet_core::RoleId,
    pub owner: String,
    pub identity: bastet_protocol::ProviderLaunchIdentity,
    pub workspace_root: String,
    pub prompt: String,
}

impl ProviderLaunchPlan {
    fn hash(&self) -> Result<String, StoreError> {
        Ok(format!(
            "sha256:{:x}",
            Sha256::digest(serde_json::to_vec(self)?)
        ))
    }

    pub(super) fn insert(&self, transaction: &rusqlite::Transaction<'_>) -> Result<(), StoreError> {
        transaction.execute(
            "INSERT INTO provider_launch_plans(run_id, plan_json, plan_hash) VALUES (?1, ?2, ?3)",
            params![
                self.run_id.value().to_string(),
                serde_json::to_string(self)?,
                self.hash()?
            ],
        )?;
        Ok(())
    }

    pub(super) fn load(connection: &Connection, run_id: RunId) -> Result<Self, StoreError> {
        let row: Option<(String, String)> = connection
            .query_row(
                "SELECT plan_json, plan_hash FROM provider_launch_plans WHERE run_id=?1",
                [run_id.value().to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let (json, hash) =
            row.ok_or_else(|| StoreError::InvalidRunState("launch plan unavailable".into()))?;
        let plan: Self = serde_json::from_str(&json)?;
        if plan.version != 1 || plan.run_id != run_id || plan.hash()? != hash {
            return Err(StoreError::InvalidRunState(
                "launch plan integrity check failed".into(),
            ));
        }
        let ledger: Option<(String, String, u64)> = connection
            .query_row(
                "SELECT execution_id, node_id, attempt FROM graph_node_runs WHERE run_id=?1",
                [run_id.value().to_string()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        if ledger
            != Some((
                plan.execution_id.value().to_string(),
                plan.node_id.value().to_string(),
                plan.attempt,
            ))
            || load_launch_identity(connection, run_id)?.as_ref() != Some(&plan.identity)
        {
            return Err(StoreError::InvalidRunState(
                "launch plan differs from attempt ledger".into(),
            ));
        }
        Ok(plan)
    }

    pub(super) fn matches_receipt(&self, receipt: &BeginGraphNodeRunReceipt) -> bool {
        self.execution_id == receipt.execution_id
            && self.node_id == receipt.node_id
            && self.session_id == receipt.session_id
            && self.run_id == receipt.run_id
            && Some(&self.identity) == receipt.launch_identity.as_ref()
            && self.identity.adapter_kind == receipt.adapter_kind
            && self.identity.model == receipt.model
            && self.workspace_root == receipt.workspace_root
            && self.prompt == receipt.prompt
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{begin_fixture_node, provider_execution_fixture};

    #[test]
    fn exact_plan_survives_restart_without_reauthorizing_interrupted_work() {
        let (directory, _workspace, store, id) = provider_execution_fixture();
        let receipt = begin_fixture_node(&store, id);
        let plan = ProviderLaunchPlan::load(&store.connection().unwrap(), receipt.run_id).unwrap();
        assert!(plan.matches_receipt(&receipt));
        assert_eq!(plan.attempt, 1);
        store.preflight_provider_launch(&receipt).unwrap();
        drop(store);
        let restored = Store::open(directory.path().join("provider-execution.db")).unwrap();
        assert_eq!(
            ProviderLaunchPlan::load(&restored.connection().unwrap(), receipt.run_id).unwrap(),
            plan
        );
        assert!(restored.preflight_provider_launch(&receipt).is_err());
    }

    #[test]
    fn plans_are_append_only_and_integrity_checked() {
        let (_directory, _workspace, store, id) = provider_execution_fixture();
        let receipt = begin_fixture_node(&store, id);
        let connection = store.connection().unwrap();
        assert!(connection
            .execute("UPDATE provider_launch_plans SET plan_hash='changed'", [])
            .is_err());
        assert!(connection
            .execute("DELETE FROM provider_launch_plans", [])
            .is_err());
        assert!(connection.execute(
            "INSERT OR REPLACE INTO provider_launch_plans(run_id,plan_json,plan_hash) VALUES (?1,'{}','changed')",
            [receipt.run_id.value().to_string()],
        ).is_err());
        // Simulate corruption outside supported writes. The hash is a checksum,
        // not protection from an attacker able to replace the entire database.
        connection
            .execute_batch("DROP TRIGGER provider_launch_plans_immutable;")
            .unwrap();
        connection
            .execute("UPDATE provider_launch_plans SET plan_hash='changed'", [])
            .unwrap();
        assert!(ProviderLaunchPlan::load(&connection, receipt.run_id).is_err());
        drop(connection);
        assert!(store.preflight_provider_launch(&receipt).is_err());
    }

    #[test]
    fn current_catalog_and_modified_receipt_cannot_replace_saved_workspace() {
        let (_directory, _workspace, store, id) = provider_execution_fixture();
        let mut receipt = begin_fixture_node(&store, id);
        let replacement = tempfile::tempdir().unwrap();
        let mut catalog = store.catalog().unwrap();
        let project_id = receipt.launch_identity.as_ref().unwrap().project_id;
        let project = catalog
            .catalog
            .projects
            .iter_mut()
            .find(|project| project.metadata.id == project_id)
            .unwrap();
        project.workspace_root = replacement.path().to_string_lossy().into_owned();
        receipt.workspace_root = project.workspace_root.clone();
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: catalog.revision,
                catalog: catalog.catalog,
            })
            .unwrap();
        assert!(store.preflight_provider_launch(&receipt).is_err());
    }

    #[test]
    fn plan_write_failure_rolls_back_run_graph_and_journal() {
        let (_directory, _workspace, store, id) = provider_execution_fixture();
        let catalog = store.catalog().unwrap();
        let graph = store.graph_execution(id).unwrap();
        let events = store.events_after(0).unwrap();
        store
            .connection()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER fixture_reject_plan BEFORE INSERT ON provider_launch_plans BEGIN
             SELECT RAISE(ABORT, 'fixture write failure'); END;",
            )
            .unwrap();
        assert!(store
            .begin_graph_node_run(
                id,
                BeginGraphNodeRunCommand {
                    expected_catalog_revision: catalog.revision,
                    expected_graph_revision: graph.revision,
                    node_id: graph.nodes[0].node_id,
                    owner: "fixture".into(),
                }
            )
            .is_err());
        assert_eq!(store.catalog().unwrap(), catalog);
        assert_eq!(store.graph_execution(id).unwrap(), graph);
        assert_eq!(store.events_after(0).unwrap(), events);
        let count: u64 = store
            .connection()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM graph_node_runs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn v10_migration_does_not_infer_launch_plans_from_mutable_current_inputs() {
        let (directory, _workspace, store, id) = provider_execution_fixture();
        let receipt = begin_fixture_node(&store, id);
        store
            .connection()
            .unwrap()
            .execute_batch(
                "DROP TABLE provider_launch_plans; DELETE FROM schema_migrations WHERE version=11;",
            )
            .unwrap();
        drop(store);
        let restored = Store::open(directory.path().join("provider-execution.db")).unwrap();
        assert_eq!(restored.schema_version().unwrap(), SCHEMA_VERSION);
        assert!(ProviderLaunchPlan::load(&restored.connection().unwrap(), receipt.run_id).is_err());
        assert!(restored.preflight_provider_launch(&receipt).is_err());
        assert!(
            load_launch_identity(&restored.connection().unwrap(), receipt.run_id)
                .unwrap()
                .is_some()
        );
    }
}
