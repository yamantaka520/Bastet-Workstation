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
    /// Missing in v1: retain historical checksums but never infer authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<LaunchPolicySnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LaunchPolicySnapshot {
    pub project: bastet_core::ScopedPolicy,
    pub role: bastet_core::ScopedPolicy,
}

impl LaunchPolicySnapshot {
    pub(super) fn capture(
        catalog: &IdentityCatalog,
        project_id: bastet_core::ProjectId,
        role_id: bastet_core::RoleId,
    ) -> Result<Self, StoreError> {
        let project = catalog
            .projects
            .iter()
            .find(|value| {
                value.metadata.id == project_id
                    && value.metadata.lifecycle == bastet_core::EntityLifecycle::Active
            })
            .ok_or_else(|| {
                StoreError::InvalidRunState("launch project policy unavailable".into())
            })?;
        let role = catalog
            .roles
            .iter()
            .find(|value| {
                value.metadata.id == role_id
                    && value.metadata.lifecycle == bastet_core::EntityLifecycle::Active
            })
            .ok_or_else(|| StoreError::InvalidRunState("launch role policy unavailable".into()))?;
        let snapshot = Self {
            project: project.policy.clone(),
            role: role.policy.clone(),
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    fn validate(&self) -> Result<(), StoreError> {
        if self.project.layer != bastet_core::PolicyLayer::Project
            || self.role.layer != bastet_core::PolicyLayer::RoleOrAgent
        {
            return Err(StoreError::InvalidRunState(
                "launch policy layers invalid".into(),
            ));
        }
        self.project.restrict(self.role.clone()).map_err(|_| {
            StoreError::InvalidRunState("launch role policy exceeds project ceiling".into())
        })?;
        Ok(())
    }
}

impl ProviderLaunchPlan {
    /// Necessary operations of the current online CLI compatibility runner.
    /// Passing this ceiling check is NOT sandbox enforcement or a credential
    /// grant; it only prevents starting work with known-forbidden prerequisites.
    pub(super) fn validate_online_cli_requirements(&self) -> Result<(), StoreError> {
        use bastet_core::{PermissionLevel, PolicyCeiling, PolicyLayer, ScopedPolicy};
        let saved = self
            .policy
            .as_ref()
            .ok_or_else(|| StoreError::InvalidRunState("launch policy unavailable".into()))?;
        saved.validate()?;
        saved
            .role
            .restrict(ScopedPolicy {
                layer: PolicyLayer::SingleRun,
                ceiling: PolicyCeiling {
                    filesystem: PermissionLevel::Observe,
                    network: PermissionLevel::Use,
                    process: PermissionLevel::Use,
                    device: PermissionLevel::Deny,
                    // The compatibility CLI can read its own ambient login. That
                    // is still credential use, not an exemption from the ceiling.
                    credential: PermissionLevel::Use,
                    persistent_approval: false,
                },
            })
            .map_err(|_| StoreError::InvalidRunState("online CLI exceeds launch policy".into()))?;
        Ok(())
    }

    pub(super) fn validate_current_policy(
        &self,
        catalog: &IdentityCatalog,
    ) -> Result<(), StoreError> {
        let saved = self.policy.as_ref().ok_or_else(|| {
            StoreError::InvalidRunState("legacy launch has no saved policy authority".into())
        })?;
        if self.version != 2
            || *saved
                != LaunchPolicySnapshot::capture(catalog, self.identity.project_id, self.role_id)?
        {
            return Err(StoreError::InvalidRunState("launch policy changed".into()));
        }
        Ok(())
    }

    pub(super) fn hash(&self) -> Result<String, StoreError> {
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
        let version_valid = matches!((plan.version, &plan.policy), (1, None) | (2, Some(_)));
        if !version_valid || plan.run_id != run_id || plan.hash()? != hash {
            return Err(StoreError::InvalidRunState(
                "launch plan integrity check failed".into(),
            ));
        }
        if let Some(policy) = &plan.policy {
            policy.validate()?;
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
    fn online_cli_requires_each_known_operation_without_granting_others() {
        use bastet_core::{PermissionLevel, PolicyCeiling};
        let (_directory, _workspace, store, id) = provider_execution_fixture();
        let receipt = begin_fixture_node(&store, id);
        let mut plan =
            ProviderLaunchPlan::load(&store.connection().unwrap(), receipt.run_id).unwrap();
        let permitted = PolicyCeiling {
            filesystem: PermissionLevel::Observe,
            network: PermissionLevel::Use,
            process: PermissionLevel::Use,
            device: PermissionLevel::Deny,
            credential: PermissionLevel::Use,
            persistent_approval: false,
        };
        plan.policy.as_mut().unwrap().project.ceiling = permitted.clone();
        plan.policy.as_mut().unwrap().role.ceiling = permitted.clone();
        plan.validate_online_cli_requirements().unwrap();
        for field in ["filesystem", "network", "process", "credential"] {
            for level in [PermissionLevel::Deny, PermissionLevel::Observe] {
                let mut changed = plan.clone();
                let ceiling = &mut changed.policy.as_mut().unwrap().role.ceiling;
                match field {
                    "filesystem" => ceiling.filesystem = level,
                    "network" => ceiling.network = level,
                    "process" => ceiling.process = level,
                    _ => ceiling.credential = level,
                }
                assert_eq!(
                    changed.validate_online_cli_requirements().is_ok(),
                    field == "filesystem" && level == PermissionLevel::Observe
                );
            }
        }
        assert_eq!(plan.policy.unwrap().role.ceiling, permitted);
    }

    #[test]
    fn saved_policy_is_hashed_and_current_policy_drift_blocks_dispatch() {
        let (_directory, _workspace, store, id) = provider_execution_fixture();
        let receipt = begin_fixture_node(&store, id);
        let plan = ProviderLaunchPlan::load(&store.connection().unwrap(), receipt.run_id).unwrap();
        assert_eq!(plan.version, 2);
        let mut changed = plan.clone();
        changed.policy.as_mut().unwrap().role.ceiling.network = bastet_core::PermissionLevel::Deny;
        assert_ne!(changed.hash().unwrap(), plan.hash().unwrap());
        let mut catalog = store.catalog().unwrap();
        catalog
            .catalog
            .roles
            .iter_mut()
            .find(|role| role.metadata.id == plan.role_id)
            .unwrap()
            .policy = changed.policy.unwrap().role;
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: catalog.revision,
                catalog: catalog.catalog,
            })
            .unwrap();
        let before = store.events_after(0).unwrap();
        assert!(store.preflight_provider_launch(&receipt).is_err());
        assert_eq!(store.events_after(0).unwrap(), before);
        assert_eq!(
            ProviderLaunchPlan::load(&store.connection().unwrap(), receipt.run_id).unwrap(),
            plan
        );
    }

    #[test]
    fn legacy_plan_remains_readable_but_has_no_inferred_policy_authority() {
        let (_directory, _workspace, store, id) = provider_execution_fixture();
        let receipt = begin_fixture_node(&store, id);
        let connection = store.connection().unwrap();
        let mut legacy = ProviderLaunchPlan::load(&connection, receipt.run_id).unwrap();
        legacy.version = 1;
        legacy.policy = None;
        let json = serde_json::to_string(&legacy).unwrap();
        assert!(!json.contains("\"policy\""));
        // Historical fixture, not a supported mutation of live launch plans.
        connection
            .execute_batch("DROP TRIGGER provider_launch_plans_immutable;")
            .unwrap();
        connection
            .execute(
                "UPDATE provider_launch_plans SET plan_json=?1,plan_hash=?2 WHERE run_id=?3",
                params![
                    json,
                    legacy.hash().unwrap(),
                    receipt.run_id.value().to_string()
                ],
            )
            .unwrap();
        assert_eq!(
            ProviderLaunchPlan::load(&connection, receipt.run_id).unwrap(),
            legacy
        );
        drop(connection);
        assert!(store.preflight_provider_launch(&receipt).is_err());
    }

    #[test]
    fn role_expansion_beyond_project_rolls_back_launch_creation() {
        let (_directory, _workspace, store, id) = provider_execution_fixture();
        let mut catalog = store.catalog().unwrap();
        for project in &mut catalog.catalog.projects {
            project.policy.ceiling.network = bastet_core::PermissionLevel::Deny;
        }
        for role in &mut catalog.catalog.roles {
            role.policy.ceiling.network = bastet_core::PermissionLevel::Use;
        }
        store
            .replace_catalog(ReplaceCatalogCommand {
                expected_revision: catalog.revision,
                catalog: catalog.catalog,
            })
            .unwrap();
        let catalog = store.catalog().unwrap();
        let graph = store.graph_execution(id).unwrap();
        let events = store.events_after(0).unwrap();
        assert!(store
            .begin_graph_node_run(
                id,
                BeginGraphNodeRunCommand {
                    expected_catalog_revision: catalog.revision,
                    expected_graph_revision: graph.revision,
                    node_id: graph.nodes[0].node_id,
                    owner: "fixture".into()
                }
            )
            .is_err());
        assert_eq!(store.catalog().unwrap(), catalog);
        assert_eq!(store.graph_execution(id).unwrap(), graph);
        assert_eq!(store.events_after(0).unwrap(), events);
    }

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
                "DROP TABLE provider_dispatch_claims; DROP TABLE staged_provider_launches; DROP TABLE provider_launch_plans; DELETE FROM schema_migrations WHERE version>=11;",
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
