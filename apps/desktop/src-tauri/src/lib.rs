mod supervisor;

#[cfg(target_os = "macos")]
mod macos_power;

use bastet_client::DaemonClient;
use bastet_core::{
    ApprovalDecision, ApprovalRequestId, EntityLifecycle, EntityMetadata, PetProfile, PetProfileId,
    PetStateAsset, Provenance, REQUIRED_PET_STATES,
};
use bastet_protocol::{
    ApprovalList, ApprovalReceipt, CheckpointReceipt, DaemonLifecycle, DaemonSnapshot,
    PROTOCOL_VERSION,
};
use serde::Serialize;
use std::{env, path::PathBuf};
use supervisor::DaemonSupervisor;
use tauri::{
    menu::{Menu, MenuItem, Submenu},
    tray::TrayIconBuilder,
    AppHandle, Emitter, Manager, State, WindowEvent,
};

#[derive(Serialize)]
struct BootstrapState {
    product_name: &'static str,
    protocol_version: u32,
    daemon_authoritative: bool,
}

#[derive(Serialize)]
struct AgentStatus {
    adapter_kind: &'static str,
    display_name: &'static str,
    installed: bool,
    version: Option<String>,
    authenticated: Option<bool>,
    model_count: Option<usize>,
    reasoning_controls: Vec<String>,
    operations: Vec<String>,
    error_key: Option<&'static str>,
}

#[derive(Serialize)]
struct AgentCenterSnapshot {
    agents: Vec<AgentStatus>,
}

#[derive(Serialize)]
struct RunProjection {
    run_id: String,
    session_id: String,
    state: String,
    can_cancel: bool,
}

#[derive(Serialize)]
struct WorkProjection {
    revision: u64,
    sessions: usize,
    runs: Vec<RunProjection>,
}

#[derive(Serialize)]
struct M3Projection {
    revision: u64,
    pet_profiles: Vec<PetProfile>,
    pet_assignments: usize,
    rooms: usize,
    meetings: usize,
    documents: usize,
    costs: usize,
}

#[tauri::command]
async fn m3_projection(client: State<'_, DaemonClient>) -> Result<M3Projection, String> {
    let snapshot = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    Ok(project_m3(snapshot))
}

#[tauri::command]
async fn apply_builtin_pet(client: State<'_, DaemonClient>) -> Result<M3Projection, String> {
    let mut snapshot = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    let builtin = builtin_pet_profile();
    if !snapshot
        .catalog
        .office
        .pet_profiles
        .iter()
        .any(|profile| profile.metadata.id == builtin.metadata.id)
    {
        snapshot.catalog.office.pet_profiles.push(builtin);
        client
            .replace_m3_catalog(bastet_protocol::ReplaceM3CatalogCommand {
                expected_revision: snapshot.revision,
                catalog: snapshot.catalog,
            })
            .await
            .map_err(|error| error.to_string())?;
    }
    let updated = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    Ok(project_m3(updated))
}

#[tauri::command]
async fn rollback_builtin_pet(client: State<'_, DaemonClient>) -> Result<M3Projection, String> {
    let mut snapshot = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    let id = builtin_pet_profile().metadata.id;
    if snapshot
        .catalog
        .office
        .pet_assignments
        .iter()
        .any(|assignment| assignment.pet_profile_id == id)
    {
        return Err("pet profile is assigned and cannot be rolled back".into());
    }
    let before = snapshot.catalog.office.pet_profiles.len();
    snapshot
        .catalog
        .office
        .pet_profiles
        .retain(|profile| profile.metadata.id != id);
    if snapshot.catalog.office.pet_profiles.len() != before {
        client
            .replace_m3_catalog(bastet_protocol::ReplaceM3CatalogCommand {
                expected_revision: snapshot.revision,
                catalog: snapshot.catalog,
            })
            .await
            .map_err(|error| error.to_string())?;
    }
    let updated = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    Ok(project_m3(updated))
}

fn project_m3(snapshot: bastet_protocol::M3CatalogSnapshot) -> M3Projection {
    M3Projection {
        revision: snapshot.revision,
        pet_profiles: snapshot.catalog.office.pet_profiles,
        pet_assignments: snapshot.catalog.office.pet_assignments.len(),
        rooms: snapshot.catalog.office.rooms.len(),
        meetings: snapshot.catalog.meetings.meetings.len(),
        documents: snapshot.catalog.deliverables.documents.len(),
        costs: snapshot.catalog.deliverables.costs.len(),
    }
}

fn builtin_pet_profile() -> PetProfile {
    PetProfile {
        metadata: EntityMetadata {
            id: PetProfileId::from_bytes([0xBA; 16]),
            revision: 0,
            created_at: "builtin-v1".into(),
            updated_at: "builtin-v1".into(),
            provenance: Provenance {
                source_kind: "first_party".into(),
                source_id: "bastet-cat-v1".into(),
                recorded_by: "bastet-workstation".into(),
            },
            lifecycle: EntityLifecycle::Active,
        },
        name: "Bastet Cat".into(),
        version: 1,
        states: REQUIRED_PET_STATES
            .into_iter()
            .map(|state| PetStateAsset {
                state_key: state.into(),
                asset_ref: format!("builtin://bastet-cat/{state}"),
                accessible_label_key: format!("pet.state.{state}"),
            })
            .collect(),
    }
}

#[tauri::command]
async fn work_projection(client: State<'_, DaemonClient>) -> Result<WorkProjection, String> {
    let snapshot = client.catalog().await.map_err(|error| error.to_string())?;
    Ok(WorkProjection {
        revision: snapshot.revision,
        sessions: snapshot.catalog.sessions.len(),
        runs: snapshot
            .catalog
            .runs
            .into_iter()
            .map(|run| {
                let can_cancel = matches!(
                    run.state,
                    bastet_core::NormalizedRunState::Starting
                        | bastet_core::NormalizedRunState::Running
                        | bastet_core::NormalizedRunState::Recovering
                );
                RunProjection {
                    run_id: run.metadata.id.value().to_string(),
                    session_id: run.session_id.value().to_string(),
                    state: format!("{:?}", run.state).to_lowercase(),
                    can_cancel,
                }
            })
            .collect(),
    })
}

#[tauri::command]
async fn cancel_run(
    client: State<'_, DaemonClient>,
    run_id: bastet_core::RunId,
    expected_catalog_revision: u64,
) -> Result<bastet_protocol::CancelRunReceipt, String> {
    client
        .cancel_run(run_id, expected_catalog_revision)
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
async fn agent_center_snapshot() -> Result<AgentCenterSnapshot, String> {
    tauri::async_runtime::spawn_blocking(inspect_agents)
        .await
        .map_err(|error| error.to_string())
}

fn inspect_agents() -> AgentCenterSnapshot {
    AgentCenterSnapshot {
        agents: vec![inspect_codex(), inspect_agy()],
    }
}

fn inspect_codex() -> AgentStatus {
    let Some(path) = configured_executable("BASTET_CODEX_BIN", "codex") else {
        return missing_agent("codex_cli", "Codex CLI");
    };
    let adapter = bastet_adapter_codex::CodexAdapter::new(path);
    let version = adapter.version().ok().map(|report| report.version);
    let authenticated = adapter
        .authentication_status()
        .ok()
        .map(|status| status.authenticated);
    let capabilities = adapter.capabilities();
    AgentStatus {
        adapter_kind: "codex_cli",
        display_name: "Codex CLI",
        installed: true,
        version,
        authenticated,
        model_count: None,
        reasoning_controls: capabilities.reasoning_controls,
        operations: capabilities
            .operations
            .into_iter()
            .map(|operation| format!("{operation:?}").to_lowercase())
            .collect(),
        error_key: None,
    }
}

fn inspect_agy() -> AgentStatus {
    let Some(path) = configured_executable("BASTET_AGY_BIN", "agy") else {
        return missing_agent("agy_cli", "Agy CLI");
    };
    let adapter = bastet_adapter_agy::AgyAdapter::new(path);
    let version = adapter.version().ok().map(|report| report.version);
    let model_count = adapter.list_models().ok().map(|models| models.len());
    let capabilities = adapter.capabilities();
    AgentStatus {
        adapter_kind: "agy_cli",
        display_name: "Agy CLI",
        installed: true,
        version,
        authenticated: None,
        model_count,
        reasoning_controls: capabilities.reasoning_controls,
        operations: capabilities
            .operations
            .into_iter()
            .map(|operation| format!("{operation:?}").to_lowercase())
            .collect(),
        error_key: None,
    }
}

fn missing_agent(adapter_kind: &'static str, display_name: &'static str) -> AgentStatus {
    AgentStatus {
        adapter_kind,
        display_name,
        installed: false,
        version: None,
        authenticated: None,
        model_count: None,
        reasoning_controls: Vec::new(),
        operations: Vec::new(),
        error_key: Some("agent.binary_missing"),
    }
}

fn configured_executable(variable: &str, name: &str) -> Option<PathBuf> {
    if let Some(path) = env::var_os(variable).map(PathBuf::from) {
        return path.is_file().then_some(path);
    }
    env::split_paths(&env::var_os("PATH")?).find_map(|directory| {
        let candidate = directory.join(if cfg!(windows) {
            format!("{name}.exe")
        } else {
            name.to_owned()
        });
        candidate.is_file().then_some(candidate)
    })
}

#[tauri::command]
fn bootstrap_state() -> BootstrapState {
    BootstrapState {
        product_name: "Bastet Workstation",
        protocol_version: PROTOCOL_VERSION,
        daemon_authoritative: true,
    }
}

#[tauri::command]
async fn daemon_snapshot(
    client: State<'_, DaemonClient>,
    supervisor: State<'_, DaemonSupervisor>,
) -> Result<DaemonSnapshot, String> {
    if client.snapshot().await.is_err() {
        supervisor.ensure_running(&client).await?;
    }
    client.snapshot().await.map_err(|error| error.to_string())
}

#[tauri::command]
async fn approval_center_snapshot(client: State<'_, DaemonClient>) -> Result<ApprovalList, String> {
    client.approvals().await.map_err(|error| error.to_string())
}

#[tauri::command]
async fn decide_approval(
    client: State<'_, DaemonClient>,
    request_id: ApprovalRequestId,
    request_hash: String,
    kind: bastet_core::ApprovalDecisionKind,
    decided_at_ms: u64,
) -> Result<ApprovalReceipt, String> {
    client
        .decide_approval(ApprovalDecision {
            request_id,
            request_hash,
            kind,
            decided_at_ms,
            actor: "local-desktop-user".into(),
        })
        .await
        .map_err(|error| error.to_string())
}

async fn checkpoint_for_quit(client: &DaemonClient) -> Result<CheckpointReceipt, String> {
    let snapshot = client.snapshot().await.map_err(|error| error.to_string())?;
    client
        .shutdown(snapshot.revision, "explicit desktop quit")
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
async fn prepare_for_sleep(client: State<'_, DaemonClient>) -> Result<CheckpointReceipt, String> {
    suspend_if_ready(&client).await
}

async fn suspend_if_ready(client: &DaemonClient) -> Result<CheckpointReceipt, String> {
    let snapshot = client.snapshot().await.map_err(|error| error.to_string())?;
    if snapshot.lifecycle != DaemonLifecycle::Ready {
        return Err(format!(
            "daemon must be ready before suspend (currently {:?})",
            snapshot.lifecycle
        ));
    }
    client
        .suspend(snapshot.revision, "desktop preparing for system sleep")
        .await
        .map_err(|error| error.to_string())
}

async fn resume_if_suspended(client: &DaemonClient) -> Result<DaemonSnapshot, String> {
    let snapshot = client.snapshot().await.map_err(|error| error.to_string())?;
    if snapshot.lifecycle == DaemonLifecycle::Suspended {
        client
            .resume(snapshot.revision, "desktop resumed after system wake")
            .await
            .map_err(|error| error.to_string())?;
        return client.snapshot().await.map_err(|error| error.to_string());
    }
    Ok(snapshot)
}

#[tauri::command]
async fn resume_after_wake(client: State<'_, DaemonClient>) -> Result<DaemonSnapshot, String> {
    resume_if_suspended(&client).await
}

fn request_resume(app: AppHandle) {
    tauri::async_runtime::spawn(async move {
        let client = app.state::<DaemonClient>().inner().clone();
        match resume_if_suspended(&client).await {
            Ok(snapshot) => {
                let _ = app.emit("daemon-resumed-after-wake", snapshot);
            }
            Err(error) => {
                let _ = app.emit("daemon-resume-failed", error);
            }
        }
    });
}

#[cfg(target_os = "macos")]
fn request_suspend(app: AppHandle) {
    tauri::async_runtime::spawn(async move {
        let client = app.state::<DaemonClient>().inner().clone();
        match suspend_if_ready(&client).await {
            Ok(receipt) => {
                let _ = app.emit("daemon-suspended-for-sleep", receipt);
            }
            Err(error) => {
                let _ = app.emit("daemon-suspend-failed", error);
            }
        }
    });
}

fn request_checkpointed_exit(app: AppHandle) {
    let supervisor = app.state::<DaemonSupervisor>().inner().clone();
    if !supervisor.begin_shutdown() {
        return;
    }
    tauri::async_runtime::spawn(async move {
        let client = app.state::<DaemonClient>().inner().clone();
        match checkpoint_for_quit(&client).await {
            Ok(receipt) => {
                let _ = app.emit("daemon-checkpointed-for-quit", &receipt);
                supervisor.authorize_exit();
                app.exit(0);
            }
            Err(error) => {
                supervisor.cancel_shutdown();
                let _ = app.emit("quit-checkpoint-failed", error);
            }
        }
    });
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_autostart::init(
            tauri_plugin_autostart::MacosLauncher::LaunchAgent,
            None,
        ))
        .manage(DaemonClient::from_env())
        .menu(|app| {
            let quit = MenuItem::with_id(
                app,
                "app-quit",
                "Quit Bastet Workstation",
                true,
                Some("CmdOrCtrl+Q"),
            )?;
            let application = Submenu::with_items(app, "Bastet Workstation", true, &[&quit])?;
            Menu::with_items(app, &[&application])
        })
        .on_menu_event(|app, event| {
            if event.id().as_ref() == "app-quit" {
                request_checkpointed_exit(app.clone());
            }
        })
        .setup(|app| {
            let supervisor = DaemonSupervisor::new(&app.path().app_local_data_dir()?)?;
            app.manage(supervisor.clone());
            #[cfg(target_os = "macos")]
            macos_power::install(app.handle().clone());
            let app_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let client = app_handle.state::<DaemonClient>().inner().clone();
                loop {
                    if let Err(error) = supervisor.ensure_running(&client).await {
                        let _ = app_handle.emit("daemon-supervision-failed", error);
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            });
            let show =
                MenuItem::with_id(app, "show", "Show Bastet Workstation", true, None::<&str>)?;
            let quit =
                MenuItem::with_id(app, "quit", "Quit Bastet Workstation", true, None::<&str>)?;
            let menu = Menu::with_items(app, &[&show, &quit])?;
            TrayIconBuilder::new()
                .icon(
                    app.default_window_icon()
                        .cloned()
                        .ok_or("application icon is required")?,
                )
                .tooltip("Bastet Workstation")
                .menu(&menu)
                .on_menu_event(|app, event| match event.id().as_ref() {
                    "show" => {
                        if let Some(window) = app.get_webview_window("main") {
                            let _ = window.show();
                            let _ = window.set_focus();
                        }
                    }
                    "quit" => request_checkpointed_exit(app.clone()),
                    _ => {}
                })
                .build(app)?;
            Ok(())
        })
        .on_window_event(|window, event| {
            if let WindowEvent::CloseRequested { api, .. } = event {
                api.prevent_close();
                let _ = window.hide();
            }
        })
        .invoke_handler(tauri::generate_handler![
            bootstrap_state,
            daemon_snapshot,
            agent_center_snapshot,
            work_projection,
            m3_projection,
            apply_builtin_pet,
            rollback_builtin_pet,
            cancel_run,
            approval_center_snapshot,
            decide_approval,
            prepare_for_sleep,
            resume_after_wake
        ])
        .build(tauri::generate_context!())
        .expect("error while building Bastet Workstation")
        .run(|app, event| match event {
            tauri::RunEvent::Resumed => request_resume(app.clone()),
            tauri::RunEvent::ExitRequested { api, .. }
                if !app.state::<DaemonSupervisor>().exit_authorized() =>
            {
                api.prevent_exit();
                request_checkpointed_exit(app.clone());
            }
            _ => {}
        });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_declares_daemon_authority() {
        let state = bootstrap_state();
        assert_eq!(state.product_name, "Bastet Workstation");
        assert_eq!(state.protocol_version, 1);
        assert!(state.daemon_authoritative);
    }
}
