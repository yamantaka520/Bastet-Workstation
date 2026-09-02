mod supervisor;

#[cfg(target_os = "macos")]
mod macos_power;

use bastet_client::DaemonClient;
use bastet_protocol::{CheckpointReceipt, DaemonLifecycle, DaemonSnapshot, PROTOCOL_VERSION};
use serde::Serialize;
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
