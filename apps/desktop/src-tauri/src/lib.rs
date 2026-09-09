mod supervisor;

#[cfg(target_os = "macos")]
mod macos_power;

use bastet_client::DaemonClient;
use bastet_core::{
    builtin_pet_profile, ApprovalDecision, ApprovalRequestId, MeetingId, PetProfile,
};
use bastet_protocol::{
    ApprovalList, ApprovalReceipt, CheckpointReceipt, DaemonLifecycle, DaemonSnapshot,
    PROTOCOL_VERSION,
};
use serde::Serialize;
#[cfg(test)]
use std::time::Duration;
use std::{
    env,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};
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
    models: Vec<String>,
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

fn run_can_cancel(state: bastet_core::NormalizedRunState) -> bool {
    // Provider startup/handshake is not yet interruptible. Do not advertise
    // cancellation until there is a confirmed provider run to interrupt.
    matches!(
        state,
        bastet_core::NormalizedRunState::Running | bastet_core::NormalizedRunState::Recovering
    )
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
    graph_nodes: Vec<GraphNodeProjection>,
    awaiting_meetings: Vec<MeetingProjection>,
    document_versions: Vec<DocumentProjection>,
    knowledge_deliveries: Vec<KnowledgeDeliveryProjection>,
    joined_drafts: Vec<JoinedDraftProjection>,
    missing_output_execution_id: Option<String>,
}

#[derive(Serialize)]
struct JoinedDraftProjection {
    execution_id: String,
    title: String,
    markdown: String,
}

#[derive(Serialize)]
struct GraphNodeProjection {
    execution_id: String,
    node_id: String,
    failure_kind: Option<&'static str>,
    title: String,
    state: String,
    pet_state: &'static str,
}

#[derive(Serialize)]
struct DocumentProjection {
    project_id: String,
    artifact_id: String,
    version_id: String,
    title: String,
    markdown: String,
    content_hash: String,
    accepted: bool,
}

#[derive(Serialize)]
struct KnowledgeDeliveryProjection {
    delivery_id: String,
    target: String,
    preview: String,
    state: String,
}

#[derive(Serialize)]
struct MeetingProjection {
    meeting_id: String,
    summary: String,
}

fn failure_kind_label(kind: bastet_core::AdapterFailureKind) -> &'static str {
    use bastet_core::AdapterFailureKind::*;
    match kind {
        BinaryMissing | Unsupported => "provider_unavailable",
        Authentication => "provider_authentication",
        Quota => "provider_quota",
        PermissionDenied => "provider_rejected",
        Timeout => "provider_timeout",
        Cancelled => "cancelled",
        Crashed => "provider_crashed",
        ProtocolDrift | MalformedOutput => "invalid_output",
        Unknown => "unknown",
    }
}

#[tauri::command]
async fn m3_projection(client: State<'_, DaemonClient>) -> Result<M3Projection, String> {
    let snapshot = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    let graphs = client
        .graph_executions()
        .await
        .map_err(|error| error.to_string())?;
    Ok(project_m3(snapshot, graphs.executions))
}

#[tauri::command]
async fn retry_missing_graph_outputs(client: State<'_, DaemonClient>) -> Result<(), String> {
    let executions = client
        .graph_executions()
        .await
        .map_err(|error| error.to_string())?
        .executions;
    let execution = executions
        .iter()
        .find(|execution| {
            execution
                .nodes
                .iter()
                .all(|node| node.state == bastet_core::GraphNodeState::Succeeded)
                && execution
                    .nodes
                    .iter()
                    .any(|node| execution.output(node.node_id).is_none())
                && !executions
                    .iter()
                    .any(|next| next.restarted_from_execution_id == Some(execution.id))
        })
        .ok_or("no completed graph needs output recovery")?;
    client
        .restart_missing_output_graph(
            execution.id,
            bastet_protocol::RestartMissingOutputGraphCommand {
                expected_graph_revision: execution.revision,
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[tauri::command]
async fn retry_failed_mvp_node(
    client: State<'_, DaemonClient>,
    execution_id: bastet_core::GraphRunId,
    node_id: bastet_core::GraphNodeId,
) -> Result<(), String> {
    let executions = client
        .graph_executions()
        .await
        .map_err(|error| error.to_string())?
        .executions;
    let execution = executions
        .iter()
        .find(|execution| execution.id == execution_id)
        .ok_or("graph not found")?;
    client
        .retry_failed_graph_node(
            execution_id,
            bastet_protocol::RetryFailedGraphNodeCommand {
                node_id,
                expected_graph_revision: execution.revision,
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[tauri::command]
async fn run_ready_mvp_nodes(client: State<'_, DaemonClient>) -> Result<M3Projection, String> {
    let identity = client.catalog().await.map_err(|error| error.to_string())?;
    let execution = client
        .graph_executions()
        .await
        .map_err(|error| error.to_string())?
        .executions
        .into_iter()
        .find(|execution| {
            execution
                .nodes
                .iter()
                .any(|node| node.state == bastet_core::GraphNodeState::Pending)
        })
        .ok_or("no pending MVP graph nodes")?;
    client
        .execute_ready_graph(
            execution.id,
            bastet_protocol::ExecuteReadyGraphCommand {
                expected_catalog_revision: identity.revision,
                expected_graph_revision: execution.revision,
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    let updated = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    let graphs = client
        .graph_executions()
        .await
        .map_err(|error| error.to_string())?;
    Ok(project_m3(updated, graphs.executions))
}

#[tauri::command]
async fn prepare_mvp(
    client: State<'_, DaemonClient>,
    project_name: String,
    workspace_root: String,
    codex_model: String,
    agy_model: String,
) -> Result<bastet_protocol::PrepareMvpReceipt, String> {
    let identity = client.catalog().await.map_err(|error| error.to_string())?;
    let m3 = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    client
        .prepare_mvp(bastet_protocol::PrepareMvpCommand {
            expected_catalog_revision: identity.revision,
            expected_m3_revision: m3.revision,
            project_name,
            workspace_root,
            codex_model,
            agy_model,
        })
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
async fn accept_mvp_decision(
    client: State<'_, DaemonClient>,
    meeting_id: MeetingId,
    content: String,
) -> Result<bastet_protocol::AcceptDecisionBaselineReceipt, String> {
    let m3 = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    client
        .accept_mvp_decision(bastet_protocol::AcceptDecisionBaselineCommand {
            expected_m3_revision: m3.revision,
            meeting_id,
            content,
            accepted_by: "local-desktop-user".into(),
            accepted_at: timestamp_ms().to_string(),
        })
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
async fn create_mvp_document(
    client: State<'_, DaemonClient>,
    graph_execution_id: bastet_core::GraphRunId,
    title: String,
    markdown: String,
) -> Result<bastet_protocol::DocumentReceipt, String> {
    let m3 = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    client
        .create_mvp_document(bastet_protocol::CreateDocumentCommand {
            expected_m3_revision: m3.revision,
            graph_execution_id,
            title,
            markdown,
        })
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
async fn export_mvp_document(
    client: State<'_, DaemonClient>,
    artifact_id: bastet_core::ArtifactId,
    version_id: bastet_core::ArtifactVersionId,
) -> Result<String, String> {
    let m3 = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    let identity = client.catalog().await.map_err(|error| error.to_string())?;
    let artifact = m3
        .catalog
        .deliverables
        .documents
        .iter()
        .find(|item| item.metadata.id == artifact_id)
        .ok_or("document not found")?;
    let version = artifact
        .versions
        .iter()
        .find(|item| item.id == version_id)
        .ok_or("document version not found")?;
    version
        .validate_unchanged()
        .map_err(|error| error.to_string())?;
    if version.accepted_by.is_none() {
        return Err("accept the document before exporting".into());
    }
    let project = identity
        .catalog
        .projects
        .iter()
        .find(|item| item.metadata.id == artifact.project_id)
        .ok_or("project not found")?;
    export_document_file(Path::new(&project.workspace_root), version)
}

fn export_document_file(
    root: &Path,
    version: &bastet_core::DocumentVersion,
) -> Result<String, String> {
    let root = root.canonicalize().map_err(|error| error.to_string())?;
    if !root.is_dir() {
        return Err("workspace is not a directory".into());
    }
    let path = root.join(format!("bastet-report-{}.md", version.id.value()));
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
    {
        Ok(mut file) => {
            file.write_all(version.markdown.as_bytes())
                .map_err(|error| error.to_string())?;
            file.sync_all().map_err(|error| error.to_string())?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = std::fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
            if !metadata.is_file()
                || metadata.file_type().is_symlink()
                || std::fs::read_to_string(&path).map_err(|error| error.to_string())?
                    != version.markdown
            {
                return Err(
                    "export path contains different content; nothing was overwritten".into(),
                );
            }
        }
        Err(error) => return Err(error.to_string()),
    }
    Ok(path.to_string_lossy().into_owned())
}

#[tauri::command]
async fn accept_mvp_document(
    client: State<'_, DaemonClient>,
    artifact_id: bastet_core::ArtifactId,
    version_id: bastet_core::ArtifactVersionId,
    content_hash: String,
) -> Result<bastet_protocol::DocumentReceipt, String> {
    let m3 = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    client
        .accept_mvp_document(bastet_protocol::AcceptDocumentCommand {
            expected_m3_revision: m3.revision,
            artifact_id,
            version_id,
            content_hash,
            accepted_by: "local-desktop-user".into(),
            accepted_at: timestamp_ms().to_string(),
        })
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
async fn prepare_knowledge_delivery(
    client: State<'_, DaemonClient>,
    project_id: bastet_core::ProjectId,
    artifact_version_id: bastet_core::ArtifactVersionId,
    target: bastet_core::KnowledgeTarget,
    preview: String,
) -> Result<bastet_protocol::KnowledgeDeliveryReceipt, String> {
    let m3 = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    client
        .prepare_knowledge_delivery(bastet_protocol::PrepareKnowledgeDeliveryCommand {
            expected_m3_revision: m3.revision,
            project_id,
            artifact_version_id,
            target,
            preview,
        })
        .await
        .map_err(|error| error.to_string())
}

#[tauri::command]
async fn deliver_knowledge(
    client: State<'_, DaemonClient>,
    delivery_id: bastet_core::KnowledgeDeliveryId,
    delivered_on: String,
) -> Result<bastet_protocol::KnowledgeDeliveryReceipt, String> {
    if delivered_on.len() != 10
        || delivered_on
            .as_bytes()
            .iter()
            .enumerate()
            .any(|(index, byte)| {
                if matches!(index, 4 | 7) {
                    *byte != b'-'
                } else {
                    !byte.is_ascii_digit()
                }
            })
    {
        return Err("delivery date must be YYYY-MM-DD".into());
    }
    let snapshot = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    let delivery = snapshot
        .catalog
        .deliverables
        .knowledge_deliveries
        .iter()
        .find(|delivery| delivery.metadata.id == delivery_id)
        .ok_or_else(|| "knowledge delivery not found".to_string())?;
    if delivery.state != bastet_core::DeliveryState::Prepared {
        return Err("knowledge delivery is not prepared".into());
    }
    let preview = delivery.preview.clone();
    let target = delivery.target;
    let receipt = tauri::async_runtime::spawn_blocking(move || match target {
        bastet_core::KnowledgeTarget::AgentMemoryOs => deliver_agent_memory(delivery_id, &preview),
        bastet_core::KnowledgeTarget::BastetMind => {
            let root = env::var_os("BASTET_MIND_ROOT")
                .map(PathBuf::from)
                .ok_or_else(|| "BASTET_MIND_ROOT is not configured".to_string())?;
            deliver_bastet_mind(&root, delivery_id, &preview, &delivered_on)
        }
    })
    .await
    .map_err(|error| error.to_string())??;
    client
        .complete_knowledge_delivery(bastet_protocol::CompleteKnowledgeDeliveryCommand {
            expected_m3_revision: snapshot.revision,
            delivery_id,
            destination_receipt: receipt,
        })
        .await
        .map_err(|error| error.to_string())
}

fn deliver_agent_memory(
    delivery_id: bastet_core::KnowledgeDeliveryId,
    preview: &str,
) -> Result<String, String> {
    let executable = configured_executable("BASTET_AGENT_MEMORY_BIN", "agent-memory")
        .ok_or_else(|| "AgentMemoryOS CLI is unavailable".to_string())?;
    deliver_agent_memory_with(&executable, delivery_id, preview)
}

fn deliver_agent_memory_with(
    executable: &Path,
    delivery_id: bastet_core::KnowledgeDeliveryId,
    preview: &str,
) -> Result<String, String> {
    let marker = format!("bastet-delivery:{}", delivery_id.value());
    let content = format!("[{marker}] {}", preview.trim());
    let search = Command::new(executable)
        .args([
            "search",
            &marker,
            "--owner",
            "bastet-workstation",
            "--limit",
            "20",
            "--json",
        ])
        .output()
        .map_err(|error| error.to_string())?;
    if !search.status.success() {
        return Err("AgentMemoryOS reconciliation search failed".into());
    }
    let hits: serde_json::Value =
        serde_json::from_slice(&search.stdout).map_err(|error| error.to_string())?;
    if let Some(id) = hits
        .as_array()
        .and_then(|items| {
            items
                .iter()
                .find(|item| item.get("content").and_then(|value| value.as_str()) == Some(&content))
        })
        .and_then(|item| item.get("id"))
        .and_then(|value| value.as_str())
    {
        return Ok(format!("agent-memory:{id}"));
    }
    let output = Command::new(executable)
        .args([
            "add",
            &content,
            "--owner",
            "bastet-workstation",
            "--scope",
            "project",
            "--type",
            "fact",
            "--tag",
            &marker,
            "--confidence",
            "1",
            "--importance",
            "0.9",
        ])
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err("AgentMemoryOS rejected the delivery".into());
    }
    let id = std::str::from_utf8(&output.stdout)
        .map_err(|error| error.to_string())?
        .trim();
    if id.is_empty()
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err("AgentMemoryOS returned an invalid receipt".into());
    }
    Ok(format!("agent-memory:{id}"))
}

fn deliver_bastet_mind(
    root: &Path,
    delivery_id: bastet_core::KnowledgeDeliveryId,
    preview: &str,
    delivered_on: &str,
) -> Result<String, String> {
    let root = root.canonicalize().map_err(|error| error.to_string())?;
    let output_dir = root
        .join("40-輸出成果")
        .canonicalize()
        .map_err(|error| error.to_string())?;
    let index = root.join("index.md");
    let log = root.join("log.md");
    if !root.join("AGENTS.md").is_file()
        || !index.is_file()
        || !log.is_file()
        || !output_dir.starts_with(&root)
    {
        return Err("BASTET_MIND_ROOT is not a valid BastetMind vault".into());
    }
    let marker = format!("bastet-delivery:{}", delivery_id.value());
    let name = format!("Bastet Workstation Delivery {}", delivery_id.value());
    let path = output_dir.join(format!("{name}.md"));
    let body = format!("---\ntype: output\nstatus: active\ncreated: {delivered_on}\nupdated: {delivered_on}\naliases: []\ntags: [Bastet-Workstation]\nsources: [\"[[40-輸出成果/Bastet Workstation Master Plan]]\"]\nconfidence: high\n---\n\n<!-- {marker} -->\n\n# {name}\n\n{}\n", preview.trim());
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
    {
        Ok(mut file) => file
            .write_all(body.as_bytes())
            .map_err(|error| error.to_string())?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if std::fs::read_to_string(&path).map_err(|error| error.to_string())? != body {
                return Err("BastetMind delivery path conflicts with different content".into());
            }
        }
        Err(error) => return Err(error.to_string()),
    }
    append_once(
        &index,
        &marker,
        &format!("\n- [[40-輸出成果/{name}]] — Bastet Workstation 明確交付。 <!-- {marker} -->\n"),
    )?;
    append_once(&log, &marker, &format!("\n\n## [{delivered_on}] knowledge delivery | Bastet Workstation\n\n- 新增 [[40-輸出成果/{name}]]；來源為已接受且已遮蔽的 Workstation artifact。 <!-- {marker} -->\n"))?;
    Ok(format!("bastetmind:{marker}"))
}

fn append_once(path: &Path, marker: &str, content: &str) -> Result<(), String> {
    if std::fs::read_to_string(path)
        .map_err(|error| error.to_string())?
        .contains(marker)
    {
        return Ok(());
    }
    std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .and_then(|mut file| file.write_all(content.as_bytes()))
        .map_err(|error| error.to_string())
}

fn timestamp_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_millis()
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
    let graphs = client
        .graph_executions()
        .await
        .map_err(|error| error.to_string())?;
    Ok(project_m3(updated, graphs.executions))
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
    let graphs = client
        .graph_executions()
        .await
        .map_err(|error| error.to_string())?;
    Ok(project_m3(updated, graphs.executions))
}

fn project_m3(
    snapshot: bastet_protocol::M3CatalogSnapshot,
    executions: Vec<bastet_core::GraphExecution>,
) -> M3Projection {
    let missing_output_execution_id = executions
        .iter()
        .find(|execution| {
            execution
                .nodes
                .iter()
                .all(|node| node.state == bastet_core::GraphNodeState::Succeeded)
                && execution
                    .nodes
                    .iter()
                    .any(|node| execution.output(node.node_id).is_none())
                && !executions
                    .iter()
                    .any(|next| next.restarted_from_execution_id == Some(execution.id))
        })
        .map(|execution| execution.id.value().to_string());
    let joined_drafts = executions
        .iter()
        .filter_map(|execution| {
            if !execution
                .nodes
                .iter()
                .all(|node| node.state == bastet_core::GraphNodeState::Succeeded)
                || snapshot
                    .catalog
                    .deliverables
                    .documents
                    .iter()
                    .any(|artifact| {
                        artifact
                            .versions
                            .iter()
                            .any(|version| version.source_execution_id == Some(execution.id))
                    })
            {
                return None;
            }
            let join = execution
                .graph
                .nodes
                .iter()
                .find(|node| node.kind == bastet_core::GraphNodeKind::Join)?;
            let output = execution.output(join.id)?;
            Some(JoinedDraftProjection {
                execution_id: execution.id.value().to_string(),
                title: output
                    .markdown
                    .lines()
                    .find_map(|line| line.strip_prefix("# "))
                    .unwrap_or(&join.title)
                    .to_owned(),
                markdown: output.markdown.clone(),
            })
        })
        .collect();
    let awaiting_meetings = snapshot
        .catalog
        .meetings
        .meetings
        .iter()
        .filter(|meeting| meeting.state == bastet_core::MeetingState::AwaitingDecision)
        .map(|meeting| MeetingProjection {
            meeting_id: meeting.metadata.id.value().to_string(),
            summary: meeting
                .rounds
                .last()
                .map(|round| round.summary.clone())
                .unwrap_or_default(),
        })
        .collect();
    let document_versions = snapshot
        .catalog
        .deliverables
        .documents
        .iter()
        .flat_map(|document| {
            document.versions.iter().map(|version| DocumentProjection {
                project_id: document.project_id.value().to_string(),
                artifact_id: document.metadata.id.value().to_string(),
                version_id: version.id.value().to_string(),
                title: document.title.clone(),
                markdown: version.markdown.clone(),
                content_hash: version.content_hash.clone(),
                accepted: version.accepted_by.is_some(),
            })
        })
        .collect();
    let knowledge_deliveries = snapshot
        .catalog
        .deliverables
        .knowledge_deliveries
        .iter()
        .map(|delivery| KnowledgeDeliveryProjection {
            delivery_id: delivery.metadata.id.value().to_string(),
            target: format!("{:?}", delivery.target).to_lowercase(),
            preview: delivery.preview.clone(),
            state: format!("{:?}", delivery.state).to_lowercase(),
        })
        .collect();
    let graph_nodes = executions
        .into_iter()
        .flat_map(|execution| {
            execution.nodes.into_iter().filter_map(move |node| {
                let definition = execution
                    .graph
                    .nodes
                    .iter()
                    .find(|definition| definition.id == node.node_id)?;
                Some(GraphNodeProjection {
                    execution_id: execution.id.value().to_string(),
                    node_id: node.node_id.value().to_string(),
                    failure_kind: node
                        .failure
                        .as_ref()
                        .map(|failure| failure_kind_label(failure.kind)),
                    title: definition.title.clone(),
                    state: format!("{:?}", node.state).to_lowercase(),
                    pet_state: pet_state(node.state),
                })
            })
        })
        .collect();
    M3Projection {
        joined_drafts,
        missing_output_execution_id,
        revision: snapshot.revision,
        pet_profiles: snapshot.catalog.office.pet_profiles,
        pet_assignments: snapshot.catalog.office.pet_assignments.len(),
        rooms: snapshot.catalog.office.rooms.len(),
        meetings: snapshot.catalog.meetings.meetings.len(),
        documents: snapshot.catalog.deliverables.documents.len(),
        costs: snapshot.catalog.deliverables.costs.len(),
        graph_nodes,
        awaiting_meetings,
        document_versions,
        knowledge_deliveries,
    }
}

fn pet_state(state: bastet_core::GraphNodeState) -> &'static str {
    match state {
        bastet_core::GraphNodeState::Pending => "idle",
        bastet_core::GraphNodeState::Running => "working",
        bastet_core::GraphNodeState::Succeeded => "succeeded",
        bastet_core::GraphNodeState::Failed | bastet_core::GraphNodeState::Cancelled => "failed",
        bastet_core::GraphNodeState::Blocked => "blocked",
        bastet_core::GraphNodeState::Uncertain => "waiting",
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
                let can_cancel = run_can_cancel(run.state);
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
    let models: Vec<String> = adapter
        .connect_app_server(std::time::Duration::from_secs(10))
        .and_then(|mut server| server.list_models(None, 100))
        .map(|page| page.models.into_iter().map(|model| model.model).collect())
        .unwrap_or_default();
    let capabilities = adapter.capabilities();
    AgentStatus {
        adapter_kind: "codex_cli",
        display_name: "Codex CLI",
        installed: true,
        version,
        authenticated,
        model_count: Some(models.len()),
        models,
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
    let models = adapter
        .list_models()
        .map(|models| models.into_iter().map(|model| model.id).collect::<Vec<_>>())
        .unwrap_or_default();
    let capabilities = adapter.capabilities();
    AgentStatus {
        adapter_kind: "agy_cli",
        display_name: "Agy CLI",
        installed: true,
        version,
        authenticated: None,
        model_count: Some(models.len()),
        models,
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
        models: Vec::new(),
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
    credential_scope_acknowledged: Option<bool>,
) -> Result<ApprovalReceipt, String> {
    client
        .decide_approval_with_scope_review(
            ApprovalDecision {
                request_id,
                request_hash,
                kind,
                decided_at_ms,
                actor: "local-desktop-user".into(),
            },
            credential_scope_acknowledged.unwrap_or(false),
        )
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
            app.manage(supervisor.client()?);
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
            run_ready_mvp_nodes,
            retry_missing_graph_outputs,
            retry_failed_mvp_node,
            prepare_mvp,
            accept_mvp_decision,
            create_mvp_document,
            export_mvp_document,
            accept_mvp_document,
            prepare_knowledge_delivery,
            deliver_knowledge,
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
    fn cancellation_is_not_advertised_during_uninterruptible_startup() {
        use bastet_core::NormalizedRunState::*;
        assert!(run_can_cancel(Running));
        assert!(run_can_cancel(Recovering));
        for state in [
            Starting, Cancelling, Succeeded, Failed, Cancelled, Blocked, Uncertain,
        ] {
            assert!(!run_can_cancel(state));
        }
    }

    #[test]
    fn document_export_contains_markdown_and_never_overwrites_other_content() {
        let root = tempfile::tempdir().unwrap();
        let version = bastet_core::DocumentVersion::create(
            bastet_core::ArtifactVersionId::new(),
            1,
            None,
            "# Research\n\nActual joined research content.".into(),
            vec![
                bastet_core::GraphNodeId::new(),
                bastet_core::GraphNodeId::new(),
            ],
        )
        .unwrap();
        let path = export_document_file(root.path(), &version).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), version.markdown);
        assert_eq!(export_document_file(root.path(), &version).unwrap(), path);
        std::fs::write(&path, "user edits").unwrap();
        assert!(export_document_file(root.path(), &version).is_err());
        assert_eq!(std::fs::read_to_string(path).unwrap(), "user edits");
    }

    #[test]
    fn bootstrap_declares_daemon_authority() {
        let state = bootstrap_state();
        assert_eq!(state.product_name, "Bastet Workstation");
        assert_eq!(state.protocol_version, 1);
        assert!(state.daemon_authoritative);
    }

    #[test]
    fn bastet_mind_delivery_is_idempotent_and_updates_required_indexes() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("40-輸出成果")).unwrap();
        std::fs::write(root.path().join("AGENTS.md"), "fixture").unwrap();
        std::fs::write(root.path().join("index.md"), "# Index\n").unwrap();
        std::fs::write(root.path().join("log.md"), "# Log\n").unwrap();
        let id = bastet_core::KnowledgeDeliveryId::from_bytes([91; 16]);
        let first = deliver_bastet_mind(root.path(), id, "Redacted result", "2026-09-07").unwrap();
        let second = deliver_bastet_mind(root.path(), id, "Redacted result", "2026-09-07").unwrap();
        assert_eq!(first, second);
        let marker = format!("bastet-delivery:{}", id.value());
        assert_eq!(
            std::fs::read_to_string(root.path().join("index.md"))
                .unwrap()
                .matches(&marker)
                .count(),
            1
        );
        assert_eq!(
            std::fs::read_to_string(root.path().join("log.md"))
                .unwrap()
                .matches(&marker)
                .count(),
            1
        );
        assert_eq!(
            std::fs::read_dir(root.path().join("40-輸出成果"))
                .unwrap()
                .count(),
            1
        );
    }

    #[cfg(unix)]
    #[test]
    fn agent_memory_delivery_requires_and_returns_a_real_cli_receipt() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let executable = root.path().join("agent-memory-fixture");
        std::fs::write(&executable, "#!/bin/sh\nif [ \"$1\" = search ]; then printf '[]'; else printf 'mem_fixture_receipt\\n'; fi\n").unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let receipt = deliver_agent_memory_with(
            &executable,
            bastet_core::KnowledgeDeliveryId::from_bytes([92; 16]),
            "Redacted result",
        )
        .unwrap();
        assert_eq!(receipt, "agent-memory:mem_fixture_receipt");
    }

    #[tokio::test]
    #[ignore = "requires explicit installed/authenticated Codex and Agy CLIs; starts three real read-only provider runs"]
    async fn real_two_branch_mvp_and_join_survive_restart() {
        let codex_model = env::var("BASTET_CODEX_MODEL").expect("BASTET_CODEX_MODEL must be set");
        let agy_model = env::var("BASTET_AGY_MODEL").expect("BASTET_AGY_MODEL must be set");
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("mvp.db");
        let store = bastet_daemon::Store::open(&database).unwrap();
        let prepared = store
            .prepare_mvp(bastet_protocol::PrepareMvpCommand {
                expected_catalog_revision: 0,
                expected_m3_revision: 0,
                project_name: "Real M3 provider gate".into(),
                workspace_root: root.path().to_string_lossy().into_owned(),
                codex_model,
                agy_model,
            })
            .unwrap();
        let accepted = store.accept_mvp_decision(bastet_protocol::AcceptDecisionBaselineCommand {
            expected_m3_revision: prepared.m3_revision, meeting_id: prepared.meeting_id,
            content: "Independently assess this statement: a matching SHA-256 digest can verify that document bytes have not changed, but does not prove that its factual claims are correct. Explain the distinction briefly, then integrate the two assessments. No external tools are needed for this bounded conceptual task.".into(),
            accepted_by: "m3-real-gate".into(), accepted_at: "2026-09-07T00:00:00Z".into(),
        }).unwrap();
        store.mark_ready().unwrap();
        let endpoint = bastet_local_ipc::Endpoint::for_database(&database).unwrap();
        let (listener, _endpoint_guard) = bastet_local_ipc::bind(&endpoint).unwrap();
        let client = DaemonClient::for_database(&database).unwrap();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let router =
            bastet_daemon::production_router_with_shutdown(store.clone(), shutdown_tx.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.changed().await;
                })
                .await
                .unwrap();
        });
        for expected_runs in [2, 1] {
            let graph = store.graph_execution(accepted.graph_execution_id).unwrap();
            let receipt = client
                .execute_ready_graph(
                    graph.id,
                    bastet_protocol::ExecuteReadyGraphCommand {
                        expected_catalog_revision: store.catalog().unwrap().revision,
                        expected_graph_revision: graph.revision,
                    },
                )
                .await
                .unwrap();
            assert_eq!(receipt.run_ids.len(), expected_runs);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(240);
            loop {
                let snapshot = store.catalog().unwrap();
                let runs = snapshot
                    .catalog
                    .runs
                    .iter()
                    .filter(|run| receipt.run_ids.contains(&run.metadata.id))
                    .collect::<Vec<_>>();
                assert_eq!(runs.len(), expected_runs);
                if runs.iter().all(|run| {
                    !matches!(
                        run.state,
                        bastet_core::NormalizedRunState::Starting
                            | bastet_core::NormalizedRunState::Running
                            | bastet_core::NormalizedRunState::Cancelling
                            | bastet_core::NormalizedRunState::Recovering
                    )
                }) {
                    assert!(
                        runs.iter()
                            .all(|run| run.state == bastet_core::NormalizedRunState::Succeeded),
                        "daemon-owned provider run did not succeed"
                    );
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    // Do not silently abandon live work if this opt-in canary
                    // times out. Request cancellation using the same client path.
                    for run in runs {
                        let revision = store.catalog().unwrap().revision;
                        let _ = client.cancel_run(run.metadata.id, revision).await;
                    }
                    panic!("daemon-owned provider canary exceeded its bounded wait");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        let graph = store.graph_execution(accepted.graph_execution_id).unwrap();
        assert_eq!(graph.outputs.len(), 3);
        let join_node = graph
            .graph
            .nodes
            .iter()
            .find(|node| !node.needs.is_empty())
            .unwrap();
        let joined_markdown = graph
            .outputs
            .iter()
            .find(|output| output.node_id == join_node.id)
            .expect("join must persist actual text")
            .markdown
            .clone();
        assert!(!joined_markdown.trim().is_empty());
        shutdown_tx.send(true).unwrap();
        server.await.unwrap();
        let document = store
            .create_mvp_document(bastet_protocol::CreateDocumentCommand {
                expected_m3_revision: store.m3_catalog().unwrap().revision,
                graph_execution_id: graph.id,
                title: "Real provider report".into(),
                markdown: joined_markdown.clone(),
            })
            .unwrap();
        let accepted_document = store
            .accept_mvp_document(bastet_protocol::AcceptDocumentCommand {
                expected_m3_revision: document.m3_revision,
                artifact_id: document.artifact_id,
                version_id: document.version_id,
                content_hash: document.content_hash,
                accepted_by: "m3-real-gate".into(),
                accepted_at: "2026-09-07T00:01:00Z".into(),
            })
            .unwrap();
        let memory = store
            .prepare_knowledge_delivery(bastet_protocol::PrepareKnowledgeDeliveryCommand {
                expected_m3_revision: accepted_document.m3_revision,
                project_id: prepared.project_id,
                artifact_version_id: document.version_id,
                target: bastet_core::KnowledgeTarget::AgentMemoryOs,
                preview: "Redacted real-provider result".into(),
            })
            .unwrap();
        let memory = store
            .complete_knowledge_delivery(bastet_protocol::CompleteKnowledgeDeliveryCommand {
                expected_m3_revision: memory.m3_revision,
                delivery_id: memory.delivery_id,
                destination_receipt: "agent-memory:m3-real-gate-fixture".into(),
            })
            .unwrap();
        let mind = store
            .prepare_knowledge_delivery(bastet_protocol::PrepareKnowledgeDeliveryCommand {
                expected_m3_revision: memory.m3_revision,
                project_id: prepared.project_id,
                artifact_version_id: document.version_id,
                target: bastet_core::KnowledgeTarget::BastetMind,
                preview: joined_markdown.clone(),
            })
            .unwrap();
        let vault = tempfile::tempdir().unwrap();
        std::fs::create_dir(vault.path().join("40-輸出成果")).unwrap();
        for name in ["AGENTS.md", "index.md", "log.md"] {
            std::fs::write(vault.path().join(name), "fixture\n").unwrap();
        }
        let mind_receipt = deliver_bastet_mind(
            vault.path(),
            mind.delivery_id,
            &joined_markdown,
            "2026-09-07",
        )
        .unwrap();
        let published = vault.path().join("40-輸出成果").join(format!(
            "Bastet Workstation Delivery {}.md",
            mind.delivery_id.value()
        ));
        assert!(std::fs::read_to_string(published)
            .unwrap()
            .contains(&joined_markdown));
        let accepted_version =
            store.m3_catalog().unwrap().catalog.deliverables.documents[0].versions[0].clone();
        let exported = export_document_file(root.path(), &accepted_version).unwrap();
        assert_eq!(std::fs::read_to_string(exported).unwrap(), joined_markdown);
        store
            .complete_knowledge_delivery(bastet_protocol::CompleteKnowledgeDeliveryCommand {
                expected_m3_revision: mind.m3_revision,
                delivery_id: mind.delivery_id,
                destination_receipt: mind_receipt,
            })
            .unwrap();
        drop(store);
        let reopened = bastet_daemon::Store::open(&database).unwrap();
        assert!(reopened
            .graph_execution(graph.id)
            .unwrap()
            .nodes
            .iter()
            .all(|node| node.state == bastet_core::GraphNodeState::Succeeded));
        assert_eq!(reopened.catalog().unwrap().catalog.runs.len(), 3);
        let reopened_m3 = reopened.m3_catalog().unwrap().catalog;
        assert_eq!(
            reopened_m3.deliverables.documents[0].versions[0].markdown,
            joined_markdown
        );
        assert_eq!(reopened_m3.deliverables.costs.len(), 3);
        assert!(reopened_m3.deliverables.documents[0].versions[0]
            .accepted_by
            .is_some());
        assert_eq!(reopened_m3.deliverables.knowledge_deliveries.len(), 2);
        assert!(reopened_m3
            .deliverables
            .knowledge_deliveries
            .iter()
            .all(|delivery| delivery.state == bastet_core::DeliveryState::Delivered));
    }
}
