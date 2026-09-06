mod supervisor;

#[cfg(target_os = "macos")]
mod macos_power;

use bastet_client::DaemonClient;
use bastet_core::{
    ApprovalDecision, ApprovalRequestId, EntityLifecycle, EntityMetadata, MeetingId, PetProfile,
    PetProfileId, PetStateAsset, Provenance, REQUIRED_PET_STATES,
};
use bastet_protocol::{
    ApprovalList, ApprovalReceipt, CheckpointReceipt, DaemonLifecycle, DaemonSnapshot,
    PROTOCOL_VERSION,
};
use serde::Serialize;
use std::{
    env,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
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
}

#[derive(Serialize)]
struct GraphNodeProjection {
    execution_id: String,
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

struct ProviderOutcome {
    terminal_state: bastet_core::NormalizedRunState,
    provider_session_id: Option<String>,
    cost: bastet_core::CostEvidence,
}

fn unknown_cost() -> bastet_core::CostEvidence {
    bastet_core::CostEvidence {
        evidence_class: bastet_core::EvidenceClass::Unknown,
        currency: None,
        amount: None,
        input_tokens: None,
        output_tokens: None,
        confidence: 0.0,
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
async fn run_ready_mvp_nodes(client: State<'_, DaemonClient>) -> Result<M3Projection, String> {
    let identity = client.catalog().await.map_err(|error| error.to_string())?;
    let m3 = client
        .m3_catalog()
        .await
        .map_err(|error| error.to_string())?;
    let executions = client
        .graph_executions()
        .await
        .map_err(|error| error.to_string())?
        .executions;
    let execution = executions
        .into_iter()
        .find(|execution| {
            execution
                .nodes
                .iter()
                .any(|node| node.state == bastet_core::GraphNodeState::Pending)
        })
        .ok_or_else(|| "no pending MVP graph nodes".to_string())?;
    let states = execution
        .nodes
        .iter()
        .map(|node| (node.node_id, node.state))
        .collect::<std::collections::HashMap<_, _>>();
    let ready = execution
        .graph
        .nodes
        .iter()
        .filter(|definition| {
            states.get(&definition.id) == Some(&bastet_core::GraphNodeState::Pending)
                && definition.needs.iter().all(|dependency| {
                    states.get(dependency) == Some(&bastet_core::GraphNodeState::Succeeded)
                })
        })
        .take(2)
        .cloned()
        .collect::<Vec<_>>();
    if ready.is_empty() {
        return Err("no graph node is ready; reconcile uncertain or failed work first".into());
    }
    let mut catalog_revision = identity.revision;
    let mut graph_revision = execution.revision;
    let mut begun = Vec::new();
    for definition in ready {
        let receipt = client
            .begin_graph_node_run(
                execution.id,
                bastet_protocol::BeginGraphNodeRunCommand {
                    expected_catalog_revision: catalog_revision,
                    expected_graph_revision: graph_revision,
                    node_id: definition.id,
                    owner: format!("desktop-provider-{}", definition.id.value()),
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        catalog_revision = receipt.catalog_revision;
        graph_revision = receipt.graph_revision;
        begun.push(receipt);
    }
    let handles = begun
        .iter()
        .map(|receipt| {
            let adapter = receipt.adapter_kind.clone();
            let model = receipt.model.clone();
            let prompt = receipt.prompt.clone();
            let root = PathBuf::from(&receipt.workspace_root);
            let run_id = receipt.run_id;
            tauri::async_runtime::spawn_blocking(move || {
                run_provider(&adapter, run_id, model, prompt, root)
            })
        })
        .collect::<Vec<_>>();
    let mut outcomes = Vec::new();
    for handle in handles {
        outcomes.push(match handle.await {
            Ok(Ok(outcome)) => outcome,
            _ => ProviderOutcome {
                terminal_state: bastet_core::NormalizedRunState::Uncertain,
                provider_session_id: None,
                cost: unknown_cost(),
            },
        });
    }
    let mut m3_revision = m3.revision;
    for (receipt, outcome) in begun.into_iter().zip(outcomes) {
        let finished = client
            .finish_graph_node_run(
                execution.id,
                bastet_protocol::FinishGraphNodeRunCommand {
                    expected_catalog_revision: catalog_revision,
                    expected_graph_revision: graph_revision,
                    expected_m3_revision: m3_revision,
                    node_id: receipt.node_id,
                    run_id: receipt.run_id,
                    owner: format!("desktop-provider-{}", receipt.node_id.value()),
                    terminal_state: outcome.terminal_state,
                    provider_session_id: outcome.provider_session_id,
                    cost: outcome.cost,
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        catalog_revision = finished.catalog_revision;
        graph_revision = finished.graph_revision;
        m3_revision = finished.m3_revision;
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

fn run_provider(
    adapter: &str,
    run_id: bastet_core::RunId,
    model: String,
    prompt: String,
    root: PathBuf,
) -> Result<ProviderOutcome, String> {
    match adapter {
        "codex_cli" => run_codex(run_id, model, prompt, root),
        "agy_cli" => run_agy(run_id, model, prompt, root),
        _ => Err(format!("unsupported reference adapter: {adapter}")),
    }
}

fn run_codex(
    run_id: bastet_core::RunId,
    model: String,
    prompt: String,
    root: PathBuf,
) -> Result<ProviderOutcome, String> {
    let executable =
        configured_executable("BASTET_CODEX_BIN", "codex").ok_or("Codex CLI is unavailable")?;
    let adapter = bastet_adapter_codex::CodexAdapter::new(executable);
    let mut server = adapter
        .connect_app_server(Duration::from_secs(120))
        .map_err(|error| error.to_string())?;
    let mut started = server
        .start_tracked_run(bastet_adapter_codex::CodexRunRequest {
            run_id,
            model,
            prompt,
            cwd: root,
            approval_policy: bastet_adapter_codex::ApprovalPolicy::Never,
            sandbox_policy: bastet_adapter_codex::TurnSandboxPolicy::ReadOnly,
            effort: Some("medium".into()),
        })
        .map_err(|error| error.to_string())?;
    let provider_session_id = Some(started.thread.thread_id.clone());
    let mut cost = unknown_cost();
    loop {
        match started
            .tracker
            .next_update(&mut server, &timestamp_ms().to_string())
            .map_err(|error| error.to_string())?
        {
            bastet_adapter_codex::CodexRunUpdate::Evidence(
                bastet_adapter_codex::CodexRunEvidenceUpdate::Cost(observed),
            ) => cost = observed,
            bastet_adapter_codex::CodexRunUpdate::Evidence(_) => {}
            bastet_adapter_codex::CodexRunUpdate::Lifecycle(event) => match event.event.state {
                bastet_core::NormalizedRunState::Succeeded
                | bastet_core::NormalizedRunState::Failed
                | bastet_core::NormalizedRunState::Cancelled
                | bastet_core::NormalizedRunState::Blocked
                | bastet_core::NormalizedRunState::Uncertain => {
                    return Ok(ProviderOutcome {
                        terminal_state: event.event.state,
                        provider_session_id,
                        cost,
                    })
                }
                _ => {}
            },
        }
    }
}

fn run_agy(
    run_id: bastet_core::RunId,
    model: String,
    prompt: String,
    root: PathBuf,
) -> Result<ProviderOutcome, String> {
    let executable =
        configured_executable("BASTET_AGY_BIN", "agy").ok_or("Agy CLI is unavailable")?;
    let mut process = bastet_adapter_agy::AgyProcess::spawn(
        executable,
        bastet_adapter_agy::AgyRunRequest {
            run_id,
            model,
            effort: Some("medium".into()),
            prompt,
            cwd: root,
            read_only: true,
            timeout: Duration::from_secs(120),
            conversation_id: None,
        },
    )
    .map_err(|error| error.to_string())?;
    let mut cost = unknown_cost();
    loop {
        match process
            .next_update(&timestamp_ms().to_string())
            .map_err(|error| error.to_string())?
        {
            bastet_adapter_agy::AgyRunUpdate::Cost(observed) => cost = observed,
            bastet_adapter_agy::AgyRunUpdate::WriteReceipt(_) => {
                return Err("read-only Agy run reported a write".into())
            }
            bastet_adapter_agy::AgyRunUpdate::Lifecycle { event, .. } => match event.state {
                bastet_core::NormalizedRunState::Succeeded
                | bastet_core::NormalizedRunState::Failed
                | bastet_core::NormalizedRunState::Cancelled
                | bastet_core::NormalizedRunState::Blocked
                | bastet_core::NormalizedRunState::Uncertain => {
                    return Ok(ProviderOutcome {
                        terminal_state: event.state,
                        provider_session_id: process.conversation_id().map(str::to_owned),
                        cost,
                    })
                }
                _ => {}
            },
        }
    }
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
    if search.status.success() {
        let hits: serde_json::Value =
            serde_json::from_slice(&search.stdout).map_err(|error| error.to_string())?;
        if let Some(id) = hits
            .as_array()
            .and_then(|items| {
                items.iter().find(|item| {
                    item.get("content").and_then(|value| value.as_str()) == Some(&content)
                })
            })
            .and_then(|item| item.get("id"))
            .and_then(|value| value.as_str())
        {
            return Ok(format!("agent-memory:{id}"));
        }
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
                    title: definition.title.clone(),
                    state: format!("{:?}", node.state).to_lowercase(),
                    pet_state: pet_state(node.state),
                })
            })
        })
        .collect();
    M3Projection {
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
        bastet_core::GraphNodeState::Failed => "failed",
        bastet_core::GraphNodeState::Blocked => "blocked",
        bastet_core::GraphNodeState::Uncertain => "waiting",
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
            run_ready_mvp_nodes,
            prepare_mvp,
            accept_mvp_decision,
            create_mvp_document,
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
}
