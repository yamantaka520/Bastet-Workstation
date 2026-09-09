use std::{
    env,
    panic::{catch_unwind, AssertUnwindSafe},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender},
        Arc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bastet_core::{
    AdapterFailure, AdapterFailureKind, CostEvidence, EvidenceClass, NormalizedRunState, RunId,
};
use bastet_protocol::{BeginGraphNodeRunReceipt, FinishGraphNodeRunCommand};
use thiserror::Error;

use crate::{RunControlError, RunController, RunControllerRegistry, Store, StoreError};

const POLL_INTERVAL: Duration = Duration::from_millis(50);
const CANCEL_REQUEST_LIFETIME: Duration = Duration::from_millis(1_500);
const CANCEL_ACK_TIMEOUT: Duration = Duration::from_secs(2);
const PROVIDER_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Error)]
pub(crate) enum ProviderExecutorError {
    #[error("provider controller registration failed")]
    Controller,
    #[error("provider worker could not be started")]
    Spawn,
}

#[derive(Debug, Clone)]
pub(crate) struct ProviderOutcome {
    pub terminal_state: NormalizedRunState,
    pub provider_session_id: Option<String>,
    pub cost: CostEvidence,
    pub output_markdown: Option<String>,
    pub failure: Option<AdapterFailure>,
}

impl ProviderOutcome {
    fn unavailable() -> Self {
        Self {
            terminal_state: NormalizedRunState::Uncertain,
            provider_session_id: None,
            cost: unknown_cost(),
            output_markdown: None,
            failure: Some(AdapterFailure {
                kind: AdapterFailureKind::Unknown,
                message_key: "mvp.failure.unknown".into(),
                retryable: false,
                provider_code: None,
                redacted_detail: None,
            }),
        }
    }
}

pub(crate) struct CancelRequest {
    run_id: RunId,
    expires_at: Instant,
    acknowledgement: SyncSender<Result<(), RunControlError>>,
}

impl CancelRequest {
    #[cfg(test)]
    pub(crate) fn run_id(&self) -> RunId {
        self.run_id
    }

    #[cfg(test)]
    pub(crate) fn acknowledge(self, accepted: bool) {
        let _ = self.acknowledgement.try_send(if accepted {
            Ok(())
        } else {
            Err(RunControlError)
        });
    }
}

struct ChannelRunController {
    run_id: RunId,
    sender: SyncSender<CancelRequest>,
}

impl RunController for ChannelRunController {
    fn cancel(&self, run_id: RunId) -> Result<(), RunControlError> {
        if run_id != self.run_id {
            return Err(RunControlError);
        }
        let (acknowledgement, receiver) = mpsc::sync_channel(1);
        self.sender
            .try_send(CancelRequest {
                run_id,
                expires_at: Instant::now() + CANCEL_REQUEST_LIFETIME,
                acknowledgement,
            })
            .map_err(|_| RunControlError)?;
        receiver
            .recv_timeout(CANCEL_ACK_TIMEOUT)
            .map_err(|_| RunControlError)?
    }
}

pub(crate) trait ProviderRunner: Send + Sync + 'static {
    fn run(
        &self,
        receipt: &BeginGraphNodeRunReceipt,
        control: Receiver<CancelRequest>,
    ) -> ProviderOutcome;
}

#[derive(Clone)]
pub(crate) struct ProviderExecutor {
    store: Store,
    registry: RunControllerRegistry,
    runner: Arc<dyn ProviderRunner>,
    active: Arc<AtomicUsize>,
}

impl ProviderExecutor {
    pub(crate) fn production(store: Store, registry: RunControllerRegistry) -> Self {
        Self::with_runner(
            store.clone(),
            registry,
            Arc::new(SystemProviderRunner { store }),
        )
    }

    pub(crate) fn with_runner(
        store: Store,
        registry: RunControllerRegistry,
        runner: Arc<dyn ProviderRunner>,
    ) -> Self {
        Self {
            store,
            registry,
            runner,
            active: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn has_active(&self) -> bool {
        self.active.load(Ordering::Acquire) != 0
    }

    pub(crate) fn start(
        &self,
        receipt: BeginGraphNodeRunReceipt,
    ) -> Result<(), ProviderExecutorError> {
        let run_id = receipt.run_id;
        let (sender, control) = mpsc::sync_channel(1);
        self.registry
            .register(run_id, Arc::new(ChannelRunController { run_id, sender }))
            .map_err(|_| ProviderExecutorError::Controller)?;

        let store = self.store.clone();
        let registry = self.registry.clone();
        let runner = self.runner.clone();
        let active = self.active.clone();
        active.fetch_add(1, Ordering::AcqRel);
        let spawn = thread::Builder::new()
            .name(format!("bastet-provider-{}", run_id.value()))
            .spawn(move || {
                let _registration = RegistrationGuard {
                    registry,
                    run_id,
                    active,
                };
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    if store.preflight_provider_launch(&receipt).is_err() {
                        return ProviderOutcome::unavailable();
                    }
                    runner.run(&receipt, control)
                }))
                .unwrap_or_else(|_| ProviderOutcome::unavailable());
                if persist_outcome(&store, &receipt, outcome).is_err() {
                    eprintln!("provider terminal outcome could not be persisted safely");
                }
            });
        if spawn.is_err() {
            self.active.fetch_sub(1, Ordering::AcqRel);
            let _ = self.registry.unregister(run_id);
            return Err(ProviderExecutorError::Spawn);
        }
        Ok(())
    }

    pub(crate) fn persist_start_failure(&self, receipt: &BeginGraphNodeRunReceipt) {
        if persist_outcome(&self.store, receipt, ProviderOutcome::unavailable()).is_err() {
            eprintln!("provider start failure could not be persisted safely");
        }
    }
}

struct RegistrationGuard {
    registry: RunControllerRegistry,
    run_id: RunId,
    active: Arc<AtomicUsize>,
}

impl Drop for RegistrationGuard {
    fn drop(&mut self) {
        let _ = self.registry.unregister(self.run_id);
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

struct SystemProviderRunner {
    store: Store,
}

impl ProviderRunner for SystemProviderRunner {
    fn run(
        &self,
        receipt: &BeginGraphNodeRunReceipt,
        control: Receiver<CancelRequest>,
    ) -> ProviderOutcome {
        // A metadata selection must never silently use a different ambient
        // CLI login. Enable selected accounts only when a credential broker
        // can bind the exact account/reference/grant to this process.
        if receipt
            .launch_identity
            .as_ref()
            .is_none_or(|identity| identity.account.is_some())
        {
            return ProviderOutcome {
                terminal_state: NormalizedRunState::Blocked,
                provider_session_id: None,
                cost: unknown_cost(),
                output_markdown: None,
                failure: Some(AdapterFailure {
                    kind: AdapterFailureKind::Unsupported,
                    message_key: "adapter.failure.unsupported".into(),
                    retryable: false,
                    provider_code: None,
                    redacted_detail: None,
                }),
            };
        }
        match receipt.adapter_kind.as_str() {
            "codex_cli" => run_codex(receipt, control, &self.store)
                .unwrap_or_else(|_| ProviderOutcome::unavailable()),
            "agy_cli" => run_agy(receipt, control, &self.store)
                .unwrap_or_else(|_| ProviderOutcome::unavailable()),
            _ => ProviderOutcome::unavailable(),
        }
    }
}

fn run_codex(
    receipt: &BeginGraphNodeRunReceipt,
    control: Receiver<CancelRequest>,
    store: &Store,
) -> Result<ProviderOutcome, String> {
    let executable = configured_executable("BASTET_CODEX_BIN", "codex")
        .ok_or_else(|| "Codex CLI is unavailable".to_string())?;
    let adapter = bastet_adapter_codex::CodexAdapter::new(executable);
    let mut server = adapter
        .connect_app_server(PROVIDER_TIMEOUT)
        .map_err(|error| error.to_string())?;
    let mut started = server
        .start_tracked_run(bastet_adapter_codex::CodexRunRequest {
            run_id: receipt.run_id,
            model: receipt.model.clone(),
            prompt: receipt.prompt.clone(),
            cwd: PathBuf::from(&receipt.workspace_root),
            approval_policy: bastet_adapter_codex::ApprovalPolicy::Never,
            sandbox_policy: bastet_adapter_codex::TurnSandboxPolicy::ReadOnly,
            effort: Some("medium".into()),
        })
        .map_err(|error| error.to_string())?;
    let provider_session_id = Some(started.thread.thread_id.clone());
    let mut cost = unknown_cost();
    let mut saw_running = false;
    loop {
        if saw_running {
            service_cancel(&control, receipt.run_id, || {
                started
                    .tracker
                    .request_cancellation_with_timeout(
                        &mut server,
                        &timestamp_ms(),
                        Duration::from_secs(1),
                    )
                    .map(|_| ())
                    .map_err(|_| RunControlError)
            });
        }
        let Some(update) = started
            .tracker
            .poll_update(&mut server, &timestamp_ms(), POLL_INTERVAL)
            .map_err(|error| error.to_string())?
        else {
            continue;
        };
        match update {
            bastet_adapter_codex::CodexRunUpdate::Evidence(
                bastet_adapter_codex::CodexRunEvidenceUpdate::Cost(observed),
            ) => cost = observed,
            bastet_adapter_codex::CodexRunUpdate::Evidence(_) => {}
            bastet_adapter_codex::CodexRunUpdate::Lifecycle(event)
                if is_terminal(event.event.state) =>
            {
                return Ok(outcome(
                    event.event.state,
                    provider_session_id,
                    cost,
                    started.tracker.final_output(),
                    event.failure,
                ));
            }
            bastet_adapter_codex::CodexRunUpdate::Lifecycle(event) => {
                if event.event.state == NormalizedRunState::Running && !saw_running {
                    store
                        .record_provider_running(receipt.run_id, provider_session_id.as_deref())
                        .map_err(|_| "provider running state could not be persisted".to_string())?;
                    saw_running = true;
                }
            }
        }
    }
}

fn run_agy(
    receipt: &BeginGraphNodeRunReceipt,
    control: Receiver<CancelRequest>,
    store: &Store,
) -> Result<ProviderOutcome, String> {
    let executable = configured_executable("BASTET_AGY_BIN", "agy")
        .ok_or_else(|| "Agy CLI is unavailable".to_string())?;
    let mut process = bastet_adapter_agy::AgyProcess::spawn(
        executable,
        bastet_adapter_agy::AgyRunRequest {
            run_id: receipt.run_id,
            model: receipt.model.clone(),
            effort: None,
            prompt: receipt.prompt.clone(),
            cwd: PathBuf::from(&receipt.workspace_root),
            read_only: true,
            timeout: PROVIDER_TIMEOUT,
            conversation_id: None,
        },
    )
    .map_err(|error| error.to_string())?;
    let mut cost = unknown_cost();
    let mut saw_running = false;
    loop {
        if saw_running {
            service_cancel(&control, receipt.run_id, || {
                process
                    .request_cancellation(&timestamp_ms())
                    .map(|_| ())
                    .map_err(|_| RunControlError)
            });
        }
        let Some(update) = process
            .poll_update(&timestamp_ms(), POLL_INTERVAL)
            .map_err(|error| error.to_string())?
        else {
            continue;
        };
        match update {
            bastet_adapter_agy::AgyRunUpdate::Cost(observed) => cost = observed,
            bastet_adapter_agy::AgyRunUpdate::WriteReceipt(_) => {
                return Err("read-only Agy run reported a write".into())
            }
            bastet_adapter_agy::AgyRunUpdate::Lifecycle { event, failure }
                if is_terminal(event.state) =>
            {
                return Ok(outcome(
                    event.state,
                    process.conversation_id().map(str::to_owned),
                    cost,
                    process.final_output(),
                    failure,
                ));
            }
            bastet_adapter_agy::AgyRunUpdate::Lifecycle { event, .. } => {
                if event.state == NormalizedRunState::Running && !saw_running {
                    store
                        .record_provider_running(receipt.run_id, process.conversation_id())
                        .map_err(|_| "provider running state could not be persisted".to_string())?;
                    saw_running = true;
                }
            }
        }
    }
}

fn service_cancel(
    control: &Receiver<CancelRequest>,
    run_id: RunId,
    cancel: impl FnOnce() -> Result<(), RunControlError>,
) {
    let Ok(request) = control.try_recv() else {
        return;
    };
    let result = if request.run_id != run_id || Instant::now() > request.expires_at {
        Err(RunControlError)
    } else {
        cancel()
    };
    let _ = request.acknowledgement.try_send(result);
}

fn outcome(
    terminal_state: NormalizedRunState,
    provider_session_id: Option<String>,
    cost: CostEvidence,
    output: Option<&str>,
    failure: Option<AdapterFailure>,
) -> ProviderOutcome {
    let output_missing = terminal_state == NormalizedRunState::Succeeded
        && output.is_none_or(|text| text.trim().is_empty());
    ProviderOutcome {
        terminal_state: if output_missing {
            NormalizedRunState::Failed
        } else {
            terminal_state
        },
        provider_session_id,
        cost,
        output_markdown: output.map(str::to_owned),
        failure: if output_missing {
            Some(AdapterFailure {
                kind: AdapterFailureKind::MalformedOutput,
                message_key: "mvp.failure.invalid_output".into(),
                retryable: true,
                provider_code: None,
                redacted_detail: None,
            })
        } else {
            failure
        },
    }
}

fn persist_outcome(
    store: &Store,
    receipt: &BeginGraphNodeRunReceipt,
    mut outcome: ProviderOutcome,
) -> Result<(), ()> {
    if !matches!(
        outcome.terminal_state,
        NormalizedRunState::Succeeded | NormalizedRunState::Failed
    ) {
        outcome.output_markdown = None;
    }
    let mut backoff = Duration::from_millis(1);
    loop {
        let catalog = match store.catalog() {
            Ok(value) => value,
            Err(StoreError::Database(_)) | Err(StoreError::Poisoned) => {
                thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(1));
                continue;
            }
            Err(_) => return Err(()),
        };
        let m3 = match store.m3_catalog() {
            Ok(value) => value,
            Err(StoreError::Database(_)) | Err(StoreError::Poisoned) => {
                thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(1));
                continue;
            }
            Err(_) => return Err(()),
        };
        let graph = match store.graph_execution(receipt.execution_id) {
            Ok(value) => value,
            Err(StoreError::Database(_)) | Err(StoreError::Poisoned) => {
                thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(1));
                continue;
            }
            Err(_) => return Err(()),
        };
        let result = store.finish_graph_node_run(
            receipt.execution_id,
            FinishGraphNodeRunCommand {
                expected_catalog_revision: catalog.revision,
                expected_graph_revision: graph.revision,
                expected_m3_revision: m3.revision,
                node_id: receipt.node_id,
                run_id: receipt.run_id,
                owner: owner(receipt.node_id),
                terminal_state: outcome.terminal_state,
                failure: outcome.failure.clone(),
                provider_session_id: outcome.provider_session_id.clone(),
                output_markdown: outcome.output_markdown.clone(),
                cost: outcome.cost.clone(),
            },
        );
        match result {
            Ok(_) => return Ok(()),
            Err(StoreError::RevisionConflict { .. }) => {
                thread::yield_now();
                continue;
            }
            Err(StoreError::Database(_)) | Err(StoreError::Poisoned) => {
                thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_secs(1));
            }
            // A different current attempt or terminal state means this exact
            // run lost authority; never replay the provider to overcome it.
            Err(StoreError::InvalidRunState(_)) | Err(StoreError::RunNotFound) => return Ok(()),
            Err(_) => return Err(()),
        }
    }
}

pub(crate) fn owner(node_id: bastet_core::GraphNodeId) -> String {
    format!("daemon-provider-{}", node_id.value())
}

fn is_terminal(state: NormalizedRunState) -> bool {
    matches!(
        state,
        NormalizedRunState::Succeeded
            | NormalizedRunState::Failed
            | NormalizedRunState::Cancelled
            | NormalizedRunState::Blocked
            | NormalizedRunState::Uncertain
    )
}

fn unknown_cost() -> CostEvidence {
    CostEvidence {
        evidence_class: EvidenceClass::Unknown,
        currency: None,
        amount: None,
        input_tokens: None,
        output_tokens: None,
        confidence: 0.0,
    }
}

fn timestamp_ms() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_account_cannot_fall_back_to_ambient_authentication() {
        let directory = tempfile::tempdir().unwrap();
        let runner = SystemProviderRunner {
            store: Store::open(directory.path().join("account-boundary.db")).unwrap(),
        };
        let identity = bastet_protocol::ProviderLaunchIdentity {
            agent_instance_id: bastet_core::AgentInstanceId::new(),
            agent_provider_id: bastet_core::AgentProviderId::new(),
            project_id: bastet_core::ProjectId::new(),
            model_id: bastet_core::ModelId::new(),
            adapter_kind: "nonexistent-fixture-adapter".into(),
            model: "fixture-model".into(),
            account: Some(bastet_protocol::ProviderAccountSelection {
                account_id: bastet_core::AccountId::new(),
                provider_identity: "selected-not-authenticated".into(),
                credential: None,
            }),
        };
        let receipt = BeginGraphNodeRunReceipt {
            protocol_version: bastet_protocol::PROTOCOL_VERSION,
            execution_id: bastet_core::GraphRunId::new(),
            node_id: bastet_core::GraphNodeId::new(),
            session_id: bastet_core::SessionId::new(),
            run_id: RunId::new(),
            adapter_kind: identity.adapter_kind.clone(),
            model: identity.model.clone(),
            launch_identity: Some(identity),
            workspace_root: directory.path().display().to_string(),
            prompt: "fixture never starts a provider".into(),
            catalog_revision: 0,
            graph_revision: 0,
            event_sequence: 0,
        };
        let (_sender, control) = mpsc::sync_channel(1);
        let result = runner.run(&receipt, control);
        assert_eq!(result.terminal_state, NormalizedRunState::Blocked);
        assert_eq!(
            result.failure.unwrap().kind,
            AdapterFailureKind::Unsupported
        );
        assert_eq!(result.provider_session_id, None);
    }

    #[test]
    fn successful_provider_without_bounded_output_becomes_safe_failure() {
        let observed = outcome(
            NormalizedRunState::Succeeded,
            Some("provider-session".into()),
            unknown_cost(),
            Some("  "),
            None,
        );
        assert_eq!(observed.terminal_state, NormalizedRunState::Failed);
        let failure = observed.failure.unwrap();
        assert_eq!(failure.kind, AdapterFailureKind::MalformedOutput);
        assert_eq!(failure.provider_code, None);
        assert_eq!(failure.redacted_detail, None);
        assert_eq!(observed.output_markdown, Some("  ".into()));
    }
}
