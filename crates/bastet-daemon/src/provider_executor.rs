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
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bastet_core::{
    AdapterFailure, AdapterFailureKind, CostEvidence, EvidenceClass, NormalizedRunState, RunId,
};
use bastet_protocol::{BeginGraphNodeRunReceipt, FinishGraphNodeRunCommand};
use thiserror::Error;

use crate::{RunControlError, RunController, RunControllerRegistry, Store, StoreError};

const POLL_INTERVAL: Duration = Duration::from_millis(50);
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
    fn cancelled() -> Self {
        Self {
            terminal_state: NormalizedRunState::Cancelled,
            provider_session_id: None,
            cost: unknown_cost(),
            output_markdown: None,
            failure: Some(AdapterFailure {
                kind: AdapterFailureKind::Cancelled,
                message_key: "adapter.failure.cancelled".into(),
                retryable: false,
                provider_code: None,
                redacted_detail: None,
            }),
        }
    }

    fn cancelled_with_evidence(provider_session_id: Option<String>, cost: CostEvidence) -> Self {
        Self {
            provider_session_id,
            cost,
            ..Self::cancelled()
        }
    }
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
    cancellation: bastet_core::CancellationToken,
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
                acknowledgement,
            })
            .map_err(|_| RunControlError)?;
        // Intent remains sticky if the HTTP acknowledgement wait times out.
        // Only the worker can acknowledge successful checked cleanup.
        self.cancellation.cancel();
        receiver
            .recv_timeout(CANCEL_ACK_TIMEOUT)
            .map_err(|_| RunControlError)?
    }
}

pub(crate) struct ProviderControl {
    receiver: Receiver<CancelRequest>,
    cancellation: bastet_core::CancellationToken,
}

impl std::ops::Deref for ProviderControl {
    type Target = Receiver<CancelRequest>;
    fn deref(&self) -> &Self::Target {
        &self.receiver
    }
}

pub(crate) trait ProviderRunner: Send + Sync + 'static {
    fn run(&self, receipt: &BeginGraphNodeRunReceipt, control: &ProviderControl)
        -> ProviderOutcome;
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
        let (sender, receiver) = mpsc::sync_channel(1);
        let cancellation = bastet_core::CancellationToken::default();
        let control = ProviderControl {
            receiver,
            cancellation: cancellation.clone(),
        };
        self.registry
            .register(
                run_id,
                Arc::new(ChannelRunController {
                    run_id,
                    sender,
                    cancellation,
                }),
            )
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
                    if control.cancellation.is_cancelled() {
                        return ProviderOutcome::cancelled();
                    }
                    if store.preflight_provider_launch(&receipt).is_err() {
                        return if control.cancellation.is_cancelled() {
                            ProviderOutcome::cancelled()
                        } else {
                            ProviderOutcome::unavailable()
                        };
                    }
                    runner.run(&receipt, &control)
                }))
                .unwrap_or_else(|_| ProviderOutcome::unavailable());
                while let Ok(request) = control.try_recv() {
                    let accepted = request.run_id == run_id
                        && outcome.terminal_state == NormalizedRunState::Cancelled;
                    let _ = request.acknowledgement.try_send(if accepted {
                        Ok(())
                    } else {
                        Err(RunControlError)
                    });
                }
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
        control: &ProviderControl,
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
        // Check before binary discovery, spawning, or ambient provider login.
        // Frozen policy is intent, not a substitute for this operation check.
        if self.store.validate_online_cli_launch(receipt).is_err() {
            return ProviderOutcome {
                terminal_state: NormalizedRunState::Blocked,
                provider_session_id: None,
                cost: unknown_cost(),
                output_markdown: None,
                failure: Some(AdapterFailure {
                    kind: AdapterFailureKind::PermissionDenied,
                    message_key: "adapter.failure.permission_denied".into(),
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

impl Store {
    fn validate_online_cli_launch(
        &self,
        receipt: &BeginGraphNodeRunReceipt,
    ) -> Result<(), StoreError> {
        self.preflight_provider_launch(receipt)?;
        let connection = self.connection()?;
        let plan = crate::provider_launch::ProviderLaunchPlan::load(&connection, receipt.run_id)?;
        plan.validate_online_cli_requirements()
    }
}

fn run_codex(
    receipt: &BeginGraphNodeRunReceipt,
    control: &ProviderControl,
    store: &Store,
) -> Result<ProviderOutcome, String> {
    let executable = configured_executable("BASTET_CODEX_BIN", "codex")
        .ok_or_else(|| "Codex CLI is unavailable".to_string())?;
    run_codex_with_launcher(
        receipt,
        control,
        store,
        executable,
        &bastet_core::DirectAdapterProcessLauncher,
    )
}

fn run_codex_with_launcher(
    receipt: &BeginGraphNodeRunReceipt,
    control: &ProviderControl,
    store: &Store,
    executable: PathBuf,
    launcher: &dyn bastet_core::AdapterProcessLauncher,
) -> Result<ProviderOutcome, String> {
    let adapter = bastet_adapter_codex::CodexAdapter::new(executable);
    let mut server = match adapter.connect_app_server_with_launcher_and_cancel(
        PROVIDER_TIMEOUT,
        launcher,
        control.cancellation.clone(),
    ) {
        Ok(server) => server,
        Err(bastet_adapter_codex::AppServerError::Transport(
            bastet_adapter_codex::TransportError::Cancelled,
        )) => return Ok(ProviderOutcome::cancelled()),
        Err(error) => return Err(error.to_string()),
    };
    let mut started = match server.start_tracked_run(bastet_adapter_codex::CodexRunRequest {
        run_id: receipt.run_id,
        model: receipt.model.clone(),
        prompt: receipt.prompt.clone(),
        cwd: PathBuf::from(&receipt.workspace_root),
        approval_policy: bastet_adapter_codex::ApprovalPolicy::Never,
        sandbox_policy: bastet_adapter_codex::TurnSandboxPolicy::ReadOnly,
        effort: Some("medium".into()),
    }) {
        Ok(started) => started,
        Err(bastet_adapter_codex::RunTrackerError::AppServer(
            bastet_adapter_codex::AppServerError::Transport(
                bastet_adapter_codex::TransportError::Cancelled,
            ),
        )) => return Ok(ProviderOutcome::cancelled()),
        Err(error) => return Err(error.to_string()),
    };
    let provider_session_id = Some(started.thread.thread_id.clone());
    let mut cost = unknown_cost();
    let mut saw_running = false;
    loop {
        if control.cancellation.is_cancelled() {
            server
                .into_transport()
                .close_checked()
                .map_err(|_| "provider cleanup could not be confirmed".to_string())?;
            return Ok(ProviderOutcome::cancelled_with_evidence(
                provider_session_id,
                cost,
            ));
        }
        let update = match started
            .tracker
            .poll_update(&mut server, &timestamp_ms(), POLL_INTERVAL)
        {
            Ok(update) => update,
            Err(bastet_adapter_codex::RunTrackerError::AppServer(
                bastet_adapter_codex::AppServerError::Transport(
                    bastet_adapter_codex::TransportError::Cancelled,
                ),
            )) => {
                return Ok(ProviderOutcome::cancelled_with_evidence(
                    provider_session_id,
                    cost,
                ))
            }
            Err(error) => return Err(error.to_string()),
        };
        let Some(update) = update else {
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
                // Final provider output does not prove host cleanup succeeded.
                // Preserve cleanup failure as Uncertain through the runner's
                // existing sanitized error mapping before publishing output.
                server
                    .into_transport()
                    .close_checked()
                    .map_err(|_| "provider cleanup could not be confirmed".to_string())?;
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
    control: &ProviderControl,
    store: &Store,
) -> Result<ProviderOutcome, String> {
    let executable = configured_executable("BASTET_AGY_BIN", "agy")
        .ok_or_else(|| "Agy CLI is unavailable".to_string())?;
    run_agy_with_launcher(
        receipt,
        control,
        store,
        executable,
        &bastet_core::DirectAdapterProcessLauncher,
    )
}

fn run_agy_with_launcher(
    receipt: &BeginGraphNodeRunReceipt,
    control: &ProviderControl,
    store: &Store,
    executable: PathBuf,
    launcher: &dyn bastet_core::AdapterProcessLauncher,
) -> Result<ProviderOutcome, String> {
    let mut process = match bastet_adapter_agy::AgyProcess::spawn_with_launcher_and_cancel(
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
        launcher,
        &control.cancellation,
    ) {
        Ok(process) => process,
        Err(bastet_adapter_agy::AgyProcessError::Cancelled) => {
            return Ok(ProviderOutcome::cancelled())
        }
        Err(error) => return Err(error.to_string()),
    };
    let mut cost = unknown_cost();
    let mut saw_running = false;
    loop {
        if control.cancellation.is_cancelled() {
            process
                .cancel_and_close()
                .map_err(|error| error.to_string())?;
            return Ok(ProviderOutcome::cancelled_with_evidence(
                process.conversation_id().map(str::to_owned),
                cost,
            ));
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
    use std::time::Instant;

    #[test]
    fn controller_keeps_intent_after_ack_timeout_and_rejects_wrong_run() {
        let run_id = RunId::new();
        let (sender, receiver) = mpsc::sync_channel(1);
        let cancellation = bastet_core::CancellationToken::default();
        let controller = ChannelRunController {
            run_id,
            sender,
            cancellation: cancellation.clone(),
        };
        assert!(controller.cancel(RunId::new()).is_err());
        assert!(!cancellation.is_cancelled());
        assert!(receiver.try_recv().is_err());
        assert!(controller.cancel(run_id).is_err());
        assert!(
            cancellation.is_cancelled(),
            "HTTP timeout must not withdraw intent"
        );
        let request = receiver.try_recv().unwrap();
        assert_eq!(request.run_id, run_id);
        // Late cleanup acknowledgement cannot fabricate a successful HTTP response.
        assert!(request.acknowledgement.try_send(Ok(())).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn starting_adapter_cancel_is_acknowledged_only_after_checked_cleanup() {
        use std::sync::Mutex;
        struct SilentLauncher(PathBuf);
        impl bastet_core::AdapterProcessLauncher for SilentLauncher {
            fn command(&self, _: &std::path::Path) -> std::io::Result<std::process::Command> {
                let mut command = std::process::Command::new("/bin/sh");
                command.current_dir(&self.0).args([
                    "-c",
                    "IFS= read -r request; printf ready > startup-ready; exec /bin/sleep 30",
                ]);
                Ok(command)
            }
        }
        struct StartupRunner {
            store: Store,
            launcher: SilentLauncher,
            codex: bool,
            cleaned: SyncSender<()>,
            release: Mutex<Receiver<()>>,
            cleanup_failure: bool,
        }
        impl ProviderRunner for StartupRunner {
            fn run(
                &self,
                receipt: &BeginGraphNodeRunReceipt,
                control: &ProviderControl,
            ) -> ProviderOutcome {
                // Agy validates the executable exists even with an injected
                // launcher. Only the synthetic shell command is ever executed.
                let executable = PathBuf::from("/bin/sh");
                let outcome = if self.codex {
                    run_codex_with_launcher(
                        receipt,
                        control,
                        &self.store,
                        executable,
                        &self.launcher,
                    )
                } else {
                    run_agy_with_launcher(receipt, control, &self.store, executable, &self.launcher)
                }
                .unwrap();
                assert_eq!(outcome.terminal_state, NormalizedRunState::Cancelled);
                self.cleaned.send(()).unwrap();
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
                // Conservative worker boundary when a runner cannot certify cleanup.
                if self.cleanup_failure {
                    ProviderOutcome::unavailable()
                } else {
                    outcome
                }
            }
        }
        for (codex, cleanup_failure) in [(true, false), (false, false), (true, true)] {
            let (_directory, workspace, store, id) = crate::tests::provider_execution_fixture();
            let receipt = crate::tests::begin_fixture_node(&store, id);
            let registry = RunControllerRegistry::default();
            let (cleaned, cleanup) = mpsc::sync_channel(1);
            let (release, gate) = mpsc::sync_channel(1);
            let executor = ProviderExecutor::with_runner(
                store.clone(),
                registry.clone(),
                Arc::new(StartupRunner {
                    store: store.clone(),
                    launcher: SilentLauncher(workspace.path().into()),
                    codex,
                    cleaned,
                    release: Mutex::new(gate),
                    cleanup_failure,
                }),
            );
            executor.start(receipt.clone()).unwrap();
            let marker = workspace.path().join("startup-ready");
            let deadline = Instant::now() + Duration::from_secs(5);
            while !marker.exists() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(5));
            }
            assert!(marker.exists());
            store
                .preflight_provider_cancel(receipt.run_id, store.catalog().unwrap().revision)
                .unwrap();
            let run_id = receipt.run_id;
            let (ack, response) = mpsc::sync_channel(1);
            let controller = thread::spawn(move || {
                ack.send(registry.cancel(run_id)).unwrap();
            });
            cleanup.recv_timeout(Duration::from_secs(3)).unwrap();
            assert!(matches!(
                response.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            assert_eq!(
                store
                    .catalog()
                    .unwrap()
                    .catalog
                    .runs
                    .iter()
                    .find(|run| run.metadata.id == run_id)
                    .unwrap()
                    .state,
                NormalizedRunState::Starting
            );
            assert!(!store
                .events_after(0)
                .unwrap()
                .iter()
                .any(|event| event.event_type.starts_with("run.cancel_accepted")));
            release.send(()).unwrap();
            assert_eq!(
                response
                    .recv_timeout(Duration::from_secs(3))
                    .unwrap()
                    .is_ok(),
                !cleanup_failure
            );
            controller.join().unwrap();
            let deadline = Instant::now() + Duration::from_secs(3);
            while executor.has_active() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(5));
            }
            assert!(!executor.has_active());
            assert_eq!(
                store
                    .catalog()
                    .unwrap()
                    .catalog
                    .runs
                    .iter()
                    .find(|run| run.metadata.id == run_id)
                    .unwrap()
                    .state,
                if cleanup_failure {
                    NormalizedRunState::Uncertain
                } else {
                    NormalizedRunState::Cancelled
                }
            );
        }
    }

    #[test]
    fn system_runner_rejects_denied_prerequisites_before_adapter_selection() {
        let (_directory, _workspace, store, id) = crate::tests::provider_execution_fixture();
        let mut catalog = store.catalog().unwrap();
        // Unknown synthetic adapter guarantees no real provider can be reached
        // even if this regression test's permission guard is accidentally lost.
        for provider in &mut catalog.catalog.agent_providers {
            provider.adapter_kind = "fixture-no-executable".into();
        }
        store
            .replace_catalog(bastet_protocol::ReplaceCatalogCommand {
                expected_revision: catalog.revision,
                catalog: catalog.catalog,
            })
            .unwrap();
        let receipt = crate::tests::begin_fixture_node(&store, id);
        assert!(receipt.launch_identity.as_ref().unwrap().account.is_none());
        let before = store.events_after(0).unwrap();
        let runner = SystemProviderRunner {
            store: store.clone(),
        };
        let (_sender, control) = mpsc::sync_channel(1);
        let result = runner.run(
            &receipt,
            &ProviderControl {
                receiver: control,
                cancellation: Default::default(),
            },
        );
        assert_eq!(result.terminal_state, NormalizedRunState::Blocked);
        assert_eq!(
            result.failure.unwrap().kind,
            AdapterFailureKind::PermissionDenied
        );
        assert!(result.output_markdown.is_none());
        assert!(result.provider_session_id.is_none());
        assert_eq!(store.events_after(0).unwrap(), before);
        // Exercise the actual worker/persistence boundary too, not only the
        // returned value. The synthetic adapter cannot execute external code.
        let executor =
            ProviderExecutor::production(store.clone(), RunControllerRegistry::default());
        executor.start(receipt.clone()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while executor.has_active() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(!executor.has_active());
        let catalog = store.catalog().unwrap().catalog;
        assert_eq!(
            catalog
                .runs
                .iter()
                .find(|run| run.metadata.id == receipt.run_id)
                .unwrap()
                .state,
            NormalizedRunState::Blocked
        );
        let graph = store.graph_execution(id).unwrap();
        assert_eq!(
            graph
                .nodes
                .iter()
                .find(|node| node.node_id == receipt.node_id)
                .unwrap()
                .state,
            bastet_core::GraphNodeState::Blocked
        );
        assert!(graph.outputs.is_empty());
    }

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
        let result = runner.run(
            &receipt,
            &ProviderControl {
                receiver: control,
                cancellation: Default::default(),
            },
        );
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
