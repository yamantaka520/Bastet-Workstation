use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use thiserror::Error;

use crate::{
    evidence::{CodexRunEvidence, CodexRunEvidenceUpdate},
    lifecycle::{CodexRunStream, NormalizedCodexEvent},
};

pub trait AppServerTransport {
    fn request(&mut self, method: &str, params: Value) -> Result<Value, TransportError>;
    /// Sends a request whose response wait is bounded by `max_wait`. Implementors
    /// must not fall back to an unbounded or configured-default request wait.
    fn request_with_timeout(
        &mut self,
        method: &str,
        params: Value,
        max_wait: Duration,
    ) -> Result<Value, TransportError>;
    fn notify(&mut self, method: &str, params: Value) -> Result<(), TransportError>;
    fn next_notification(&mut self) -> Result<AppServerNotification, TransportError>;

    /// Bounded notification observation. `None` is not a transport failure:
    /// it means only that this caller's observation window elapsed.
    fn poll_notification(
        &mut self,
        max_wait: Duration,
    ) -> Result<Option<AppServerNotification>, TransportError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppServerNotification {
    pub method: String,
    pub params: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CodexRunUpdate {
    Lifecycle(NormalizedCodexEvent),
    Evidence(CodexRunEvidenceUpdate),
}

/// The bounded, run-scoped final assistant Markdown retained for a caller that
/// explicitly asks for it. It is deliberately separate from normalized events
/// and evidence, whose payloads must remain redacted.
pub(crate) struct CodexFinalOutput {
    provider_thread_id: String,
    provider_turn_id: String,
    text: String,
    overflowed: bool,
}

impl CodexFinalOutput {
    pub(crate) const MAX_BYTES: usize = 256 * 1024;

    pub(crate) fn new(
        provider_thread_id: impl Into<String>,
        provider_turn_id: impl Into<String>,
    ) -> Result<Self, AppServerError> {
        let provider_thread_id = provider_thread_id.into();
        let provider_turn_id = provider_turn_id.into();
        require_text(&provider_thread_id)?;
        require_text(&provider_turn_id)?;
        Ok(Self {
            provider_thread_id,
            provider_turn_id,
            text: String::new(),
            overflowed: false,
        })
    }

    pub(crate) fn final_output(&self) -> Option<&str> {
        (!self.overflowed && !self.text.is_empty()).then_some(self.text.as_str())
    }

    pub(crate) fn overflowed(&self) -> bool {
        self.overflowed
    }

    fn ingest(&mut self, notification: &AppServerNotification) -> Result<(), AppServerError> {
        // The generated app-server schema defines this as a completed
        // `agentMessage` item with `phase: final_answer`; streaming deltas,
        // commentary, tool items, and reasoning are intentionally excluded.
        if notification.method != "item/completed" {
            return Ok(());
        }
        let params = &notification.params;
        if required_value_string(params, "threadId")? != self.provider_thread_id
            || required_value_string(params, "turnId")? != self.provider_turn_id
        {
            return Ok(());
        }
        let item = params.get("item").ok_or(AppServerError::ProtocolDrift)?;
        if required_value_string(item, "type")? != "agentMessage" {
            return Ok(());
        }
        if item.get("phase").and_then(Value::as_str) != Some("final_answer") {
            return Ok(());
        }
        let text = item
            .get("text")
            .and_then(Value::as_str)
            .ok_or(AppServerError::ProtocolDrift)?;
        if !self.overflowed && !append_if_within_limit(&mut self.text, text, Self::MAX_BYTES) {
            self.text.clear();
            self.overflowed = true;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum TransportError {
    #[error("Codex app-server transport is unavailable")]
    Unavailable,
    #[error("Codex app-server transport timed out")]
    TimedOut,
    #[error("Codex app-server transport violated the expected protocol")]
    ProtocolDrift,
    #[error("Codex app-server rejected the request with JSON-RPC code {code}")]
    RemoteRejected { code: i64, retryable: bool },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AppServerError {
    #[error(transparent)]
    Transport(TransportError),
    #[error("Codex app-server connection is not initialized")]
    NotInitialized,
    #[error("Codex app-server output did not match the expected protocol")]
    ProtocolDrift,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ApprovalPolicy {
    Never,
    UnlessTrusted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ThreadSandbox {
    ReadOnly,
    WorkspaceWrite,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum TurnSandboxPolicy {
    ReadOnly,
    WorkspaceWrite {
        #[serde(rename = "writableRoots")]
        writable_roots: Vec<PathBuf>,
        #[serde(rename = "networkAccess")]
        network_access: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadStartRequest {
    pub model: String,
    pub cwd: PathBuf,
    pub approval_policy: ApprovalPolicy,
    pub sandbox: ThreadSandbox,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnStartRequest {
    pub thread_id: String,
    pub prompt: String,
    pub cwd: PathBuf,
    pub approval_policy: ApprovalPolicy,
    pub sandbox_policy: TurnSandboxPolicy,
    pub model: Option<String>,
    pub effort: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadHandle {
    pub thread_id: String,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnHandle {
    pub turn_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReasoningEffort {
    pub reasoning_effort: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelDescriptor {
    pub id: String,
    pub model: String,
    pub display_name: String,
    pub hidden: bool,
    pub default_reasoning_effort: Option<String>,
    pub supported_reasoning_efforts: Vec<ReasoningEffort>,
    pub input_modalities: Vec<String>,
    pub supports_personality: bool,
    pub is_default: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCatalogPage {
    pub models: Vec<ModelDescriptor>,
    pub next_cursor: Option<String>,
}

pub struct CodexAppServer<T> {
    transport: T,
    initialized: bool,
}

impl<T: AppServerTransport> CodexAppServer<T> {
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            initialized: false,
        }
    }

    pub fn initialize(&mut self) -> Result<(), AppServerError> {
        let result = self
            .transport
            .request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "bastet_workstation",
                        "title": "Bastet Workstation",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
            )
            .map_err(AppServerError::Transport)?;
        if !result.is_object() {
            return Err(AppServerError::ProtocolDrift);
        }
        self.transport
            .notify("initialized", json!({}))
            .map_err(AppServerError::Transport)?;
        self.initialized = true;
        Ok(())
    }

    pub fn list_models(
        &mut self,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ModelCatalogPage, AppServerError> {
        if !self.initialized {
            return Err(AppServerError::NotInitialized);
        }
        if limit == 0 {
            return Err(AppServerError::ProtocolDrift);
        }
        let result = self
            .transport
            .request(
                "model/list",
                json!({
                    "cursor": cursor,
                    "limit": limit,
                    "includeHidden": false
                }),
            )
            .map_err(AppServerError::Transport)?;
        let wire: ModelPageWire =
            serde_json::from_value(result).map_err(|_| AppServerError::ProtocolDrift)?;
        let models = wire
            .data
            .into_iter()
            .map(ModelDescriptor::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        if wire
            .next_cursor
            .as_ref()
            .is_some_and(|cursor| cursor.trim().is_empty())
        {
            return Err(AppServerError::ProtocolDrift);
        }
        Ok(ModelCatalogPage {
            models,
            next_cursor: wire.next_cursor,
        })
    }

    pub fn start_thread(
        &mut self,
        request: ThreadStartRequest,
    ) -> Result<ThreadHandle, AppServerError> {
        self.require_initialized()?;
        require_text(&request.model)?;
        require_absolute(&request.cwd)?;
        let result = self
            .transport
            .request(
                "thread/start",
                json!({
                    "model": request.model,
                    "cwd": request.cwd,
                    "approvalPolicy": request.approval_policy,
                    "sandbox": request.sandbox
                }),
            )
            .map_err(AppServerError::Transport)?;
        parse_thread_handle(result)
    }

    pub fn resume_thread(&mut self, thread_id: &str) -> Result<ThreadHandle, AppServerError> {
        self.require_initialized()?;
        require_text(thread_id)?;
        let result = self
            .transport
            .request("thread/resume", json!({ "threadId": thread_id }))
            .map_err(AppServerError::Transport)?;
        parse_thread_handle(result)
    }

    pub fn start_turn(&mut self, request: TurnStartRequest) -> Result<TurnHandle, AppServerError> {
        self.require_initialized()?;
        require_text(&request.thread_id)?;
        require_text(&request.prompt)?;
        require_absolute(&request.cwd)?;
        validate_sandbox_policy(&request.sandbox_policy)?;
        if request
            .model
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
            || request
                .effort
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err(AppServerError::ProtocolDrift);
        }
        let mut params = json!({
            "threadId": request.thread_id,
            "input": [{ "type": "text", "text": request.prompt }],
            "cwd": request.cwd,
            "approvalPolicy": request.approval_policy,
            "sandboxPolicy": request.sandbox_policy
        });
        let object = params
            .as_object_mut()
            .expect("turn parameters are an object");
        if let Some(model) = request.model {
            object.insert("model".into(), Value::String(model));
        }
        if let Some(effort) = request.effort {
            object.insert("effort".into(), Value::String(effort));
        }
        let result = self
            .transport
            .request("turn/start", params)
            .map_err(AppServerError::Transport)?;
        let wire: TurnResultWire =
            serde_json::from_value(result).map_err(|_| AppServerError::ProtocolDrift)?;
        require_text(&wire.turn.id)?;
        Ok(TurnHandle {
            turn_id: wire.turn.id,
        })
    }

    pub fn interrupt_turn(&mut self, thread_id: &str, turn_id: &str) -> Result<(), AppServerError> {
        self.interrupt_turn_inner(thread_id, turn_id, None)
    }

    /// Interrupts a turn, waiting no longer than `max_wait` for the provider
    /// acknowledgement. A timeout is not an accepted cancellation.
    pub fn interrupt_turn_with_timeout(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        max_wait: Duration,
    ) -> Result<(), AppServerError> {
        self.interrupt_turn_inner(thread_id, turn_id, Some(max_wait))
    }

    fn interrupt_turn_inner(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        max_wait: Option<Duration>,
    ) -> Result<(), AppServerError> {
        self.require_initialized()?;
        require_text(thread_id)?;
        require_text(turn_id)?;
        let params = json!({ "threadId": thread_id, "turnId": turn_id });
        let result = match max_wait {
            Some(max_wait) => {
                self.transport
                    .request_with_timeout("turn/interrupt", params, max_wait)
            }
            None => self.transport.request("turn/interrupt", params),
        }
        .map_err(AppServerError::Transport)?;
        if result.as_object().is_none_or(|object| !object.is_empty()) {
            return Err(AppServerError::ProtocolDrift);
        }
        Ok(())
    }

    pub fn next_notification(&mut self) -> Result<AppServerNotification, AppServerError> {
        self.require_initialized()?;
        self.transport
            .next_notification()
            .map_err(AppServerError::Transport)
    }

    pub fn poll_notification(
        &mut self,
        max_wait: Duration,
    ) -> Result<Option<AppServerNotification>, AppServerError> {
        self.require_initialized()?;
        self.transport
            .poll_notification(max_wait)
            .map_err(AppServerError::Transport)
    }

    pub fn next_run_event(
        &mut self,
        stream: &mut CodexRunStream,
        occurred_at: &str,
    ) -> Result<NormalizedCodexEvent, AppServerError> {
        loop {
            let notification = self.next_notification()?;
            if let Some(event) = stream
                .ingest(&notification, occurred_at)
                .map_err(|_| AppServerError::ProtocolDrift)?
            {
                return Ok(event);
            }
        }
    }

    pub fn next_run_update(
        &mut self,
        stream: &mut CodexRunStream,
        evidence: &CodexRunEvidence,
        occurred_at: &str,
    ) -> Result<CodexRunUpdate, AppServerError> {
        self.next_run_update_inner(stream, evidence, None, occurred_at)
    }

    pub(crate) fn next_run_update_with_output(
        &mut self,
        stream: &mut CodexRunStream,
        evidence: &CodexRunEvidence,
        final_output: &mut CodexFinalOutput,
        occurred_at: &str,
    ) -> Result<CodexRunUpdate, AppServerError> {
        self.next_run_update_inner(stream, evidence, Some(final_output), occurred_at)
    }

    /// Observes a run for no longer than `max_wait`. A `None` result is an
    /// observation timeout, not a terminal lifecycle transition.
    pub(crate) fn poll_run_update_with_output(
        &mut self,
        stream: &mut CodexRunStream,
        evidence: &CodexRunEvidence,
        final_output: &mut CodexFinalOutput,
        occurred_at: &str,
        max_wait: Duration,
    ) -> Result<Option<CodexRunUpdate>, AppServerError> {
        let deadline = Instant::now().checked_add(max_wait);
        let mut first_observation = true;
        loop {
            let remaining = deadline
                .map(|deadline| deadline.saturating_duration_since(Instant::now()))
                .unwrap_or(Duration::MAX);
            if remaining.is_zero() && !first_observation {
                return Ok(None);
            }
            first_observation = false;
            let notification = match self.poll_notification(remaining) {
                Ok(Some(notification)) => notification,
                Ok(None) => return Ok(None),
                Err(AppServerError::Transport(TransportError::TimedOut)) => {
                    return stream
                        .deadline_exceeded(occurred_at)
                        .map(CodexRunUpdate::Lifecycle)
                        .map(Some)
                        .map_err(|_| AppServerError::ProtocolDrift);
                }
                Err(AppServerError::Transport(TransportError::Unavailable)) => {
                    return stream
                        .transport_lost(occurred_at)
                        .map(CodexRunUpdate::Lifecycle)
                        .map(Some)
                        .map_err(|_| AppServerError::ProtocolDrift);
                }
                Err(error) => return Err(error),
            };
            final_output.ingest(&notification)?;
            if let Some(event) = stream
                .ingest(&notification, occurred_at)
                .map_err(|_| AppServerError::ProtocolDrift)?
            {
                return Ok(Some(CodexRunUpdate::Lifecycle(event)));
            }
            if let Some(update) = evidence
                .ingest(&notification)
                .map_err(|_| AppServerError::ProtocolDrift)?
            {
                return Ok(Some(CodexRunUpdate::Evidence(update)));
            }
        }
    }

    fn next_run_update_inner(
        &mut self,
        stream: &mut CodexRunStream,
        evidence: &CodexRunEvidence,
        mut final_output: Option<&mut CodexFinalOutput>,
        occurred_at: &str,
    ) -> Result<CodexRunUpdate, AppServerError> {
        loop {
            let notification = match self.next_notification() {
                Ok(notification) => notification,
                Err(AppServerError::Transport(TransportError::TimedOut)) => {
                    return stream
                        .deadline_exceeded(occurred_at)
                        .map(CodexRunUpdate::Lifecycle)
                        .map_err(|_| AppServerError::ProtocolDrift);
                }
                Err(AppServerError::Transport(TransportError::Unavailable)) => {
                    return stream
                        .transport_lost(occurred_at)
                        .map(CodexRunUpdate::Lifecycle)
                        .map_err(|_| AppServerError::ProtocolDrift);
                }
                Err(error) => return Err(error),
            };
            if let Some(final_output) = final_output.as_deref_mut() {
                final_output.ingest(&notification)?;
            }
            if let Some(event) = stream
                .ingest(&notification, occurred_at)
                .map_err(|_| AppServerError::ProtocolDrift)?
            {
                return Ok(CodexRunUpdate::Lifecycle(event));
            }
            if let Some(update) = evidence
                .ingest(&notification)
                .map_err(|_| AppServerError::ProtocolDrift)?
            {
                return Ok(CodexRunUpdate::Evidence(update));
            }
        }
    }

    fn require_initialized(&self) -> Result<(), AppServerError> {
        if self.initialized {
            Ok(())
        } else {
            Err(AppServerError::NotInitialized)
        }
    }

    pub fn into_transport(self) -> T {
        self.transport
    }
}

fn required_value_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, AppServerError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or(AppServerError::ProtocolDrift)
}

fn append_if_within_limit(target: &mut String, addition: &str, limit: usize) -> bool {
    if target
        .len()
        .checked_add(addition.len())
        .is_some_and(|length| length <= limit)
    {
        target.push_str(addition);
        true
    } else {
        false
    }
}

#[derive(Debug, Deserialize)]
struct ThreadResultWire {
    thread: ThreadWire,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadWire {
    id: String,
    session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TurnResultWire {
    turn: TurnWire,
}

#[derive(Debug, Deserialize)]
struct TurnWire {
    id: String,
}

fn parse_thread_handle(result: Value) -> Result<ThreadHandle, AppServerError> {
    let wire: ThreadResultWire =
        serde_json::from_value(result).map_err(|_| AppServerError::ProtocolDrift)?;
    require_text(&wire.thread.id)?;
    if wire
        .thread
        .session_id
        .as_ref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(AppServerError::ProtocolDrift);
    }
    Ok(ThreadHandle {
        thread_id: wire.thread.id,
        session_id: wire.thread.session_id,
    })
}

fn require_text(value: &str) -> Result<(), AppServerError> {
    if value.trim().is_empty() {
        Err(AppServerError::ProtocolDrift)
    } else {
        Ok(())
    }
}

fn require_absolute(path: &Path) -> Result<(), AppServerError> {
    if path.is_absolute() {
        Ok(())
    } else {
        Err(AppServerError::ProtocolDrift)
    }
}

fn validate_sandbox_policy(policy: &TurnSandboxPolicy) -> Result<(), AppServerError> {
    if let TurnSandboxPolicy::WorkspaceWrite { writable_roots, .. } = policy {
        if writable_roots.is_empty() || writable_roots.iter().any(|path| !path.is_absolute()) {
            return Err(AppServerError::ProtocolDrift);
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelPageWire {
    data: Vec<ModelWire>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelWire {
    id: String,
    model: String,
    display_name: String,
    hidden: bool,
    default_reasoning_effort: Option<String>,
    supported_reasoning_efforts: Vec<ReasoningEffort>,
    #[serde(default = "default_modalities")]
    input_modalities: Vec<String>,
    supports_personality: bool,
    is_default: bool,
}

impl TryFrom<ModelWire> for ModelDescriptor {
    type Error = AppServerError;

    fn try_from(wire: ModelWire) -> Result<Self, Self::Error> {
        let required_values = [&wire.id, &wire.model, &wire.display_name];
        if required_values.iter().any(|value| value.trim().is_empty())
            || wire
                .default_reasoning_effort
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
            || wire.supported_reasoning_efforts.iter().any(|effort| {
                effort.reasoning_effort.trim().is_empty() || effort.description.trim().is_empty()
            })
            || wire
                .input_modalities
                .iter()
                .any(|modality| modality.trim().is_empty())
        {
            return Err(AppServerError::ProtocolDrift);
        }
        Ok(Self {
            id: wire.id,
            model: wire.model,
            display_name: wire.display_name,
            hidden: wire.hidden,
            default_reasoning_effort: wire.default_reasoning_effort,
            supported_reasoning_efforts: wire.supported_reasoning_efforts,
            input_modalities: wire.input_modalities,
            supports_personality: wire.supports_personality,
            is_default: wire.is_default,
        })
    }
}

fn default_modalities() -> Vec<String> {
    vec!["text".into(), "image".into()]
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    #[derive(Default)]
    struct FixtureTransport {
        responses: VecDeque<Result<Value, TransportError>>,
        incoming_notifications: VecDeque<Result<AppServerNotification, TransportError>>,
        requests: Vec<(String, Value)>,
        notifications: Vec<(String, Value)>,
    }

    impl AppServerTransport for FixtureTransport {
        fn request(&mut self, method: &str, params: Value) -> Result<Value, TransportError> {
            self.requests.push((method.into(), params));
            self.responses.pop_front().unwrap()
        }

        fn request_with_timeout(
            &mut self,
            method: &str,
            params: Value,
            _max_wait: Duration,
        ) -> Result<Value, TransportError> {
            self.request(method, params)
        }

        fn notify(&mut self, method: &str, params: Value) -> Result<(), TransportError> {
            self.notifications.push((method.into(), params));
            Ok(())
        }

        fn next_notification(&mut self) -> Result<AppServerNotification, TransportError> {
            self.incoming_notifications.pop_front().unwrap()
        }

        fn poll_notification(
            &mut self,
            _max_wait: Duration,
        ) -> Result<Option<AppServerNotification>, TransportError> {
            match self.next_notification() {
                Ok(notification) => Ok(Some(notification)),
                Err(TransportError::TimedOut) => Ok(None),
                Err(error) => Err(error),
            }
        }
    }

    fn transport_with_model(model: Value) -> FixtureTransport {
        FixtureTransport {
            responses: VecDeque::from([
                Ok(json!({})),
                Ok(json!({
                    "data": [model],
                    "nextCursor": null
                })),
            ]),
            ..FixtureTransport::default()
        }
    }

    fn model() -> Value {
        json!({
            "id": "gpt-fixture",
            "model": "gpt-fixture",
            "displayName": "GPT Fixture",
            "hidden": false,
            "defaultReasoningEffort": "medium",
            "supportedReasoningEfforts": [{
                "reasoningEffort": "low",
                "description": "Fast"
            }],
            "inputModalities": ["text"],
            "supportsPersonality": true,
            "isDefault": true
        })
    }

    fn fixture_absolute_path() -> PathBuf {
        if cfg!(windows) {
            PathBuf::from(r"C:\workspace\project")
        } else {
            PathBuf::from("/workspace/project")
        }
    }

    #[test]
    fn initialization_is_required_and_uses_stable_client_identity() {
        let mut server = CodexAppServer::new(FixtureTransport::default());
        assert_eq!(
            server.list_models(None, 20),
            Err(AppServerError::NotInitialized)
        );

        let mut server = CodexAppServer::new(transport_with_model(model()));
        server.initialize().unwrap();
        server.list_models(None, 20).unwrap();
        let transport = server.into_transport();
        assert_eq!(transport.requests[0].0, "initialize");
        assert_eq!(
            transport.requests[0].1["clientInfo"]["name"],
            "bastet_workstation"
        );
        assert_eq!(
            transport.notifications,
            vec![("initialized".into(), json!({}))]
        );
        assert_eq!(transport.requests[1].0, "model/list");
        assert_eq!(transport.requests[1].1["includeHidden"], false);
    }

    #[test]
    fn transport_classification_reaches_the_adapter_boundary() {
        let rejection = TransportError::RemoteRejected {
            code: -32001,
            retryable: true,
        };
        let transport = FixtureTransport {
            responses: VecDeque::from([Err(rejection)]),
            ..FixtureTransport::default()
        };
        let mut server = CodexAppServer::new(transport);
        assert_eq!(
            server.initialize(),
            Err(AppServerError::Transport(rejection))
        );
    }

    #[test]
    fn model_and_reasoning_catalog_is_normalized() {
        let mut server = CodexAppServer::new(transport_with_model(model()));
        server.initialize().unwrap();
        let page = server.list_models(None, 20).unwrap();
        assert_eq!(page.models[0].id, "gpt-fixture");
        assert_eq!(
            page.models[0].default_reasoning_effort.as_deref(),
            Some("medium")
        );
        assert_eq!(
            page.models[0].supported_reasoning_efforts[0].reasoning_effort,
            "low"
        );
    }

    #[test]
    fn older_catalog_without_modalities_uses_documented_default() {
        let mut value = model();
        value.as_object_mut().unwrap().remove("inputModalities");
        let mut server = CodexAppServer::new(transport_with_model(value));
        server.initialize().unwrap();
        let page = server.list_models(None, 20).unwrap();
        assert_eq!(page.models[0].input_modalities, ["text", "image"]);
    }

    #[test]
    fn malformed_catalog_fails_closed() {
        let mut value = model();
        value["id"] = json!("");
        let mut server = CodexAppServer::new(transport_with_model(value));
        server.initialize().unwrap();
        assert_eq!(
            server.list_models(None, 20),
            Err(AppServerError::ProtocolDrift)
        );
    }

    fn initialized_server_with(
        responses: impl IntoIterator<Item = Value>,
    ) -> CodexAppServer<FixtureTransport> {
        let mut queue = VecDeque::from([Ok(json!({}))]);
        queue.extend(responses.into_iter().map(Ok));
        let mut server = CodexAppServer::new(FixtureTransport {
            responses: queue,
            ..FixtureTransport::default()
        });
        server.initialize().unwrap();
        server
    }

    #[test]
    fn thread_start_and_resume_use_allowlisted_protocol_fields() {
        let cwd = fixture_absolute_path();
        let mut server = initialized_server_with([
            json!({ "thread": { "id": "thr_1", "sessionId": "session_1", "preview": "discard" } }),
            json!({ "thread": { "id": "thr_1", "ephemeral": false } }),
        ]);
        let started = server
            .start_thread(ThreadStartRequest {
                model: "gpt-fixture".into(),
                cwd: cwd.clone(),
                approval_policy: ApprovalPolicy::Never,
                sandbox: ThreadSandbox::WorkspaceWrite,
            })
            .unwrap();
        assert_eq!(started.session_id.as_deref(), Some("session_1"));
        let resumed = server.resume_thread("thr_1").unwrap();
        assert_eq!(resumed.thread_id, "thr_1");

        let transport = server.into_transport();
        assert_eq!(transport.requests[1].0, "thread/start");
        assert_eq!(
            transport.requests[1].1,
            json!({
                "model": "gpt-fixture",
                "cwd": cwd,
                "approvalPolicy": "never",
                "sandbox": "workspace-write"
            })
        );
        assert_eq!(
            transport.requests[2],
            ("thread/resume".into(), json!({ "threadId": "thr_1" }))
        );
    }

    #[test]
    fn turn_start_and_interrupt_use_typed_policy_and_exact_ids() {
        let cwd = fixture_absolute_path();
        let mut server = initialized_server_with([
            json!({ "turn": { "id": "turn_1", "status": "inProgress", "items": [] } }),
            json!({}),
        ]);
        let turn = server
            .start_turn(TurnStartRequest {
                thread_id: "thr_1".into(),
                prompt: "Run tests".into(),
                cwd: cwd.clone(),
                approval_policy: ApprovalPolicy::UnlessTrusted,
                sandbox_policy: TurnSandboxPolicy::WorkspaceWrite {
                    writable_roots: vec![cwd.clone()],
                    network_access: false,
                },
                model: Some("gpt-fixture".into()),
                effort: Some("medium".into()),
            })
            .unwrap();
        assert_eq!(turn.turn_id, "turn_1");
        server.interrupt_turn("thr_1", "turn_1").unwrap();

        let transport = server.into_transport();
        assert_eq!(transport.requests[1].0, "turn/start");
        assert_eq!(
            transport.requests[1].1["sandboxPolicy"],
            json!({
                "type": "workspaceWrite",
                "writableRoots": [cwd],
                "networkAccess": false
            })
        );
        assert_eq!(
            transport.requests[1].1["input"],
            json!([{ "type": "text", "text": "Run tests" }])
        );
        assert_eq!(
            transport.requests[2],
            (
                "turn/interrupt".into(),
                json!({
                    "threadId": "thr_1",
                    "turnId": "turn_1"
                })
            )
        );
    }

    #[test]
    fn unsafe_or_ambiguous_requests_fail_before_transport() {
        let mut server = initialized_server_with([]);
        assert_eq!(
            server.start_thread(ThreadStartRequest {
                model: "gpt-fixture".into(),
                cwd: PathBuf::from("relative/project"),
                approval_policy: ApprovalPolicy::Never,
                sandbox: ThreadSandbox::ReadOnly,
            }),
            Err(AppServerError::ProtocolDrift)
        );
        assert_eq!(
            server.start_turn(TurnStartRequest {
                thread_id: "thr_1".into(),
                prompt: "Run tests".into(),
                cwd: fixture_absolute_path(),
                approval_policy: ApprovalPolicy::Never,
                sandbox_policy: TurnSandboxPolicy::WorkspaceWrite {
                    writable_roots: vec![PathBuf::from("relative/project")],
                    network_access: false,
                },
                model: None,
                effort: None,
            }),
            Err(AppServerError::ProtocolDrift)
        );
        assert_eq!(server.into_transport().requests.len(), 1);
    }

    #[test]
    fn omitted_turn_overrides_are_not_serialized_as_null() {
        let mut server = initialized_server_with([json!({ "turn": { "id": "turn_1" } })]);
        server
            .start_turn(TurnStartRequest {
                thread_id: "thr_1".into(),
                prompt: "Inspect status".into(),
                cwd: fixture_absolute_path(),
                approval_policy: ApprovalPolicy::Never,
                sandbox_policy: TurnSandboxPolicy::ReadOnly,
                model: None,
                effort: None,
            })
            .unwrap();
        let transport = server.into_transport();
        let params = transport.requests[1].1.as_object().unwrap();
        assert!(!params.contains_key("model"));
        assert!(!params.contains_key("effort"));
        assert_eq!(params["sandboxPolicy"], json!({ "type": "readOnly" }));
    }

    #[test]
    fn malformed_handles_and_interrupt_acknowledgements_fail_closed() {
        let mut server = initialized_server_with([
            json!({ "thread": { "id": "" } }),
            json!({ "unexpected": true }),
        ]);
        assert_eq!(
            server.resume_thread("thr_1"),
            Err(AppServerError::ProtocolDrift)
        );
        assert_eq!(
            server.interrupt_turn("thr_1", "turn_1"),
            Err(AppServerError::ProtocolDrift)
        );
    }

    #[test]
    fn notifications_are_available_only_after_initialization() {
        let notification = AppServerNotification {
            method: "turn/started".into(),
            params: json!({ "turn": { "id": "turn_1", "status": "inProgress" } }),
        };
        let transport = FixtureTransport {
            responses: VecDeque::from([Ok(json!({}))]),
            incoming_notifications: VecDeque::from([Ok(notification.clone())]),
            ..FixtureTransport::default()
        };
        let mut server = CodexAppServer::new(transport);
        assert_eq!(
            server.next_notification(),
            Err(AppServerError::NotInitialized)
        );
        server.initialize().unwrap();
        assert_eq!(server.next_notification().unwrap(), notification);
    }

    #[test]
    fn final_output_keeps_only_matching_completed_final_agent_messages() {
        let mut output = CodexFinalOutput::new("thr_target", "turn_target").unwrap();
        for notification in [
            AppServerNotification {
                method: "item/completed".into(),
                params: json!({
                    "threadId": "thr_target", "turnId": "turn_target",
                    "completedAtMs": 1,
                    "item": {"id": "commentary", "type": "agentMessage", "phase": "commentary", "text": "do not retain"}
                }),
            },
            AppServerNotification {
                method: "item/completed".into(),
                params: json!({
                    "threadId": "thr_target", "turnId": "turn_other",
                    "completedAtMs": 2,
                    "item": {"id": "wrong-turn", "type": "agentMessage", "phase": "final_answer", "text": "wrong turn"}
                }),
            },
            AppServerNotification {
                method: "item/agentMessage/delta".into(),
                params: json!({"threadId": "thr_target", "turnId": "turn_target", "itemId": "delta", "delta": "not completed"}),
            },
            AppServerNotification {
                method: "item/completed".into(),
                params: json!({
                    "threadId": "thr_target", "turnId": "turn_target",
                    "completedAtMs": 3,
                    "item": {"id": "final", "type": "agentMessage", "phase": "final_answer", "text": "# Retained\n\nMarkdown"}
                }),
            },
        ] {
            output.ingest(&notification).unwrap();
        }
        assert_eq!(output.final_output(), Some("# Retained\n\nMarkdown"));
    }

    #[test]
    fn over_limit_final_output_is_discarded_instead_of_truncated() {
        let mut output = CodexFinalOutput::new("thr_1", "turn_1").unwrap();
        output
            .ingest(&AppServerNotification {
                method: "item/completed".into(),
                params: json!({
                    "threadId": "thr_1", "turnId": "turn_1", "completedAtMs": 1,
                    "item": {"id": "final", "type": "agentMessage", "phase": "final_answer", "text": "é".repeat(CodexFinalOutput::MAX_BYTES)}
                }),
            })
            .unwrap();
        assert_eq!(output.final_output(), None);
        assert!(output.overflowed());
    }

    #[test]
    fn run_events_skip_unrelated_notifications_and_keep_stream_sequence() {
        let notifications = [
            AppServerNotification {
                method: "item/started".into(),
                params: json!({"item": {"id": "item_1"}}),
            },
            AppServerNotification {
                method: "turn/started".into(),
                params: json!({"turn": {"id": "turn_other", "status": "inProgress"}}),
            },
            AppServerNotification {
                method: "turn/started".into(),
                params: json!({"turn": {"id": "turn_target", "status": "inProgress"}}),
            },
            AppServerNotification {
                method: "turn/completed".into(),
                params: json!({"turn": {"id": "turn_target", "status": "completed"}}),
            },
        ];
        let transport = FixtureTransport {
            responses: VecDeque::from([Ok(json!({}))]),
            incoming_notifications: notifications.into_iter().map(Ok).collect(),
            ..FixtureTransport::default()
        };
        let mut server = CodexAppServer::new(transport);
        server.initialize().unwrap();
        let mut stream =
            CodexRunStream::new(bastet_core::RunId::from_bytes([4; 16]), "turn_target").unwrap();
        let started = server
            .next_run_event(&mut stream, "2026-09-03T00:00:00Z")
            .unwrap();
        let completed = server
            .next_run_event(&mut stream, "2026-09-03T00:00:01Z")
            .unwrap();
        assert_eq!(started.event.sequence, 1);
        assert_eq!(completed.event.sequence, 2);
        assert_eq!(
            completed.event.state,
            bastet_core::NormalizedRunState::Succeeded
        );
    }

    #[test]
    fn unified_run_updates_preserve_evidence_and_lifecycle_order() {
        let notifications = [
            AppServerNotification {
                method: "thread/tokenUsage/updated".into(),
                params: json!({
                    "threadId": "thr_1",
                    "turnId": "turn_1",
                    "tokenUsage": {
                        "last": {"inputTokens": 3, "outputTokens": 5, "totalTokens": 8}
                    }
                }),
            },
            AppServerNotification {
                method: "turn/started".into(),
                params: json!({"turn": {"id": "turn_1", "status": "inProgress"}}),
            },
        ];
        let transport = FixtureTransport {
            responses: VecDeque::from([Ok(json!({}))]),
            incoming_notifications: notifications.into_iter().map(Ok).collect(),
            ..FixtureTransport::default()
        };
        let mut server = CodexAppServer::new(transport);
        server.initialize().unwrap();
        let mut stream =
            CodexRunStream::new(bastet_core::RunId::from_bytes([5; 16]), "turn_1").unwrap();
        let evidence = CodexRunEvidence::new("thr_1", "turn_1").unwrap();

        let CodexRunUpdate::Evidence(CodexRunEvidenceUpdate::Cost(cost)) = server
            .next_run_update(&mut stream, &evidence, "2026-09-03T00:00:00Z")
            .unwrap()
        else {
            panic!("first update must preserve provider cost evidence");
        };
        assert_eq!(cost.input_tokens, Some(3));
        assert_eq!(cost.output_tokens, Some(5));

        let CodexRunUpdate::Lifecycle(started) = server
            .next_run_update(&mut stream, &evidence, "2026-09-03T00:00:01Z")
            .unwrap()
        else {
            panic!("second update must preserve lifecycle state");
        };
        assert_eq!(started.event.sequence, 1);
        assert_eq!(
            started.event.state,
            bastet_core::NormalizedRunState::Running
        );
    }

    #[test]
    fn local_notification_failures_become_distinct_terminal_events() {
        for (failure, expected_state, expected_kind) in [
            (
                TransportError::TimedOut,
                bastet_core::NormalizedRunState::Failed,
                bastet_core::AdapterFailureKind::Timeout,
            ),
            (
                TransportError::Unavailable,
                bastet_core::NormalizedRunState::Uncertain,
                bastet_core::AdapterFailureKind::Crashed,
            ),
        ] {
            let transport = FixtureTransport {
                responses: VecDeque::from([Ok(json!({}))]),
                incoming_notifications: VecDeque::from([Err(failure)]),
                ..FixtureTransport::default()
            };
            let mut server = CodexAppServer::new(transport);
            server.initialize().unwrap();
            let mut stream =
                CodexRunStream::new(bastet_core::RunId::from_bytes([6; 16]), "turn_1").unwrap();
            let evidence = CodexRunEvidence::new("thr_1", "turn_1").unwrap();

            let CodexRunUpdate::Lifecycle(update) = server
                .next_run_update(&mut stream, &evidence, "2026-09-03T00:00:00Z")
                .unwrap()
            else {
                panic!("transport failure must become lifecycle evidence");
            };
            assert_eq!(update.event.state, expected_state);
            assert_eq!(update.failure.unwrap().kind, expected_kind);
        }
    }

    #[test]
    fn protocol_and_remote_notification_failures_still_fail_closed() {
        for failure in [
            TransportError::ProtocolDrift,
            TransportError::RemoteRejected {
                code: -32001,
                retryable: true,
            },
        ] {
            let transport = FixtureTransport {
                responses: VecDeque::from([Ok(json!({}))]),
                incoming_notifications: VecDeque::from([Err(failure)]),
                ..FixtureTransport::default()
            };
            let mut server = CodexAppServer::new(transport);
            server.initialize().unwrap();
            let mut stream =
                CodexRunStream::new(bastet_core::RunId::from_bytes([7; 16]), "turn_1").unwrap();
            let evidence = CodexRunEvidence::new("thr_1", "turn_1").unwrap();

            assert_eq!(
                server.next_run_update(&mut stream, &evidence, "2026-09-03T00:00:00Z"),
                Err(AppServerError::Transport(failure))
            );
        }
    }
}
