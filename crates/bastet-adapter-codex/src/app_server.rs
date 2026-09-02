use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use thiserror::Error;

use crate::lifecycle::{CodexRunStream, NormalizedCodexEvent};

pub trait AppServerTransport {
    fn request(&mut self, method: &str, params: Value) -> Result<Value, TransportError>;
    fn notify(&mut self, method: &str, params: Value) -> Result<(), TransportError>;
    fn next_notification(&mut self) -> Result<AppServerNotification, TransportError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppServerNotification {
    pub method: String,
    pub params: Value,
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error("Codex app-server transport failed")]
pub struct TransportError;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AppServerError {
    #[error("Codex app-server transport failed")]
    Transport,
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
#[serde(rename_all = "camelCase")]
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
            .map_err(|_| AppServerError::Transport)?;
        if !result.is_object() {
            return Err(AppServerError::ProtocolDrift);
        }
        self.transport
            .notify("initialized", json!({}))
            .map_err(|_| AppServerError::Transport)?;
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
            .map_err(|_| AppServerError::Transport)?;
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
                    "sandbox": request.sandbox,
                    "serviceName": "bastet_workstation"
                }),
            )
            .map_err(|_| AppServerError::Transport)?;
        parse_thread_handle(result)
    }

    pub fn resume_thread(&mut self, thread_id: &str) -> Result<ThreadHandle, AppServerError> {
        self.require_initialized()?;
        require_text(thread_id)?;
        let result = self
            .transport
            .request("thread/resume", json!({ "threadId": thread_id }))
            .map_err(|_| AppServerError::Transport)?;
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
            .map_err(|_| AppServerError::Transport)?;
        let wire: TurnResultWire =
            serde_json::from_value(result).map_err(|_| AppServerError::ProtocolDrift)?;
        require_text(&wire.turn.id)?;
        Ok(TurnHandle {
            turn_id: wire.turn.id,
        })
    }

    pub fn interrupt_turn(&mut self, thread_id: &str, turn_id: &str) -> Result<(), AppServerError> {
        self.require_initialized()?;
        require_text(thread_id)?;
        require_text(turn_id)?;
        let result = self
            .transport
            .request(
                "turn/interrupt",
                json!({ "threadId": thread_id, "turnId": turn_id }),
            )
            .map_err(|_| AppServerError::Transport)?;
        if result.as_object().is_none_or(|object| !object.is_empty()) {
            return Err(AppServerError::ProtocolDrift);
        }
        Ok(())
    }

    pub fn next_notification(&mut self) -> Result<AppServerNotification, AppServerError> {
        self.require_initialized()?;
        self.transport
            .next_notification()
            .map_err(|_| AppServerError::Transport)
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

        fn notify(&mut self, method: &str, params: Value) -> Result<(), TransportError> {
            self.notifications.push((method.into(), params));
            Ok(())
        }

        fn next_notification(&mut self) -> Result<AppServerNotification, TransportError> {
            self.incoming_notifications.pop_front().unwrap()
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
                "sandbox": "workspaceWrite",
                "serviceName": "bastet_workstation"
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
}
