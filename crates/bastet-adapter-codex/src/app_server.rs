use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

pub trait AppServerTransport {
    fn request(&mut self, method: &str, params: Value) -> Result<Value, TransportError>;
    fn notify(&mut self, method: &str, params: Value) -> Result<(), TransportError>;
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

    pub fn into_transport(self) -> T {
        self.transport
    }
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
}
