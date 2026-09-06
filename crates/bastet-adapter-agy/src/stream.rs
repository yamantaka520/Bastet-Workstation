use bastet_core::{
    AdapterFailure, AdapterFailureKind, CostEvidence, EvidenceClass, NormalizedAdapterEvent,
    NormalizedRunState, RunId, AGENT_ADAPTER_CONTRACT_VERSION,
};
use serde_json::{json, Value};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub enum AgyRunUpdate {
    Lifecycle {
        event: NormalizedAdapterEvent,
        failure: Option<AdapterFailure>,
    },
    Cost(CostEvidence),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AgyStreamError {
    #[error("Agy stream output did not match the expected protocol")]
    ProtocolDrift,
    #[error("Agy stream emitted an event after its terminal result")]
    Closed,
}

pub struct AgyRunStream {
    run_id: RunId,
    sequence: u64,
    conversation_id: Option<String>,
    closed: bool,
}

impl AgyRunStream {
    pub fn new(run_id: RunId) -> Self {
        Self {
            run_id,
            sequence: 0,
            conversation_id: None,
            closed: false,
        }
    }

    pub fn conversation_id(&self) -> Option<&str> {
        self.conversation_id.as_deref()
    }

    pub fn consume_line(
        &mut self,
        line: &str,
        occurred_at: &str,
    ) -> Result<Option<AgyRunUpdate>, AgyStreamError> {
        if self.closed {
            return Err(AgyStreamError::Closed);
        }
        let value: Value = serde_json::from_str(line).map_err(|_| AgyStreamError::ProtocolDrift)?;
        match value.get("event").and_then(Value::as_str) {
            Some("init") => self.consume_init(&value, occurred_at).map(Some),
            Some("step_update") => self.consume_step(&value),
            Some("result") => self.consume_result(&value, occurred_at).map(Some),
            _ => Err(AgyStreamError::ProtocolDrift),
        }
    }

    fn consume_init(
        &mut self,
        value: &Value,
        occurred_at: &str,
    ) -> Result<AgyRunUpdate, AgyStreamError> {
        if self.conversation_id.is_some() {
            return Err(AgyStreamError::ProtocolDrift);
        }
        let conversation_id = required_text(value, "conversation_id")?;
        self.conversation_id = Some(conversation_id.into());
        self.lifecycle(
            NormalizedRunState::Running,
            "run.started",
            occurred_at,
            None,
        )
    }

    fn consume_step(&self, value: &Value) -> Result<Option<AgyRunUpdate>, AgyStreamError> {
        let update = value
            .get("step_update")
            .and_then(Value::as_object)
            .ok_or(AgyStreamError::ProtocolDrift)?;
        self.require_matching_conversation(update.get("conversation_id"))?;
        let Some(usage) = update.get("usage") else {
            return Ok(None);
        };
        parse_usage(usage).map(AgyRunUpdate::Cost).map(Some)
    }

    fn consume_result(
        &mut self,
        value: &Value,
        occurred_at: &str,
    ) -> Result<AgyRunUpdate, AgyStreamError> {
        let result = value
            .get("result")
            .and_then(Value::as_object)
            .ok_or(AgyStreamError::ProtocolDrift)?;
        let status = required_text_value(result.get("status"))?;
        if let Some(expected) = self.conversation_id.as_deref() {
            let actual = required_text_value(result.get("conversation_id"))?;
            if actual != expected {
                return Err(AgyStreamError::ProtocolDrift);
            }
        } else if status != "ERROR" {
            return Err(AgyStreamError::ProtocolDrift);
        }
        let (state, event_type, failure) = match status {
            "SUCCESS" => (NormalizedRunState::Succeeded, "run.succeeded", None),
            "ERROR" => (
                NormalizedRunState::Failed,
                "run.failed",
                Some(AdapterFailure {
                    kind: classify_error(result.get("error").and_then(Value::as_str)),
                    message_key: "adapter.agy.failed".into(),
                    retryable: false,
                    provider_code: None,
                    redacted_detail: None,
                }),
            ),
            _ => return Err(AgyStreamError::ProtocolDrift),
        };
        self.closed = true;
        self.lifecycle(state, event_type, occurred_at, failure)
    }

    fn require_matching_conversation(&self, actual: Option<&Value>) -> Result<(), AgyStreamError> {
        let expected = self
            .conversation_id
            .as_deref()
            .ok_or(AgyStreamError::ProtocolDrift)?;
        if required_text_value(actual)? != expected {
            return Err(AgyStreamError::ProtocolDrift);
        }
        Ok(())
    }

    fn lifecycle(
        &mut self,
        state: NormalizedRunState,
        event_type: &str,
        occurred_at: &str,
        failure: Option<AdapterFailure>,
    ) -> Result<AgyRunUpdate, AgyStreamError> {
        if occurred_at.trim().is_empty() {
            return Err(AgyStreamError::ProtocolDrift);
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(AgyStreamError::ProtocolDrift)?;
        Ok(AgyRunUpdate::Lifecycle {
            event: NormalizedAdapterEvent {
                contract_version: AGENT_ADAPTER_CONTRACT_VERSION,
                run_id: self.run_id,
                sequence: self.sequence,
                state,
                event_type: event_type.into(),
                occurred_at: occurred_at.into(),
                evidence_class: EvidenceClass::ProviderReported,
                provider_event_id: None,
                redacted_payload_json: json!({"provider": ADAPTER_PROVIDER}).to_string(),
            },
            failure,
        })
    }
}

const ADAPTER_PROVIDER: &str = "agy_cli";

fn parse_usage(value: &Value) -> Result<CostEvidence, AgyStreamError> {
    let usage = value.as_object().ok_or(AgyStreamError::ProtocolDrift)?;
    let input_tokens = required_u64(usage.get("input_tokens"))?;
    let output_tokens = required_u64(usage.get("output_tokens"))?;
    required_u64(usage.get("thinking_tokens"))?;
    required_u64(usage.get("cache_read_tokens"))?;
    let total_tokens = required_u64(usage.get("total_tokens"))?;
    if input_tokens.checked_add(output_tokens).is_none()
        || total_tokens < input_tokens + output_tokens
    {
        return Err(AgyStreamError::ProtocolDrift);
    }
    Ok(CostEvidence {
        input_tokens: Some(input_tokens),
        output_tokens: Some(output_tokens),
        currency: None,
        amount: None,
        evidence_class: EvidenceClass::ProviderReported,
        confidence: 1.0,
    })
}

fn classify_error(error: Option<&str>) -> AdapterFailureKind {
    let Some(error) = error else {
        return AdapterFailureKind::Unknown;
    };
    let lower = error.to_ascii_lowercase();
    if lower.contains("not logged in")
        || lower.contains("authentication")
        || lower.contains("unauthorized")
    {
        AdapterFailureKind::Authentication
    } else if lower.contains("quota") || lower.contains("rate limit") {
        AdapterFailureKind::Quota
    } else if lower.contains("permission") {
        AdapterFailureKind::PermissionDenied
    } else {
        AdapterFailureKind::Unknown
    }
}

fn required_text<'a>(value: &'a Value, key: &str) -> Result<&'a str, AgyStreamError> {
    required_text_value(value.get(key))
}

fn required_text_value(value: Option<&Value>) -> Result<&str, AgyStreamError> {
    value
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or(AgyStreamError::ProtocolDrift)
}

fn required_u64(value: Option<&Value>) -> Result<u64, AgyStreamError> {
    value
        .and_then(Value::as_u64)
        .ok_or(AgyStreamError::ProtocolDrift)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "2200c74a-8a2f-4d0e-90f2-524463c987f4";

    fn stream() -> AgyRunStream {
        AgyRunStream::new(RunId::from_bytes([21; 16]))
    }

    #[test]
    fn real_shape_is_normalized_without_text_paths_or_tools() {
        let mut stream = stream();
        let AgyRunUpdate::Lifecycle { event, .. } = stream
            .consume_line(
                &json!({"event":"init","conversation_id":ID,"init":{"cwd":"/secret","tools":["danger"]}}).to_string(),
                "2026-09-06T00:00:00Z",
            )
            .unwrap()
            .unwrap()
        else { panic!("init must be lifecycle") };
        assert_eq!(event.state, NormalizedRunState::Running);
        assert_eq!(event.sequence, 1);
        assert!(!event.redacted_payload_json.contains("secret"));
        assert!(!event.redacted_payload_json.contains("danger"));

        let AgyRunUpdate::Cost(cost) = stream
            .consume_line(
                &json!({"event":"step_update","step_update":{"conversation_id":ID,"text_delta":"private","usage":{"input_tokens":10,"output_tokens":2,"thinking_tokens":1,"cache_read_tokens":0,"total_tokens":13}}}).to_string(),
                "ignored",
            )
            .unwrap()
            .unwrap()
        else { panic!("usage must be cost") };
        assert_eq!((cost.input_tokens, cost.output_tokens), (Some(10), Some(2)));
        assert_eq!((cost.currency, cost.amount), (None, None));

        let AgyRunUpdate::Lifecycle { event, failure } = stream
            .consume_line(
                &json!({"event":"result","result":{"conversation_id":ID,"status":"SUCCESS","response":"private","usage":{"input_tokens":10,"output_tokens":2,"thinking_tokens":1,"cache_read_tokens":0,"total_tokens":13}}}).to_string(),
                "2026-09-06T00:00:01Z",
            )
            .unwrap()
            .unwrap()
        else { panic!("result must be lifecycle") };
        assert_eq!(event.state, NormalizedRunState::Succeeded);
        assert_eq!(event.sequence, 2);
        assert!(failure.is_none());
    }

    #[test]
    fn errors_are_classified_but_raw_error_is_discarded() {
        for (error, expected) in [
            ("Not logged in: secret", AdapterFailureKind::Authentication),
            ("Quota exhausted: secret", AdapterFailureKind::Quota),
            ("unexpected secret", AdapterFailureKind::Unknown),
        ] {
            let mut stream = stream();
            let AgyRunUpdate::Lifecycle { event, failure } = stream
                .consume_line(
                    &json!({"event":"result","result":{"conversation_id":"","status":"ERROR","response":"","error":error}}).to_string(),
                    "2026-09-06T00:00:00Z",
                )
                .unwrap()
                .unwrap()
            else { panic!("error result must be lifecycle") };
            assert_eq!(event.state, NormalizedRunState::Failed);
            assert_eq!(failure.unwrap().kind, expected);
            assert!(!event.redacted_payload_json.contains("secret"));
        }
    }

    #[test]
    fn mismatches_unknown_events_and_post_terminal_data_fail_closed() {
        let mut stream = stream();
        assert_eq!(
            stream.consume_line("not json", "now"),
            Err(AgyStreamError::ProtocolDrift)
        );
        assert_eq!(
            stream.consume_line(&json!({"event":"unknown"}).to_string(), "now"),
            Err(AgyStreamError::ProtocolDrift)
        );
        stream
            .consume_line(
                &json!({"event":"init","conversation_id":ID,"init":{}}).to_string(),
                "now",
            )
            .unwrap();
        assert_eq!(
            stream.consume_line(
                &json!({"event":"step_update","step_update":{"conversation_id":"other"}})
                    .to_string(),
                "now"
            ),
            Err(AgyStreamError::ProtocolDrift)
        );
        stream
            .consume_line(
                &json!({"event":"result","result":{"conversation_id":ID,"status":"SUCCESS"}})
                    .to_string(),
                "now",
            )
            .unwrap();
        assert_eq!(
            stream.consume_line(
                &json!({"event":"result","result":{"conversation_id":ID,"status":"SUCCESS"}})
                    .to_string(),
                "now"
            ),
            Err(AgyStreamError::Closed)
        );
    }

    #[test]
    fn malformed_usage_fails_closed() {
        for usage in [
            json!({}),
            json!({"input_tokens":10,"output_tokens":2,"thinking_tokens":1,"cache_read_tokens":0,"total_tokens":5}),
            json!({"input_tokens":-1,"output_tokens":2,"thinking_tokens":1,"cache_read_tokens":0,"total_tokens":2}),
        ] {
            assert_eq!(parse_usage(&usage), Err(AgyStreamError::ProtocolDrift));
        }
    }
}
