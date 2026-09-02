use bastet_core::{
    AdapterFailure, AdapterFailureKind, EvidenceClass, NormalizedAdapterEvent, NormalizedRunState,
    RunId, AGENT_ADAPTER_CONTRACT_VERSION,
};
use serde_json::{json, Value};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LifecycleError {
    #[error("Codex lifecycle event did not match the expected protocol")]
    ProtocolDrift,
    #[error("Codex lifecycle event sequence overflowed")]
    SequenceOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedCodexEvent {
    pub event: NormalizedAdapterEvent,
    pub failure: Option<AdapterFailure>,
}

pub struct CodexEventNormalizer {
    run_id: RunId,
    next_sequence: u64,
}

impl CodexEventNormalizer {
    pub fn new(run_id: RunId) -> Self {
        Self {
            run_id,
            next_sequence: 1,
        }
    }

    pub fn cancellation_requested(
        &mut self,
        occurred_at: &str,
    ) -> Result<NormalizedCodexEvent, LifecycleError> {
        self.event(
            NormalizedRunState::Cancelling,
            "codex.turn_interrupt_requested",
            occurred_at,
            EvidenceClass::LocallyMeasured,
            None,
            json!({"status": "cancelling"}),
            None,
        )
    }

    pub fn recovery_started(
        &mut self,
        provider_thread_id: &str,
        occurred_at: &str,
    ) -> Result<NormalizedCodexEvent, LifecycleError> {
        if provider_thread_id.trim().is_empty() {
            return Err(LifecycleError::ProtocolDrift);
        }
        self.event(
            NormalizedRunState::Recovering,
            "codex.thread_resume_requested",
            occurred_at,
            EvidenceClass::LocallyMeasured,
            Some(provider_thread_id.into()),
            json!({"thread_id": provider_thread_id, "status": "recovering"}),
            None,
        )
    }

    pub fn normalize_notification(
        &mut self,
        method: &str,
        params: &Value,
        occurred_at: &str,
    ) -> Result<Option<NormalizedCodexEvent>, LifecycleError> {
        match method {
            "turn/started" => self.turn_started(params, occurred_at).map(Some),
            "turn/completed" => self.turn_completed(params, occurred_at).map(Some),
            _ => Ok(None),
        }
    }

    fn turn_started(
        &mut self,
        params: &Value,
        occurred_at: &str,
    ) -> Result<NormalizedCodexEvent, LifecycleError> {
        let turn = turn(params)?;
        let turn_id = required_string(turn, "id")?;
        if required_string(turn, "status")? != "inProgress" {
            return Err(LifecycleError::ProtocolDrift);
        }
        self.event(
            NormalizedRunState::Running,
            "codex.turn_started",
            occurred_at,
            EvidenceClass::ProviderReported,
            Some(turn_id.into()),
            json!({"turn_id": turn_id, "status": "running"}),
            None,
        )
    }

    fn turn_completed(
        &mut self,
        params: &Value,
        occurred_at: &str,
    ) -> Result<NormalizedCodexEvent, LifecycleError> {
        let turn = turn(params)?;
        let turn_id = required_string(turn, "id")?;
        let provider_status = required_string(turn, "status")?;
        let (state, failure) = match provider_status {
            "completed" => (NormalizedRunState::Succeeded, None),
            "interrupted" => (
                NormalizedRunState::Cancelled,
                Some(failure(
                    AdapterFailureKind::Cancelled,
                    Some("Interrupted"),
                    false,
                )),
            ),
            "failed" => {
                let provider_code = failure_code(turn).unwrap_or("Other");
                let (kind, retryable, safe_provider_code) = map_failure(provider_code);
                (
                    NormalizedRunState::Failed,
                    Some(failure(kind, safe_provider_code, retryable)),
                )
            }
            _ => return Err(LifecycleError::ProtocolDrift),
        };
        self.event(
            state,
            "codex.turn_completed",
            occurred_at,
            EvidenceClass::ProviderReported,
            Some(turn_id.into()),
            json!({"turn_id": turn_id, "status": provider_status}),
            failure,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn event(
        &mut self,
        state: NormalizedRunState,
        event_type: &str,
        occurred_at: &str,
        evidence_class: EvidenceClass,
        provider_event_id: Option<String>,
        payload: Value,
        failure: Option<AdapterFailure>,
    ) -> Result<NormalizedCodexEvent, LifecycleError> {
        if occurred_at.trim().is_empty() {
            return Err(LifecycleError::ProtocolDrift);
        }
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or(LifecycleError::SequenceOverflow)?;
        Ok(NormalizedCodexEvent {
            event: NormalizedAdapterEvent {
                contract_version: AGENT_ADAPTER_CONTRACT_VERSION,
                run_id: self.run_id,
                sequence,
                state,
                event_type: event_type.into(),
                occurred_at: occurred_at.into(),
                evidence_class,
                provider_event_id,
                redacted_payload_json: payload.to_string(),
            },
            failure,
        })
    }
}

fn turn(params: &Value) -> Result<&serde_json::Map<String, Value>, LifecycleError> {
    params
        .get("turn")
        .and_then(Value::as_object)
        .ok_or(LifecycleError::ProtocolDrift)
}

fn required_string<'a>(
    object: &'a serde_json::Map<String, Value>,
    key: &str,
) -> Result<&'a str, LifecycleError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(LifecycleError::ProtocolDrift)
}

fn failure_code(turn: &serde_json::Map<String, Value>) -> Option<&str> {
    let info = turn.get("error")?.get("codexErrorInfo")?;
    info.as_str().or_else(|| info.get("type")?.as_str())
}

fn map_failure(provider_code: &str) -> (AdapterFailureKind, bool, Option<&'static str>) {
    match provider_code {
        "Unauthorized" => (
            AdapterFailureKind::Authentication,
            false,
            Some("Unauthorized"),
        ),
        "UsageLimitExceeded" => (AdapterFailureKind::Quota, true, Some("UsageLimitExceeded")),
        "SandboxError" => (
            AdapterFailureKind::PermissionDenied,
            false,
            Some("SandboxError"),
        ),
        "ResponseStreamConnectionFailed"
        | "ResponseStreamDisconnected"
        | "ResponseTooManyFailedAttempts"
        | "HttpConnectionFailed" => (AdapterFailureKind::Crashed, true, known_code(provider_code)),
        "InternalServerError" => (
            AdapterFailureKind::Crashed,
            true,
            Some("InternalServerError"),
        ),
        "ContextWindowExceeded" | "BadRequest" | "Other" => (
            AdapterFailureKind::Unknown,
            false,
            known_code(provider_code),
        ),
        _ => (AdapterFailureKind::Unknown, false, None),
    }
}

fn known_code(provider_code: &str) -> Option<&'static str> {
    match provider_code {
        "ResponseStreamConnectionFailed" => Some("ResponseStreamConnectionFailed"),
        "ResponseStreamDisconnected" => Some("ResponseStreamDisconnected"),
        "ResponseTooManyFailedAttempts" => Some("ResponseTooManyFailedAttempts"),
        "HttpConnectionFailed" => Some("HttpConnectionFailed"),
        "ContextWindowExceeded" => Some("ContextWindowExceeded"),
        "BadRequest" => Some("BadRequest"),
        "Other" => Some("Other"),
        _ => None,
    }
}

fn failure(
    kind: AdapterFailureKind,
    provider_code: Option<&str>,
    retryable: bool,
) -> AdapterFailure {
    AdapterFailure {
        kind,
        message_key: provider_code.map_or_else(
            || "codex.failure.unknown".into(),
            |code| format!("codex.failure.{}", snake_case(code)),
        ),
        retryable,
        provider_code: provider_code.map(str::to_owned),
        redacted_detail: None,
    }
}

fn snake_case(value: &str) -> String {
    let mut result = String::new();
    for (index, character) in value.chars().enumerate() {
        if character.is_ascii_uppercase() && index > 0 {
            result.push('_');
        }
        result.push(character.to_ascii_lowercase());
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: &str = "2026-09-03T00:00:00Z";

    fn normalizer() -> CodexEventNormalizer {
        CodexEventNormalizer::new(RunId::from_bytes([7; 16]))
    }

    #[test]
    fn start_and_completion_map_to_monotonic_normalized_events() {
        let mut normalizer = normalizer();
        let started = normalizer
            .normalize_notification(
                "turn/started",
                &json!({"turn": {"id": "turn_1", "status": "inProgress", "items": []}}),
                NOW,
            )
            .unwrap()
            .unwrap();
        let completed = normalizer
            .normalize_notification(
                "turn/completed",
                &json!({"turn": {"id": "turn_1", "status": "completed"}}),
                NOW,
            )
            .unwrap()
            .unwrap();
        assert_eq!(started.event.state, NormalizedRunState::Running);
        assert_eq!(completed.event.state, NormalizedRunState::Succeeded);
        assert_eq!((started.event.sequence, completed.event.sequence), (1, 2));
    }

    #[test]
    fn cancellation_exposes_cancelling_then_cancelled() {
        let mut normalizer = normalizer();
        let cancelling = normalizer.cancellation_requested(NOW).unwrap();
        let cancelled = normalizer
            .normalize_notification(
                "turn/completed",
                &json!({"turn": {"id": "turn_1", "status": "interrupted"}}),
                NOW,
            )
            .unwrap()
            .unwrap();
        assert_eq!(cancelling.event.state, NormalizedRunState::Cancelling);
        assert_eq!(cancelled.event.state, NormalizedRunState::Cancelled);
        assert_eq!(
            cancelled.failure.unwrap().kind,
            AdapterFailureKind::Cancelled
        );
    }

    #[test]
    fn resume_exposes_recovering_without_claiming_provider_completion() {
        let event = normalizer().recovery_started("thr_1", NOW).unwrap();
        assert_eq!(event.event.state, NormalizedRunState::Recovering);
        assert_eq!(event.event.evidence_class, EvidenceClass::LocallyMeasured);
    }

    #[test]
    fn provider_failures_are_normalized_without_error_text() {
        let event = normalizer()
            .normalize_notification(
                "turn/completed",
                &json!({"turn": {
                    "id": "turn_1",
                    "status": "failed",
                    "error": {
                        "message": "secret details",
                        "codexErrorInfo": {"type": "UsageLimitExceeded"}
                    }
                }}),
                NOW,
            )
            .unwrap()
            .unwrap();
        let failure = event.failure.unwrap();
        assert_eq!(failure.kind, AdapterFailureKind::Quota);
        assert!(failure.retryable);
        assert_eq!(failure.redacted_detail, None);
        assert!(!event.event.redacted_payload_json.contains("secret details"));
    }

    #[test]
    fn unknown_provider_error_codes_are_not_retained() {
        let event = normalizer()
            .normalize_notification(
                "turn/completed",
                &json!({"turn": {
                    "id": "turn_1",
                    "status": "failed",
                    "error": {"codexErrorInfo": {"type": "secret-custom-code"}}
                }}),
                NOW,
            )
            .unwrap()
            .unwrap();
        let failure = event.failure.unwrap();
        assert_eq!(failure.kind, AdapterFailureKind::Unknown);
        assert_eq!(failure.provider_code, None);
        assert_eq!(failure.message_key, "codex.failure.unknown");
    }

    #[test]
    fn unknown_events_are_ignored_and_invalid_statuses_fail_closed() {
        let mut normalizer = normalizer();
        assert_eq!(
            normalizer.normalize_notification("item/started", &json!({}), NOW),
            Ok(None)
        );
        assert_eq!(
            normalizer.normalize_notification(
                "turn/completed",
                &json!({"turn": {"id": "turn_1", "status": "mystery"}}),
                NOW
            ),
            Err(LifecycleError::ProtocolDrift)
        );
    }
}
