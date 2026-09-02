use bastet_core::{CostEvidence, EvidenceClass};
use serde_json::{json, Value};
use thiserror::Error;

use crate::app_server::AppServerNotification;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EvidenceError {
    #[error("Codex evidence event did not match the expected protocol")]
    ProtocolDrift,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CodexRunEvidenceUpdate {
    WriteReceipt(String),
    Cost(CostEvidence),
}

pub struct CodexRunEvidence {
    provider_thread_id: String,
    provider_turn_id: String,
}

impl CodexRunEvidence {
    pub fn new(
        provider_thread_id: impl Into<String>,
        provider_turn_id: impl Into<String>,
    ) -> Result<Self, EvidenceError> {
        let provider_thread_id = provider_thread_id.into();
        let provider_turn_id = provider_turn_id.into();
        if provider_thread_id.trim().is_empty() || provider_turn_id.trim().is_empty() {
            return Err(EvidenceError::ProtocolDrift);
        }
        Ok(Self {
            provider_thread_id,
            provider_turn_id,
        })
    }

    pub fn ingest(
        &self,
        notification: &AppServerNotification,
    ) -> Result<Option<CodexRunEvidenceUpdate>, EvidenceError> {
        match notification.method.as_str() {
            "turn/diff/updated" => self.diff_updated(&notification.params),
            "thread/tokenUsage/updated" => self.token_usage_updated(&notification.params),
            _ => Ok(None),
        }
    }

    fn diff_updated(
        &self,
        params: &Value,
    ) -> Result<Option<CodexRunEvidenceUpdate>, EvidenceError> {
        if !self.matches_run(params)? {
            return Ok(None);
        }
        let diff = required_string(params, "diff")?;
        if diff.is_empty() {
            return Ok(None);
        }
        Ok(Some(CodexRunEvidenceUpdate::WriteReceipt(
            json!({
                "evidence": "codex.turn_diff.updated",
                "turn_id": self.provider_turn_id
            })
            .to_string(),
        )))
    }

    fn token_usage_updated(
        &self,
        params: &Value,
    ) -> Result<Option<CodexRunEvidenceUpdate>, EvidenceError> {
        if !self.matches_run(params)? {
            return Ok(None);
        }
        let last = params
            .get("tokenUsage")
            .and_then(|value| value.get("last"))
            .ok_or(EvidenceError::ProtocolDrift)?;
        let input_tokens = required_u64(last, "inputTokens")?;
        let output_tokens = required_u64(last, "outputTokens")?;
        let total_tokens = required_u64(last, "totalTokens")?;
        let minimum_total = input_tokens
            .checked_add(output_tokens)
            .ok_or(EvidenceError::ProtocolDrift)?;
        if total_tokens < minimum_total {
            return Err(EvidenceError::ProtocolDrift);
        }
        Ok(Some(CodexRunEvidenceUpdate::Cost(CostEvidence {
            evidence_class: EvidenceClass::ProviderReported,
            currency: None,
            amount: None,
            input_tokens: Some(input_tokens),
            output_tokens: Some(output_tokens),
            confidence: 1.0,
        })))
    }

    fn matches_run(&self, params: &Value) -> Result<bool, EvidenceError> {
        let thread_id = required_string(params, "threadId")?;
        let turn_id = required_string(params, "turnId")?;
        Ok(thread_id == self.provider_thread_id && turn_id == self.provider_turn_id)
    }
}

fn required_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, EvidenceError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(EvidenceError::ProtocolDrift)
}

fn required_u64(value: &Value, key: &str) -> Result<u64, EvidenceError> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or(EvidenceError::ProtocolDrift)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collector() -> CodexRunEvidence {
        CodexRunEvidence::new("thr_1", "turn_1").unwrap()
    }

    #[test]
    fn diff_becomes_redacted_write_receipt_without_paths_or_content() {
        let secret = "BASTET_CONFORMANCE_SECRET_DO_NOT_LOG";
        let update = collector()
            .ingest(&AppServerNotification {
                method: "turn/diff/updated".into(),
                params: json!({
                    "threadId": "thr_1",
                    "turnId": "turn_1",
                    "diff": format!("--- /secret/path\n+{secret}")
                }),
            })
            .unwrap()
            .unwrap();
        let CodexRunEvidenceUpdate::WriteReceipt(receipt) = update else {
            panic!("expected write receipt");
        };
        assert!(!receipt.contains(secret));
        assert!(!receipt.contains("/secret/path"));
        assert_eq!(
            serde_json::from_str::<Value>(&receipt).unwrap(),
            json!({"evidence": "codex.turn_diff.updated", "turn_id": "turn_1"})
        );
    }

    #[test]
    fn last_turn_usage_becomes_provider_token_evidence_without_fake_currency() {
        let update = collector()
            .ingest(&AppServerNotification {
                method: "thread/tokenUsage/updated".into(),
                params: json!({
                    "threadId": "thr_1",
                    "turnId": "turn_1",
                    "tokenUsage": {
                        "last": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15},
                        "total": {"inputTokens": 100, "outputTokens": 50, "totalTokens": 150}
                    }
                }),
            })
            .unwrap()
            .unwrap();
        let CodexRunEvidenceUpdate::Cost(cost) = update else {
            panic!("expected cost evidence");
        };
        assert_eq!((cost.input_tokens, cost.output_tokens), (Some(10), Some(5)));
        assert_eq!((cost.currency, cost.amount), (None, None));
        assert_eq!(cost.evidence_class, EvidenceClass::ProviderReported);
    }

    #[test]
    fn unrelated_runs_are_ignored_and_malformed_target_usage_fails_closed() {
        let unrelated = AppServerNotification {
            method: "thread/tokenUsage/updated".into(),
            params: json!({"threadId": "thr_1", "turnId": "turn_other"}),
        };
        assert_eq!(collector().ingest(&unrelated), Ok(None));

        let malformed = AppServerNotification {
            method: "thread/tokenUsage/updated".into(),
            params: json!({
                "threadId": "thr_1",
                "turnId": "turn_1",
                "tokenUsage": {"last": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 2}}
            }),
        };
        assert_eq!(
            collector().ingest(&malformed),
            Err(EvidenceError::ProtocolDrift)
        );
    }
}
