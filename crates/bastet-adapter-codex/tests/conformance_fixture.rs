use bastet_adapter_codex::{
    AppServerNotification, CodexRunEvidence, CodexRunEvidenceUpdate, CodexRunStream,
};
use bastet_adapter_conformance::{
    run_required_suite, ConformanceAdapter, ConformanceCase, ConformanceObservation,
    ConformanceScenario, SECRET_SENTINEL,
};
use bastet_core::{
    AdapterCapabilities, AdapterFailure, AdapterOperation, CostEvidence, NormalizedAdapterEvent,
};
use serde_json::json;

const THREAD_ID: &str = "thr_fixture";
const TURN_ID: &str = "turn_fixture";
const NOW: &str = "2026-09-03T00:00:00Z";

struct CodexProtocolFixture;

impl ConformanceAdapter for CodexProtocolFixture {
    fn adapter_kind(&self) -> &str {
        "codex_cli_protocol_fixture"
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            operations: vec![
                AdapterOperation::Start,
                AdapterOperation::Cancel,
                AdapterOperation::Authenticate,
                AdapterOperation::Attach,
                AdapterOperation::ExportUsage,
            ],
            reasoning_controls: vec!["low".into(), "medium".into(), "high".into()],
            supports_read_only: true,
            supports_write: true,
            supports_resume: true,
            supports_structured_events: true,
        }
    }

    fn run_case(&mut self, case: &ConformanceCase) -> ConformanceObservation {
        let mut stream = CodexRunStream::new(case.run_id, TURN_ID).unwrap();
        let mut events = Vec::new();
        let mut failure = None;
        let mut write_receipts = Vec::new();
        let mut cost = None;

        match case.scenario {
            ConformanceScenario::Cancel => {
                push(&mut events, &mut failure, started(&mut stream));
                push(
                    &mut events,
                    &mut failure,
                    stream.cancellation_requested(NOW).unwrap(),
                );
                push(
                    &mut events,
                    &mut failure,
                    completed(&mut stream, "interrupted", None),
                );
            }
            ConformanceScenario::Timeout => {
                push(&mut events, &mut failure, started(&mut stream));
                push(
                    &mut events,
                    &mut failure,
                    stream.deadline_exceeded(NOW).unwrap(),
                );
            }
            ConformanceScenario::AuthenticationFailure => {
                push(&mut events, &mut failure, started(&mut stream));
                push(
                    &mut events,
                    &mut failure,
                    completed(&mut stream, "failed", Some("Unauthorized")),
                );
            }
            ConformanceScenario::QuotaFailure => {
                push(&mut events, &mut failure, started(&mut stream));
                push(
                    &mut events,
                    &mut failure,
                    completed(&mut stream, "failed", Some("UsageLimitExceeded")),
                );
            }
            ConformanceScenario::Crash => {
                push(&mut events, &mut failure, started(&mut stream));
                push(
                    &mut events,
                    &mut failure,
                    stream.transport_lost(NOW).unwrap(),
                );
            }
            ConformanceScenario::Resume => {
                push(
                    &mut events,
                    &mut failure,
                    stream.recovery_started(THREAD_ID, NOW).unwrap(),
                );
                push(&mut events, &mut failure, started(&mut stream));
                push(
                    &mut events,
                    &mut failure,
                    completed(&mut stream, "completed", None),
                );
            }
            ConformanceScenario::Write => {
                push(&mut events, &mut failure, started(&mut stream));
                let update = evidence()
                    .ingest(&diff_notification(SECRET_SENTINEL))
                    .unwrap()
                    .unwrap();
                let CodexRunEvidenceUpdate::WriteReceipt(receipt) = update else {
                    panic!("expected write receipt")
                };
                write_receipts.push(receipt);
                push(
                    &mut events,
                    &mut failure,
                    completed(&mut stream, "completed", None),
                );
            }
            ConformanceScenario::CostEvidence => {
                push(&mut events, &mut failure, started(&mut stream));
                let update = evidence().ingest(&usage_notification()).unwrap().unwrap();
                let CodexRunEvidenceUpdate::Cost(provider_cost) = update else {
                    panic!("expected cost evidence")
                };
                cost = Some(provider_cost);
                push(
                    &mut events,
                    &mut failure,
                    completed(&mut stream, "completed", None),
                );
            }
            ConformanceScenario::Redaction => {
                push(&mut events, &mut failure, started(&mut stream));
                assert!(stream
                    .ingest(
                        &AppServerNotification {
                            method: "error".into(),
                            params: json!({"error": {"message": SECRET_SENTINEL}}),
                        },
                        NOW,
                    )
                    .unwrap()
                    .is_none());
                push(
                    &mut events,
                    &mut failure,
                    completed(&mut stream, "completed", None),
                );
            }
            ConformanceScenario::ReadOnly => {
                push(&mut events, &mut failure, started(&mut stream));
                push(
                    &mut events,
                    &mut failure,
                    completed(&mut stream, "completed", None),
                );
            }
        }

        ConformanceObservation {
            scenario: case.scenario,
            final_state: events.last().unwrap().state,
            events,
            failure,
            write_receipts,
            cost,
        }
    }
}

fn push(
    events: &mut Vec<NormalizedAdapterEvent>,
    failure: &mut Option<AdapterFailure>,
    normalized: bastet_adapter_codex::NormalizedCodexEvent,
) {
    if normalized.failure.is_some() {
        *failure = normalized.failure;
    }
    events.push(normalized.event);
}

fn started(stream: &mut CodexRunStream) -> bastet_adapter_codex::NormalizedCodexEvent {
    stream
        .ingest(
            &AppServerNotification {
                method: "turn/started".into(),
                params: json!({"turn": {"id": TURN_ID, "status": "inProgress", "items": []}}),
            },
            NOW,
        )
        .unwrap()
        .unwrap()
}

fn completed(
    stream: &mut CodexRunStream,
    status: &str,
    provider_code: Option<&str>,
) -> bastet_adapter_codex::NormalizedCodexEvent {
    let mut turn = json!({"id": TURN_ID, "status": status});
    if let Some(code) = provider_code {
        turn["error"] = json!({
            "message": SECRET_SENTINEL,
            "codexErrorInfo": {"type": code}
        });
    }
    stream
        .ingest(
            &AppServerNotification {
                method: "turn/completed".into(),
                params: json!({"turn": turn}),
            },
            NOW,
        )
        .unwrap()
        .unwrap()
}

fn evidence() -> CodexRunEvidence {
    CodexRunEvidence::new(THREAD_ID, TURN_ID).unwrap()
}

fn diff_notification(secret: &str) -> AppServerNotification {
    AppServerNotification {
        method: "turn/diff/updated".into(),
        params: json!({
            "threadId": THREAD_ID,
            "turnId": TURN_ID,
            "diff": format!("+{secret}")
        }),
    }
}

fn usage_notification() -> AppServerNotification {
    AppServerNotification {
        method: "thread/tokenUsage/updated".into(),
        params: json!({
            "threadId": THREAD_ID,
            "turnId": TURN_ID,
            "tokenUsage": {
                "last": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15},
                "total": {"inputTokens": 10, "outputTokens": 5, "totalTokens": 15}
            }
        }),
    }
}

#[test]
fn codex_protocol_fixture_passes_all_required_scenarios() {
    let report = run_required_suite(&mut CodexProtocolFixture);
    assert!(report.passed, "{:#?}", report.results);
    assert_eq!(report.results.len(), 10);
}

#[test]
fn fixture_evidence_never_claims_a_provider_currency_amount() {
    let report = run_required_suite(&mut CodexProtocolFixture);
    let result = report
        .results
        .iter()
        .find(|result| result.scenario == ConformanceScenario::CostEvidence)
        .unwrap();
    assert!(result.passed);

    let observation = CodexProtocolFixture.run_case(
        &bastet_adapter_conformance::required_cases()
            .into_iter()
            .find(|case| case.scenario == ConformanceScenario::CostEvidence)
            .unwrap(),
    );
    let CostEvidence {
        currency, amount, ..
    } = observation.cost.unwrap();
    assert_eq!((currency, amount), (None, None));
}

#[test]
fn production_adapter_still_does_not_claim_fixture_only_execution() {
    let adapter = bastet_adapter_codex::CodexAdapter::new("missing-fixture-binary");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.operations.contains(&AdapterOperation::Start));
    assert!(!capabilities.operations.contains(&AdapterOperation::Cancel));
    assert!(!capabilities.supports_structured_events);
}
