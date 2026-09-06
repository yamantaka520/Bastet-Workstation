use bastet_adapter_agy::{AgyRunStream, AgyRunUpdate};
use bastet_adapter_conformance::{
    run_required_suite, ConformanceAdapter, ConformanceCase, ConformanceObservation,
    ConformanceScenario, SECRET_SENTINEL,
};
use bastet_core::{
    AdapterCapabilities, AdapterFailure, AdapterOperation, CostEvidence, NormalizedAdapterEvent,
    WorkspaceSnapshot,
};
use serde_json::json;

const CONVERSATION_ID: &str = "2200c74a-8a2f-4d0e-90f2-524463c987f4";
const NOW: &str = "2026-09-06T00:00:00Z";

struct AgyProtocolFixture;

impl ConformanceAdapter for AgyProtocolFixture {
    fn adapter_kind(&self) -> &str {
        "agy_cli_protocol_fixture"
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
        let mut stream = match case.scenario {
            ConformanceScenario::Resume => {
                AgyRunStream::resuming(case.run_id, CONVERSATION_ID).unwrap()
            }
            _ => AgyRunStream::new(case.run_id),
        };
        let mut events = Vec::new();
        let mut failure = None;
        let mut write_receipts = Vec::new();
        let mut cost = None;

        if case.scenario == ConformanceScenario::Resume {
            collect(
                stream.recovery_started(NOW).unwrap(),
                &mut events,
                &mut failure,
                &mut write_receipts,
                &mut cost,
            );
        }
        collect_line(
            &mut stream,
            json!({"event":"init","conversation_id":CONVERSATION_ID,"init":{"cwd":SECRET_SENTINEL,"tools":[SECRET_SENTINEL]}}),
            &mut events,
            &mut failure,
            &mut write_receipts,
            &mut cost,
        );

        match case.scenario {
            ConformanceScenario::Cancel => {
                collect(
                    stream.cancellation_started(NOW).unwrap(),
                    &mut events,
                    &mut failure,
                    &mut write_receipts,
                    &mut cost,
                );
                collect_line(
                    &mut stream,
                    result("INTERRUPTED", None),
                    &mut events,
                    &mut failure,
                    &mut write_receipts,
                    &mut cost,
                );
            }
            ConformanceScenario::Timeout => collect(
                stream.timed_out(NOW).unwrap(),
                &mut events,
                &mut failure,
                &mut write_receipts,
                &mut cost,
            ),
            ConformanceScenario::AuthenticationFailure => collect_line(
                &mut stream,
                result(
                    "ERROR",
                    Some("authentication required: BASTET_CONFORMANCE_SECRET_DO_NOT_LOG"),
                ),
                &mut events,
                &mut failure,
                &mut write_receipts,
                &mut cost,
            ),
            ConformanceScenario::QuotaFailure => collect_line(
                &mut stream,
                result(
                    "ERROR",
                    Some("quota exhausted: BASTET_CONFORMANCE_SECRET_DO_NOT_LOG"),
                ),
                &mut events,
                &mut failure,
                &mut write_receipts,
                &mut cost,
            ),
            ConformanceScenario::Crash => collect(
                stream.crashed(NOW).unwrap(),
                &mut events,
                &mut failure,
                &mut write_receipts,
                &mut cost,
            ),
            ConformanceScenario::Write => {
                let root = tempfile::tempdir().unwrap();
                let before = WorkspaceSnapshot::capture(root.path()).unwrap();
                std::fs::write(root.path().join("secret.txt"), SECRET_SENTINEL).unwrap();
                let after = WorkspaceSnapshot::capture(root.path()).unwrap();
                write_receipts.push(before.write_receipt(&after).unwrap().unwrap());
                collect_line(
                    &mut stream,
                    result("SUCCESS", None),
                    &mut events,
                    &mut failure,
                    &mut write_receipts,
                    &mut cost,
                );
            }
            ConformanceScenario::CostEvidence => {
                collect_line(
                    &mut stream,
                    json!({"event":"step_update","step_update":{"conversation_id":CONVERSATION_ID,"step_index":1,"state":"DONE","step_type":"agent_response","text_delta":SECRET_SENTINEL,"usage":{"input_tokens":10,"output_tokens":5,"thinking_tokens":2,"cache_read_tokens":0,"total_tokens":15}}}),
                    &mut events,
                    &mut failure,
                    &mut write_receipts,
                    &mut cost,
                );
                collect_line(
                    &mut stream,
                    result("SUCCESS", None),
                    &mut events,
                    &mut failure,
                    &mut write_receipts,
                    &mut cost,
                );
            }
            ConformanceScenario::ReadOnly
            | ConformanceScenario::Resume
            | ConformanceScenario::Redaction => collect_line(
                &mut stream,
                result("SUCCESS", None),
                &mut events,
                &mut failure,
                &mut write_receipts,
                &mut cost,
            ),
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

fn result(status: &str, error: Option<&str>) -> serde_json::Value {
    let mut value = json!({"event":"result","result":{"conversation_id":CONVERSATION_ID,"status":status,"response":SECRET_SENTINEL,"usage":{"input_tokens":10,"output_tokens":5,"thinking_tokens":2,"cache_read_tokens":0,"total_tokens":15}}});
    if let Some(error) = error {
        value["result"]["error"] = json!(error);
    }
    value
}

fn collect_line(
    stream: &mut AgyRunStream,
    value: serde_json::Value,
    events: &mut Vec<NormalizedAdapterEvent>,
    failure: &mut Option<AdapterFailure>,
    receipts: &mut Vec<String>,
    cost: &mut Option<CostEvidence>,
) {
    if let Some(update) = stream.consume_line(&value.to_string(), NOW).unwrap() {
        collect(update, events, failure, receipts, cost);
    }
}

fn collect(
    update: AgyRunUpdate,
    events: &mut Vec<NormalizedAdapterEvent>,
    failure: &mut Option<AdapterFailure>,
    receipts: &mut Vec<String>,
    cost: &mut Option<CostEvidence>,
) {
    match update {
        AgyRunUpdate::Lifecycle {
            event,
            failure: observed,
        } => {
            if observed.is_some() {
                *failure = observed;
            }
            events.push(event);
        }
        AgyRunUpdate::Cost(observed) => *cost = Some(observed),
        AgyRunUpdate::WriteReceipt(receipt) => receipts.push(receipt),
    }
}

#[test]
fn agy_protocol_fixture_passes_all_required_scenarios() {
    let report = run_required_suite(&mut AgyProtocolFixture);
    assert!(report.passed, "{:#?}", report.results);
    assert_eq!(report.results.len(), 10);
}

#[test]
fn fixture_never_retains_secret_or_invents_currency() {
    let report = run_required_suite(&mut AgyProtocolFixture);
    let serialized = serde_json::to_string(&report).unwrap();
    assert!(!serialized.contains(SECRET_SENTINEL));
    let observation = AgyProtocolFixture.run_case(
        &bastet_adapter_conformance::required_cases()
            .into_iter()
            .find(|case| case.scenario == ConformanceScenario::CostEvidence)
            .unwrap(),
    );
    let cost = observation.cost.unwrap();
    assert_eq!((cost.currency, cost.amount), (None, None));
}
