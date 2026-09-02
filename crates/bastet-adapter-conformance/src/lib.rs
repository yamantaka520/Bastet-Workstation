//! Deterministic, side-effect-free validation for Agent Adapter implementations.

use std::collections::HashSet;

use bastet_core::{
    AdapterCapabilities, AdapterFailure, AdapterFailureKind, AdapterOperation, CostEvidence,
    EvidenceClass, NormalizedAdapterEvent, NormalizedRunState, RunId,
    AGENT_ADAPTER_CONTRACT_VERSION,
};
use serde::{Deserialize, Serialize};

pub const SECRET_SENTINEL: &str = "BASTET_CONFORMANCE_SECRET_DO_NOT_LOG";

pub const REQUIRED_SCENARIOS: [ConformanceScenario; 10] = [
    ConformanceScenario::ReadOnly,
    ConformanceScenario::Write,
    ConformanceScenario::Cancel,
    ConformanceScenario::Timeout,
    ConformanceScenario::AuthenticationFailure,
    ConformanceScenario::QuotaFailure,
    ConformanceScenario::Crash,
    ConformanceScenario::Resume,
    ConformanceScenario::Redaction,
    ConformanceScenario::CostEvidence,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConformanceScenario {
    ReadOnly,
    Write,
    Cancel,
    Timeout,
    AuthenticationFailure,
    QuotaFailure,
    Crash,
    Resume,
    Redaction,
    CostEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConformanceCase {
    pub scenario: ConformanceScenario,
    pub run_id: RunId,
    pub secret_sentinels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConformanceObservation {
    pub scenario: ConformanceScenario,
    pub events: Vec<NormalizedAdapterEvent>,
    pub final_state: NormalizedRunState,
    pub failure: Option<AdapterFailure>,
    pub write_receipts: Vec<String>,
    pub cost: Option<CostEvidence>,
}

pub trait ConformanceAdapter {
    fn adapter_kind(&self) -> &str;
    fn capabilities(&self) -> AdapterCapabilities;
    fn run_case(&mut self, case: &ConformanceCase) -> ConformanceObservation;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingKind {
    ScenarioMismatch,
    MissingCapability,
    InvalidFinalState,
    InvalidFailure,
    InvalidEvent,
    UnexpectedWrite,
    MissingWriteReceipt,
    SecretLeak,
    InvalidCostEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConformanceFinding {
    pub scenario: ConformanceScenario,
    pub kind: FindingKind,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioResult {
    pub scenario: ConformanceScenario,
    pub passed: bool,
    pub findings: Vec<ConformanceFinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConformanceReport {
    pub contract_version: u32,
    pub adapter_kind: String,
    pub passed: bool,
    pub results: Vec<ScenarioResult>,
}

pub fn required_cases() -> Vec<ConformanceCase> {
    REQUIRED_SCENARIOS
        .into_iter()
        .enumerate()
        .map(|(index, scenario)| {
            let mut bytes = [0u8; 16];
            bytes[..15].copy_from_slice(b"BASTET-M2-CASE!");
            bytes[15] = index as u8;
            ConformanceCase {
                scenario,
                run_id: RunId::from_bytes(bytes),
                secret_sentinels: vec![SECRET_SENTINEL.into()],
            }
        })
        .collect()
}

pub fn run_required_suite(adapter: &mut impl ConformanceAdapter) -> ConformanceReport {
    let capabilities = adapter.capabilities();
    let adapter_kind = adapter.adapter_kind().to_owned();
    let results = required_cases()
        .iter()
        .map(|case| {
            let observation = adapter.run_case(case);
            validate_observation(case, &capabilities, &observation)
        })
        .collect::<Vec<_>>();
    ConformanceReport {
        contract_version: AGENT_ADAPTER_CONTRACT_VERSION,
        adapter_kind,
        passed: results.iter().all(|result| result.passed),
        results,
    }
}

fn validate_observation(
    case: &ConformanceCase,
    capabilities: &AdapterCapabilities,
    observation: &ConformanceObservation,
) -> ScenarioResult {
    let mut findings = Vec::new();
    if observation.scenario != case.scenario {
        finding(
            &mut findings,
            case.scenario,
            FindingKind::ScenarioMismatch,
            "adapter returned a different scenario",
        );
    }
    require_capability(capabilities.supports_structured_events, case, &mut findings);
    validate_events(case, observation, &mut findings);
    validate_secrets(case, observation, &mut findings);
    validate_scenario(case, capabilities, observation, &mut findings);
    ScenarioResult {
        scenario: case.scenario,
        passed: findings.is_empty(),
        findings,
    }
}

fn validate_events(
    case: &ConformanceCase,
    observation: &ConformanceObservation,
    findings: &mut Vec<ConformanceFinding>,
) {
    if observation.events.is_empty() {
        finding(
            findings,
            case.scenario,
            FindingKind::InvalidEvent,
            "at least one normalized event is required",
        );
        return;
    }
    let mut sequences = HashSet::new();
    let mut previous = None;
    for event in &observation.events {
        let sequence_is_valid =
            previous.is_none_or(|value| event.sequence > value) && sequences.insert(event.sequence);
        let payload_is_json =
            serde_json::from_str::<serde_json::Value>(&event.redacted_payload_json).is_ok();
        if event.contract_version != AGENT_ADAPTER_CONTRACT_VERSION
            || event.run_id != case.run_id
            || !sequence_is_valid
            || !payload_is_json
        {
            finding(
                findings,
                case.scenario,
                FindingKind::InvalidEvent,
                "event version, run identity, sequence, or JSON payload is invalid",
            );
            break;
        }
        previous = Some(event.sequence);
    }
    if observation.events.last().map(|event| event.state) != Some(observation.final_state) {
        finding(
            findings,
            case.scenario,
            FindingKind::InvalidEvent,
            "last event state must match final state",
        );
    }
}

fn validate_secrets(
    case: &ConformanceCase,
    observation: &ConformanceObservation,
    findings: &mut Vec<ConformanceFinding>,
) {
    let values = observation
        .events
        .iter()
        .map(|event| event.redacted_payload_json.as_str())
        .chain(
            observation
                .failure
                .iter()
                .filter_map(|failure| failure.redacted_detail.as_deref()),
        )
        .chain(observation.write_receipts.iter().map(String::as_str));
    for value in values {
        if case
            .secret_sentinels
            .iter()
            .any(|secret| value.contains(secret))
        {
            finding(
                findings,
                case.scenario,
                FindingKind::SecretLeak,
                "secret sentinel appeared in adapter evidence",
            );
            return;
        }
    }
}

fn validate_scenario(
    case: &ConformanceCase,
    capabilities: &AdapterCapabilities,
    observation: &ConformanceObservation,
    findings: &mut Vec<ConformanceFinding>,
) {
    match case.scenario {
        ConformanceScenario::ReadOnly => {
            require_operation(AdapterOperation::Start, capabilities, case, findings);
            require_capability(capabilities.supports_read_only, case, findings);
            require_state(NormalizedRunState::Succeeded, case, observation, findings);
            if !observation.write_receipts.is_empty() {
                finding(
                    findings,
                    case.scenario,
                    FindingKind::UnexpectedWrite,
                    "read-only scenario emitted a write receipt",
                );
            }
        }
        ConformanceScenario::Write => {
            require_operation(AdapterOperation::Start, capabilities, case, findings);
            require_capability(capabilities.supports_write, case, findings);
            require_state(NormalizedRunState::Succeeded, case, observation, findings);
            if observation.write_receipts.is_empty() {
                finding(
                    findings,
                    case.scenario,
                    FindingKind::MissingWriteReceipt,
                    "write scenario requires an evidence receipt",
                );
            }
        }
        ConformanceScenario::Cancel => {
            require_operation(AdapterOperation::Cancel, capabilities, case, findings);
            require_state(NormalizedRunState::Cancelled, case, observation, findings);
            require_failure(AdapterFailureKind::Cancelled, case, observation, findings);
            if !observation
                .events
                .iter()
                .any(|event| event.state == NormalizedRunState::Cancelling)
            {
                finding(
                    findings,
                    case.scenario,
                    FindingKind::InvalidFinalState,
                    "cancel scenario never entered cancelling",
                );
            }
        }
        ConformanceScenario::Timeout => {
            require_operation(AdapterOperation::Start, capabilities, case, findings);
            require_state(NormalizedRunState::Failed, case, observation, findings);
            require_failure(AdapterFailureKind::Timeout, case, observation, findings);
        }
        ConformanceScenario::AuthenticationFailure => {
            require_operation(AdapterOperation::Authenticate, capabilities, case, findings);
            require_state(NormalizedRunState::Failed, case, observation, findings);
            require_failure(
                AdapterFailureKind::Authentication,
                case,
                observation,
                findings,
            );
        }
        ConformanceScenario::QuotaFailure => {
            require_operation(AdapterOperation::Start, capabilities, case, findings);
            require_state(NormalizedRunState::Failed, case, observation, findings);
            require_failure(AdapterFailureKind::Quota, case, observation, findings);
        }
        ConformanceScenario::Crash => {
            require_operation(AdapterOperation::Start, capabilities, case, findings);
            require_state(NormalizedRunState::Uncertain, case, observation, findings);
            require_failure(AdapterFailureKind::Crashed, case, observation, findings);
        }
        ConformanceScenario::Resume => {
            require_operation(AdapterOperation::Attach, capabilities, case, findings);
            require_capability(capabilities.supports_resume, case, findings);
            require_state(NormalizedRunState::Succeeded, case, observation, findings);
            if !observation
                .events
                .iter()
                .any(|event| event.state == NormalizedRunState::Recovering)
            {
                finding(
                    findings,
                    case.scenario,
                    FindingKind::InvalidFinalState,
                    "resume scenario never entered recovering",
                );
            }
        }
        ConformanceScenario::Redaction => {
            require_operation(AdapterOperation::Start, capabilities, case, findings);
            require_state(NormalizedRunState::Succeeded, case, observation, findings);
        }
        ConformanceScenario::CostEvidence => {
            require_operation(AdapterOperation::ExportUsage, capabilities, case, findings);
            require_state(NormalizedRunState::Succeeded, case, observation, findings);
            if !valid_cost(observation.cost.as_ref()) {
                finding(
                    findings,
                    case.scenario,
                    FindingKind::InvalidCostEvidence,
                    "cost evidence is missing, unknown, inconsistent, or outside confidence range",
                );
            }
        }
    }
}

fn valid_cost(cost: Option<&CostEvidence>) -> bool {
    let Some(cost) = cost else {
        return false;
    };
    if cost.evidence_class == EvidenceClass::Unknown || !(0.0..=1.0).contains(&cost.confidence) {
        return false;
    }
    match cost.amount {
        Some(amount) => {
            amount >= 0.0
                && cost
                    .currency
                    .as_ref()
                    .is_some_and(|value| !value.is_empty())
        }
        None => cost.currency.is_none(),
    }
}

fn require_capability(
    supported: bool,
    case: &ConformanceCase,
    findings: &mut Vec<ConformanceFinding>,
) {
    if !supported {
        finding(
            findings,
            case.scenario,
            FindingKind::MissingCapability,
            "adapter does not declare the required capability",
        );
    }
}

fn require_operation(
    operation: AdapterOperation,
    capabilities: &AdapterCapabilities,
    case: &ConformanceCase,
    findings: &mut Vec<ConformanceFinding>,
) {
    if !capabilities.operations.contains(&operation) {
        finding(
            findings,
            case.scenario,
            FindingKind::MissingCapability,
            "adapter does not declare the required operation",
        );
    }
}

fn require_state(
    expected: NormalizedRunState,
    case: &ConformanceCase,
    observation: &ConformanceObservation,
    findings: &mut Vec<ConformanceFinding>,
) {
    if observation.final_state != expected {
        finding(
            findings,
            case.scenario,
            FindingKind::InvalidFinalState,
            "adapter returned the wrong final state",
        );
    }
}

fn require_failure(
    expected: AdapterFailureKind,
    case: &ConformanceCase,
    observation: &ConformanceObservation,
    findings: &mut Vec<ConformanceFinding>,
) {
    if observation.failure.as_ref().map(|failure| failure.kind) != Some(expected) {
        finding(
            findings,
            case.scenario,
            FindingKind::InvalidFailure,
            "adapter returned the wrong normalized failure",
        );
    }
}

fn finding(
    findings: &mut Vec<ConformanceFinding>,
    scenario: ConformanceScenario,
    kind: FindingKind,
    detail: &str,
) {
    findings.push(ConformanceFinding {
        scenario,
        kind,
        detail: detail.into(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixtureAdapter {
        leak_secret: bool,
        read_only_writes: bool,
        invalid_cost: bool,
        omit_required_operations: bool,
        structured_events: bool,
        omit_cancelling_state: bool,
    }

    impl FixtureAdapter {
        fn valid() -> Self {
            Self {
                leak_secret: false,
                read_only_writes: false,
                invalid_cost: false,
                omit_required_operations: false,
                structured_events: true,
                omit_cancelling_state: false,
            }
        }
    }

    impl ConformanceAdapter for FixtureAdapter {
        fn adapter_kind(&self) -> &str {
            "fixture"
        }

        fn capabilities(&self) -> AdapterCapabilities {
            AdapterCapabilities {
                operations: if self.omit_required_operations {
                    Vec::new()
                } else {
                    vec![
                        AdapterOperation::Start,
                        AdapterOperation::Cancel,
                        AdapterOperation::Authenticate,
                        AdapterOperation::Attach,
                        AdapterOperation::ExportUsage,
                    ]
                },
                reasoning_controls: vec!["low".into()],
                supports_read_only: true,
                supports_write: true,
                supports_resume: true,
                supports_structured_events: self.structured_events,
            }
        }

        fn run_case(&mut self, case: &ConformanceCase) -> ConformanceObservation {
            let (states, failure) = match case.scenario {
                ConformanceScenario::Cancel => (
                    if self.omit_cancelling_state {
                        vec![NormalizedRunState::Running, NormalizedRunState::Cancelled]
                    } else {
                        vec![
                            NormalizedRunState::Running,
                            NormalizedRunState::Cancelling,
                            NormalizedRunState::Cancelled,
                        ]
                    },
                    Some(failure(AdapterFailureKind::Cancelled)),
                ),
                ConformanceScenario::Timeout => (
                    vec![NormalizedRunState::Running, NormalizedRunState::Failed],
                    Some(failure(AdapterFailureKind::Timeout)),
                ),
                ConformanceScenario::AuthenticationFailure => (
                    vec![NormalizedRunState::Starting, NormalizedRunState::Failed],
                    Some(failure(AdapterFailureKind::Authentication)),
                ),
                ConformanceScenario::QuotaFailure => (
                    vec![NormalizedRunState::Running, NormalizedRunState::Failed],
                    Some(failure(AdapterFailureKind::Quota)),
                ),
                ConformanceScenario::Crash => (
                    vec![NormalizedRunState::Running, NormalizedRunState::Uncertain],
                    Some(failure(AdapterFailureKind::Crashed)),
                ),
                ConformanceScenario::Resume => (
                    vec![
                        NormalizedRunState::Recovering,
                        NormalizedRunState::Running,
                        NormalizedRunState::Succeeded,
                    ],
                    None,
                ),
                _ => (
                    vec![NormalizedRunState::Running, NormalizedRunState::Succeeded],
                    None,
                ),
            };
            let final_state = *states.last().unwrap();
            let events = states
                .into_iter()
                .enumerate()
                .map(|(index, state)| NormalizedAdapterEvent {
                    contract_version: AGENT_ADAPTER_CONTRACT_VERSION,
                    run_id: case.run_id,
                    sequence: index as u64 + 1,
                    state,
                    event_type: format!("fixture.{state:?}").to_lowercase(),
                    occurred_at: format!("2026-09-03T00:00:0{index}Z"),
                    evidence_class: EvidenceClass::LocallyMeasured,
                    provider_event_id: None,
                    redacted_payload_json: if self.leak_secret {
                        format!("{{\"output\":\"{SECRET_SENTINEL}\"}}")
                    } else {
                        "{\"output\":\"[redacted]\"}".into()
                    },
                })
                .collect();
            let write_receipts = match case.scenario {
                ConformanceScenario::Write => vec!["fixture-write-receipt".into()],
                ConformanceScenario::ReadOnly if self.read_only_writes => {
                    vec!["unexpected-write".into()]
                }
                _ => Vec::new(),
            };
            let cost =
                (case.scenario == ConformanceScenario::CostEvidence).then_some(CostEvidence {
                    evidence_class: EvidenceClass::ProviderReported,
                    currency: Some("USD".into()),
                    amount: Some(0.01),
                    input_tokens: Some(10),
                    output_tokens: Some(5),
                    confidence: if self.invalid_cost { 1.5 } else { 1.0 },
                });
            ConformanceObservation {
                scenario: case.scenario,
                events,
                final_state,
                failure,
                write_receipts,
                cost,
            }
        }
    }

    fn failure(kind: AdapterFailureKind) -> AdapterFailure {
        AdapterFailure {
            kind,
            message_key: "fixture.failure".into(),
            retryable: false,
            provider_code: None,
            redacted_detail: Some("fixture detail".into()),
        }
    }

    #[test]
    fn valid_fixture_passes_all_required_scenarios() {
        let report = run_required_suite(&mut FixtureAdapter::valid());
        assert!(report.passed);
        assert_eq!(report.results.len(), REQUIRED_SCENARIOS.len());
        assert!(report.results.iter().all(|result| result.passed));
    }

    #[test]
    fn required_cases_are_stable_and_unique() {
        let first = required_cases();
        let second = required_cases();
        assert_eq!(first, second);
        assert_eq!(first.len(), REQUIRED_SCENARIOS.len());
        assert_eq!(
            first
                .iter()
                .map(|case| case.run_id)
                .collect::<HashSet<_>>()
                .len(),
            REQUIRED_SCENARIOS.len()
        );
    }

    #[test]
    fn reports_are_byte_for_byte_replayable() {
        let first = run_required_suite(&mut FixtureAdapter::valid());
        let second = run_required_suite(&mut FixtureAdapter::valid());
        assert_eq!(
            serde_json::to_vec(&first).unwrap(),
            serde_json::to_vec(&second).unwrap()
        );
    }

    #[test]
    fn secret_leak_fails_closed() {
        let mut adapter = FixtureAdapter::valid();
        adapter.leak_secret = true;
        let report = run_required_suite(&mut adapter);
        assert!(!report.passed);
        assert!(report.results.iter().all(|result| {
            result
                .findings
                .iter()
                .any(|finding| finding.kind == FindingKind::SecretLeak)
        }));
    }

    #[test]
    fn read_only_write_receipt_is_rejected() {
        let mut adapter = FixtureAdapter::valid();
        adapter.read_only_writes = true;
        let report = run_required_suite(&mut adapter);
        let result = report
            .results
            .iter()
            .find(|result| result.scenario == ConformanceScenario::ReadOnly)
            .unwrap();
        assert!(!result.passed);
        assert_eq!(result.findings[0].kind, FindingKind::UnexpectedWrite);
    }

    #[test]
    fn invalid_cost_confidence_is_rejected() {
        let mut adapter = FixtureAdapter::valid();
        adapter.invalid_cost = true;
        let report = run_required_suite(&mut adapter);
        let result = report
            .results
            .iter()
            .find(|result| result.scenario == ConformanceScenario::CostEvidence)
            .unwrap();
        assert!(!result.passed);
        assert_eq!(result.findings[0].kind, FindingKind::InvalidCostEvidence);
    }

    #[test]
    fn missing_declared_operations_are_rejected() {
        let mut adapter = FixtureAdapter::valid();
        adapter.omit_required_operations = true;
        let report = run_required_suite(&mut adapter);
        assert!(!report.passed);
        assert!(report.results.iter().all(|result| {
            result
                .findings
                .iter()
                .any(|finding| finding.kind == FindingKind::MissingCapability)
        }));
    }

    #[test]
    fn unstructured_adapter_is_rejected_for_every_scenario() {
        let mut adapter = FixtureAdapter::valid();
        adapter.structured_events = false;
        let report = run_required_suite(&mut adapter);
        assert!(!report.passed);
        assert!(report.results.iter().all(|result| {
            result
                .findings
                .iter()
                .any(|finding| finding.kind == FindingKind::MissingCapability)
        }));
    }

    #[test]
    fn cancel_must_expose_cancelling_transition() {
        let mut adapter = FixtureAdapter::valid();
        adapter.omit_cancelling_state = true;
        let report = run_required_suite(&mut adapter);
        let result = report
            .results
            .iter()
            .find(|result| result.scenario == ConformanceScenario::Cancel)
            .unwrap();
        assert!(!result.passed);
        assert!(result.findings.iter().any(|finding| {
            finding.kind == FindingKind::InvalidFinalState
                && finding.detail == "cancel scenario never entered cancelling"
        }));
    }
}
