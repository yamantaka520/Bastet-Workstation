use std::{path::PathBuf, time::Duration};

use bastet_adapter_codex::{
    ApprovalPolicy, CodexAppServer, CodexRunEvidence, CodexRunEvidenceUpdate, CodexRunStream,
    StdioTransport, ThreadSandbox, ThreadStartRequest, TurnSandboxPolicy, TurnStartRequest,
};
use bastet_core::{AdapterFailureKind, NormalizedRunState, RunId};

#[test]
#[ignore = "requires an explicitly supplied installed Codex CLI and may query its model catalog"]
fn installed_codex_lists_models_over_stdio() {
    let executable = PathBuf::from(
        std::env::var_os("BASTET_CODEX_BINARY").expect("BASTET_CODEX_BINARY must be set"),
    );
    let transport = StdioTransport::spawn(&executable, Duration::from_secs(15)).unwrap();
    let mut server = CodexAppServer::new(transport);
    server.initialize().unwrap();
    let page = server.list_models(None, 20).unwrap();
    assert!(!page.models.is_empty());
    assert!(page.models.iter().all(|model| !model.hidden));
}

#[test]
#[ignore = "requires explicit Codex CLI and isolated empty root; starts one real read-only turn"]
fn installed_codex_completes_a_read_only_turn_over_stdio() {
    let executable = PathBuf::from(
        std::env::var_os("BASTET_CODEX_BINARY").expect("BASTET_CODEX_BINARY must be set"),
    );
    let probe_root = PathBuf::from(
        std::env::var_os("BASTET_CODEX_PROBE_ROOT").expect("BASTET_CODEX_PROBE_ROOT must be set"),
    );
    assert!(probe_root.is_absolute());
    assert!(probe_root.is_dir());
    assert_eq!(std::fs::read_dir(&probe_root).unwrap().count(), 0);

    let transport = StdioTransport::spawn(&executable, Duration::from_secs(60)).unwrap();
    let mut server = CodexAppServer::new(transport);
    server.initialize().unwrap();
    let page = server.list_models(None, 20).unwrap();
    let model = page
        .models
        .iter()
        .find(|model| model.is_default)
        .or_else(|| page.models.first())
        .expect("Codex must report at least one visible model");
    let thread = server
        .start_thread(ThreadStartRequest {
            model: model.model.clone(),
            cwd: probe_root.clone(),
            approval_policy: ApprovalPolicy::Never,
            sandbox: ThreadSandbox::ReadOnly,
        })
        .unwrap();
    let turn = server
        .start_turn(TurnStartRequest {
            thread_id: thread.thread_id.clone(),
            prompt: "Reply with exactly BASTET_READ_ONLY_OK. Do not call tools.".into(),
            cwd: probe_root.clone(),
            approval_policy: ApprovalPolicy::Never,
            sandbox_policy: TurnSandboxPolicy::ReadOnly,
            model: Some(model.model.clone()),
            effort: model.default_reasoning_effort.clone(),
        })
        .unwrap();
    let mut stream = CodexRunStream::new(RunId::from_bytes([42; 16]), &turn.turn_id).unwrap();
    let evidence = CodexRunEvidence::new(&thread.thread_id, &turn.turn_id).unwrap();
    let mut saw_running = false;
    let mut saw_cost = false;

    loop {
        let notification = server.next_notification().unwrap();
        if let Some(update) = evidence.ingest(&notification).unwrap() {
            match update {
                CodexRunEvidenceUpdate::Cost(cost) => {
                    assert!(cost.input_tokens.is_some());
                    assert!(cost.output_tokens.is_some());
                    assert_eq!((cost.currency, cost.amount), (None, None));
                    saw_cost = true;
                }
                CodexRunEvidenceUpdate::WriteReceipt(_) => {
                    panic!("read-only probe unexpectedly reported a write")
                }
            }
        }
        let Some(event) = stream
            .ingest(&notification, "2026-09-03T00:00:00Z")
            .unwrap()
        else {
            continue;
        };
        match event.event.state {
            NormalizedRunState::Running => saw_running = true,
            NormalizedRunState::Succeeded => break,
            state => panic!("read-only probe ended in unexpected state: {state:?}"),
        }
    }

    assert!(saw_running);
    assert!(saw_cost);
    assert_eq!(std::fs::read_dir(&probe_root).unwrap().count(), 0);
}

#[test]
#[ignore = "requires explicit Codex CLI and isolated empty root; starts and interrupts one real read-only turn"]
fn installed_codex_interrupts_a_read_only_turn_over_stdio() {
    let executable = PathBuf::from(
        std::env::var_os("BASTET_CODEX_BINARY").expect("BASTET_CODEX_BINARY must be set"),
    );
    let probe_root = PathBuf::from(
        std::env::var_os("BASTET_CODEX_PROBE_ROOT").expect("BASTET_CODEX_PROBE_ROOT must be set"),
    );
    assert!(probe_root.is_absolute());
    assert!(probe_root.is_dir());
    assert_eq!(std::fs::read_dir(&probe_root).unwrap().count(), 0);

    let transport = StdioTransport::spawn(&executable, Duration::from_secs(60)).unwrap();
    let mut server = CodexAppServer::new(transport);
    server.initialize().unwrap();
    let page = server.list_models(None, 20).unwrap();
    let model = page
        .models
        .iter()
        .find(|model| model.is_default)
        .or_else(|| page.models.first())
        .expect("Codex must report at least one visible model");
    let thread = server
        .start_thread(ThreadStartRequest {
            model: model.model.clone(),
            cwd: probe_root.clone(),
            approval_policy: ApprovalPolicy::Never,
            sandbox: ThreadSandbox::ReadOnly,
        })
        .unwrap();
    let turn = server
        .start_turn(TurnStartRequest {
            thread_id: thread.thread_id.clone(),
            prompt: "Run /bin/sleep 30, then reply with BASTET_CANCEL_TOO_LATE.".into(),
            cwd: probe_root.clone(),
            approval_policy: ApprovalPolicy::Never,
            sandbox_policy: TurnSandboxPolicy::ReadOnly,
            model: Some(model.model.clone()),
            effort: model.default_reasoning_effort.clone(),
        })
        .unwrap();
    let mut stream = CodexRunStream::new(RunId::from_bytes([43; 16]), &turn.turn_id).unwrap();
    loop {
        let notification = server.next_notification().unwrap();
        let Some(event) = stream
            .ingest(&notification, "2026-09-03T00:00:00Z")
            .unwrap()
        else {
            continue;
        };
        assert_eq!(event.event.state, NormalizedRunState::Running);
        assert_eq!(event.event.sequence, 1);
        break;
    }
    let requested = stream
        .cancellation_requested("2026-09-03T00:00:01Z")
        .unwrap();
    assert_eq!(requested.event.state, NormalizedRunState::Cancelling);
    assert_eq!(requested.event.sequence, 2);
    server
        .interrupt_turn(&thread.thread_id, &turn.turn_id)
        .unwrap();

    loop {
        let notification = server.next_notification().unwrap();
        let Some(event) = stream
            .ingest(&notification, "2026-09-03T00:00:02Z")
            .unwrap()
        else {
            continue;
        };
        assert_eq!(event.event.state, NormalizedRunState::Cancelled);
        assert_eq!(event.event.sequence, 3);
        assert_eq!(
            event
                .failure
                .expect("cancelled event has failure kind")
                .kind,
            AdapterFailureKind::Cancelled
        );
        break;
    }

    assert_eq!(std::fs::read_dir(&probe_root).unwrap().count(), 0);
}
