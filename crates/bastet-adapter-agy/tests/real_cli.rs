use std::{path::PathBuf, time::Duration};

use bastet_adapter_agy::{AgyAdapter, AgyProcess, AgyRunRequest, AgyRunUpdate};
use bastet_core::{NormalizedRunState, RunId};

#[test]
#[ignore = "requires an explicitly supplied installed Agy CLI and reads its model catalog"]
fn installed_agy_reports_version_and_models() {
    let executable = PathBuf::from(
        std::env::var_os("BASTET_AGY_BINARY").expect("BASTET_AGY_BINARY must be set"),
    );
    let adapter = AgyAdapter::new(executable);

    let version = adapter.version().unwrap();
    assert!(!version.version.is_empty());
    let models = adapter.list_models().unwrap();
    assert!(!models.is_empty());
    assert!(models
        .iter()
        .all(|model| !model.id.is_empty() && !model.display_name.is_empty()));
}

#[test]
#[ignore = "requires explicit Agy CLI and isolated empty root; starts one real read-only turn"]
fn installed_agy_completes_a_secure_stdin_read_only_turn() {
    let executable = PathBuf::from(
        std::env::var_os("BASTET_AGY_BINARY").expect("BASTET_AGY_BINARY must be set"),
    );
    let probe_root = PathBuf::from(
        std::env::var_os("BASTET_AGY_PROBE_ROOT").expect("BASTET_AGY_PROBE_ROOT must be set"),
    );
    assert!(probe_root.is_absolute());
    assert_eq!(std::fs::read_dir(&probe_root).unwrap().count(), 0);
    let adapter = AgyAdapter::new(&executable);
    let model = adapter.list_models().unwrap().remove(0).id;
    let mut process = AgyProcess::spawn(
        executable,
        AgyRunRequest {
            run_id: RunId::from_bytes([31; 16]),
            model,
            effort: None,
            prompt: "Reply with exactly BASTET_AGY_STDIN_OK. Do not call tools.".into(),
            cwd: probe_root.clone(),
            read_only: true,
            timeout: Duration::from_secs(60),
            conversation_id: None,
        },
    )
    .unwrap();
    let mut saw_running = false;
    let mut saw_cost = false;
    loop {
        match process.next_update("2026-09-06T00:00:00Z").unwrap() {
            AgyRunUpdate::Cost(cost) => {
                assert!(cost.input_tokens.is_some());
                assert!(cost.output_tokens.is_some());
                assert_eq!((cost.currency, cost.amount), (None, None));
                saw_cost = true;
            }
            AgyRunUpdate::WriteReceipt(_) => panic!("read-only run reported a write"),
            AgyRunUpdate::Lifecycle { event, failure } => match event.state {
                NormalizedRunState::Running => saw_running = true,
                NormalizedRunState::Succeeded => {
                    assert!(failure.is_none());
                    break;
                }
                state => panic!("unexpected terminal state: {state:?}"),
            },
        }
    }
    assert!(saw_running);
    assert!(saw_cost);
    assert!(process.conversation_id().is_some());
    assert!(!process.final_output_overflowed());
    assert!(process
        .final_output()
        .expect("successful turn must retain agent_response text")
        .contains("BASTET_AGY_STDIN_OK"));
    assert_eq!(std::fs::read_dir(&probe_root).unwrap().count(), 0);
}

#[test]
#[ignore = "requires explicit Agy CLI and isolated empty root; starts and cancels one real turn"]
fn installed_agy_cancels_a_secure_stdin_turn() {
    let executable = PathBuf::from(
        std::env::var_os("BASTET_AGY_BINARY").expect("BASTET_AGY_BINARY must be set"),
    );
    let probe_root = PathBuf::from(
        std::env::var_os("BASTET_AGY_PROBE_ROOT").expect("BASTET_AGY_PROBE_ROOT must be set"),
    );
    assert_eq!(std::fs::read_dir(&probe_root).unwrap().count(), 0);
    let model = AgyAdapter::new(&executable)
        .list_models()
        .unwrap()
        .remove(0)
        .id;
    let mut process = AgyProcess::spawn(
        executable,
        AgyRunRequest {
            run_id: RunId::from_bytes([32; 16]),
            model,
            effort: None,
            prompt: "Run /bin/sleep 30, then reply too late.".into(),
            cwd: probe_root.clone(),
            read_only: true,
            timeout: Duration::from_secs(60),
            conversation_id: None,
        },
    )
    .unwrap();
    loop {
        if let AgyRunUpdate::Lifecycle { event, .. } =
            process.next_update("2026-09-06T00:00:00Z").unwrap()
        {
            assert_eq!(event.state, NormalizedRunState::Running);
            break;
        }
    }
    let AgyRunUpdate::Lifecycle { event, .. } = process
        .request_cancellation("2026-09-06T00:00:01Z")
        .unwrap()
    else {
        panic!("cancel request must emit lifecycle")
    };
    assert_eq!(event.state, NormalizedRunState::Cancelling);
    let AgyRunUpdate::Lifecycle { event, failure } =
        process.next_update("2026-09-06T00:00:02Z").unwrap()
    else {
        panic!("cancel terminal must emit lifecycle")
    };
    assert_eq!(event.state, NormalizedRunState::Cancelled);
    assert_eq!(
        failure.unwrap().kind,
        bastet_core::AdapterFailureKind::Cancelled
    );
    assert_eq!(std::fs::read_dir(&probe_root).unwrap().count(), 0);
}

#[test]
#[ignore = "requires explicit Agy CLI and isolated empty root; resumes one real conversation"]
fn installed_agy_resumes_a_secure_stdin_conversation() {
    let executable = PathBuf::from(
        std::env::var_os("BASTET_AGY_BINARY").expect("BASTET_AGY_BINARY must be set"),
    );
    let probe_root = PathBuf::from(
        std::env::var_os("BASTET_AGY_PROBE_ROOT").expect("BASTET_AGY_PROBE_ROOT must be set"),
    );
    assert_eq!(std::fs::read_dir(&probe_root).unwrap().count(), 0);
    let model = AgyAdapter::new(&executable)
        .list_models()
        .unwrap()
        .remove(0)
        .id;
    let mut first = AgyProcess::spawn(
        executable.clone(),
        AgyRunRequest {
            run_id: RunId::from_bytes([33; 16]),
            model: model.clone(),
            effort: None,
            prompt: "Remember the word BASTET_RESUME_WORD and reply READY.".into(),
            cwd: probe_root.clone(),
            read_only: true,
            timeout: Duration::from_secs(60),
            conversation_id: None,
        },
    )
    .unwrap();
    loop {
        if let AgyRunUpdate::Lifecycle { event, .. } =
            first.next_update("2026-09-06T00:00:00Z").unwrap()
        {
            if event.state == NormalizedRunState::Succeeded {
                break;
            }
        }
    }
    let conversation_id = first.conversation_id().unwrap().to_owned();
    drop(first);

    let mut resumed = AgyProcess::spawn(
        executable,
        AgyRunRequest {
            run_id: RunId::from_bytes([34; 16]),
            model,
            effort: None,
            prompt: "Reply with the exact word I asked you to remember.".into(),
            cwd: probe_root.clone(),
            read_only: true,
            timeout: Duration::from_secs(60),
            conversation_id: Some(conversation_id.clone()),
        },
    )
    .unwrap();
    let AgyRunUpdate::Lifecycle { event, .. } =
        resumed.next_update("2026-09-06T00:00:01Z").unwrap()
    else {
        panic!("resume must begin with lifecycle")
    };
    assert_eq!(event.state, NormalizedRunState::Recovering);
    loop {
        if let AgyRunUpdate::Lifecycle { event, .. } =
            resumed.next_update("2026-09-06T00:00:02Z").unwrap()
        {
            match event.state {
                NormalizedRunState::Running => {}
                NormalizedRunState::Succeeded => break,
                state => panic!("unexpected resume state: {state:?}"),
            }
        }
    }
    assert_eq!(resumed.conversation_id(), Some(conversation_id.as_str()));
    assert_eq!(std::fs::read_dir(&probe_root).unwrap().count(), 0);
}

#[test]
#[ignore = "requires explicit Agy CLI and isolated empty root; writes one bounded fixture file"]
fn installed_agy_reports_a_bounded_workspace_write() {
    let executable = PathBuf::from(
        std::env::var_os("BASTET_AGY_BINARY").expect("BASTET_AGY_BINARY must be set"),
    );
    let probe_root = PathBuf::from(
        std::env::var_os("BASTET_AGY_PROBE_ROOT").expect("BASTET_AGY_PROBE_ROOT must be set"),
    );
    assert_eq!(std::fs::read_dir(&probe_root).unwrap().count(), 0);
    let model = AgyAdapter::new(&executable)
        .list_models()
        .unwrap()
        .remove(0)
        .id;
    let mut process = AgyProcess::spawn(
        executable,
        AgyRunRequest {
            run_id: RunId::from_bytes([35; 16]),
            model,
            effort: None,
            prompt: concat!(
                "Create exactly one file named receipt.txt in the current directory. ",
                "Its content must be exactly BASTET_AGY_WRITE_OK followed by one newline. ",
                "Do not create any other file and do not use the network."
            )
            .into(),
            cwd: probe_root.clone(),
            read_only: false,
            timeout: Duration::from_secs(90),
            conversation_id: None,
        },
    )
    .unwrap();
    let mut saw_receipt = false;
    loop {
        match process.next_update("2026-09-06T00:00:00Z").unwrap() {
            AgyRunUpdate::WriteReceipt(receipt) => {
                assert!(receipt.contains("locally_measured"));
                assert!(!receipt.contains("receipt.txt"));
                assert!(!receipt.contains("BASTET_AGY_WRITE_OK"));
                saw_receipt = true;
            }
            AgyRunUpdate::Lifecycle { event, .. } => match event.state {
                NormalizedRunState::Running => {}
                NormalizedRunState::Succeeded => break,
                state => panic!("unexpected write state: {state:?}"),
            },
            AgyRunUpdate::Cost(_) => {}
        }
    }
    assert!(saw_receipt);
    assert_eq!(
        std::fs::read_to_string(probe_root.join("receipt.txt")).unwrap(),
        "BASTET_AGY_WRITE_OK\n"
    );
    std::fs::remove_file(probe_root.join("receipt.txt")).unwrap();
    assert_eq!(std::fs::read_dir(&probe_root).unwrap().count(), 0);
}

#[test]
#[ignore = "requires explicit Agy CLI and isolated empty root; forces one bounded timeout"]
fn installed_agy_reports_a_bounded_timeout() {
    let executable = PathBuf::from(
        std::env::var_os("BASTET_AGY_BINARY").expect("BASTET_AGY_BINARY must be set"),
    );
    let probe_root = PathBuf::from(
        std::env::var_os("BASTET_AGY_PROBE_ROOT").expect("BASTET_AGY_PROBE_ROOT must be set"),
    );
    assert_eq!(std::fs::read_dir(&probe_root).unwrap().count(), 0);
    let model = AgyAdapter::new(&executable)
        .list_models()
        .unwrap()
        .remove(0)
        .id;
    let mut process = AgyProcess::spawn(
        executable,
        AgyRunRequest {
            run_id: RunId::from_bytes([36; 16]),
            model,
            effort: None,
            prompt: "Run /bin/sleep 30 before replying.".into(),
            cwd: probe_root.clone(),
            read_only: true,
            timeout: Duration::from_secs(2),
            conversation_id: None,
        },
    )
    .unwrap();
    loop {
        match process.next_update("2026-09-06T00:00:00Z").unwrap() {
            AgyRunUpdate::Lifecycle { event, failure }
                if event.state == NormalizedRunState::Failed =>
            {
                assert_eq!(
                    failure.unwrap().kind,
                    bastet_core::AdapterFailureKind::Timeout
                );
                break;
            }
            AgyRunUpdate::Lifecycle { event, .. } => {
                assert_eq!(event.state, NormalizedRunState::Running)
            }
            AgyRunUpdate::Cost(_) => {}
            AgyRunUpdate::WriteReceipt(_) => panic!("read-only timeout reported a write"),
        }
    }
    assert_eq!(std::fs::read_dir(&probe_root).unwrap().count(), 0);
}
