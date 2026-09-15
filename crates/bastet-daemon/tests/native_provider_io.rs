//! A harness-free executable doubles as the synthetic provider, so Windows
//! exercises the actual adapter pipes without sh, installed CLIs, or credentials.
use std::{
    io::{BufRead, Write},
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant},
};

use bastet_adapter_agy::{AgyProcess, AgyProcessError, AgyRunRequest, AgyRunUpdate};
use bastet_adapter_codex::{
    AppServerError, AppServerTransport, CodexAppServer, StdioTransport, TransportError,
};
use bastet_core::{AdapterProcessLauncher, CancellationToken, NormalizedRunState, RunId};
use serde_json::{json, Value};

const CHILD_FLAG: &str = "--bastet-native-provider-fixture";

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    if args.get(1).is_some_and(|arg| arg == CHILD_FLAG) {
        fixture(args[2].to_str().expect("ASCII fixture mode"));
        return;
    }
    codex_blocked_write();
    codex_handshake_cancel();
    agy_blocked_startup();
    positive_controls();
    #[cfg(windows)]
    windows_argument_roundtrip();
    println!("native provider I/O: all four adapter controls passed");
}

fn fixture(mode: &str) {
    #[cfg(windows)]
    if mode == "echo-args" {
        use std::os::windows::ffi::OsStrExt;
        let arguments: Vec<Vec<u16>> = std::env::args_os()
            .skip(3)
            .map(|arg| arg.encode_wide().collect())
            .collect();
        std::fs::write("arguments.json", serde_json::to_vec(&arguments).unwrap()).unwrap();
        // Marker after closing the file, never merely after process startup.
        std::fs::write("arguments-ready", b"ready").unwrap();
        return;
    }
    if mode == "never-read" {
        std::fs::write("ready", b"ready").unwrap();
        thread::sleep(Duration::from_secs(30));
        return;
    }
    let mut lines = std::io::stdin().lock().lines();
    let line = lines.next().expect("input frame").unwrap();
    let request: Value = serde_json::from_str(&line).unwrap();
    std::fs::write("received", b"received").unwrap();
    match mode {
        "never-reply" => thread::sleep(Duration::from_secs(30)),
        "codex-reply" => {
            // Positive proof that the request arrived intact, not merely that
            // the process was spawned or stopped before writing anything.
            assert_eq!(
                request["params"]["text"].as_str().unwrap().len(),
                512 * 1024
            );
            println!("{}", json!({"id":request["id"], "result":{"native":true}}));
            std::io::stdout().flush().unwrap();
        }
        "agy-reply" => {
            assert_eq!(
                request["message"]["content"].as_str().unwrap().len(),
                512 * 1024
            );
            println!(
                "{}",
                json!({"event":"init", "conversation_id":"native", "init":{}})
            );
            println!(
                "{}",
                json!({"event":"result", "result":{"conversation_id":"native", "status":"SUCCESS"}})
            );
            std::io::stdout().flush().unwrap();
        }
        _ => panic!("unknown native fixture mode"),
    }
}

#[cfg(windows)]
fn windows_argument_roundtrip() {
    use std::os::windows::{
        ffi::{OsStrExt, OsStringExt},
        process::CommandExt,
    };
    let root = tempfile::tempdir().unwrap();
    let executable = std::env::current_exe().unwrap();
    let executable_wide: Vec<_> = executable.as_os_str().encode_wide().collect();
    let wide = |value: &str| value.encode_utf16().collect::<Vec<_>>();
    let expected = vec![
        wide(""),
        wide("a b"),
        wide("a\"b"),
        wide("ends\\"),
        wide("\\\""),
        vec![0xd83d, 0xde00, 0xd800],
    ];
    let mut arguments = vec![wide(CHILD_FLAG), wide("echo-args")];
    arguments.extend(expected.clone());
    let encoded =
        bastet_core::windows_launch_encoding::windows_command_line(&executable_wide, &arguments)
            .unwrap();
    let mut command = Command::new(&executable);
    // Command supplies argv[0]; pass our remaining encoded command line
    // verbatim so this test exercises Windows argument parsing, not Rust's encoder.
    command.raw_arg(std::ffi::OsString::from_wide(
        &encoded[executable_wide.len() + 3..encoded.len() - 1],
    ));
    command.current_dir(root.path());
    bastet_core::configure_adapter_process_environment(&mut command);
    let mut child = bastet_core::OwnedAdapterChild::spawn(&mut command).unwrap();
    wait_marker(&root.path().join("arguments-ready"));
    let observed: Vec<Vec<u16>> =
        serde_json::from_slice(&std::fs::read(root.path().join("arguments.json")).unwrap())
            .unwrap();
    assert_eq!(observed, expected);
    child.shutdown(Duration::ZERO).unwrap();
}

struct NativeLauncher {
    root: PathBuf,
    mode: &'static str,
}

impl AdapterProcessLauncher for NativeLauncher {
    fn command(&self, executable: &Path) -> std::io::Result<Command> {
        assert_eq!(executable, std::env::current_exe().unwrap());
        let mut command = Command::new(executable);
        command
            .arg(CHILD_FLAG)
            .arg(self.mode)
            .current_dir(&self.root);
        Ok(command)
    }
}

fn wait_marker(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.is_file() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert!(path.is_file(), "synthetic provider never reached readiness");
}

fn cancel_at(path: PathBuf, token: CancellationToken) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        wait_marker(&path);
        thread::sleep(Duration::from_millis(50));
        token.cancel();
    })
}

fn codex_blocked_write() {
    for cancel in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let token = CancellationToken::default();
        let mut transport = StdioTransport::spawn_with_launcher_and_cancel(
            &std::env::current_exe().unwrap(),
            if cancel {
                Duration::from_secs(10)
            } else {
                Duration::from_millis(200)
            },
            &NativeLauncher {
                root: root.path().into(),
                mode: "never-read",
            },
            token.clone(),
        )
        .unwrap();
        wait_marker(&root.path().join("ready"));
        let trigger = cancel.then(|| cancel_at(root.path().join("ready"), token));
        let start = Instant::now();
        let result = transport.request("fixture", json!({"text":"x".repeat(2 * 1024 * 1024)}));
        assert_eq!(
            result,
            Err(if cancel {
                TransportError::Cancelled
            } else {
                TransportError::TimedOut
            })
        );
        assert!(start.elapsed() < Duration::from_secs(3));
        if let Some(trigger) = trigger {
            trigger.join().unwrap();
        }
        transport.close_checked().unwrap();
    }
}

fn codex_handshake_cancel() {
    let root = tempfile::tempdir().unwrap();
    let token = CancellationToken::default();
    let transport = StdioTransport::spawn_with_launcher_and_cancel(
        &std::env::current_exe().unwrap(),
        Duration::from_secs(10),
        &NativeLauncher {
            root: root.path().into(),
            mode: "never-reply",
        },
        token.clone(),
    )
    .unwrap();
    let trigger = cancel_at(root.path().join("received"), token);
    let mut server = CodexAppServer::new(transport);
    let start = Instant::now();
    assert!(matches!(
        server.initialize(),
        Err(AppServerError::Transport(TransportError::Cancelled))
    ));
    assert!(start.elapsed() < Duration::from_secs(3));
    trigger.join().unwrap();
    server.into_transport().close_checked().unwrap();
}

fn agy_request(root: &Path, timeout: Duration) -> AgyRunRequest {
    AgyRunRequest {
        run_id: RunId::new(),
        model: "native-fixture".into(),
        effort: None,
        prompt: "x".repeat(512 * 1024),
        cwd: root.into(),
        read_only: true,
        timeout,
        conversation_id: None,
    }
}

fn agy_blocked_startup() {
    for cancel in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let token = CancellationToken::default();
        let trigger = cancel.then(|| cancel_at(root.path().join("ready"), token.clone()));
        let start = Instant::now();
        let result = AgyProcess::spawn_with_launcher_and_cancel(
            std::env::current_exe().unwrap(),
            agy_request(
                root.path(),
                if cancel {
                    Duration::from_secs(10)
                } else {
                    Duration::from_secs(1)
                },
            ),
            &NativeLauncher {
                root: root.path().into(),
                mode: "never-read",
            },
            &token,
        );
        if cancel {
            assert!(matches!(result, Err(AgyProcessError::Cancelled)));
        } else {
            assert!(matches!(result, Err(AgyProcessError::TimedOut)));
        }
        assert!(start.elapsed() < Duration::from_secs(3));
        assert!(root.path().join("ready").is_file());
        if let Some(trigger) = trigger {
            trigger.join().unwrap();
        }
    }
}

fn positive_controls() {
    let root = tempfile::tempdir().unwrap();
    let mut transport = StdioTransport::spawn_with_launcher(
        &std::env::current_exe().unwrap(),
        Duration::from_secs(5),
        &NativeLauncher {
            root: root.path().into(),
            mode: "codex-reply",
        },
    )
    .unwrap();
    assert_eq!(
        transport
            .request("fixture", json!({"text":"x".repeat(512 * 1024)}))
            .unwrap(),
        json!({"native":true})
    );
    transport.close_checked().unwrap();

    let mut process = AgyProcess::spawn_with_launcher(
        std::env::current_exe().unwrap(),
        agy_request(root.path(), Duration::from_secs(5)),
        &NativeLauncher {
            root: root.path().into(),
            mode: "agy-reply",
        },
    )
    .unwrap();
    let mut succeeded = false;
    for _ in 0..8 {
        if let AgyRunUpdate::Lifecycle { event, .. } =
            process.next_update("native-fixture").unwrap()
        {
            if event.state == NormalizedRunState::Succeeded {
                succeeded = true;
                break;
            }
        }
    }
    assert!(succeeded);
    process.cancel_and_close().unwrap();
}
