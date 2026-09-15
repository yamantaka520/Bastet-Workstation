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
    #[cfg(windows)]
    windows_job_controls();
    println!("native provider I/O: all four adapter controls passed");
}

// The Windows tree fixture deliberately leaves a live descendant for its Job
// owner to terminate after the leader exits; waiting here would defeat the test.
#[cfg_attr(windows, allow(clippy::zombie_processes))]
fn fixture(mode: &str) {
    #[cfg(windows)]
    if mode == "job-leaf" {
        std::fs::write("leaf-ready", b"ready").unwrap();
        thread::sleep(Duration::from_secs(30));
        return;
    }
    #[cfg(windows)]
    if mode == "job-tree" {
        let mut descendant = Command::new(std::env::current_exe().unwrap())
            .args([CHILD_FLAG, "job-leaf"])
            .spawn()
            .unwrap();
        wait_marker(Path::new("leaf-ready"));
        assert!(descendant.try_wait().unwrap().is_none());
        std::fs::write("tree-ready", b"ready").unwrap();
        wait_marker(Path::new("release-leader"));
        // Child deliberately outlives the leader. Job cleanup owns it.
        return;
    }
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
fn windows_job_controls() {
    use bastet_core::windows_job::WindowsJob;
    use std::os::windows::{
        ffi::OsStrExt,
        io::{AsRawHandle, FromRawHandle, OwnedHandle},
    };
    use windows_sys::Win32::{Foundation::WAIT_OBJECT_0, System::Threading::*};

    fn launch(job: &WindowsJob, root: &Path, mode: &str) -> OwnedHandle {
        let executable: Vec<u16> = std::env::current_exe()
            .unwrap()
            .as_os_str()
            .encode_wide()
            .collect();
        let mut app = executable.clone();
        app.push(0);
        let args = [CHILD_FLAG, mode].map(|arg| arg.encode_utf16().collect());
        let mut command =
            bastet_core::windows_launch_encoding::windows_command_line(&executable, &args).unwrap();
        let cwd: Vec<_> = root
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        let environment =
            bastet_core::windows_launch_encoding::windows_environment_block(&[]).unwrap();
        let attributes = job.attributes(&[]).unwrap();
        let mut startup = STARTUPINFOEXW::default();
        startup.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
        startup.lpAttributeList = attributes.as_ptr();
        let mut process = PROCESS_INFORMATION::default();
        // SAFETY: non-null explicit app/cwd/environment, writable command,
        // initialized extended startup and all attribute backing storage live.
        // No handles are inherited, and assignment is part of CreateProcessW.
        assert_ne!(
            unsafe {
                CreateProcessW(
                    app.as_ptr(),
                    command.as_mut_ptr(),
                    std::ptr::null(),
                    std::ptr::null(),
                    0,
                    EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT | CREATE_NO_WINDOW,
                    environment.as_ptr().cast(),
                    cwd.as_ptr(),
                    &startup.StartupInfo,
                    &mut process,
                )
            },
            0,
            "{}",
            std::io::Error::last_os_error()
        );
        // SAFETY: successful process creation transfers exactly these handles.
        let thread = unsafe { OwnedHandle::from_raw_handle(process.hThread) };
        drop(thread);
        unsafe { OwnedHandle::from_raw_handle(process.hProcess) }
    }

    let root = tempfile::tempdir().unwrap();
    let unrelated_root = tempfile::tempdir().unwrap();
    let mut unrelated_command = Command::new(std::env::current_exe().unwrap());
    unrelated_command
        .args([CHILD_FLAG, "job-leaf"])
        .current_dir(unrelated_root.path());
    let mut unrelated = unrelated_command.spawn().unwrap();
    wait_marker(&unrelated_root.path().join("leaf-ready"));
    let mut job = WindowsJob::new().unwrap();
    assert_eq!(job.active_processes().unwrap(), 0);
    let leader = launch(&job, root.path(), "job-tree");
    wait_marker(&root.path().join("tree-ready"));
    assert_eq!(job.active_processes().unwrap(), 2);
    std::fs::write(root.path().join("release-leader"), b"release").unwrap();
    // SAFETY: owned leader process handle; bounded wait, no PID-based signaling.
    assert_eq!(
        unsafe { WaitForSingleObject(leader.as_raw_handle(), 3000) },
        WAIT_OBJECT_0
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    while job.active_processes().unwrap() != 1 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(job.active_processes().unwrap(), 1);
    job.terminate_and_wait(Duration::from_secs(3)).unwrap();
    assert_eq!(job.active_processes().unwrap(), 0);
    job.terminate_and_wait(Duration::ZERO).unwrap();
    assert!(
        unrelated.try_wait().unwrap().is_none(),
        "other Job must not be signaled"
    );
    unrelated.kill().unwrap();
    unrelated.wait().unwrap();

    let drop_root = tempfile::tempdir().unwrap();
    let job = WindowsJob::new().unwrap();
    let child = launch(&job, drop_root.path(), "job-leaf");
    wait_marker(&drop_root.path().join("leaf-ready"));
    assert_eq!(job.active_processes().unwrap(), 1);
    drop(job);
    // SAFETY: owned process handle survives closing our last Job handle.
    assert_eq!(
        unsafe { WaitForSingleObject(child.as_raw_handle(), 3000) },
        WAIT_OBJECT_0
    );
    println!("native Windows Job: creation-time tree, unrelated child, and kill-on-close passed");
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
