//! Explicit native tests: no installed provider or credential is used.
use crate::sandbox::{SandboxPlatform, SandboxProfile};
use bastet_core::{configure_adapter_process_environment, OwnedAdapterChild, ProcessOutputReader};
use std::{
    io::{BufRead, BufReader, Read},
    net::TcpListener,
    path::{Path, PathBuf},
    process::Stdio,
    sync::mpsc,
    thread,
    time::Duration,
};

// Deliberate runtime guard: fixtures also compile on macOS for linting.
#[allow(clippy::assertions_on_constants)]
fn profile(workspace: &Path) -> SandboxProfile {
    assert!(
        cfg!(target_os = "linux"),
        "native Bubblewrap evidence requires Linux"
    );
    SandboxProfile {
        workspace_root: workspace.to_path_buf(),
        read_only_roots: ["/usr", "/bin", "/lib", "/lib64"]
            .into_iter()
            .map(PathBuf::from)
            .filter(|path| path.exists())
            .collect(),
        allow_workspace_write: false,
        allow_network: false,
    }
}

fn probe(profile: &SandboxProfile, executable: &str, arguments: &[&str]) -> String {
    let arguments = arguments
        .iter()
        .map(|arg| arg.to_string())
        .collect::<Vec<_>>();
    let mut command = profile
        .launch_command(
            SandboxPlatform::LinuxBubblewrap,
            Path::new(executable),
            &arguments,
        )
        .expect("native enforcer must be installed");
    configure_adapter_process_environment(&mut command);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = OwnedAdapterChild::spawn(&mut command).expect("native enforcer must start");
    let (sender, receiver) = mpsc::channel();
    let mut reader = ProcessOutputReader::spawn(child.take_stdout().unwrap(), move |line| {
        sender.send(line).is_ok()
    })
    .unwrap();
    let result = receiver.recv_timeout(Duration::from_secs(5));
    child.shutdown(Duration::from_millis(100)).unwrap();
    reader.close();
    result
        .expect("fixture must bootstrap and report a result")
        .expect("fixture stdout must be valid")
}

#[test]
#[ignore = "requires native Linux Bubblewrap; mandatory explicit Linux CI gate"]
fn filesystem_controls_have_positive_and_negative_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let inside = root.join("inside");
    let outside_file = outside.path().canonicalize().unwrap().join("outside");
    let escape = root.join("escape");
    let written = root.join("written");
    std::fs::write(&inside, b"fixture-inside").unwrap();
    std::fs::write(&outside_file, b"fixture-outside").unwrap();
    std::os::unix::fs::symlink(&outside_file, &escape).unwrap();
    let script = r#"
inside=false; outside=false; escape=false; write=false; outside_write=false
value=$(/bin/cat "$1") && [ "$value" = fixture-inside ] && inside=true
value=$(/bin/cat "$2") && outside=true
value=$(/bin/cat "$3") && escape=true
(printf changed > "$4") && write=true
(printf forbidden > "$2") && outside_write=true
printf '{"inside":%s,"outside":%s,"escape":%s,"write":%s,"outside_write":%s}\n' "$inside" "$outside" "$escape" "$write" "$outside_write"
"#;
    let args = [
        "-c",
        script,
        "fixture",
        inside.to_str().unwrap(),
        outside_file.to_str().unwrap(),
        escape.to_str().unwrap(),
        written.to_str().unwrap(),
    ];
    let mut policy = profile(&root);
    let observed: serde_json::Value =
        serde_json::from_str(&probe(&policy, "/bin/sh", &args)).unwrap();
    assert_eq!(
        observed,
        serde_json::json!({"inside":true,"outside":false,"escape":false,"write":false,"outside_write":false})
    );
    assert!(!written.exists());
    policy.allow_workspace_write = true;
    policy.read_only_roots.push(outside_file.clone());
    let observed: serde_json::Value =
        serde_json::from_str(&probe(&policy, "/bin/sh", &args)).unwrap();
    assert_eq!(
        observed,
        serde_json::json!({"inside":true,"outside":true,"escape":true,"write":true,"outside_write":false})
    );
    assert_eq!(std::fs::read(&written).unwrap(), b"changed");
    assert_eq!(std::fs::read(&outside_file).unwrap(), b"fixture-outside");
}

#[test]
#[ignore = "requires native Linux Bubblewrap; mandatory explicit Linux CI gate"]
fn network_namespace_blocks_host_loopback_with_a_positive_control() {
    let directory = tempfile::tempdir().unwrap();
    let mut policy = profile(&directory.path().canonicalize().unwrap());
    for allow in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port().to_string();
        policy.allow_network = allow;
        let script="import socket,sys\ns=socket.socket(); s.settimeout(1)\ntry:\n s.connect(('127.0.0.1',int(sys.argv[1]))); print('connected')\nexcept OSError:\n print('denied')\nfinally:\n s.close()";
        let result = probe(
            &policy,
            "/usr/bin/python3",
            &["-I", "-S", "-c", script, &port],
        );
        assert_eq!(result, if allow { "connected" } else { "denied" });
        if allow {
            assert!(listener.accept().is_ok());
        } else {
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
    }
}

#[test]
#[ignore = "requires native Linux Bubblewrap; mandatory explicit Linux CI gate"]
fn outer_wrapper_shutdown_closes_descendant_held_stdout() {
    let directory = tempfile::tempdir().unwrap();
    let policy = profile(&directory.path().canonicalize().unwrap());
    let args = vec![
        "-c".into(),
        "/bin/sleep 30 <&0 & printf 'ready\\n'; wait".into(),
    ];
    let mut command = policy
        .launch_command(
            SandboxPlatform::LinuxBubblewrap,
            Path::new("/bin/sh"),
            &args,
        )
        .unwrap();
    configure_adapter_process_environment(&mut command);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = OwnedAdapterChild::spawn(&mut command).unwrap();
    let stdout = child.take_stdout().unwrap();
    let (sender, receiver) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut stdout = BufReader::new(stdout);
        let mut ready = String::new();
        stdout.read_line(&mut ready).unwrap();
        sender.send(ready).unwrap();
        let mut remaining = Vec::new();
        stdout.read_to_end(&mut remaining).unwrap();
        sender.send("EOF".into()).unwrap();
    });
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(5)).unwrap(),
        "ready\n"
    );
    child.shutdown(Duration::ZERO).unwrap();
    // Raw reader, no cancellation: EOF must result from native wrapper/tree
    // cleanup, not merely closing our cancellable reader. Sleep lasts 30 sec.
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(5)).unwrap(),
        "EOF"
    );
    reader.join().unwrap();
}
