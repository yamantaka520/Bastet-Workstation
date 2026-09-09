use std::{net::TcpListener, process::Command};

#[test]
fn occupied_listener_fails_before_creating_or_recovering_a_database() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("must-not-open.db");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bastet-daemon"))
        .env("BASTET_DATABASE", &database)
        .env("BASTET_LISTEN", listener.local_addr().unwrap().to_string())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!database.exists());
}

#[test]
fn non_loopback_override_fails_before_creating_a_database() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("must-not-open.db");
    let output = Command::new(env!("CARGO_BIN_EXE_bastet-daemon"))
        .env("BASTET_DATABASE", &database)
        .env("BASTET_LISTEN", "0.0.0.0:0")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("must be loopback"));
    assert!(!database.exists());
}

#[test]
fn occupied_listener_does_not_advance_an_existing_store() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("existing.db");
    let store = bastet_daemon::Store::open(&database).unwrap();
    store.mark_ready().unwrap();
    let before = store.snapshot().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bastet-daemon"))
        .env("BASTET_DATABASE", &database)
        .env("BASTET_LISTEN", listener.local_addr().unwrap().to_string())
        .output()
        .unwrap();
    assert!(!output.status.success());
    let after = store.snapshot().unwrap();
    assert_eq!(before.revision, after.revision);
    assert_eq!(before.lifecycle, after.lifecycle);
}
