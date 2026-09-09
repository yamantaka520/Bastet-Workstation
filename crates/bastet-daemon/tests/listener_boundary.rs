use std::process::Command;

#[tokio::test]
async fn occupied_listener_fails_before_creating_or_recovering_a_database() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("must-not-open.db");
    let endpoint = bastet_local_ipc::Endpoint::for_database(&database).unwrap();
    let (_listener, _guard) = bastet_local_ipc::bind(&endpoint).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bastet-daemon"))
        .env("BASTET_DATABASE", &database)
        .env_remove("BASTET_LISTEN")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!database.exists());
}

#[test]
fn legacy_tcp_override_fails_before_creating_a_database() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("must-not-open.db");
    let output = Command::new(env!("CARGO_BIN_EXE_bastet-daemon"))
        .env("BASTET_DATABASE", &database)
        .env("BASTET_LISTEN", "0.0.0.0:0")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires local IPC"));
    assert!(!database.exists());
}

#[tokio::test]
async fn occupied_listener_does_not_advance_an_existing_store() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("existing.db");
    let store = bastet_daemon::Store::open(&database).unwrap();
    store.mark_ready().unwrap();
    let before = store.snapshot().unwrap();
    let endpoint = bastet_local_ipc::Endpoint::for_database(&database).unwrap();
    let (_listener, _guard) = bastet_local_ipc::bind(&endpoint).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bastet-daemon"))
        .env("BASTET_DATABASE", &database)
        .env_remove("BASTET_LISTEN")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let after = store.snapshot().unwrap();
    assert_eq!(before.revision, after.revision);
    assert_eq!(before.lifecycle, after.lifecycle);
}
