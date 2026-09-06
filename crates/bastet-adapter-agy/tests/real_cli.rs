use std::path::PathBuf;

use bastet_adapter_agy::AgyAdapter;

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
