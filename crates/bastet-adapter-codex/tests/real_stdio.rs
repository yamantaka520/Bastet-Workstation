use std::{path::PathBuf, time::Duration};

use bastet_adapter_codex::{CodexAppServer, StdioTransport};

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
