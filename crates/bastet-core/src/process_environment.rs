//! Minimal parent-environment inheritance for external adapter processes.
//!
//! `HOME` and the Windows profile locations are intentionally retained so a
//! provider CLI can use its existing local login/configuration. This is not a
//! credential selection mechanism: callers must bind launch identity and any
//! credential grant separately. Provider-specific override variables (for
//! example `CODEX_HOME`, API endpoints, or API keys) are never inherited.

use std::{
    ffi::{OsStr, OsString},
    process::Command,
};

const ALLOWED_ENVIRONMENT_KEYS: &[&str] = &[
    // Executable and operating-system basics.
    "PATH",
    "PATHEXT",
    "COMSPEC",
    "SYSTEMROOT",
    "WINDIR",
    // Existing user profile/configuration locations. No provider-specific
    // configuration-root override is permitted.
    "HOME",
    "USERPROFILE",
    "HOMEDRIVE",
    "HOMEPATH",
    "APPDATA",
    "LOCALAPPDATA",
    // Temporary files and locale are non-secret process requirements.
    "TMPDIR",
    "TMP",
    "TEMP",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
];

/// Clears a child process environment and restores only the fixed safe list.
pub fn configure_adapter_process_environment(command: &mut Command) {
    configure_adapter_process_environment_from(command, std::env::vars_os());
}

/// Same policy as [`configure_adapter_process_environment`] with an explicit
/// inherited environment, primarily for deterministic tests.
pub fn configure_adapter_process_environment_from<I>(command: &mut Command, inherited: I)
where
    I: IntoIterator<Item = (OsString, OsString)>,
{
    command.env_clear();
    for (key, value) in inherited {
        if allowed_key(&key) {
            command.env(key, value);
        }
    }
}

fn allowed_key(key: &OsStr) -> bool {
    #[cfg(windows)]
    {
        let key = key.to_string_lossy();
        ALLOWED_ENVIRONMENT_KEYS
            .iter()
            .any(|candidate| key.eq_ignore_ascii_case(candidate))
    }
    #[cfg(not(windows))]
    {
        ALLOWED_ENVIRONMENT_KEYS
            .iter()
            .any(|candidate| key == OsStr::new(candidate))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, env, ffi::OsString, process::Command};

    use super::*;

    #[test]
    fn clears_parent_and_restores_only_the_fixed_allowlist() {
        let inherited = [
            ("PATH", "/safe/bin"),
            ("HOME", "/safe/home"),
            ("LANG", "en_US.UTF-8"),
            ("OPENAI_API_KEY", "secret"),
            ("OPENAI_BASE_URL", "https://override.invalid"),
            ("AGY_API_KEY", "secret"),
            ("GOOGLE_API_KEY", "secret"),
            ("GEMINI_API_KEY", "secret"),
            ("AWS_SECRET_ACCESS_KEY", "secret"),
            ("GH_TOKEN", "secret"),
            ("HTTP_PROXY", "http://proxy.invalid"),
            ("HTTPS_PROXY", "http://proxy.invalid"),
            ("NODE_OPTIONS", "--require injected"),
            ("RUST_LOG", "trace"),
            ("LD_PRELOAD", "injected.so"),
            ("DYLD_INSERT_LIBRARIES", "injected.dylib"),
            ("CODEX_HOME", "/provider-override"),
            ("AGY_HOME", "/provider-override"),
        ]
        .into_iter()
        .map(|(key, value)| (OsString::from(key), OsString::from(value)));
        let mut command = Command::new("fixture");
        configure_adapter_process_environment_from(&mut command, inherited);
        let configured = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_os_string(),
                    value.map(OsStr::to_os_string).unwrap_or_default(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            configured.get(OsStr::new("PATH")),
            Some(&OsString::from("/safe/bin"))
        );
        assert_eq!(
            configured.get(OsStr::new("HOME")),
            Some(&OsString::from("/safe/home"))
        );
        assert_eq!(
            configured.get(OsStr::new("LANG")),
            Some(&OsString::from("en_US.UTF-8"))
        );
        for denied in [
            "OPENAI_API_KEY",
            "OPENAI_BASE_URL",
            "AGY_API_KEY",
            "GOOGLE_API_KEY",
            "GEMINI_API_KEY",
            "AWS_SECRET_ACCESS_KEY",
            "GH_TOKEN",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "NODE_OPTIONS",
            "RUST_LOG",
            "LD_PRELOAD",
            "DYLD_INSERT_LIBRARIES",
            "CODEX_HOME",
            "AGY_HOME",
        ] {
            assert!(!configured.contains_key(OsStr::new(denied)));
        }
    }

    #[test]
    fn child_process_cannot_observe_cleared_sentinel() {
        const SENTINEL: &str = "BASTET_PARENT_ENV_SENTINEL";
        const STAGE: &str = "BASTET_PROCESS_ENV_TEST_STAGE";
        let executable = env::current_exe().unwrap();
        let test_name = "process_environment::tests::child_process_cannot_observe_cleared_sentinel";

        match env::var_os(STAGE).as_deref() {
            // The middle process received the sentinel through its actual OS
            // environment. It observes it before applying the production
            // helper to an inner child command.
            Some(stage) if stage == OsStr::new("middle") => {
                assert_eq!(
                    env::var_os(SENTINEL).as_deref(),
                    Some(OsStr::new("must-not-reach-inner-child"))
                );
                let mut inner = Command::new(executable);
                configure_adapter_process_environment(&mut inner);
                // This marker is set after clearing solely to route the test
                // binary; it is not inherited from the middle process.
                let status = inner
                    .env(STAGE, "inner")
                    .args(["--exact", test_name, "--nocapture"])
                    .status()
                    .unwrap();
                assert!(status.success());
            }
            Some(stage) if stage == OsStr::new("inner") => {
                assert!(env::var_os(SENTINEL).is_none());
            }
            Some(_) => panic!("unexpected process-environment fixture stage"),
            // The ordinary test process launches the middle fixture without
            // mutating its own environment, so concurrent tests remain safe.
            None => {
                let status = Command::new(executable)
                    .env(STAGE, "middle")
                    .env(SENTINEL, "must-not-reach-inner-child")
                    .args(["--exact", test_name, "--nocapture"])
                    .status()
                    .unwrap();
                assert!(status.success());
            }
        }
    }

    #[test]
    fn platform_key_matching_is_explicit() {
        let mut command = Command::new("fixture");
        configure_adapter_process_environment_from(
            &mut command,
            [(OsString::from("Path"), OsString::from("fixture-path"))],
        );
        let configured = command
            .get_envs()
            .map(|(key, value)| (key.to_os_string(), value.map(OsStr::to_os_string)))
            .collect::<BTreeMap<_, _>>();
        #[cfg(windows)]
        assert!(configured.iter().any(|(key, value)| key
            .to_string_lossy()
            .eq_ignore_ascii_case("PATH")
            && value.as_deref() == Some(OsStr::new("fixture-path"))));
        #[cfg(not(windows))]
        assert!(configured.is_empty());
    }
}
