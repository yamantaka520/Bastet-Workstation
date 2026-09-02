# M1 validation record

This document records evidence for M1 without replacing the milestone definition in
[`MASTER_PLAN.md`](MASTER_PLAN.md). A row is complete only when the stated evidence exists.

## Automated checks

| Check | Local macOS evidence (2026-09-03) | Remote platform evidence |
|---|---|---|
| React shell | 4 Vitest tests passed; production Vite build passed | Pending CI |
| Rust protocol, daemon, client, desktop | 11 workspace tests passed | Pending CI |
| Static analysis | `cargo fmt --check` and Clippy with `-D warnings` passed | Pending CI |
| Dependency audit | `pnpm audit --audit-level high`: no known vulnerabilities | Pending CI |
| M0 regression | Validator passed 13 required files and 7 ADRs; Python tests 3/3 | Pending CI |
| Target-specific sidecar build | `pnpm tauri build --debug --no-bundle` passed | Configured for Linux, macOS, and Windows; pending CI run |

## Local macOS smoke evidence

| Scenario | Evidence | Status |
|---|---|---|
| Cold start | Bundled desktop started bundled daemon; protocol 1 reached `ready` | Pass |
| State recovery | Stable daemon identity survived restart; revisions advanced through `recovering` to `ready` | Pass |
| Graceful shutdown | Shutdown returned durable checkpoint revision 4; listener then closed | Pass |
| Forced process kill | PID 46416 received SIGKILL; watchdog started PID 46572 within 6 seconds; revision advanced 8 → 10 | Pass |
| Crash diagnostics | `last-exit.txt` recorded `signal: 9 (SIGKILL)` | Pass |
| Backup/restore | Online backup reopened with schema version, checkpoint, event journal, and recovery event intact | Pass |
| Upgrade fixture | v0 fixture retained daemon identity, migrated to v1, advanced revision 41 → 42, and accepted a checkpoint | Pass |
| Future schema | Schema version 99 was rejected without mutation | Pass |
| macOS app bundle | `.app/Contents/MacOS` contains `bastet-desktop` and `bastet-daemon`; bundled sidecar reached revision 13 `ready` | Pass |
| Close to tray | Closing the main window hid it while daemon health remained `ready` | Pass |
| Opt-in auto-start | UI exposed an unchecked auto-start control | Pass |
| Signed distribution | Debug app is unsigned and fails strict `codesign` verification | Pending release work |
| Tray-menu explicit quit | Backend path is covered by shutdown integration tests; direct tray click not yet captured | Pending manual smoke |
| Sleep/wake | Not run because putting the active workstation to sleep is disruptive | Pending explicit manual smoke |

## Remaining M1 gate

- Run cold-start, upgrade, crash, sleep/wake, and daemon reconnect smoke tests on macOS,
  Windows, and Linux reference environments.
- Capture a direct tray-menu quit smoke result.
- Validate signed distribution artifacts when release signing is authorized.
- Record CI run links only after a future authorized push.
