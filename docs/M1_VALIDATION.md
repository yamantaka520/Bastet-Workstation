# M1 validation record

This document records evidence for M1 without replacing the milestone definition in
[`MASTER_PLAN.md`](MASTER_PLAN.md). A row is complete only when the stated evidence exists.

## Automated checks

| Check | Local macOS evidence (2026-09-03) | Remote platform evidence |
|---|---|---|
| React shell | 4 Vitest tests passed; production Vite build passed | [Run 33661058474](https://github.com/yamantaka520/Bastet-Workstation/actions/runs/33661058474): pass |
| Rust protocol, daemon, client, desktop | 13 workspace tests passed | [Run 33661058474](https://github.com/yamantaka520/Bastet-Workstation/actions/runs/33661058474): Linux, macOS, and Windows passed |
| Static analysis | `cargo fmt --check` and Clippy with `-D warnings` passed | [Run 33661058474](https://github.com/yamantaka520/Bastet-Workstation/actions/runs/33661058474): Linux, macOS, and Windows passed |
| Dependency audit | `pnpm audit --audit-level high`: no known vulnerabilities | Local-only check; not configured as a remote job |
| M0 regression | Validator passed 13 required files and 7 ADRs; Python tests 3/3 | [Run 33661058474](https://github.com/yamantaka520/Bastet-Workstation/actions/runs/33661058474): all 6 OS/Python jobs passed |
| Target-specific sidecar build | `pnpm tauri build --debug --bundles app` passed | [Run 33661058474](https://github.com/yamantaka520/Bastet-Workstation/actions/runs/33661058474): Linux, macOS, and Windows passed |
| Compiled daemon smoke harness | Real binary passed suspend/resume, graceful restart, identity retention, forced kill, and crash recovery | [Run 33661058474](https://github.com/yamantaka520/Bastet-Workstation/actions/runs/33661058474): Linux, macOS, and Windows passed |

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
| Application quit | Cmd+Q used the same durable-exit handler as tray Quit; database advanced revision 21 → 22, persisted `stopping`, reason `explicit desktop quit`, and `daemon.shutdown_requested`; listener closed and desktop exited 0 | Pass |
| Opt-in auto-start | UI exposed an unchecked auto-start control | Pass |
| Simulated sleep/wake | Real loopback daemon transitioned `ready` revision 1 → `suspended` revision 2, rejected ordinary checkpoint with HTTP 409, resumed to `ready` revision 3, then shut down with checkpoint revision 4 | Pass |
| Physical sleep/wake | First run exposed that generic Tauri `RunEvent::Resumed` did not handle macOS system wake. After installing native `NSWorkspaceWillSleepNotification` and `NSWorkspaceDidWakeNotification` observers, the bundled app transitioned the same daemon identity from revision 32 `ready` → 33 `suspended` → 34 `ready`, with durable reasons `desktop preparing for system sleep` and `desktop resumed after system wake` | Pass |
| Signed distribution | Debug app is unsigned and fails strict `codesign` verification | Pending release work |
| Tray-menu explicit quit | Direct tray Quit exited the desktop with code 0, closed the listener, and advanced the same daemon identity from revision 37 `ready` to revision 38 `stopping`; checkpoint reason was `explicit desktop quit` and the final event was `daemon.shutdown_requested` | Pass |

## Remaining release validation

- Validate signed distribution artifacts during authorized release work; signing is not part of the
  M1 gate in `MASTER_PLAN.md`.
