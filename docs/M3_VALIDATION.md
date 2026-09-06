# M3 validation

`docs/MASTER_PLAN.md` is the sole implementation authority. This record binds M3 evidence to an
exact commit and keeps automated evidence separate from the required human usability decision.

## Automated evidence

Record the final commit before signing the human gate.

| Gate | Evidence | Result |
|---|---|---|
| Core and persistence | Workspace Rust tests, including daemon restart reconciliation | Pending final commit |
| Desktop workflow | Desktop unit/integration suite; real provider scenario is opt-in | Pending final commit |
| Real provider scenario | Actual Codex/Agy answer text persists, both outputs feed the join, actual joined Markdown becomes the accepted document, survives reopen, exports byte-for-byte to an isolated workspace, and appears in a real BastetMind connector write to a temporary vault. AgentMemory receipt remains a fixture in this scenario. | Pass locally, 1/1 in 74.92 seconds; not a human acceptance or live-vault publishing test |
| Frontend | Node 22 Vitest, TypeScript, and Vite production build | Pending final commit |
| Cross-platform | Ubuntu, macOS, and Windows GitHub Actions matrix | Pending final commit |
| Recovery | Existing M1 forced-kill harness plus M3 graph/document/delivery reopen assertions | Pending final commit |

## Human five-locale usability protocol

This gate must be performed by a non-technical tester on the final commit. A facilitator may install,
authenticate, and configure the two local Agents and the two knowledge destinations beforehand, but
must not operate the workflow for the tester. The tester must not open a terminal.

Test each locale: `zh-Hant`, `zh-Hans`, `en`, `ja`, and `ko`. Use a fresh application-data profile
for each full pass, or allocate one fresh machine/account to each locale. Do not reset state while a
provider process is active.

For every locale, the tester performs this checklist using only visible application controls:

1. Launch Bastet Workstation and select the locale.
2. Confirm Codex and Agy are installed, authenticated, and expose selectable real model IDs.
3. Preview every Pet state, apply the built-in Pet, and confirm every icon has equivalent visible or
   screen-reader text.
4. Enter a Project name and absolute local workspace, select one model per Agent, and prepare the
   meeting.
5. Read the meeting summary, enter a DecisionBaseline, explicitly accept it, and inspect the visible
   two-branch graph plus join.
6. Start ready work. Confirm controls become unavailable while busy, failures are announced, both
   research branches reach a terminal state, and the join is unavailable until both succeed.
7. Start the join, create a Markdown document, verify the displayed hash, and explicitly accept the
   exact version.
8. Confirm three cost records are visible. Prepare AgentMemoryOS and BastetMind separately, review
   each redacted preview, deliver each once, and confirm each reaches `Delivered`.
9. Close the app normally, reopen it, and confirm Project, meeting, assignments, graph, accepted
   document, costs, and both delivery states recover.
10. While the app is idle, force-close it with the operating system's graphical process manager,
    reopen it, and confirm the same accepted state recovers without rerunning either delivery.
11. Repeat the critical controls with keyboard only. Check 200% text scaling, reduced motion, high
    contrast, and the platform screen reader. Confirm no clipped/overlapping control blocks progress,
    no untranslated missing-key marker is visible, and locale switching does not corrupt entered
    CJK text or IME composition.

## Evidence form

Do not replace this table with an Agent statement. Attach screenshots or recordings that contain no
secret, private prompt, or sensitive workspace content.

| Field | Required value |
|---|---|
| Commit | Exact 40-character Git commit |
| Build | Platform, OS version, and application artifact identity |
| Tester | Human tester identifier; confirm non-technical and no-terminal execution |
| Locale | One signed row each for zh-Hant, zh-Hans, en, ja, ko |
| Scenario | Steps 1–11 pass/fail, timestamps, and redacted evidence paths |
| Recovery | Normal restart and graphical forced-close results |
| Accessibility | Keyboard, 200% text, reduced motion, high contrast, screen reader |
| Findings | Severity, reproduction steps, and linked fix/retest commit |
| Decision | Human name, date, and explicit M3 accept/reject |

## Human evidence in progress

2026-09-07 blocking content-flow finding: user completed research/join but the document editor was
empty. Entering a filename published that literal string to BastetMind; the project folder stayed
empty. Earlier E2E used fixture document text and delivery receipts, so its passing state assertions
did NOT prove research-output propagation. M3 automated completion claims above are superseded for
this flow. Required correction: retain actual provider answers, feed both into join, persist joined
content with provenance, prefill the document, and verify exact content in a real connector output
and an explicitly exported workspace Markdown file. Existing outputless graphs need a new explicit
run; preserve their accepted artifacts and delivered notes as history.

Correction verification: bounded provider answer capture, hash/run-bound durable outputs,
dependency-fed join prompts, provenance-bound document creation, editor prefill, and non-overwriting
explicit workspace export are implemented. Local workspace tests, Clippy with warnings denied,
12 frontend tests under Node 24, TypeScript, and macOS debug bundle build pass. The real-provider
scenario above now asserts actual content instead of injecting a canned document. Upgrade recovery
creates an idempotent child graph using the existing accepted decision and preserves historical
artifacts. M3 remains open pending the full human gate and final cross-platform CI.

Rebuilt-app operator check on the existing `Test-Prj`: old accepted filename-only document and
delivery remain intact, schema upgrade loads, and the recovery action creates a new child graph.
Its Codex branch retained 2,911 characters of actual research, but Agy finished `failed` without
output and join is `blocked`. Failure detail was not retained by the desktop outcome, so the exact
provider cause is unresolved (the configured 120-second timeout is not proof of a timeout).
The user's actual report has NOT been recovered, accepted, or republished. Do not treat the passing
isolated-provider scenario as a passing retest of this user's complete research task.

2026-09-07 follow-up correction (supersedes the failure-only status above): schema v8 adds
revision-guarded failed-node retry with immutable attempt history and current-run binding.
Successful sibling output and old failed costs/outputs are preserved; cancelled/uncertain work is
not automatically retried. Failure classifications now persist without raw provider details and
are localized in the UI. Agy's auto-approval bypass flag was removed. Safe diagnostic runs with the
user's `gemini-3.8-flash-high` model reproduced SUCCESS with empty output/denied actions; an explicit
read-only final-report contract returned text without permission bypass. This establishes a
reproducible correction, not proof of the exact unrecorded cause of the earlier failure.

Local final workspace suite, Clippy, formatting, 13 frontend tests and bundle build pass. The
actual-content provider gate passes in 31.21 seconds with the user's model and normal permissions;
its formerly underspecified question now includes a concrete bounded statement to assess.
On `Test-Prj`, retrying only Agy succeeded and persisted 7,439 characters while retaining the
original 2,911-character Codex answer and old failure history. Human document acceptance and
publishing remain separate actions; this is still not full M3 acceptance.

Final operator recovery evidence for code commit `5354964328c138df157b9adc67fbdd021b1c0ac5`:
the existing project's join completed and its 3,395-character Markdown was visibly prefilled,
then saved through the desktop as `羅技與雷蛇旗艦級滑鼠整合研究報告`. After closing and reopening
the final debug bundle, the same document remains unaccepted and its document hash equals the
join-source hash (`sha256:035468b45aaf7959c1b80f655339d3f24475adf0841ddf8f3fbea7dc4ae00323`).
The old filename-only accepted artifact and its prior delivery remain unchanged. No new user
document was accepted, exported, or published by the operator. macOS debug executable SHA-256:
`dd107c51a57669e5e684737221b58faff3c9e94f5088a35f8cb81ec0ad9ba443`.
GitHub Actions run `34053296763` for that code commit completed successfully: frontend, all three
Rust/Tauri platforms, and six M0 baseline matrix jobs. This closes the recovery-fix automated gate,
not the outstanding M3 human usability gate.

2026-09-07 blocking finding: after applying the built-in Pet, preparing `Test-Prj` failed.
Desktop and core constructed identical Pet assets with different metadata timestamps, so the daemon
rejected the desktop profile as existing data. The global error was outside the scrolled form.
The correction shares the core factory, accepts the exact legacy desktop profile while preserving
it, and adds submission feedback beside the form. Rebuilt-app operator retest using the user's
existing Pet and `Test-Prj` input passed: revision 1 → 2, one room, one meeting, DecisionBaseline
input displayed. This was an Agent-operated regression check, not human acceptance. Daemon 24/24,
desktop 5/5, Clippy, TypeScript, and bundle build passed; M3 remains open.

| Date | Commit/build | Reporter | Check | Result | Remaining scope |
|---|---|---|---|---|---|
| 2026-09-07 | `fe594972f35b0230268d17b2fa98e59de9824fab`; local macOS debug app SHA-256 `b17d1608a4ab7fbfa00c16e7cda03af4130aa6cf130406dd4d2bf7a9e388f659` | User | Locale switching | Pass — user reported no issue | Locale-by-locale full workflow, long strings/IME, accessibility, normal restart, and graphical forced-close |

M3 is complete only after all automated rows are bound to the same final commit, GitHub Actions is
green, every locale row is signed, and every blocking finding is fixed and retested.
