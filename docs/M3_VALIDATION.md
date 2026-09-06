# M3 validation

`docs/MASTER_PLAN.md` is the sole implementation authority. This record binds M3 evidence to an
exact commit and keeps automated evidence separate from the required human usability decision.

## Automated evidence

Record the final commit before signing the human gate.

| Gate | Evidence | Result |
|---|---|---|
| Core and persistence | Workspace Rust tests, including daemon restart reconciliation | Pending final commit |
| Desktop workflow | Desktop unit/integration suite; real provider scenario is opt-in | Pending final commit |
| Real provider scenario | Codex and Agy run two research branches concurrently, Codex joins, the document is accepted, two delivery receipts and three cost records persist after SQLite reopen | Pass locally, 1/1 in 60.03 seconds |
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
