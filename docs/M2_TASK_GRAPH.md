# M2 task graph

`MASTER_PLAN.md` remains the sole implementation authority. This file records decomposition and
verification status; it does not add scope.

## Dependency graph

| Node | Work | Depends on | May run with | Acceptance evidence |
|---|---|---|---|---|
| M2.1 | Typed core identity, opaque OS credential references, policy ceiling, adapter wire contract | M1 | — | Serialization and invariant tests; no secret-bearing fields; child policy cannot exceed parent |
| M2.2 | Deterministic Agent Adapter conformance harness and fixtures | M2.1 | M2.5 design | Contract-version, event, redaction, cancellation, timeout, crash/resume, auth/quota fixtures |
| M2.3 | Codex CLI reference adapter | M2.2 | M2.4, M2.5 | Full M2 gate matrix with captured provider/locally-measured evidence |
| M2.4 | Agy CLI reference adapter | M2.2 | M2.3, M2.5 | Full M2 gate matrix with captured provider/locally-measured evidence |
| M2.5 | Forward-only daemon migration, identity/policy/adapter persistence and API | M2.1 | M2.2–M2.4 | Previous fixture upgrade, backup/reopen, revision conflicts, restart recovery |
| M2.6 | Immutable approval requests and initial OS-enforced sandbox profiles | M2.1, M2.2 | M2.5 | Request hash/change rejection, expiry/deny, policy ceiling, per-OS enforcement probes |
| M2.7 | Install/doctor/auth/model/reasoning/session/run/status/cancel and Approval Center UI | M2.3–M2.6 | — | Five-locale, keyboard/accessibility, reconnect and authoritative-state tests |
| M2.8 | Integrated Codex and Agy milestone gate | M2.3–M2.7 | — | Both adapters pass read-only, write, cancel, timeout, auth, quota, crash, resume, redaction, and cost evidence tests |

## Risks and human decisions

- Real authentication must store secrets only in Keychain, Credential Manager, or Secret Service;
  tests use opaque references and isolated fixtures, never copied credentials.
- Provider CLI output and flags may drift. Facts reported by providers stay distinct from parsed
  inference and unknown output fails closed.
- OS sandbox profiles require platform-specific probes. Prompt-only restrictions cannot satisfy
  M2.6.
- Any real installation, login, spending, network transmission, or external mutation requires the
  applicable explicit authorization; conformance starts with local fixtures.

## Status

### 2026-09-09 sandbox probe positive control

The macOS Seatbelt write probe now requires proof that the sandbox actually
started its child. It checks denial inside a read-only workspace, successful
creation at the same canonical path when workspace writes are enabled, and
denial outside that workspace. An enforcer startup error can no longer pass
as evidence of filesystem isolation. The focused native test and daemon
warnings-denied Clippy pass locally. This strengthens test evidence only;
production provider sandbox wiring and the full M2.6 gate remain open.

CI run `34320304121` for `f837018` passed all ten jobs, including macOS,
Windows, Linux, frontend, and baseline checks. This result predates the probe
change above and must not be attributed to it.

### 2026-09-09 durable launch selection and process environment slice

New graph attempts persist a typed launch selection before work starts (schema
9). The account/reference locator now participates in selection-to-begin checks;
the executor also compares the receipt, durable selection and current inputs
immediately before dispatch. Completion uses the captured model/account metadata
instead of re-resolving mutable catalog labels. Captured account metadata is
explicitly **not provider-authenticated identity**. Legacy rows retain NULL and
unknown attribution rather than being backfilled from today's catalog.

Malformed selection data never authorizes successful output: the exact attempt
becomes Uncertain with unknown cost attribution, the corrupt row is retained,
and the executor can drain safely. Generic catalog replacement preserves all
prior runs and their sessions, accepts only never-started placeholders as new
runs, and validates persisted M3/graph references before committing. It cannot
inject an unowned active worker or retarget a historical run.

Every Codex/Agy adapter child launch now clears inherited environment variables
and restores only the fixed executable/system, profile, temp and locale list.
API keys, provider endpoint/home overrides, proxies, loader injection, arbitrary
runtime flags and unrelated credentials are not inherited. Existing HOME/profile
configuration remains accessible; environment filtering is not OS sandboxing.
Installations relying on removed overrides need an explicit supported setup
path, not silent restoration of inherited variables.

Production launches with a selected Account are blocked as Unsupported until
the credential broker can enforce its exact reference/grant; they never fall
back to the ambient CLI login. Accountless compatibility launches remain
unverified and their account attribution is unknown. This slice is a prerequisite,
not completion of M2 authentication, credential storage, or sandbox gates.

Tests cover schema 8-to-9 preservation, selection changes, durable attribution,
corrupt selection drain, catalog authority, and additive wire compatibility.
The environment test uses a real root/middle/inner child chain; temporarily
removing `env_clear()` makes it fail. Provider canaries remain ignored.

### 2026-09-09 approval identity binding slice

Approval validation now resolves the acting Agent's exact Account and requires
every scoped credential reference to match that Account's reference. Missing
account/reference and cross-account scopes fail closed. An optional run scope
must resolve through its Session to the same Agent and Project; globally existing
but unrelated IDs are insufficient. Seven focused cases use a validated identity
catalog, including another real Account, accountless/reference-less negatives,
cross-Agent/cross-Project runs, and matching positive controls.

Local core tests (48), full workspace tests, formatting, and warnings-denied
Clippy pass. This does not implement native credential storage, a credential
grant lifecycle, launch-time grant consumption, or selected-account provider
authentication. Those remain open M2 work; no credential access or provider run
is needed for these fixture checks.

CI run `34318618536` for `880b527` passed all ten jobs, including all three
platform builds and daemon lifecycle smoke.

### 2026-09-09 authenticated local IPC slice

Production desktop/daemon traffic now uses Unix-domain sockets on macOS/Linux
and local named pipes on Windows, with no TCP/environment-URL fallback. Unix
checks a private owner-only directory, socket permissions, and the connected
peer UID. Windows gives each instance a protected current-user DACL, rejects
remote clients, and checks the connected server process SID on the actual handle
used for HTTP. This authenticates an OS account, not applications sharing that
account; provider sandbox enforcement remains a separate open M2 requirement.

The daemon claims a canonical-database-path singleton before opening SQLite.
Unix retains a locked file and Windows a first-instance sentinel through graceful
HTTP drain. Existing symlinks are resolved, but this is not a universal file-ID
lock: hard-link aliases and concurrent case aliases are outside its guarantee.
The desktop uses one fixed database path. Legacy TCP builds must first Quit at a
safe checkpoint before replacement, because those binaries do not honor this
new guard. No installed application was replaced as part of this slice.

Native HTTP is bounded, does not follow redirects, and sanitizes transport
errors. Local macOS workspace tests, Clippy, formatting, and a compiled-daemon
smoke passed. The smoke covers readiness, suspend/resume, checkpoint rejection,
graceful restart, forced termination/restart, and stable identity without starting
providers. Windows compilation/runtime and Linux runtime verification remain
pending in the original local snapshot; follow-up CI run `34317757966` for
`c5d6e71` passed all ten jobs, including Rust/Clippy, desktop build and compiled
IPC lifecycle/recovery smoke on all three platforms. A real second-account
denial test is still outstanding. No credential
endpoint or real-provider canary was introduced or executed. M2 remains open.

The preceding `1aa4ba1` daemon-execution fixture correction passed all ten jobs
in CI run `34315939591`, including macOS, Windows, and Linux. This result does
not establish the new IPC slice's cross-platform behavior.

### 2026-09-09 completion audit — supersedes earlier completion claims

M2 is **open**. The historical slice results below describe tested boundaries,
not proof that the complete desktop product satisfies the milestone. Inspection
of `a602572` found these production integration gaps:

- Desktop graph execution owned provider processes while the daemon's real
  `RunControllerRegistry` was never populated. Live cancellation therefore failed
  closed despite the registry fixture passing. Daemon-owned execution and actual
  controller registration are being implemented; fixture success alone will not
  close this item.
- Agent Center projects discovery/auth/model/reasoning status but lacks the full
  install, doctor, interactive authentication, and configuration workflow. A label
  displaying a capability is not an implementation of its operation.
- OS credential references are typed and durable but have no production credential
  storage/retrieval integration. No secrets should be added to catalog state as a
  shortcut.
- Initial sandbox plans exist, but production provider execution does not use
  them. Windows fails closed as unavailable; Linux lacks an actual enforcement
  probe. Provider read-only flags do not prove the required OS enforcement.
- Both ten-case conformance reports use protocol fixtures with capabilities
  broader than the production declarations (including Authenticate). They prove
  normalization, not the whole production adapter gate. Explicit real canaries
  cover only the scenarios recorded below; missing scenarios remain unverified.
- Existing locale tests do not establish non-technical usability testing. M3's
  complete five-locale human gate remains open independently of the accepted
  September 7 research report.

Closure requires production-path evidence for each item, with failure injection
distinguished from live-provider evidence. No release or deployment is authorized
by this audit.

### 2026-09-09 daemon-owned execution slice

The desktop now submits ready graph work through the daemon's execution endpoint
and returns without owning provider processes. The production daemon registers
exact-run controllers, persists provider Running/session evidence and terminal
output/cost/failure records, and rechecks originally selected launch bindings.
Short observation polls remain nonterminal; genuine inactivity deadlines still
produce failures. Codex interrupt acknowledgement has a separate one-second
deadline, and late RPC replies cannot acknowledge a different request.

Cancellation is available only after confirmed Running/Recovering, not during
the still-uninterruptible startup handshake. Accepted interruption and immediate
terminal completion can race without regressing state or reporting a false CAS
failure. Partial text from Cancelled/Blocked/Uncertain outcomes is not accepted as
document output. A started-but-not-yet-Running attempt reopens as Uncertain, with
no automatic replay. Never-launched catalog placeholders are not assigned an
invented start timestamp.

Normal desktop shutdown refuses while work remains active. Ctrl-C and Unix
SIGTERM wait for a safe checkpoint; store/signal errors do not authorize abandoning
workers. Forced process death remains a separate recovery/enforcement gate.
Permanent database faults still need better structured diagnostics; completion
data is retained for persistence retry rather than replaying provider work.

Local verification passed: workspace Rust tests (including 38 daemon tests),
Clippy with warnings denied, 16 frontend tests, TypeScript, and Vite build. The
desktop's ignored two-branch/join canary was changed to use the real production
daemon route instead of a duplicate desktop runner. **It was not executed**:
safety review rejected the attempted invocation because transmitting the test
prompt to authenticated Codex/Agy services and consuming provider quota requires
explicit user approval. No real provider work was started by that attempt.
Cross-platform CI and live-provider evidence for this new slice remain separate
from these local results. M2 and M3 remain open for the requirements listed above.

CI run `34314839204` for `41e9ae8` completed with Linux and seven other jobs
passing, but macOS and Windows failed test scheduling assumptions. macOS hit a
30–80 ms deadline before an expected nonterminal observation. Windows committed
the unrelated sibling between cancel revision capture and preflight, correctly
returning 409. The follow-up tests inspect exact deadline preservation/expiry and
settle the sibling before testing the post-preflight terminal race; production
timeouts and CAS checks are unchanged. The corrected workspace tests pass locally;
the new cross-platform run must still establish the correction remotely.

The listener boundary now rejects non-loopback overrides and claims its socket
before opening/recovering SQLite, so a second process losing the same-port claim
cannot alter a live store. Compiled-binary fixtures cover non-loopback rejection,
occupied-port rejection before database creation, and unchanged existing state.
This is not authenticated IPC or a same-database/different-port process lock.
Before adding credential operations, M2 still needs authenticated local transport,
exact account/reference launch binding, and provider environment allowlisting;
no credential read/write endpoint has been added to the unauthenticated router.

The `d77fc03` follow-up run `34315405532` exposed a separate Linux fixture
startup failure (`Unavailable` before polling). The normalized error did not
retain the underlying OS reason, so no specific errno is claimed. Agy process
tests now use a committed executable fixture instead of writing/chmodding an
executable immediately before parallel startup, removing that possible file-handle
race. Their inactivity-expiry assertion also sets an elapsed deadline directly.
This changes the test harness only; provider failures remain fail-closed.

- M2.1a contract primitives: committed as `774d075`; typed IDs, durable metadata, opaque
  credential references, policy inheritance, and versioned normalized adapter wire types have
  passing tests.
- M2.1b concrete entity relationships and validation: committed as `af72129`; Project, Role,
  Session, and Run relationships fail closed on duplicate IDs, missing references, provider
  mismatch, invalid policy layers, empty provenance, and invalid run timing.
- M2.1 overall: complete; GitHub Actions run `33667253868` passed for `af72129`.
- M2.2 deterministic conformance harness: complete locally; the side-effect-free fixture exercises
  all ten required scenarios from stable case IDs and produces byte-for-byte replayable reports
  containing the case Run ID and adapter capability snapshot. It fails closed on malformed or
  unknown-evidence events, undeclared capabilities or operations, missing cancel/recovery
  transitions, secret leakage, unauthorized or unevidenced writes, malformed normalized failures,
  and invalid cost evidence. Commits `8a179df`, `69b34e3`, and `a336135` passed GitHub Actions
  runs `33668014153`, `33668299526`, and `33669295915` respectively.
- M2.3 Codex CLI reference adapter: complete locally with a read-only discovery/version/doctor process
  boundary, sanitized authentication-status parsing, and a capability declaration that does not
  claim unimplemented execution. A fixture-backed app-server JSON-RPC boundary now enforces the
  required initialize/initialized handshake and strictly normalizes `model/list` model, modality,
  default, and reasoning-effort data. A bounded JSONL stdio transport now owns request IDs, ignores
  notifications while awaiting a response, rejects mismatched/error responses, and closes or kills
  its child within a deadline. An installed Codex CLI passed a real handshake and model-list probe,
  so the adapter now declares `ListModels`. Interactive authentication, execution, session
  lifecycle, and conformance integration remain pending. Fixture-only lifecycle normalization is
  in progress for provider-reported turn start/completion/failure plus locally measured
  cancellation and recovery transitions. Strongly typed fixture-only request boundaries now cover
  `thread/start`, `thread/resume`, `turn/start`, and `turn/interrupt`; they require absolute paths,
  validate workspace-write roots, retain only allowlisted response identifiers, and deliberately
  expose no danger-full-access policy. The stdio boundary now preserves ordered notifications that
  arrive while an RPC response is pending and exposes them only after initialization; malformed
  notifications and unexpected server requests fail closed. A run-scoped stream now routes only
  the configured provider turn ID into lifecycle normalization, ignores unrelated item/turn/error
  notifications, preserves event sequence, and closes authoritatively on `turn/completed`.
  Duplicate terminal events fail closed. Run-scoped evidence normalization now converts non-empty
  `turn/diff/updated` into a redacted write receipt and `thread/tokenUsage/updated.tokenUsage.last`
  into provider-reported input/output token evidence. Raw diffs are discarded, and no currency or
  amount is invented when the provider does not report one. No real thread or turn is launched by
  these tests. A Codex protocol fixture now replays all ten required conformance scenarios through
  the production lifecycle and evidence normalizers, including locally measured timeout and
  transport-loss terminals. Its fixture-only capability target is intentionally separate from the
  production adapter declaration, which continues to withhold Start, Cancel, resume, write, and
  structured-event claims until real execution coverage is complete. An explicitly invoked,
  ignored real-stdio canary now covers one `thread/start` + `turn/start` success path using an
  isolated empty root, `approvalPolicy=never`, and read-only sandboxing at both levels. It verifies
  Running -> Succeeded normalization, provider token evidence without invented currency, no write
  receipt, and an empty root before and after execution. The canary exposed and fixed a protocol
  mismatch: `thread/start.sandbox` uses kebab-case (`read-only` / `workspace-write`), while
  `turn/start.sandboxPolicy.type` uses camelCase. A second explicit real-stdio canary waits for the
  provider Running event, records the local Cancelling transition, sends `turn/interrupt`, and
  requires the provider terminal to normalize as Cancelled with the Cancelled failure kind; its
  isolated root also remains empty. This ordering avoids an observed race when interruption is
  requested before the client consumes `turn/started`. The successful canaries do not establish
  real write, failure, or crash coverage, so production execution capabilities remain withheld.
  A third canary establishes the minimum persisted-session resume path without changing the
  original read-only policy: it completes one small turn, closes the first app-server process,
  initializes a second process, emits locally measured Recovering evidence, and resumes the saved
  `thread.id` with identical thread/session identifiers. An observed negative control confirms a
  newly started thread with no completed turn is not yet resumable through a new process. The
  isolated root remains empty throughout. A fourth canary validates a bounded real write in a
  separate non-repository root using `workspaceWrite`, one explicit writable root,
  `approvalPolicy=never`, and `networkAccess=false`. It requires exactly one `receipt.txt` with
  exact fixture content, a successful terminal, and at least one redacted write receipt that
  contains neither the path nor content. The temporary evidence file is removed after verification.
  JSON-RPC transport failures are now safely classified as unavailable, timed out, protocol drift,
  or remote rejection. Remote rejection retains only its numeric code and whether it is retryable;
  provider message/data are discarded. The documented overloaded code `-32001` is retryable, while
  unknown rejection codes are not guessed to be retryable. This classification reaches the public
  app-server boundary without exposing provider detail. A unified run-update boundary now consumes
  each app-server notification exactly once and routes it to lifecycle or redacted evidence
  normalization. Locally observed notification timeout becomes a terminal timeout event and
  transport loss becomes an uncertain crash event; protocol drift and remote rejection still fail
  closed instead of being mislabeled as runtime state. A real read-only canary passes through this
  unified boundary. Real write evidence remains gated separately because the provider does not
  guarantee a diff notification for every observed filesystem write. The official definitive
  `item/completed` file-change lifecycle is therefore accepted as a second write-evidence source
  only when the target run matches, status is `completed`, and the change list is non-empty and
  structurally valid; all paths and diffs are discarded from the normalized receipt. Because
  command execution can mutate the workspace without either provider event, a bounded local
  before/after snapshot supplies `LocallyMeasured` fallback evidence. It hashes regular-file
  contents, rejects symlinks, caps scans at 10,000 files and 256 MiB, and exposes only the changed
  file count rather than paths or content.
  A run-scoped tracker now owns lifecycle, provider evidence, and the optional workspace snapshot;
  when a writable run reaches a terminal state without provider write evidence, it emits the local
  receipt before releasing the terminal event. This makes evidence ordering part of the adapter
  boundary instead of a responsibility duplicated by callers. Cancellation is also tracker-owned:
  `Cancelling` is emitted only after `turn/interrupt` is accepted, while authoritative `Cancelled`
  still requires the provider's terminal `interrupted` notification. Resume follows the same
  fail-closed rule: `Recovering` is emitted only after `thread/resume` returns a valid matching
  thread handle, and its sequence remains owned by the run tracker.
  A production-shaped `start_tracked_run` façade now derives both thread and turn sandbox policy
  from one request, captures writable-workspace evidence before `turn/start`, and rejects ambiguous
  multi-root write policy rather than starting a run whose writes cannot be fully evidenced. Unit
  fixtures prove both serialized policies and rejection before provider calls. Explicit real stdio
  canaries then passed read-only and bounded single-file workspace-write runs through this façade;
  the write canary retained the existing redacted locally-measured fallback receipt.
  `CodexAdapter::connect_app_server` now owns transport spawn plus the mandatory
  initialize/initialized handshake. Its production capability declaration exposes only the
  verified execution boundaries (start, attach/resume, status/wait, cancel, usage export,
  read-only, bounded write, and structured events); installation and interactive authentication
  remain undeclared. A real model-list canary passed through this production connection boundary.
- M2.4 Agy CLI reference adapter: complete locally. Read-only local discovery established that Agy
  1.1.25 exposes a single-line version and a tab-delimited model catalog. The initial adapter
  boundary accepts only numeric triplet versions and allowlisted non-empty model records, fails
  closed on malformed output, and deliberately withholds execution capabilities until its
  stream/event contract is normalized and tested. An explicit real-CLI canary passed version and
  non-empty catalog inspection against the installed Agy 1.1.25 binary.
  The official headless protocol documents `init`, repeated `step_update`, and one terminal
  `result`, plus stdin user envelopes for `--input-format stream-json`. The first run-scoped
  normalizer accepts that shape, correlates the conversation ID, preserves only lifecycle and
  provider token counts, and discards cwd, tool inventory, response text, tool details, and raw
  errors. Unknown events, malformed usage, mismatched conversations, and post-terminal data fail
  closed. Authentication/quota/permission failures are reduced to typed categories without
  retaining provider text. Production process execution remained withheld until prompt delivery
  moved from argv to the documented stdin envelope.
  The production runner now sends the user envelope only through piped stdin, uses structured
  stdout with stderr discarded, and owns timeout, process-loss, cancellation, resume, and terminal
  ordering. Real isolated canaries passed stdin read-only execution, local cancellation,
  cross-process conversation resume, bounded timeout, and one exact-file workspace write. Codex
  and Agy now share the same bounded, symlink-rejecting workspace snapshot in `bastet-core`; write
  receipts precede terminal state and expose neither path nor content. The Agy protocol fixture
  passes all ten mandatory conformance scenarios, including auth/quota classification and
  redaction. Production capabilities now expose only the verified start, attach/resume,
  status/wait, cancel, usage, read-only, bounded-write, and structured-event boundaries.
- M2.5 daemon persistence and API: complete locally. Forward-only schema v2 adds a singleton,
  independently revisioned identity catalog aggregate. Every replacement first passes the core
  relationship and policy validation, then commits the catalog plus a redacted count-only journal
  event in one immediate transaction. `GET/PUT /v1/catalog` and the loopback client enforce the
  protocol version and compare-and-swap revision. Tests cover v0 and previous-v1 upgrades,
  unsupported future schemas, invalid-catalog rejection before mutation, stale revisions,
  restart persistence, and online backup/reopen with identity/policy/adapter state intact.
  On daemon restart, persisted `Running`, `Cancelling`, or `Recovering` runs are atomically changed
  to `Uncertain`, with both entity and catalog revisions advanced and a count-only reconciliation
  event recorded. This prevents a process kill from silently presenting stale active work as
  authoritative or retrying a side effect without reconciliation.
- M2.6 approval and sandbox boundary: complete through `4f89e47`, `ae68e47`, `b8c5d81`,
  `3be7210`, `bca3cb6`, `4fff24e`, and `3e12341`. Approval requests are immutable, hash-bound,
  expiring, single-decision records. Initial macOS, Windows, and Linux sandbox plans fail closed;
  platform probes and cross-platform path semantics are covered by tests and CI.
- M2.7 Agent/Approval UI: complete through `6124632`, `4e40051`, `9357a57`, `0578f9c`,
  `2cb224f`, `7966541`, and `3bc7dd3`. The five-locale keyboard-native shell projects live
  installation/version/doctor/auth/model/reasoning/capability status, daemon-owned sessions/runs,
  immutable approvals, and revision-guarded cancellation. Provider cancellation must be accepted
  through the exact-run controller registry before the daemon persists `Cancelling`; unknown
  handles fail closed.
- M2.8 integrated gate: complete. On 2026-09-07, local deterministic verification passed Agy
  10 unit + 2 conformance tests, Codex 50 unit + 3 conformance tests, and conformance harness
  12 tests. Both protocol fixtures cover all ten required read-only, write, cancel, timeout,
  authentication failure, quota failure, crash, resume, redaction, and cost-evidence scenarios.
  The real-provider canaries remain explicit/ignored by default because they can start provider
  work; their prior successful evidence is recorded in M2.3/M2.4 above. GitHub Actions runs
  `34043216696`, `34043352166`, and `34043400584` passed the cancellation, localized UI, and
  controller-registry slices on macOS, Windows, and Linux.
