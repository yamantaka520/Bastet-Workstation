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
- M2.3 Codex CLI reference adapter: in progress with a read-only discovery/version/doctor process
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
  real resume, write, failure, or crash coverage, so production execution capabilities remain
  withheld.
- M2.4–M2.8: not started.
