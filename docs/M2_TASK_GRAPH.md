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
  boundary; authentication, execution, session lifecycle, and conformance integration remain
  pending.
- M2.4–M2.8: not started.
