# Bastet Workstation

Bastet Workstation is a local-first desktop workspace for personal Agent Teams. It is designed for macOS, Windows, and Linux and keeps one human in control of agent execution, approvals, artifacts, costs, memory, and project knowledge.

**M2 is open; M3 is not accepted.** The September 9 production-path audit supersedes earlier completion claims. Daemon-owned execution, cancellation, durable output, and restart handling are implemented, but Agent setup workflows, native credential integration, OS sandbox enforcement, and complete real-adapter gates still need work. M3 also has implementation gaps in meetings, Pet assets, cost inspection, and delivery preview, in addition to its five-locale human gate. Fixture and historical real-provider results prove only their recorded scenarios; they do not establish complete milestone acceptance.

## Authoritative plan

[`docs/MASTER_PLAN.md`](docs/MASTER_PLAN.md) is the single authority for product scope, architecture, milestones, gates, and accepted decisions. Architecture decisions under [`docs/adr`](docs/adr) record the M0 baseline without replacing that plan.

M1 evidence is tracked in [`docs/M1_VALIDATION.md`](docs/M1_VALIDATION.md); the active M2 audit is
in [`docs/M2_TASK_GRAPH.md`](docs/M2_TASK_GRAPH.md). The M3 automated
and human gate is tracked in [`docs/M3_VALIDATION.md`](docs/M3_VALIDATION.md).

## Local daemon connection

The desktop derives its native local IPC endpoint from its database location.
Production does not listen on TCP or use `BASTET_DAEMON_URL`; `BASTET_LISTEN` is
rejected by the standalone daemon. Unix sockets and Windows named pipes enforce
the OS-user boundary. This does not isolate malicious applications running as
the same user: provider sandboxing and credential grants remain separate work.

Before replacing a pre-IPC build, explicitly quit the old desktop/daemon so it
checkpoints its database. Do not run old TCP and new IPC daemons against the same
database. The new endpoint ownership guard coordinates new IPC builds; it cannot
retroactively lock an older binary that does not implement that guard.

## M0 checks

Run the dependency-free baseline validation:

```sh
python3 scripts/check_m0.py
python3 -m unittest discover -s tests -v

pnpm install --frozen-lockfile
pnpm test
pnpm build
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
pnpm tauri build --debug --bundles app
python3 scripts/smoke_m1.py
```

## Project policies

- [Contributing](CONTRIBUTING.md)
- [Security](SECURITY.md)
- [Apache-2.0 license](LICENSE)
- [Notices](NOTICE)
- [Third-party notices](THIRD_PARTY_NOTICES)

Bastet Workstation is distinct from BastetAgentOS. A future handoff integration is deferred to M9.
