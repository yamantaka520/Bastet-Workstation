# Bastet Workstation

Bastet Workstation is a local-first desktop workspace for personal Agent Teams. It is designed for macOS, Windows, and Linux and keeps one human in control of agent execution, approvals, artifacts, costs, memory, and project knowledge.

M0 is committed locally. Development is now in **M1: desktop and daemon foundation**. The current slice contains a versioned Rust protocol, SQLite WAL/event journal persistence and online backup, a loopback daemon API with durable graceful shutdown, a supervised daemon with crash diagnostics, opt-in auto-start, and a five-locale React/Tauri shell. Local macOS graceful-shutdown and forced-kill recovery smoke tests pass. It does not yet satisfy the complete M1 three-platform recovery gate.

## Authoritative plan

[`docs/MASTER_PLAN.md`](docs/MASTER_PLAN.md) is the single authority for product scope, architecture, milestones, gates, and accepted decisions. Architecture decisions under [`docs/adr`](docs/adr) record the M0 baseline without replacing that plan.

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
```

## Project policies

- [Contributing](CONTRIBUTING.md)
- [Security](SECURITY.md)
- [Apache-2.0 license](LICENSE)
- [Notices](NOTICE)
- [Third-party notices](THIRD_PARTY_NOTICES)

Bastet Workstation is distinct from BastetAgentOS. A future handoff integration is deferred to M9.
