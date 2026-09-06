# Bastet Workstation

Bastet Workstation is a local-first desktop workspace for personal Agent Teams. It is designed for macOS, Windows, and Linux and keeps one human in control of agent execution, approvals, artifacts, costs, memory, and project knowledge.

Development is now closing **M3: Office vertical slice**. M0–M2 are complete. The current slice contains the versioned Rust protocol, supervised local daemon and SQLite recovery foundation; Codex/Agy reference adapters; typed Projects, Roles, Role-bound Pets and meetings; a human-accepted DecisionBaseline; durable two-branch execution plus explicit join; versioned accepted documents; cost evidence; and explicit AgentMemoryOS/BastetMind delivery. Automated and real-provider gates pass locally. M3 remains open until the final cross-platform CI and the five-locale non-technical usability protocol in `docs/M3_VALIDATION.md` pass on the exact delivery commit.

## Authoritative plan

[`docs/MASTER_PLAN.md`](docs/MASTER_PLAN.md) is the single authority for product scope, architecture, milestones, gates, and accepted decisions. Architecture decisions under [`docs/adr`](docs/adr) record the M0 baseline without replacing that plan.

M1 evidence is tracked in [`docs/M1_VALIDATION.md`](docs/M1_VALIDATION.md); the active M3 automated
and human gate is tracked in [`docs/M3_VALIDATION.md`](docs/M3_VALIDATION.md).

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
