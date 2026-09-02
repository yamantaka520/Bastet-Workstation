# Bastet Workstation

Bastet Workstation is a local-first desktop workspace for personal Agent Teams. It is designed for macOS, Windows, and Linux and keeps one human in control of agent execution, approvals, artifacts, costs, memory, and project knowledge.

The project is currently at **M0: repository and specification baseline**. No desktop runtime, Pet UI, Agent adapter, or marketplace capability is implemented yet.

## Authoritative plan

[`docs/MASTER_PLAN.md`](docs/MASTER_PLAN.md) is the single authority for product scope, architecture, milestones, gates, and accepted decisions. Architecture decisions under [`docs/adr`](docs/adr) record the M0 baseline without replacing that plan.

## M0 checks

Run the dependency-free baseline validation:

```sh
python3 scripts/check_m0.py
python3 -m unittest discover -s tests -v
```

## Project policies

- [Contributing](CONTRIBUTING.md)
- [Security](SECURITY.md)
- [Apache-2.0 license](LICENSE)
- [Notices](NOTICE)
- [Third-party notices](THIRD_PARTY_NOTICES)

Bastet Workstation is distinct from BastetAgentOS. A future handoff integration is deferred to M9.
