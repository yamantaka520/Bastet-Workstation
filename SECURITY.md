# Security Policy

## Supported versions

Bastet Workstation is in pre-release M0 development. No production release is currently supported.

## Reporting a vulnerability

Do not disclose suspected vulnerabilities in a public issue. Contact the repository owner privately through the security reporting channel configured on GitHub. Include affected revision, impact, reproduction details, and any suggested mitigation. Do not include live credentials or personal data.

## Baseline security rules

- Secrets belong in the operating system credential store; repository and future databases store opaque references only.
- Filesystem, network, process, device, and credential access are deny-by-default outside declared scope.
- Model instructions are never treated as an OS security boundary.
- External side effects require durable intent, ownership, idempotency, and receipts before execution.
- Logs, artifacts, memory, Wiki output, screenshots, telemetry, and diagnostics require redaction.
- Third-party code and assets require pinned provenance, digest, license review, and explicit acceptance before installation or distribution.

The complete accepted security model is in [`docs/MASTER_PLAN.md`](docs/MASTER_PLAN.md), especially sections 7, 9–12, 16, 18, and 19.
