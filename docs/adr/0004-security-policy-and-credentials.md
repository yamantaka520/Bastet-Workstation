# ADR 0004: Security policy, credentials, and approvals

- Status: Accepted
- Date: 2026-09-02
- Authority: Master Plan sections 8 and 11

## Decision

Policy inherits from Workstation to Project to Role/Agent restriction to a single-run grant; child layers cannot exceed their parent ceiling. Secrets live in OS credential storage and durable records contain opaque references only. OS controls enforce sandbox boundaries. Approval requests are immutable, hashed, scoped, expiring, auditable, and fail closed.

## Consequences

Prompt-only safety is insufficient. High-risk actions cannot receive permanent approval. Pet surfaces and Approval Center project the same request, and changed request content requires a new approval.
