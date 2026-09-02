# ADR 0005: Workspace isolation, concurrency, and recovery

- Status: Accepted
- Date: 2026-09-02
- Authority: Master Plan sections 9, 10, and 12

## Decision

The durable graph admits concurrent work only when dependencies, health, policy, budget, isolation, and resources permit. Git writers use isolated worktrees. Non-Git work uses immutable inputs, per-node staging, ArtifactRevisions, and explicit joins. External side effects persist intent and ownership before execution and use stable idempotency keys and receipts.

## Consequences

Completion prose never substitutes for evidence. Last-writer-wins is forbidden for office and media artifacts. Infrastructure failure does not spend semantic rework budget. Crash recovery treats `running` as uncertain and reconciles before adopting or retrying work.
