# ADR 0006: Role-bound Pets and bounded meetings

- Status: Accepted
- Date: 2026-09-02
- Authority: Master Plan sections 2 and 8

## Decision

A PetAssignment binds AgentInstance, Role, project/task assignment, and Session. PetProfile changes presentation only and cannot change authority. Meetings are project-scoped and bounded by frozen participants, agenda, rounds, timeouts, and voting rules; human acceptance creates a DecisionBaseline.

## Consequences

Runtime state drives Pet presentation. Imported or generated PetPackages are data-only and require provenance, validation, accessibility fallbacks, preview, confirmation, and rollback. Agent votes remain advisory for policy-sensitive decisions.
