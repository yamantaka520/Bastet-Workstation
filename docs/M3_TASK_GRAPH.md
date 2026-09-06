# M3 task graph

`MASTER_PLAN.md` remains the sole implementation authority. This file decomposes M3 and records
verification evidence without adding scope.

| Node | Work | Depends on | May run with | Acceptance evidence |
|---|---|---|---|---|
| M3.1 | Projects, Roles, PetProfiles, PetAssignments, rooms | M2 gate | — | Typed relationships, provenance, full-state accessibility and rollback validation |
| M3.2 | Bounded project meeting and immutable DecisionBaseline | M3.1 | — | Round/participant limits, human acceptance, content hash, restart persistence |
| M3.3 | Graph compiler and durable runtime | M3.2 | M3.4 design | DAG validation, CAS ownership, two concurrent research branches and explicit join |
| M3.4 | Role-bound Pet state projection and approval cards | M3.1, M3.3 | M3.5 | State-independent accessible text; approval remains usable without Pet visuals |
| M3.5 | Versioned document artifact and human acceptance | M3.3 | M3.4 | Immutable versions, provenance, join receipt and explicit acceptance |
| M3.6 | Cost ledger, AgentMemoryOS capture, BastetMind publish | M3.3, M3.5 | — | Evidence class retained; preview/redaction/provenance; no hidden bidirectional sync |
| M3.7 | Five-locale MVP and restart-recovery gate | M3.1–M3.6 | — | Full scenario, crash reconciliation, accessibility and non-technical usability evidence |

## Risks and human decisions

- Built-in Pet artwork must have first-party provenance and accessibility labels. Generated or
  third-party assets require rights metadata and are outside this initial catalog slice.
- A meeting cannot dispatch work until a human accepts its exact DecisionBaseline.
- `running` after restart remains uncertain; recovery must never blindly duplicate provider work.
- AgentMemoryOS and BastetMind writes are explicit delivery actions with previews and redaction.
- Non-technical usability acceptance in all five locales requires human evidence; automation can
  validate locale coverage and keyboard/accessibility mechanics but cannot substitute for it.

## Status

- M3.1a started: typed PetProfile, PetAssignment, and Room domain contracts with required all-state
  accessibility metadata and fail-closed relationship validation.
- M3.2a started: bounded meeting and immutable, human-accepted DecisionBaseline contracts.
- M3.3a started: validated two-branch research DAG and ownership-checked in-memory runtime with an
  explicit join and failed-dependency blocking. Durable daemon persistence remains next.
