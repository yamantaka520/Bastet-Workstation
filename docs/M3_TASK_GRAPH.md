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
  explicit join and failed-dependency blocking.
- M3.3b started: daemon schema v4 persists graph executions with compare-and-set revisions and
  marks interrupted running nodes uncertain on restart without retaining worker identity in events.
- M3.5/M3.6 contracts started: immutable document versions require a two-source join receipt and
  explicit human acceptance; cost and explicit AgentMemoryOS/BastetMind delivery records preserve
  evidence class, redacted preview, provenance, and destination receipt semantics.
- Cross-domain M3 aggregate validation now rejects missing DecisionBaseline, Role, Project, Run,
  graph-node, artifact-version, and duplicate graph-execution references.
- Daemon schema v5 and the loopback client expose a separately revisioned M3 catalog for Office,
  meetings, documents, costs, and explicit knowledge-delivery records; graph execution remains in
  its dedicated authority table to avoid dual writes.
- M3.4a provides a five-locale Office projection with an eight-state accessible first-party Pet
  preview plus revision-guarded apply and rollback; rollback fails closed while assigned.
- M3.4b projects persisted graph-node lifecycle into role-work Pet states (`idle`, `working`,
  `succeeded`, `failed`, `blocked`, or restart-safe `waiting`) through the daemon/client boundary;
  visual icons remain supplemental to visible and screen-reader text.
- The pure MVP draft compiler now creates one Project, Codex/Agy AgentInstances, two independent
  research Roles, one integrator Role, a room, three role-bound PetAssignments, and a bounded
  meeting. It emits no graph until explicit human DecisionBaseline acceptance, then compiles
  exactly two research branches and one join.
- The daemon now persists MVP preparation atomically across identity and M3 catalogs, then persists
  the exact human-accepted DecisionBaseline and compiled graph in a second atomic transaction.
  Restart tests prove Project, three PetAssignments, accepted meeting, and graph survive reopen.
- The five-locale Office UI now authors the prepared Project/meeting from an explicit absolute
  workspace and displays the durable meeting summary after reconnect. A separate enabled-only-
  with-content action records human DecisionBaseline acceptance and compiles the graph. The real
  loopback client test covers prepare → accept → graph list → checkpoint/shutdown.
- Graph runtime claim/complete commands are now versioned daemon APIs. The loopback integration
  test claims both research branches together, completes each with revision CAS, proves the join
  becomes claimable only afterward, completes it under a distinct owner, then checkpoints and
  shuts down cleanly.
- Completed graphs can now create a versioned Markdown document only when every node succeeded;
  the version records the two research node ids and a content hash. A separate exact-hash human
  acceptance transaction is required. Restart verification proves the accepted version survives.
