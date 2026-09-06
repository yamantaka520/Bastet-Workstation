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
- The desktop now renders the exact Markdown and hash, authors a document only after the graph is
  fully successful, and exposes a separate exact-version acceptance action. The client loopback
  test covers both document transactions through HTTP before checkpoint/shutdown.
- Knowledge publication is a durable two-phase protocol: only an accepted artifact version can
  create a redacted `Prepared` delivery, and only a non-empty external destination receipt changes
  it to `Delivered`. AgentMemoryOS and BastetMind targets remain separate records. Restart tests
  prove both completed delivery receipts survive without journaling preview content or receipts.
- Safety correction: the one-shot MVP initializer now accepts only a pristine state or the same
  unassigned built-in Pet profile. Existing Projects, meetings, assignments, documents, costs, or
  deliveries cause a fail-closed error; the initializer never replaces user data.
- Provider execution now binds each ready Graph node to its Role-bound PetAssignment, persists its
  Session/Run in the same transaction as the node claim, and returns the selected adapter, exact
  discovered model, workspace, and DecisionBaseline-derived prompt. Terminal provider state,
  provider session receipt, Graph state, and normalized cost evidence commit atomically. An
  uncertain outcome releases ownership without unlocking the join; stale revisions roll back the
  whole transition.
- The desktop dispatches both ready research branches concurrently through the real Codex and Agy
  reference adapters, then exposes the explicit join only after both succeed. A real opt-in gate on
  commit `5d4ff2b` completed Codex + Agy research concurrently, ran the Codex join, persisted three
  Runs and cost records, created and accepted the joined document, completed both durable delivery
  records, reopened SQLite, and verified the entire state remained intact (1/1 in 60.03 seconds).
  Independent real read-only canaries also passed 1/1 for each adapter.
- MVP preparation no longer stores placeholder model names. It requires model IDs discovered from
  each installed adapter and presents native model selectors before creating the Project.
- AgentMemoryOS and BastetMind now have narrow first-party delivery connectors. Both reconcile a
  stable delivery marker before retry; AgentMemoryOS requires a real CLI memory-id receipt, while
  BastetMind uses create-new output files and idempotently updates its required `index.md` and
  append-only `log.md`. Only then does the daemon mark the prepared delivery `Delivered`.
- All M3 workflow and delivery controls have explicit zh-Hant, zh-Hans, English, Japanese, and
  Korean strings. Node 22 CI for `c8b49bb` passed the frontend tests and build. Long-running provider
  and delivery actions are disabled while active and expose localized status/error announcements.

## Current gate evidence

- Local automated regression after `c1bb50a`: core 37/37, daemon 23/23, client real-loopback 1/1,
  desktop 5/5 with the real-provider scenario intentionally ignored by default, workspace Clippy
  with warnings denied, TypeScript, and Vite production build all pass.
- GitHub Actions is the cross-platform authority: closing commits `5d4ff2b` and `c1bb50a`, plus all
  preceding M3 commits, passed the full Ubuntu/macOS/Windows Rust/Tauri matrix, recovery smoke, and
  Node 22 frontend job.
- The only non-automatable gate evidence still outstanding is a human, non-technical usability pass
  of the complete MVP workflow in all five locales. Automated translation coverage, keyboard-native
  controls, accessible status/alert semantics, Pet text fallbacks, and restart tests do not replace
  that human evidence; M3 must not be declared complete until it is recorded using
  `docs/M3_VALIDATION.md`.
