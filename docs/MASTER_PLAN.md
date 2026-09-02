# Bastet Workstation Master Plan

> Status: Approved for planning; implementation has not started.
>
> Product owner: Manfred
>
> Initial plan date: 2026-09-02
>
> Repository: `git@github.com:yamantaka520/Bastet-Workstation.git`
>
> License: Apache-2.0

This is the repository's authoritative product and implementation plan. It separates accepted decisions from recommendations and delivered evidence. A feature is not complete merely because it appears in this plan.

## 1. Product statement

Bastet Workstation is a portable, local-first desktop workspace for personal Agent Teams. It follows one user and their notebook across macOS, Windows, and Linux, runs while the device is available, and makes CLI-first agents approachable through a human-oriented office, room, meeting, Role-bound Pet, task graph, approval, cost, memory, and knowledge interface.

It is not BastetAgentOS Desktop. BastetAgentOS is the later 24/7 server handoff target; Workstation owns the local personal experience and remains useful without that server.

## 2. Accepted product decisions

- Desktop platforms: macOS, Windows, Linux.
- UI languages: Traditional Chinese, Simplified Chinese, English, Japanese, Korean.
- Core license: Apache-2.0.
- Herdr is a design reference, not a required runtime dependency.
- AgentMemoryOS is the shared machine-facing durable memory authority.
- BastetMind is the human-and-agent-readable LLM Wiki and must be visible in the UI.
- Agent execution is concurrent when dependencies, isolation, resource limits, budgets, and approvals permit it.
- A Pet is a Role-bound Agent execution identity, not a decorative avatar:

  `Pet Assignment = Agent Instance + Role + Project/Task Assignment + Session`

- A Pet in `awaiting_approval` exposes the same authoritative approval request as the Approval Center.
- PetProfiles may come from a first-party built-in catalog, validated custom PetPackages inspired by the Codex Pet approach, or a guided user-generation flow. Direct Codex-format compatibility is not promised until a format study and conformance tests exist.
- Project meetings are bounded: each Agent speaks at most once per discussion round; the configured discussion rounds are followed by one final decision round; unresolved or high-risk matters require human intervention.
- Gemini CLI and Agy CLI are independent full providers.
- Core Agent providers: Claude Code, Codex CLI, Agy CLI, Gemini CLI, Grok Build CLI, Prime Agent, Pi Agent.
- First expansion: OpenCode.
- Second wave 2A: Hermes Agent as a nested runtime.
- Second wave 2B: OpenClaw as a nested runtime.
- Later, demand-driven candidates: GitHub Copilot CLI, Cursor Agent CLI, Qwen Code, Kimi Code CLI, Cline.
- BastetAgentOS handoff is explicitly deferred until Workstation is complete and stable.

## 3. MVP vertical slice

### Primary user

A product planner, researcher, or knowledge worker who wants multiple agents to collaborate without operating several terminals manually.

### End-to-end scenario

1. Install or connect two reference Agents: Codex CLI and Agy CLI.
2. Create a local project that may be Git-backed or a normal folder.
3. Select models, reasoning levels, and project permission defaults.
4. Assign Agents to Roles and select Role-bound Pets.
5. Start a bounded project meeting.
6. Review the final decision round and accept the execution baseline.
7. Compile the decision into a visible task graph.
8. Run two independent research branches concurrently.
9. Surface blocked/approval states through Pets and Approval Center.
10. Join results into one reviewable document artifact.
11. Display reported or estimated cost with evidence class and confidence.
12. Publish the accepted summary to BastetMind and add durable memories to AgentMemoryOS through explicit actions.
13. Restart the application and prove that project, meeting, graph, assignments, approvals, costs, and artifacts recover correctly.

### MVP success criteria

- A non-technical test user completes the scenario without opening a terminal.
- No secret is stored in SQLite, logs, prompts, BastetMind, or AgentMemoryOS.
- Every task node, approval, artifact, cost record, and meeting decision has provenance.
- Concurrent writers never modify the same authoritative workspace unsafely.
- Sleep/restart recovery neither loses accepted work nor blindly repeats external side effects.
- All five UI locales pass missing-key, overflow, fallback, and critical-flow checks.

### MVP exclusions

- BastetAgentOS handoff.
- Public third-party marketplace.
- Cloud synchronization and multi-device live collaboration.
- Mobile clients.
- Hermes Agent and OpenClaw integration.
- Every multimedia/3D provider; only reference provider contracts and one safe demonstrator are required.
- Final production art catalog for Pets and rooms.

## 4. Reference architecture

```text
Tauri 2 Desktop
└── React + TypeScript UI
    ├── Today / Projects
    ├── Office / Rooms / Role-bound Pets
    ├── Project Meeting Room
    ├── Graph / Runs / Evidence
    ├── Approval Center
    ├── Agents / Models / Accounts
    ├── Skills / Capabilities / Resources
    ├── Cost Center
    ├── AgentMemoryOS
    └── BastetMind

Rust Local Daemon (single local authority)
├── Command / Event API
├── Scheduler and Graph Runtime
├── Agent Adapter Host
├── Skill Compiler
├── Capability Provider Host
├── PTY / Process Supervisor
├── Permission Broker
├── Workspace / Artifact Manager
├── Recovery Reconciler
├── Notification / Tray Integration
└── SQLite WAL + Append-only Event Journal

External local systems
├── OS Credential Store
├── Agent CLIs / Nested Runtimes
├── Git / Worktrees
├── AgentMemoryOS
├── BastetMind Vault
└── Capability Providers
```

### Process policy

- The Rust daemon is the sole authority for lifecycle and durable state.
- The UI renders daemon state and sends commands; it never directly owns Agent processes.
- Closing the window keeps approved work running in the system tray.
- Explicit Quit begins a bounded checkpoint and shutdown flow.
- Auto-start on login is opt-in and visible.
- One daemon serves one OS user profile. Additional profiles are data partitions, not competing daemons.
- UI and daemon versions must negotiate a protocol version and fail safely when incompatible.

## 5. Core identity model

Every durable entity has a stable opaque ID, timestamps, revision, provenance, and lifecycle state.

| Entity | Authority and purpose |
|---|---|
| AgentProvider | Product/CLI kind and adapter capabilities |
| ModelProvider | API/subscription model supplier |
| Account | Credential reference and provider identity; never raw secret |
| Model | Provider model ID and supported reasoning/capabilities |
| AgentInstance | Installed/configured provider + account + defaults |
| Role | Responsibilities, policy ceiling, skills, inputs, outputs, evidence |
| PetProfile | Visual/audio presentation only; cannot grant authority |
| PetPackage | Data-only, versioned PetProfile assets, semantic-state mappings, accessibility fallbacks, provenance, and license metadata |
| PetAssignment | AgentInstance + Role + project/task assignment + session |
| Project | Local scope, policy, budget, knowledge, workspace configuration |
| Meeting | Bounded project-scoped deliberation and decision record |
| DecisionBaseline | Human-accepted meeting decisions compiled into work |
| Graph/Node/Edge | Durable workflow, dependencies, gates, joins, and invalidation |
| Session/Run | Provider session and one execution attempt |
| ApprovalRequest | Immutable action request, scope, risk, expiry, request hash |
| Skill/SkillBuild | Canonical IR source and adapter-specific compiled package |
| CapabilityProvider | Media, office, search, voice, 3D, browser, or other capability |
| ResourceGrant | Schedulable local/provider resource and scope |
| Artifact/Revision | Versioned work product independent of Git |
| Evidence | Typed proof linked to run, artifact, command, source, and verdict |
| CostRecord | Amount/tokens/time plus evidence class and confidence |
| MemoryReference | AgentMemoryOS ID/scope/provenance |
| WikiReference | BastetMind path/source/provenance |

## 6. Lifecycle states

The daemon owns semantic states. UI labels, Pet animations, notifications, and terminal views are projections.

Minimum Agent/Pet states:

- `offline`
- `idle`
- `planning`
- `working`
- `waiting_dependency`
- `awaiting_approval`
- `blocked`
- `recovering`
- `failed`
- `done_unseen`
- `done_seen`

Every state transition records actor, cause, prior revision, timestamp, related run/node, and evidence. Unknown provider output does not automatically mean blocked or done.

## 7. Extension contracts

### 7.1 Agent Adapter Contract

Required operations:

- discover/install/update/version/doctor
- authenticate and return an opaque account reference
- list models and reasoning controls
- capability declaration
- start/attach/prompt/steer/follow-up
- stream normalized events
- status/explain/wait/cancel/terminate
- export session/result/usage
- sandbox and permission declaration
- health and conformance report

Adapters must distinguish provider-reported facts from parsed inference. Screen detection may be a fallback but cannot silently claim lifecycle authority.

### 7.2 Canonical Skill IR

Required fields:

- stable identity, version, source, author, license, digest
- purpose, trigger conditions, exclusions
- compatible Agents, platforms, models, and capabilities
- filesystem, network, credential, process, device, and human-approval permissions
- inputs, outputs, artifacts, and evidence schemas
- secret mappings and redaction rules
- timeout, cancellation, retry, idempotency, and error semantics
- adapter compilation targets and conformance fixtures

Compilation produces Agent-native skills/plugins/extensions/instruction files. A successful compilation is not sufficient; the generated package must pass static policy checks and target-specific conformance tests.

### 7.3 Capability Provider Contract

Used for image, video, voice, transcription, music, document, spreadsheet, presentation, browser, search, and 3D providers.

It declares:

- capability and input/output MIME/schema
- local/cloud execution, platform availability, model/version
- credentials and network destinations
- synchronous/asynchronous job protocol
- progress, cancellation, callback/poll recovery
- quotas, pricing evidence, estimation formula
- artifact integrity, provenance, license, and retention
- safety policy and required approvals

ElevenLabs starts as a voice Capability Provider. It becomes an Agent only if it owns goals, memory, sessions, tools, and decisions.

## 8. Roles, Pets, and meetings

### Role contract

Roles contain responsibilities, non-responsibilities, required/optional skills, input/output contracts, evidence, policy ceiling, handoff rules, and compatible Agent capabilities. Internet role Markdown is source material only; it requires license, provenance, digest, review, and conversion before use.

### Pet contract

- Pet appearance follows Role and user-selected PetProfile.
- Runtime state controls animation; visuals never control authority.
- PetProfile sources are: a small first-party built-in catalog, validated imported custom PetPackages, and guided user-generated PetPackages.
- The packaging and authoring experience may learn from Codex Pets, but compatibility is claimed only after the external format is studied, versioned, and covered by conformance tests.
- Applying or changing a PetProfile changes presentation only. It never changes AgentInstance, Role, Assignment, model, memory scope, skills, credentials, or permissions.
- `awaiting_approval` displays the same immutable request as Approval Center.
- Approval cards show Agent, Role, action, reason, project/task, filesystem/data/network/credential-reference scope, destination, risk, expiry, and consequence.
- Minimum choices: Allow once and Deny.
- Session/project/time-limited grants appear only when policy permits.
- Allow is never the default focus; timeout and close deny by default.
- High-risk actions cannot be permanently approved.

#### PetPackage and generation contract

- A PetPackage is data-only: no executable code, scripts, network calls, credentials, or hidden external references.
- Its manifest records format version, stable ID, display name, author/source, license, content digests, compatible Workstation versions, attribution/redistribution terms, resource limits, and optional audio metadata.
- Assets map to daemon-owned semantic states and must cover `offline`, `idle`, `planning`, `working`, `waiting_dependency`, `awaiting_approval`, `blocked`, `recovering`, `failed`, and `done`; unknown or invalid states use a safe built-in fallback.
- Every package includes localized/accessibility labels, color-independent cues, reduced-motion and static fallbacks, and declared dimensions/frame timing. Approval remains accessible outside the Pet surface if any asset fails.
- Import/apply flow is quarantine → schema, digest, license, resource, and asset validation → preview every state, reduced-motion mode, localized label, and approval presentation → explicit user confirmation → apply. The previous PetProfile remains available for rollback.
- Guided generation records prompt, model/provider, timestamp, user-supplied references and rights declaration, safety review, asset digests, and license/usage scope. Generated Pets are private/local by default; export or marketplace publication is a separate explicit action.

### Meeting contract

- Meetings are scoped to one project.
- Participants, agenda, discussion round count, timeout, and voting rule freeze at start.
- Each Agent emits at most one formal opinion per discussion round.
- Absence, timeout, abstention, and dissent are durable facts.
- One final decision round follows the discussion rounds.
- Agent votes are advisory for high-risk, spending, credential, destructive, external-publication, and policy-changing decisions.
- Human acceptance creates a DecisionBaseline; rejection or amendment is explicit.
- Output includes immutable transcript, summary, decisions, dissent, action items, sources, and provenance.
- A later meeting may reference accepted conclusions only from the same project.

Initial voting default: simple majority among participating Agents, ties and any policy-sensitive decision go to the human. This remains configurable per meeting template.

## 9. Graph and concurrency

- The graph is the execution authority after a DecisionBaseline is accepted.
- Nodes declare dependencies, Role, required capabilities/skills/resources, access mode, workspace isolation, inputs, outputs, evidence, budget, timeout, retry class, and gate.
- Ready nodes may run concurrently only after admission succeeds.
- Admission verifies adapter health, exact model/account availability, skill build/digest, capability health, policy, budget, workspace isolation, and resource capacity.
- Infrastructure/auth/quota/capability failures do not consume semantic rework budget.
- Failed or changed nodes invalidate only their dependent subgraph when evidence remains valid.
- Joins are explicit nodes; completion prose cannot replace an artifact/evidence receipt.

UI disclosure:

- General: goal, phase, responsible Pet, status, blocker, next decision.
- Advanced: parallel branches, dependencies, gates, cost, artifacts, evidence.
- Expert: full graph, attempts, workspace, process, events, resource locks, and raw adapter diagnostics.

## 10. Workspace and artifact model

- A project may be Git-backed or folder-backed.
- Git code tasks use isolated worktrees for concurrent writers and explicit integration receipts.
- Non-Git work uses immutable input snapshots, per-node staging workspaces, versioned ArtifactRevisions, and a deliberate publish/join operation.
- Office/media/3D artifacts never use last-writer-wins as a merge strategy.
- Conflicts result in deterministic merge where safe, side-by-side candidate review, controlled regeneration, or human decision.
- External publication is a separate approved delivery action.

## 11. Security, privacy, and credentials

Policy inheritance:

`Workstation default → Project override → Role/Agent restriction → Single-run grant`

- Child layers cannot silently exceed the parent ceiling.
- Secrets live in macOS Keychain, Windows Credential Manager, or Linux Secret Service-compatible storage.
- SQLite stores opaque credential references and metadata only.
- Every credential grant names provider, account, Agent, project, skill/capability, scope, expiry, and audit actor.
- Environment variables are allowlisted per adapter/provider.
- Filesystem and network are deny-by-default outside declared scope.
- Sandboxing is OS-enforced; prompt-only read-only is not considered a security boundary.
- Logs, memory, Wiki, screenshots, telemetry, and crash reports pass redaction policy.
- No telemetry leaves the device by default. Any future diagnostics upload is opt-in with preview.

## 12. Persistence and recovery

- Persist intent and ownership before starting side effects.
- Use compare-and-set ownership for runnable nodes and approvals.
- Use stable idempotency keys and provider receipts for external side effects.
- Checkpoint before expected sleep/shutdown and stop admitting new work.
- On wake/start, reconcile database state with processes, PTYs, Git, files, provider jobs, receipts, and event journal.
- `running` after a crash is uncertain, not success and not permission to blindly rerun.
- Orphaned local processes are adopted only when identity and ownership prove exact; otherwise quarantine or stop with user visibility.
- Recovery decisions are durable and auditable.
- Database migrations are forward-only, backed up, versioned, and tested against real previous fixtures.

## 13. Cost ledger

Evidence classes:

- `provider_reported`
- `agent_reported`
- `locally_measured`
- `estimated`
- `unknown`

Every record includes project, node, run, provider/account/model, currency, time window, token/resource units, source, formula/version, confidence, and reconciliation state. Estimated cost is never rendered as a provider bill.

## 14. AgentMemoryOS and BastetMind

- AgentMemoryOS remains the authority for searchable durable memories and ACL scopes.
- BastetMind remains the authority for human-readable sourced project knowledge.
- Workstation exposes both without creating a hidden bidirectional sync loop.
- Explicit actions: Capture memory, Publish to Wiki, Link memory to Wiki, and Report correction.
- Wiki publication includes source, project, decision/run/artifact provenance, and a preview.
- Secret and one-off operational noise are excluded.

## 15. UI information architecture

Primary navigation:

1. Today
2. Office
3. Projects
4. Meetings
5. Graph and Activity
6. Approval Center
7. Agents and Models
8. Roles and Pets
9. Skills
10. Capabilities and Resources
11. Costs
12. Memory
13. BastetMind
14. Settings and Diagnostics

Requirements:

- General, Advanced, and Expert modes.
- Five-locale design from the first component, not a late translation pass.
- English stable message keys; no business logic branching on translated text.
- Per-locale terminology glossary and review owner.
- Traditional Chinese is the product-authoring reference; English is the protocol/documentation reference.
- Fallback order is selected locale → English → visible missing-key diagnostic in development.
- Keyboard-complete operation, screen-reader labels, color-independent states, reduced-motion mode, scalable text, and Pet-independent approval access.

## 16. Licensing and supply chain

- Core and first-party SDK/plugins: Apache-2.0.
- Repository distributions include `LICENSE`, `NOTICE`, `THIRD_PARTY_NOTICES`, SPDX metadata, dependency SBOM, and attribution provenance.
- Third-party Agents, connectors, skills, role definitions, Pets, rooms, fonts, audio, generated media, and trademarks retain their own terms.
- Network content is never installed directly from a prompt. It passes source pinning, digest verification, license classification, permission review, malware/static checks where applicable, and user confirmation.
- Release artifacts are reproducible where practical, signed, checksummed, and accompanied by provenance.

## 17. Delivery roadmap

### M0 — Repository and specification baseline

Deliverables:

- Complete formal rename to Bastet Workstation.
- Apache-2.0 `LICENSE`, `NOTICE`, `THIRD_PARTY_NOTICES` baseline.
- Architecture Decision Records for this plan's accepted defaults.
- Clean repository structure, CI skeleton, security/contribution policy.
- Preserve the current MCP prototype only as a clearly labeled spike or migrate it behind the new contracts.

Gate:

- Plugin/folder/manifest/marketplace names agree.
- No stale Agent Orchestra product identity except migration history.
- Plan, decisions, terminology, and scope are cross-linked.

### M1 — Desktop and daemon foundation

Deliverables:

- Tauri 2 + React desktop shell.
- Rust daemon and versioned command/event protocol.
- SQLite schema, WAL, event journal, migrations, backup/restore fixtures.
- Tray, explicit quit, opt-in auto-start, crash diagnostics, five-locale shell.

Gate:

- UI reconnects after restart without losing daemon state.
- Cold-start, upgrade, crash, sleep/wake smoke tests pass on all three platform families.

### M2 — Identity, policy, and reference Agent adapters

Deliverables:

- Core identity model and OS credential references.
- Agent Adapter Contract and conformance harness.
- Codex CLI and Agy CLI reference adapters.
- Installation/doctor/auth/model/reasoning/session/run/status/cancel UI.
- Approval Center and OS-enforced initial sandbox profiles.

Gate:

- Both adapters pass read-only, write, cancel, timeout, auth failure, quota failure, crash, resume, redaction, and cost evidence tests.

### M3 — Office vertical slice

Deliverables:

- Projects, Roles, PetProfiles, PetAssignments, Office/rooms.
- Bounded project meeting and DecisionBaseline.
- Graph compiler/runtime, two-branch concurrent research, explicit join.
- Role-bound Pet states and approval cards.
- Small first-party built-in Pet catalog, all-state preview, accessibility fallbacks, apply, and rollback.
- Versioned document artifact and human acceptance.
- Cost ledger, AgentMemoryOS capture, BastetMind publish.

Gate:

- MVP scenario and restart recovery pass with non-technical usability testing in all five locales.

### M4 — Skill and Capability platform

Deliverables:

- Canonical Skill IR, compiler, policy scanner, provenance, conformance harness.
- Capability Provider Contract.
- Reference document and one media/voice capability provider.
- Validated custom PetPackage import and guided Pet generation through Capability Providers, with provenance and rights metadata.
- Resource/budget-aware graph admission.

Gate:

- One canonical skill compiles and behaves consistently on Codex and Agy.
- Capability jobs recover from asynchronous interruption without duplicate side effects.

### M5 — Complete core Agent wave

Deliverables:

- Claude Code, Gemini CLI, Grok Build CLI, Prime Agent, Pi Agent adapters.
- Provider-specific native event authority where available and explicit fallback confidence otherwise.
- Capability matrix and per-platform support labels.

Gate:

- Every core adapter passes the same mandatory conformance suite; unsupported features are visible, not emulated deceptively.

### M6 — Cross-platform public MVP and OpenCode

Deliverables:

- OpenCode server/API adapter.
- Signed installers/updaters for macOS, Windows, Linux.
- SBOM, provenance, license notices, migration/rollback, support bundle redaction.
- Accessibility, performance, localization, security, recovery, and upgrade release gates.

Gate:

- Clean-machine install → MVP scenario → upgrade → recovery → uninstall passes per platform.

### M7 — Nested runtime wave 2A: Hermes Agent

Deliverables:

- Nested runtime contract implementation.
- Hermes profiles/providers/worktrees/skills/memory/pets/session mapping.
- Native macOS/Linux and explicit Windows WSL2 capability presentation.

Gate:

- No duplicate scheduler, memory, Pet, permission, or cost authority; child identities remain traceable.

### M8 — Nested runtime wave 2B: OpenClaw

Deliverables:

- OpenClaw workspace/agent/auth/routing/channel/Gateway mapping.
- Explicit ownership rules for scheduling, retry, permissions, memory, and lifecycle.

Gate:

- Channel and child-agent work appears once in the Workstation graph and audit trail; cancellation and recovery are deterministic.

### M9 — BastetAgentOS handoff

Starts only after Workstation is stable.

Deliverables:

- Versioned handoff package containing graph state, Git/artifact provenance, accepted decisions, memory scopes, approvals, budgets, evidence, and return channel.
- Ownership transfer and conflict/reconciliation protocol.

Gate:

- A local task can transfer to a server, continue during notebook downtime, and return results without duplicate execution or loss of provenance.

## 18. Testing and release gates

Every milestone defines unit, contract, integration, recovery, security, accessibility, localization, and end-to-end evidence.

Mandatory matrices:

- macOS current and previous supported major release.
- Windows current supported release; WSL2 separately labeled.
- Linux reference distributions and desktop environments.
- Fresh install, upgrade from previous release, rollback/recovery, uninstall.
- Online, offline, network switch, sleep/wake, forced process kill, power-loss simulation.
- Five locales, long strings, missing keys, locale switching, IME, RTL not required.
- Reduced motion, keyboard-only, screen reader, high contrast, text scaling.
- Agent auth expired, quota exhausted, binary missing, protocol drift, output malformed, child orphaned.
- Credential redaction and approval replay/change rejection.

A milestone is complete only when evidence is linked to the exact commit/artifact and required gates pass. Agent prose is not test evidence.

## 19. Risk register

| Risk | Mitigation |
|---|---|
| Too many Agent adapters | Contract-first, conformance suite, phased support |
| Nested orchestrators fight for authority | Nested runtime contract and one Workstation lifecycle authority |
| Prompt-only permissions | OS-enforced sandbox and permission broker |
| Concurrent write corruption | Worktrees or immutable staging + explicit joins |
| Crash repeats external action | Durable intent, idempotency keys, receipts, reconcile-before-retry |
| Pet UI hides real risk | Same approval request in Pet and Approval Center; accessible non-Pet route |
| Unsafe, oversized, or unlicensed custom/generated Pet | Data-only packages, quarantine validation, provenance/rights metadata, resource limits, preview, rollback, and private-local default |
| Estimated cost appears exact | Evidence class, formula, confidence, reconciliation |
| Translation drift | Stable message keys, glossary, five-locale gates |
| Third-party role/skill/asset licensing | Provenance, digest, license policy, notices, quarantine |
| Secrets leak into logs/memory/wiki | OS credential references, allowlists, redaction, tests |
| Scope overwhelms MVP | Vertical slice and milestone entry/exit gates |

## 20. Change control and memory discipline

- This file is the implementation-plan authority inside the repository.
- Accepted changes append an entry to the decision log below and update affected milestone gates.
- Superseded decisions remain visible with their replacement.
- Delivery claims require repository evidence and are synchronized to BastetMind and AgentMemoryOS.
- BastetMind stores sourced human-readable knowledge; AgentMemoryOS stores durable recall; neither replaces this executable plan.
- Secrets and one-off noise are never copied into plan, memory, or Wiki.

### Decision log

| Date | Decision |
|---|---|
| 2026-09-02 | Product is a portable personal Agent Team workstation, distinct from BastetAgentOS. |
| 2026-09-02 | Five UI languages and macOS/Windows/Linux are required. |
| 2026-09-02 | Apache-2.0 supersedes MIT. |
| 2026-09-02 | Pet is Role-bound Agent execution identity and approval surface. |
| 2026-09-02 | PetProfiles support built-in, validated custom, and guided generated packages; profile changes never alter Role or authority. |
| 2026-09-02 | Gemini CLI and Agy CLI are independent providers. |
| 2026-09-02 | OpenCode is first expansion; Hermes 2A; OpenClaw 2B. |
| 2026-09-02 | Herdr is reference-only; BastetAgentOS handoff is last. |
| 2026-09-02 | The eight P0 defaults and this Master Plan are accepted for planning. |

## 21. Next authorized action

No implementation is authorized by this plan alone. When the product owner says to begin, start with M0 only, verify the repository baseline, and do not skip directly to UI features or Agent integrations.

This conversation session is reserved for Bastet Workstation planning, requirements, architecture, and decision records. Implementation will begin in a separate conversation session; planning discussion here does not implicitly authorize code changes.
