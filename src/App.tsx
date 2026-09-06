import { invoke } from "@tauri-apps/api/core";
import { disable, enable, isEnabled } from "@tauri-apps/plugin-autostart";
import { useCallback, useEffect, useState } from "react";
import { locales, type Locale, translate } from "./i18n";
import "./styles.css";

type ConnectionState = "connecting" | "ready" | "offline";
type DaemonSnapshot = { protocol_version: number; daemon_id: string; revision: number; lifecycle: string };
type ApprovalRecord = { request: { id: string; request_hash: string; expires_at_ms: number; action: { action_key: string; reason_key: string; consequence_key: string; risk: string } }; decision: { kind: "approve" | "deny" } | null };
type ApprovalList = { protocol_version: number; records: ApprovalRecord[] };
type AgentStatus = { adapter_kind: string; display_name: string; installed: boolean; version: string | null; authenticated: boolean | null; model_count: number | null; models: string[]; reasoning_controls: string[]; operations: string[]; error_key: string | null };
type AgentCenterSnapshot = { agents: AgentStatus[] };
type WorkProjection = { revision: number; sessions: number; runs: { run_id: string; session_id: string; state: string; can_cancel: boolean }[] };
type PetProfile = { metadata: { id: string }; name: string; version: number; states: { state_key: string; accessible_label_key: string }[] };
type M3Projection = { revision: number; pet_profiles: PetProfile[]; pet_assignments: number; rooms: number; meetings: number; documents: number; costs: number; graph_nodes: { execution_id: string; title: string; state: string; pet_state: string }[]; awaiting_meetings: { meeting_id: string; summary: string }[]; document_versions: { project_id: string; artifact_id: string; version_id: string; title: string; markdown: string; content_hash: string; accepted: boolean }[]; knowledge_deliveries: { delivery_id: string; target: string; preview: string; state: string }[] };
type View = "office" | "agents" | "approvals" | "diagnostics";

export function App() {
  const [locale, setLocale] = useState<Locale>("zh-Hant");
  const [view, setView] = useState<View>("office");
  const [connection, setConnection] = useState<ConnectionState>("connecting");
  const [snapshot, setSnapshot] = useState<DaemonSnapshot | null>(null);
  const [approvals, setApprovals] = useState<ApprovalRecord[]>([]);
  const [agents, setAgents] = useState<AgentStatus[]>([]);
  const [work, setWork] = useState<WorkProjection>({ revision: 0, sessions: 0, runs: [] });
  const [m3, setM3] = useState<M3Projection>({ revision: 0, pet_profiles: [], pet_assignments: 0, rooms: 0, meetings: 0, documents: 0, costs: 0, graph_nodes: [], awaiting_meetings: [], document_versions: [], knowledge_deliveries: [] });
  const [projectName, setProjectName] = useState("");
  const [workspaceRoot, setWorkspaceRoot] = useState("");
  const [codexModel, setCodexModel] = useState("");
  const [agyModel, setAgyModel] = useState("");
  const [decision, setDecision] = useState("");
  const [documentTitle, setDocumentTitle] = useState("");
  const [documentMarkdown, setDocumentMarkdown] = useState("");
  const [actionError, setActionError] = useState(false);
  const [autostart, setAutostart] = useState(false);

  const reconnect = useCallback(async () => {
    setConnection("connecting");
    try {
      const [next, list, agentList, workList, m3List] = await Promise.all([invoke<DaemonSnapshot>("daemon_snapshot"), invoke<ApprovalList>("approval_center_snapshot"), invoke<AgentCenterSnapshot>("agent_center_snapshot"), invoke<WorkProjection>("work_projection"), invoke<M3Projection>("m3_projection")]);
      if (next.protocol_version !== 1 || list.protocol_version !== 1) throw new Error("protocol mismatch");
      setSnapshot(next); setApprovals(list.records); setAgents(agentList.agents); setWork(workList); setM3(m3List);
      setCodexModel((current) => current || agentList.agents.find((agent) => agent.adapter_kind === "codex_cli")?.models[0] || "");
      setAgyModel((current) => current || agentList.agents.find((agent) => agent.adapter_kind === "agy_cli")?.models[0] || "");
      setConnection("ready");
    } catch { setSnapshot(null); setConnection("offline"); }
  }, []);
  useEffect(() => { void reconnect(); const timer = window.setInterval(() => void reconnect(), 5_000); return () => window.clearInterval(timer); }, [reconnect]);
  useEffect(() => { void isEnabled().then(setAutostart).catch(() => setAutostart(false)); }, []);

  const changeAutostart = async (enabled: boolean) => { if (enabled) await enable(); else await disable(); setAutostart(await isEnabled()); };
  const decide = async (record: ApprovalRecord, kind: "approve" | "deny") => {
    await invoke("decide_approval", { requestId: record.request.id, requestHash: record.request.request_hash, kind, decidedAtMs: Date.now() });
    await reconnect();
  };
  const cancelRun = async (runId: string) => {
    setActionError(false);
    try { await invoke("cancel_run", { runId, expectedCatalogRevision: work.revision }); await reconnect(); }
    catch { setActionError(true); }
  };
  const changeBuiltinPet = async (apply: boolean) => {
    setActionError(false);
    try { setM3(await invoke<M3Projection>(apply ? "apply_builtin_pet" : "rollback_builtin_pet")); }
    catch { setActionError(true); }
  };
  const prepareMvp = async () => {
    setActionError(false);
    try { await invoke("prepare_mvp", { projectName, workspaceRoot, codexModel, agyModel }); await reconnect(); }
    catch { setActionError(true); }
  };
  const acceptDecision = async (meetingId: string) => {
    setActionError(false);
    try { await invoke("accept_mvp_decision", { meetingId, content: decision }); await reconnect(); }
    catch { setActionError(true); }
  };
  const runReady = async () => {
    setActionError(false);
    try { setM3(await invoke<M3Projection>("run_ready_mvp_nodes")); await reconnect(); }
    catch { setActionError(true); }
  };
  const completedExecution = m3.graph_nodes.length > 0 && m3.graph_nodes.every((node) => node.state === "succeeded") ? m3.graph_nodes[0].execution_id : null;
  const createDocument = async () => {
    if (!completedExecution) return;
    setActionError(false);
    try { await invoke("create_mvp_document", { graphExecutionId: completedExecution, title: documentTitle, markdown: documentMarkdown }); await reconnect(); }
    catch { setActionError(true); }
  };
  const acceptDocument = async (document: M3Projection["document_versions"][number]) => {
    setActionError(false);
    try { await invoke("accept_mvp_document", { artifactId: document.artifact_id, versionId: document.version_id, contentHash: document.content_hash }); await reconnect(); }
    catch { setActionError(true); }
  };
  const prepareKnowledge = async (document: M3Projection["document_versions"][number], target: "agent_memory_os" | "bastet_mind") => {
    setActionError(false);
    try { await invoke("prepare_knowledge_delivery", { projectId: document.project_id, artifactVersionId: document.version_id, target, preview: document.markdown }); await reconnect(); }
    catch { setActionError(true); }
  };
  const deliverKnowledge = async (deliveryId: string) => {
    setActionError(false);
    try { await invoke("deliver_knowledge", { deliveryId, deliveredOn: new Date().toISOString().slice(0, 10) }); await reconnect(); }
    catch { setActionError(true); }
  };

  return <main>
    <header><div><p className="eyebrow">{translate(locale, "milestone")}</p><h1>{translate(locale, "title")}</h1></div>
      <label><span className="sr-only">Language</span><select aria-label="Language" value={locale} onChange={(event) => setLocale(event.target.value as Locale)}>
        {locales.map((candidate) => <option key={candidate} value={candidate}>{candidate}</option>)}</select></label></header>
    <nav aria-label={translate(locale, "navigation")}>{(["office", "agents", "approvals", "diagnostics"] as const).map((item) =>
      <button key={item} type="button" aria-current={view === item ? "page" : undefined} onClick={() => setView(item)}>{translate(locale, item)}</button>)}</nav>
    <p role="status" className="connection" data-state={connection}>{translate(locale, connection)}</p>

    {view === "office" && <section aria-labelledby="office-heading"><h2 id="office-heading">{translate(locale, "office")}</h2><p>{translate(locale, "officeHelp")}</p>
      <dl><dt>{translate(locale, "revision")}</dt><dd>{m3.revision}</dd><dt>{translate(locale, "rooms")}</dt><dd>{m3.rooms}</dd><dt>{translate(locale, "meetings")}</dt><dd>{m3.meetings}</dd><dt>{translate(locale, "documents")}</dt><dd>{m3.documents}</dd><dt>{translate(locale, "costs")}</dt><dd>{m3.costs}</dd></dl>
      <article><h3>{translate(locale, "builtinPet")}: Bastet Cat</h3><p>{translate(locale, m3.pet_profiles.length ? "applied" : "notApplied")}</p>
        <ul className="pet-states">{["idle", "thinking", "working", "waiting", "blocked", "approval_required", "succeeded", "failed"].map((state) => <li key={state}><span aria-hidden="true">🐈</span><span>{state}</span></li>)}</ul>
        <button type="button" onClick={() => void changeBuiltinPet(m3.pet_profiles.length === 0)}>{translate(locale, m3.pet_profiles.length ? "rollbackPet" : "applyPet")}</button></article>
      {m3.meetings === 0 && <form onSubmit={(event) => { event.preventDefault(); void prepareMvp(); }}><h3>{translate(locale, "prepareMvp")}</h3><label>{translate(locale, "projectName")}<input required value={projectName} onChange={(event) => setProjectName(event.target.value)} /></label><label>{translate(locale, "workspaceRoot")}<input required value={workspaceRoot} onChange={(event) => setWorkspaceRoot(event.target.value)} /></label><label>Codex {translate(locale, "models")}<select required value={codexModel} onChange={(event) => setCodexModel(event.target.value)}><option value="">—</option>{agents.find((agent) => agent.adapter_kind === "codex_cli")?.models.map((model) => <option key={model}>{model}</option>)}</select></label><label>Agy {translate(locale, "models")}<select required value={agyModel} onChange={(event) => setAgyModel(event.target.value)}><option value="">—</option>{agents.find((agent) => agent.adapter_kind === "agy_cli")?.models.map((model) => <option key={model}>{model}</option>)}</select></label><button type="submit">{translate(locale, "prepare")}</button></form>}
      {m3.awaiting_meetings.map((meeting) => <article key={meeting.meeting_id}><h3>{translate(locale, "decisionBaseline")}</h3><p>{meeting.summary}</p><label>{translate(locale, "decisionBaseline")}<textarea required value={decision} onChange={(event) => setDecision(event.target.value)} /></label><button type="button" disabled={!decision.trim()} onClick={() => void acceptDecision(meeting.meeting_id)}>{translate(locale, "acceptDecision")}</button></article>)}
      {m3.graph_nodes.some((node) => node.state === "pending") && <button type="button" onClick={() => void runReady()}>{translate(locale, "runReady")}</button>}
      {completedExecution && m3.document_versions.length === 0 && <form onSubmit={(event) => { event.preventDefault(); void createDocument(); }}><label>{translate(locale, "documentTitle")}<input required value={documentTitle} onChange={(event) => setDocumentTitle(event.target.value)} /></label><label>{translate(locale, "documentMarkdown")}<textarea required value={documentMarkdown} onChange={(event) => setDocumentMarkdown(event.target.value)} /></label><button type="submit">{translate(locale, "createDocument")}</button></form>}
      {m3.document_versions.map((document) => <article key={document.version_id}><h3>{document.title}</h3><pre>{document.markdown}</pre><code>{document.content_hash}</code>{document.accepted ? <><p>{translate(locale, "approved")}</p><div className="actions"><button type="button" onClick={() => void prepareKnowledge(document, "agent_memory_os")}>{translate(locale, "prepareMemory")}</button><button type="button" onClick={() => void prepareKnowledge(document, "bastet_mind")}>{translate(locale, "prepareMind")}</button></div></> : <button type="button" onClick={() => void acceptDocument(document)}>{translate(locale, "acceptDocument")}</button>}</article>)}
      {m3.knowledge_deliveries.length > 0 && <section aria-labelledby="delivery-heading"><h3 id="delivery-heading">{translate(locale, "knowledgeDeliveries")}</h3><ul>{m3.knowledge_deliveries.map((delivery) => <li key={delivery.delivery_id}>{delivery.target} — {translate(locale, delivery.state === "delivered" ? "delivered" : "prepared")} {delivery.state === "prepared" && <button type="button" onClick={() => void deliverKnowledge(delivery.delivery_id)}>{translate(locale, "deliverNow")}</button>}</li>)}</ul></section>}
      {m3.graph_nodes.length > 0 && <ul>{m3.graph_nodes.map((node) => <li key={`${node.title}-${node.state}`}><span aria-hidden="true">🐈</span> {node.title} — {node.state} <span className="sr-only">{node.pet_state}</span></li>)}</ul>}</section>}

    {view === "agents" && <section aria-labelledby="agents-heading"><h2 id="agents-heading">{translate(locale, "agents")}</h2><p>{translate(locale, "agentHelp")}</p>
      <div className="card-grid">{agents.map((agent) => <article key={agent.adapter_kind}><h3>{agent.display_name}</h3><span className="badge">{translate(locale, agent.installed ? "installed" : "notInstalled")}</span>
        <dl><dt>{translate(locale, "version")}</dt><dd>{agent.version ?? translate(locale, "unknown")}</dd><dt>{translate(locale, "authentication")}</dt><dd>{agent.authenticated == null ? translate(locale, "unknown") : translate(locale, agent.authenticated ? "authenticated" : "notAuthenticated")}</dd>
          <dt>{translate(locale, "models")}</dt><dd>{agent.model_count ?? translate(locale, "unknown")}</dd><dt>{translate(locale, "reasoning")}</dt><dd>{agent.reasoning_controls.join(" · ") || translate(locale, "unknown")}</dd>
          <dt>{translate(locale, "runControl")}</dt><dd>{agent.operations.join(" · ") || translate(locale, "unavailable")}</dd></dl></article>)}</div>
      <h3>{translate(locale, "sessionsAndRuns")}</h3><p>{translate(locale, "sessions")}: {work.sessions} · {translate(locale, "revision")}: {work.revision}</p>
      {actionError && <p role="alert">{translate(locale, "cancelRejected")}</p>}
      {work.runs.length === 0 ? <p>{translate(locale, "noRuns")}</p> : <ul>{work.runs.map((run) => <li key={run.run_id}><code>{run.run_id}</code> — {run.state} {run.can_cancel && <button type="button" onClick={() => void cancelRun(run.run_id)}>{translate(locale, "cancel")}</button>}</li>)}</ul>}</section>}

    {view === "approvals" && <section aria-labelledby="approvals-heading"><h2 id="approvals-heading">{translate(locale, "approvals")}</h2><p>{translate(locale, "approvalHelp")}</p>
      {approvals.length === 0 ? <p>{translate(locale, "noApprovals")}</p> : approvals.map((record) => <article key={record.request.id} className="approval-card"><h3>{record.request.action.action_key}</h3>
        <p>{record.request.action.reason_key}</p><p>{record.request.action.consequence_key}</p><dl><dt>{translate(locale, "risk")}</dt><dd>{record.request.action.risk}</dd>
          <dt>{translate(locale, "expires")}</dt><dd>{new Date(record.request.expires_at_ms).toLocaleString(locale)}</dd></dl>
        {record.decision ? <strong>{translate(locale, record.decision.kind === "approve" ? "approved" : "denied")}</strong> : <div className="actions">
          <button type="button" onClick={() => void decide(record, "deny")}>{translate(locale, "deny")}</button><button type="button" className="primary" onClick={() => void decide(record, "approve")}>{translate(locale, "approve")}</button></div>}</article>)}</section>}

    {view === "diagnostics" && <section aria-labelledby="diagnostics-heading"><h2 id="diagnostics-heading">{translate(locale, "diagnostics")}</h2>
      <label className="preference"><input type="checkbox" checked={autostart} onChange={(event) => void changeAutostart(event.target.checked)} />{translate(locale, "autostart")}</label>
      {snapshot ? <dl><dt>Protocol</dt><dd>{snapshot.protocol_version}</dd><dt>Revision</dt><dd>{snapshot.revision}</dd><dt>Lifecycle</dt><dd>{snapshot.lifecycle}</dd></dl>
        : <button type="button" onClick={() => void reconnect()}>{translate(locale, "retry")}</button>}</section>}
  </main>;
}
