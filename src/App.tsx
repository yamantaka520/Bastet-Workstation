import { invoke } from "@tauri-apps/api/core";
import { disable, enable, isEnabled } from "@tauri-apps/plugin-autostart";
import { useCallback, useEffect, useState } from "react";
import { locales, type Locale, translate } from "./i18n";
import "./styles.css";

type ConnectionState = "connecting" | "ready" | "offline";
type DaemonSnapshot = { protocol_version: number; daemon_id: string; revision: number; lifecycle: string };
type ApprovalRecord = { request: { id: string; request_hash: string; expires_at_ms: number; action: { action_key: string; reason_key: string; consequence_key: string; risk: string } }; decision: { kind: "approve" | "deny" } | null };
type ApprovalList = { protocol_version: number; records: ApprovalRecord[] };
type AgentStatus = { adapter_kind: string; display_name: string; installed: boolean; version: string | null; authenticated: boolean | null; model_count: number | null; reasoning_controls: string[]; operations: string[]; error_key: string | null };
type AgentCenterSnapshot = { agents: AgentStatus[] };
type WorkProjection = { revision: number; sessions: number; runs: { run_id: string; session_id: string; state: string; can_cancel: boolean }[] };
type View = "agents" | "approvals" | "diagnostics";

export function App() {
  const [locale, setLocale] = useState<Locale>("zh-Hant");
  const [view, setView] = useState<View>("agents");
  const [connection, setConnection] = useState<ConnectionState>("connecting");
  const [snapshot, setSnapshot] = useState<DaemonSnapshot | null>(null);
  const [approvals, setApprovals] = useState<ApprovalRecord[]>([]);
  const [agents, setAgents] = useState<AgentStatus[]>([]);
  const [work, setWork] = useState<WorkProjection>({ revision: 0, sessions: 0, runs: [] });
  const [actionError, setActionError] = useState(false);
  const [autostart, setAutostart] = useState(false);

  const reconnect = useCallback(async () => {
    setConnection("connecting");
    try {
      const [next, list, agentList, workList] = await Promise.all([invoke<DaemonSnapshot>("daemon_snapshot"), invoke<ApprovalList>("approval_center_snapshot"), invoke<AgentCenterSnapshot>("agent_center_snapshot"), invoke<WorkProjection>("work_projection")]);
      if (next.protocol_version !== 1 || list.protocol_version !== 1) throw new Error("protocol mismatch");
      setSnapshot(next); setApprovals(list.records); setAgents(agentList.agents); setWork(workList); setConnection("ready");
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

  return <main>
    <header><div><p className="eyebrow">{translate(locale, "milestone")}</p><h1>{translate(locale, "title")}</h1></div>
      <label><span className="sr-only">Language</span><select aria-label="Language" value={locale} onChange={(event) => setLocale(event.target.value as Locale)}>
        {locales.map((candidate) => <option key={candidate} value={candidate}>{candidate}</option>)}</select></label></header>
    <nav aria-label={translate(locale, "navigation")}>{(["agents", "approvals", "diagnostics"] as const).map((item) =>
      <button key={item} type="button" aria-current={view === item ? "page" : undefined} onClick={() => setView(item)}>{translate(locale, item)}</button>)}</nav>
    <p role="status" className="connection" data-state={connection}>{translate(locale, connection)}</p>

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
