import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";
import { hasExplicitWorkflowTranslation, locales, translate, workflowKeys } from "./i18n";

const { invokeMock, listenMock } = vi.hoisted(() => ({ invokeMock: vi.fn(), listenMock: vi.fn() }));

const defaultInvoke = (command: string) => Promise.resolve(command === "approval_center_snapshot"
    ? { protocol_version: 1, records: [] }
    : command === "agent_center_snapshot"
      ? { agents: [{ adapter_kind: "codex_cli", display_name: "Codex CLI", installed: true, version: "1.0.0", authenticated: true, model_count: 2, models: ["gpt-test"], reasoning_controls: ["low", "high"], operations: ["start", "cancel"], error_key: null }] }
      : command === "work_projection"
        ? { revision: 3, sessions: 1, runs: [] }
      : command === "m3_projection"
        ? { revision: 0, pet_profiles: [], pet_assignments: 0, rooms: 0, meetings: 0, documents: 0, costs: 0, graph_nodes: [], awaiting_meetings: [], joined_drafts: [], missing_output_execution_id: null, document_versions: [], knowledge_deliveries: [] }
      : { protocol_version: 1, daemon_id: "test-daemon", revision: 7, lifecycle: "ready" });

vi.mock("@tauri-apps/api/core", () => ({ invoke: invokeMock }));
vi.mock("@tauri-apps/api/event", () => ({ listen: listenMock }));
vi.mock("@tauri-apps/plugin-autostart", () => ({
  disable: vi.fn().mockResolvedValue(undefined),
  enable: vi.fn().mockResolvedValue(undefined),
  isEnabled: vi.fn().mockResolvedValue(false),
}));

describe("M1 shell", () => {
  beforeEach(() => { invokeMock.mockReset(); invokeMock.mockImplementation(defaultInvoke); listenMock.mockReset(); listenMock.mockResolvedValue(() => {}); });
  it("has every required locale and no missing critical keys", () => {
    expect(locales).toEqual(["zh-Hant", "zh-Hans", "en", "ja", "ko"]);
    for (const locale of locales) {
      for (const key of ["ready", "permissionDeny", "permissionObserve", "permissionUse", "yes", "no", "unknown"] as const) {
        expect(translate(locale, key)).not.toMatch(/^\[missing:/);
      }
    }
  });
  it("has explicit workflow translations for all five locales", () => {
    for (const locale of locales) {
      for (const key of workflowKeys) {
        expect(hasExplicitWorkflowTranslation(locale, key)).toBe(true);
        expect(translate(locale, key)).not.toMatch(/^\[missing:/);
      }
    }
  });

  it("shows safe localized guidance when an active run prevents quitting", async () => {
    render(<App />);
    await waitFor(() => expect(listenMock).toHaveBeenCalledWith("quit-checkpoint-failed", expect.any(Function)));
    const handler = listenMock.mock.calls.find(([name]) => name === "quit-checkpoint-failed")![1];
    act(() => handler({ payload: "raw internal error must not be displayed" }));
    for (const locale of locales) {
      fireEvent.change(screen.getByLabelText("Language"), { target: { value: locale } });
      expect(screen.getByRole("alert")).toHaveTextContent(translate(locale, "quitBlocked"));
      expect(screen.getByRole("alert")).not.toHaveTextContent("raw internal error");
      expect(translate(locale, "quitBlocked")).not.toMatch(/^\[missing:/);
    }
  });
  it("switches locale using an accessible native control", () => {
    render(<App />);
    fireEvent.change(screen.getByLabelText("Language"), { target: { value: "ja" } });
    expect(screen.getByText("M3 Office 垂直スライス")).toBeInTheDocument();
  });

  it("projects daemon state after reconnect", async () => {
    render(<App />);
    expect(await screen.findByText("本機服務已連線")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "診斷" }));
    expect(screen.getByText("7")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveAttribute("data-state", "ready");
  });

  it("exposes autostart as an unchecked opt-in preference", async () => {
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "診斷" }));
    const control = await screen.findByRole("checkbox", { name: /自動啟動/ });
    expect(control).not.toBeChecked();
  });

  it("offers keyboard-native agent and approval navigation", async () => {
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Agents 與模型" }));
    expect(await screen.findByRole("heading", { name: "Agents 與模型" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "核准中心" }));
    expect(screen.getByText("目前沒有待處理或近期核准。")).toBeInTheDocument();
  });

  it("shows the complete immutable identity, scope, and single-use credential binding before approval", async () => {
    invokeMock.mockImplementation((command: string) => command === "approval_center_snapshot"
      ? Promise.resolve({ protocol_version: 1, records: [{ request: {
        id: "approval-1", request_hash: "immutable-hash", expires_at_ms: 1_800_000_000_000,
        action: {
          action_key: "credential.use", reason_key: "Need one provider operation", consequence_key: "A provider request will be made", risk: "high",
          agent_instance_id: "agent-instance-7", role_id: null, requested_policy: { layer: "single_run", ceiling: { filesystem: "deny", network: "observe", process: "unexpected", device: "use", credential: "use", persistent_approval: false } },
          scope: {
            project_id: "project-9", run_id: null, filesystem_roots: ["/workspace/demo"], data_scopes: ["project.documents"], network_destinations: ["api.example.test"], credential_reference_ids: ["credential-ref-3"], destination: null,
            credential_binding: { agent_provider_id: "provider-2", account_id: "account-4", adapter_kind: "codex_cli", provider_identity: "Example Provider", credential_reference_id: "credential-ref-3", backend: "macos_keychain", service: "example.service", account_label: "Work account", capability_key: "provider.chat" },
          },
        },
      }, decision: null }] })
      : defaultInvoke(command));
    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "核准中心" }));
    expect(await screen.findByText("單次使用憑證核准")).toBeInTheDocument();
    for (const value of ["agent-instance-7", "project-9", "/workspace/demo", "project.documents", "api.example.test", "provider-2", "Example Provider", "codex_cli", "account-4", "Work account", "macos_keychain", "example.service", "provider.chat", "僅觀察", "否"]) {
      expect(screen.getByText(value)).toBeInTheDocument();
    }
    expect(screen.getAllByText("拒絕")).toHaveLength(2);
    expect(screen.getAllByText("使用")).toHaveLength(2);
    expect(screen.getAllByText("無")).toHaveLength(3);
    expect(screen.getByText("未知")).toBeInTheDocument();
    expect(screen.queryByText("unexpected")).not.toBeInTheDocument();
    expect(screen.getAllByText("credential-ref-3")).toHaveLength(2);
    expect(screen.getByText(/不會驗證或登入供應商/)).toBeInTheDocument();
    expect(screen.queryByText("Single-use credential approval")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "核准" }));
    expect(invokeMock).toHaveBeenCalledWith("decide_approval", { requestId: "approval-1", requestHash: "immutable-hash", kind: "approve", decidedAtMs: expect.any(Number), credentialScopeAcknowledged: true });
  });

  it("does not acknowledge a credential scope for a legacy approval without a binding", async () => {
    invokeMock.mockImplementation((command: string) => command === "approval_center_snapshot"
      ? Promise.resolve({ protocol_version: 1, records: [{ request: { id: "approval-legacy", request_hash: "legacy-hash", expires_at_ms: 1_800_000_000_000, action: { action_key: "filesystem.read", reason_key: "Read", consequence_key: "Read", risk: "low", agent_instance_id: "agent-1", requested_policy: { layer: "single_run", ceiling: { filesystem: "observe", network: "deny", process: "deny", device: "deny", credential: "deny", persistent_approval: false } }, scope: { project_id: "project-1", filesystem_roots: [], data_scopes: [], network_destinations: [], credential_reference_ids: [] } }, }, decision: null }] })
      : defaultInvoke(command));
    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "核准中心" }));
    fireEvent.click(await screen.findByRole("button", { name: "核准" }));
    expect(invokeMock).toHaveBeenCalledWith("decide_approval", { requestId: "approval-legacy", requestHash: "legacy-hash", kind: "approve", decidedAtMs: expect.any(Number), credentialScopeAcknowledged: false });
  });

  it("cancels only a daemon-projected cancellable run with its catalog revision", async () => {
    invokeMock.mockImplementation((command: string) => command === "work_projection"
      ? Promise.resolve({ revision: 9, sessions: 1, runs: [{ run_id: "run-1", session_id: "session-1", state: "running", can_cancel: true }] })
      : defaultInvoke(command));
    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Agents 與模型" }));
    fireEvent.click(await screen.findByRole("button", { name: "取消" }));

    expect(invokeMock).toHaveBeenCalledWith("cancel_run", { runId: "run-1", expectedCatalogRevision: 9 });
  });

  it("keeps daemon status and cancellation usable while provider discovery is pending", async () => {
    invokeMock.mockImplementation((command: string) => command === "agent_center_snapshot"
      ? new Promise(() => {})
      : command === "work_projection"
        ? Promise.resolve({ revision: 12, sessions: 1, runs: [{ run_id: "owned-run", session_id: "s", state: "running", can_cancel: true }] })
        : defaultInvoke(command));
    render(<App />);
    expect(await screen.findByText("本機服務已連線")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Agents 與模型" }));
    fireEvent.click(await screen.findByRole("button", { name: "取消" }));
    await waitFor(() => expect(invokeMock).toHaveBeenCalledWith("cancel_run", { runId: "owned-run", expectedCatalogRevision: 12 }));
    expect(invokeMock.mock.calls.filter(([command]) => command === "agent_center_snapshot")).toHaveLength(1);
  });

  it("disables duplicate cancellation while awaiting provider acknowledgement", async () => {
    invokeMock.mockImplementation((command: string) => command === "cancel_run"
      ? new Promise(() => {})
      : command === "work_projection"
        ? Promise.resolve({ revision: 12, sessions: 1, runs: [{ run_id: "owned-run", session_id: "s", state: "running", can_cancel: true }] })
        : defaultInvoke(command));
    render(<App />);
    fireEvent.click(screen.getByRole("button", { name: "Agents 與模型" }));
    const cancel = await screen.findByRole("button", { name: "取消" });
    fireEvent.click(cancel);
    expect(cancel).toBeDisabled();
    fireEvent.click(cancel);
    expect(invokeMock.mock.calls.filter(([command]) => command === "cancel_run")).toHaveLength(1);
  });

  it("previews every accessible Pet state before applying the built-in profile", async () => {
    render(<App />);
    expect(await screen.findByRole("heading", { name: "內建 Pet 目錄: Bastet Cat" })).toBeInTheDocument();
    for (const state of ["idle", "thinking", "working", "waiting", "blocked", "approval_required", "succeeded", "failed"]) {
      expect(screen.getByText(state)).toBeInTheDocument();
    }
    fireEvent.click(screen.getByRole("button", { name: "套用 Pet" }));
    expect(invokeMock).toHaveBeenCalledWith("apply_builtin_pet");
  });

  it("disables provider dispatch while long-running graph work is active", async () => {
    invokeMock.mockImplementation((command: string) => command === "m3_projection"
      ? Promise.resolve({ revision: 2, pet_profiles: [], pet_assignments: 3, rooms: 1, meetings: 1, documents: 0, costs: 0, graph_nodes: [{ execution_id: "graph-1", title: "Research A", state: "pending", pet_state: "idle" }], awaiting_meetings: [], joined_drafts: [], missing_output_execution_id: null, document_versions: [], knowledge_deliveries: [] })
      : command === "run_ready_mvp_nodes" ? new Promise(() => undefined) : defaultInvoke(command));
    render(<App />);
    const button = await screen.findByRole("button", { name: "執行已就緒的 Graph 工作" });
    fireEvent.click(button);
    expect(button).toBeDisabled();
    expect(screen.getByText("工作執行中…")).toBeInTheDocument();
  });

  it("shows only a localized safe failure classification and retries the failed node", async () => {
    invokeMock.mockImplementation((command: string) => command === "m3_projection"
      ? Promise.resolve({ revision: 2, pet_profiles: [], pet_assignments: 3, rooms: 1, meetings: 1, documents: 0, costs: 0, graph_nodes: [{ execution_id: "graph-1", node_id: "research-agy", title: "Research A", state: "failed", pet_state: "failed", failure_kind: "provider_timeout", failure_message_key: "provider response: secret internal detail" }], awaiting_meetings: [], joined_drafts: [], missing_output_execution_id: null, document_versions: [], knowledge_deliveries: [] })
      : command === "retry_failed_mvp_node" ? new Promise(() => undefined) : defaultInvoke(command));
    render(<App />);

    expect(await screen.findByText("這項工作失敗，因為供應商逾時。")).toBeInTheDocument();
    expect(screen.queryByText(/secret internal detail/)).not.toBeInTheDocument();
    expect(screen.getByText("只會重試這項失敗工作；成功後，被阻擋的合併會繼續。")).toBeInTheDocument();
    const button = screen.getByRole("button", { name: "重試失敗的工作" });
    fireEvent.click(button);
    expect(invokeMock).toHaveBeenCalledWith("retry_failed_mvp_node", { executionId: "graph-1", nodeId: "research-agy" });
    expect(button).toBeDisabled();
  });

  it("prefills a document from the latest joined draft and keeps manual edits after reconnect", async () => {
    const projection = { revision: 4, pet_profiles: [], pet_assignments: 3, rooms: 1, meetings: 1, documents: 0, costs: 0, graph_nodes: [{ execution_id: "graph-older", title: "Research A", state: "succeeded", pet_state: "succeeded" }, { execution_id: "graph-newer", title: "Join", state: "succeeded", pet_state: "succeeded" }], awaiting_meetings: [], joined_drafts: [{ execution_id: "graph-older", title: "Earlier draft", markdown: "# Earlier" }, { execution_id: "graph-newer", title: "Joined research", markdown: "# Joined\n\nActual graph output" }], missing_output_execution_id: null, document_versions: [], knowledge_deliveries: [] };
    invokeMock.mockImplementation((command: string) => command === "m3_projection" ? Promise.resolve(projection) : defaultInvoke(command));
    render(<App />);

    const title = await screen.findByLabelText("文件標題");
    const markdown = screen.getByLabelText("Markdown 文件");
    expect(title).toHaveValue("Joined research");
    expect(markdown).toHaveValue("# Joined\n\nActual graph output");

    fireEvent.change(title, { target: { value: "Operator title" } });
    fireEvent.change(markdown, { target: { value: "# Operator edit" } });
    fireEvent.click(screen.getByRole("button", { name: "建立合併文件" }));

    expect(invokeMock).toHaveBeenCalledWith("create_mvp_document", { graphExecutionId: "graph-newer", title: "Operator title", markdown: "# Operator edit" });
    await waitFor(() => expect(invokeMock.mock.calls.filter(([command]) => command === "m3_projection").length).toBeGreaterThan(1));
    expect(title).toHaveValue("Operator title");
    expect(markdown).toHaveValue("# Operator edit");
  });

  it("shows pending feedback and submits a prepare request only once", async () => {
    invokeMock.mockImplementation((command: string) => command === "agent_center_snapshot"
      ? Promise.resolve({ agents: [
        { adapter_kind: "codex_cli", display_name: "Codex CLI", installed: true, version: "1.0.0", authenticated: true, model_count: 1, models: ["gpt-test"], reasoning_controls: [], operations: [], error_key: null },
        { adapter_kind: "agy_cli", display_name: "Agy CLI", installed: true, version: "1.0.0", authenticated: true, model_count: 1, models: ["agy-test"], reasoning_controls: [], operations: [], error_key: null },
      ] })
      : command === "prepare_mvp" ? new Promise(() => undefined) : defaultInvoke(command));
    render(<App />);

    fireEvent.change(await screen.findByLabelText("專案名稱"), { target: { value: "Demo" } });
    await screen.findByRole("option", { name: "agy-test" });
    fireEvent.change(screen.getByLabelText("絕對工作目錄"), { target: { value: "/tmp/demo" } });
    fireEvent.change(screen.getByLabelText("Codex 模型"), { target: { value: "gpt-test" } });
    fireEvent.change(screen.getByLabelText("Agy 模型"), { target: { value: "agy-test" } });
    const button = screen.getByRole("button", { name: "準備會議" });
    const form = button.closest("form");
    expect(form).not.toBeNull();
    fireEvent.submit(form!);
    fireEvent.submit(form!);

    expect(button).toBeDisabled();
    expect(screen.getByText("工作執行中…")).toBeInTheDocument();
    expect(invokeMock.mock.calls.filter(([command]) => command === "prepare_mvp")).toHaveLength(1);
    expect(invokeMock).toHaveBeenCalledWith("prepare_mvp", { projectName: "Demo", workspaceRoot: "/tmp/demo", codexModel: "gpt-test", agyModel: "agy-test" });
  });

  it("shows a prepare error beside the form and preserves its entries", async () => {
    invokeMock.mockImplementation((command: string) => command === "agent_center_snapshot"
      ? Promise.resolve({ agents: [
        { adapter_kind: "codex_cli", display_name: "Codex CLI", installed: true, version: "1.0.0", authenticated: true, model_count: 1, models: ["gpt-test"], reasoning_controls: [], operations: [], error_key: null },
        { adapter_kind: "agy_cli", display_name: "Agy CLI", installed: true, version: "1.0.0", authenticated: true, model_count: 1, models: ["agy-test"], reasoning_controls: [], operations: [], error_key: null },
      ] })
      : command === "prepare_mvp" ? Promise.reject(new Error("unavailable")) : defaultInvoke(command));
    render(<App />);

    const projectName = await screen.findByLabelText("專案名稱");
    await screen.findByRole("option", { name: "agy-test" });
    const workspaceRoot = screen.getByLabelText("絕對工作目錄");
    fireEvent.change(projectName, { target: { value: "Demo" } });
    fireEvent.change(workspaceRoot, { target: { value: "/tmp/demo" } });
    fireEvent.change(screen.getByLabelText("Codex 模型"), { target: { value: "gpt-test" } });
    fireEvent.change(screen.getByLabelText("Agy 模型"), { target: { value: "agy-test" } });
    fireEvent.click(screen.getByRole("button", { name: "準備會議" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("操作結果無法確認。請先重新確認狀態，再決定是否重試。");
    expect(projectName).toHaveValue("Demo");
    expect(workspaceRoot).toHaveValue("/tmp/demo");
    expect(screen.getByLabelText("Codex 模型")).toHaveValue("gpt-test");
    expect(screen.getByLabelText("Agy 模型")).toHaveValue("agy-test");
    await waitFor(() => expect(screen.getByRole("button", { name: "準備會議" })).toBeEnabled());
  });
});
