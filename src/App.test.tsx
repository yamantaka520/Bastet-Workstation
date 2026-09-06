import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";
import { hasExplicitWorkflowTranslation, locales, translate, workflowKeys } from "./i18n";

const { invokeMock } = vi.hoisted(() => ({ invokeMock: vi.fn() }));

const defaultInvoke = (command: string) => Promise.resolve(command === "approval_center_snapshot"
    ? { protocol_version: 1, records: [] }
    : command === "agent_center_snapshot"
      ? { agents: [{ adapter_kind: "codex_cli", display_name: "Codex CLI", installed: true, version: "1.0.0", authenticated: true, model_count: 2, models: ["gpt-test"], reasoning_controls: ["low", "high"], operations: ["start", "cancel"], error_key: null }] }
      : command === "work_projection"
        ? { revision: 3, sessions: 1, runs: [] }
      : command === "m3_projection"
        ? { revision: 0, pet_profiles: [], pet_assignments: 0, rooms: 0, meetings: 0, documents: 0, costs: 0, graph_nodes: [], awaiting_meetings: [], document_versions: [], knowledge_deliveries: [] }
      : { protocol_version: 1, daemon_id: "test-daemon", revision: 7, lifecycle: "ready" });

vi.mock("@tauri-apps/api/core", () => ({ invoke: invokeMock }));
vi.mock("@tauri-apps/plugin-autostart", () => ({
  disable: vi.fn().mockResolvedValue(undefined),
  enable: vi.fn().mockResolvedValue(undefined),
  isEnabled: vi.fn().mockResolvedValue(false),
}));

describe("M1 shell", () => {
  beforeEach(() => invokeMock.mockImplementation(defaultInvoke));
  it("has every required locale and no missing critical keys", () => {
    expect(locales).toEqual(["zh-Hant", "zh-Hans", "en", "ja", "ko"]);
    for (const locale of locales) expect(translate(locale, "ready")).not.toMatch(/^\[missing:/);
  });
  it("has explicit workflow translations for all five locales", () => {
    for (const locale of locales) {
      for (const key of workflowKeys) {
        expect(hasExplicitWorkflowTranslation(locale, key)).toBe(true);
        expect(translate(locale, key)).not.toMatch(/^\[missing:/);
      }
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

  it("cancels only a daemon-projected cancellable run with its catalog revision", async () => {
    invokeMock.mockImplementation((command: string) => command === "work_projection"
      ? Promise.resolve({ revision: 9, sessions: 1, runs: [{ run_id: "run-1", session_id: "session-1", state: "running", can_cancel: true }] })
      : defaultInvoke(command));
    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Agents 與模型" }));
    fireEvent.click(await screen.findByRole("button", { name: "取消" }));

    expect(invokeMock).toHaveBeenCalledWith("cancel_run", { runId: "run-1", expectedCatalogRevision: 9 });
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
      ? Promise.resolve({ revision: 2, pet_profiles: [], pet_assignments: 3, rooms: 1, meetings: 1, documents: 0, costs: 0, graph_nodes: [{ execution_id: "graph-1", title: "Research A", state: "pending", pet_state: "idle" }], awaiting_meetings: [], document_versions: [], knowledge_deliveries: [] })
      : command === "run_ready_mvp_nodes" ? new Promise(() => undefined) : defaultInvoke(command));
    render(<App />);
    const button = await screen.findByRole("button", { name: "執行已就緒的 Graph 工作" });
    fireEvent.click(button);
    expect(button).toBeDisabled();
    expect(screen.getByText("工作執行中…")).toBeInTheDocument();
  });
});
