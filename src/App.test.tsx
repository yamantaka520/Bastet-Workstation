import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { App } from "./App";
import { locales, translate } from "./i18n";

vi.mock("@tauri-apps/api/core", () => ({
  invoke: vi.fn((command: string) => Promise.resolve(command === "approval_center_snapshot"
    ? { protocol_version: 1, records: [] }
    : command === "agent_center_snapshot"
      ? { agents: [{ adapter_kind: "codex_cli", display_name: "Codex CLI", installed: true, version: "1.0.0", authenticated: true, model_count: 2, reasoning_controls: ["low", "high"], operations: ["start", "cancel"], error_key: null }] }
      : { protocol_version: 1, daemon_id: "test-daemon", revision: 7, lifecycle: "ready" })),
}));
vi.mock("@tauri-apps/plugin-autostart", () => ({
  disable: vi.fn().mockResolvedValue(undefined),
  enable: vi.fn().mockResolvedValue(undefined),
  isEnabled: vi.fn().mockResolvedValue(false),
}));

describe("M1 shell", () => {
  it("has every required locale and no missing critical keys", () => {
    expect(locales).toEqual(["zh-Hant", "zh-Hans", "en", "ja", "ko"]);
    for (const locale of locales) expect(translate(locale, "ready")).not.toMatch(/^\[missing:/);
  });
  it("switches locale using an accessible native control", () => {
    render(<App />);
    fireEvent.change(screen.getByLabelText("Language"), { target: { value: "ja" } });
    expect(screen.getByText("M2 Agent と承認")).toBeInTheDocument();
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
    expect(await screen.findByRole("heading", { name: "Agents 與模型" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "核准中心" }));
    expect(screen.getByText("目前沒有待處理或近期核准。")).toBeInTheDocument();
  });
});
