import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { App } from "./App";
import { locales, translate } from "./i18n";

vi.mock("@tauri-apps/api/core", () => ({
  invoke: vi.fn().mockResolvedValue({
    protocol_version: 1,
    daemon_id: "test-daemon",
    revision: 7,
    lifecycle: "ready",
  }),
}));
vi.mock("@tauri-apps/plugin-autostart", () => ({
  disable: vi.fn().mockResolvedValue(undefined),
  enable: vi.fn().mockResolvedValue(undefined),
  isEnabled: vi.fn().mockResolvedValue(false),
}));

describe("M1 shell", () => {
  it("has every required locale and no missing critical keys", () => {
    expect(locales).toEqual(["zh-Hant", "zh-Hans", "en", "ja", "ko"]);
    for (const locale of locales) expect(translate(locale, "status")).not.toMatch(/^\[missing:/);
  });
  it("switches locale using an accessible native control", () => {
    render(<App />);
    fireEvent.change(screen.getByLabelText("Language"), { target: { value: "ja" } });
    expect(screen.getByText("M1 デスクトップとデーモン基盤")).toBeInTheDocument();
  });

  it("projects daemon state after reconnect", async () => {
    render(<App />);
    expect(await screen.findByText("本機服務已連線")).toBeInTheDocument();
    expect(screen.getByText("7")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveAttribute("data-state", "ready");
  });

  it("exposes autostart as an unchecked opt-in preference", async () => {
    render(<App />);
    const control = await screen.findByRole("checkbox", { name: /自動啟動/ });
    expect(control).not.toBeChecked();
  });
});
