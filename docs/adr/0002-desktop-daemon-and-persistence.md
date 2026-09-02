# ADR 0002: Desktop, daemon, and persistence authority

- Status: Accepted
- Date: 2026-09-02
- Authority: Master Plan sections 4, 6, and 12

## Decision

Use a Tauri 2 shell with React and TypeScript. A Rust local daemon is the sole lifecycle and durable-state authority, backed by SQLite WAL and an append-only event journal. The UI is a command/event client and never owns Agent processes.

## Consequences

Closing a window may leave approved work running in the tray; explicit Quit performs bounded checkpoint and shutdown. Protocol versions must negotiate safely. Recovery reconciles persisted uncertain state with processes, workspaces, providers, and receipts before retrying.
