# ADR 0003: Stable identity and extension contracts

- Status: Accepted
- Date: 2026-09-02
- Authority: Master Plan sections 5 and 7

## Decision

All durable entities use stable opaque IDs, revisions, lifecycle state, and provenance. Agent Adapter, Canonical Skill IR, and Capability Provider are separate versioned contracts with explicit events, permissions, cancellation, errors, cost, artifacts, evidence, health checks, and conformance tests.

## Consequences

Provider-specific facts and inferred state remain distinguishable. New Agents and capabilities enter through contracts rather than product-specific exceptions. Codex CLI and Agy CLI are the M2 reference adapters; other providers follow the roadmap.
