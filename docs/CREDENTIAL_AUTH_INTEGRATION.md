# Credential authentication integration boundary

Status: investigated, not enabled (2026-09-16). `MASTER_PLAN.md` remains the
authority; this note records evidence needed before M2 authenticated dispatch.

## Provider contract evidence

Official [Codex authentication documentation](https://learn.chatgpt.com/docs/auth)
distinguishes strict `cli_auth_credentials_store = "keyring"` (failure when the
OS store is unavailable), `auto` (may fall back to a file), and process-only
`ephemeral`. Managed ChatGPT sign-in owns token refresh. Workstation's secret-only
OS-store requirement rules out silently selecting `auto` or copying `auth.json`.

The [App Server contract](https://learn.chatgpt.com/docs/app-server) distinguishes
managed browser/device-code login, API-key login, and experimental external
ChatGPT tokens. External tokens require a host that already owns the user's auth
lifecycle and answers refresh requests; a raw keychain value is insufficient.
The documented account-read examples expose auth type and optional email/plan,
not proof of equality to Workstation's internal AccountId. An API-key login is
not a substitute for a user's ChatGPT subscription.

## Current repository evidence

`native_credentials.rs` reads an exact Workstation-owned locator. It does not
know the provider secret format, refresh lifecycle, or provider-managed keyring
layout. `credential_dispatch.rs` atomically consumes a run grant before lookup;
neither module is wired to production selected-account execution. These are
necessary boundaries, not an implemented provider authentication scheme.

Do not add a speculative token JSON decoder, read the user's existing login file,
infer identity from an email/display label, or turn on external-token mode just
because a native reader can return bytes.

## Next integration requirements

- Resolve which Codex login mode to prioritize with the user; no real login or
  API spend is authorized by that implementation preference alone.
- For managed sign-in, prove per-account profile isolation, strict OS storage,
  exact account binding, token renewal, cancellation and no ambient-profile
  fallback against the supported CLI version before dispatch. Define how a
  provider-managed credential reference relates to the current broker contract;
  do not pretend its locator is the generic reader's version-1 namespace.
- For API keys, obtain the explicit credential decision before API-backed work;
  use a provider-specific, secret-safe injection path and prevent unintended
  persistence, logging, or inherited environment selection.
- Reject unsupported backend/format/capability before spending a one-use grant.
  After a successful claim, preserve the existing no-refund/no-replay behavior.
- Validate each enabled path with synthetic transport/store fixtures, then the
  separately authorized real-provider gates. Preserve the M2 sandbox and setup
  requirements; successful authentication alone does not close the milestone.

No existing credentials were inspected to establish this note. The public
documentation is current evidence, not a claim about the installed CLI version.
