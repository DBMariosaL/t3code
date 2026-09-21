# Provider constraints

Orchestration records intent and state without knowing which provider runs a thread. Provider
protocols, account ownership, permissions, and capabilities belong at the
[adapter boundary](../../apps/server/src/provider/Services/ProviderAdapter.ts). Normalize there
instead of spreading provider checks through reactors and clients.

A driver kind identifies an integration; an instance identifies one configuration and account
lifecycle. Route work by instance, so two accounts using the same driver do not share mutable
session or catalog state.

## Process and account isolation

T3-managed OpenCode chat uses one server per thread. Its MCP registrations are directory-scoped, while
T3's MCP connection is thread-scoped. Sharing a chat server between threads in one directory would
let them replace each other's connection. Catalog and text-generation work can share the
[instance-owned helper](../../apps/server/src/provider/OpenCodeServerOwner.ts), which closes
after an idle period. External OpenCode servers remain externally owned and can require an
external restart to pick up configuration changes.

OpenCode also stores persistent approval grants per directory. Automatic full-access replies use
`once` so they cannot widen a supervised thread's permissions on a shared external server.
See the [adapter](../../apps/server/src/provider/Layers/OpenCodeAdapter.ts).

Antigravity separates account profiles per instance while sharing installed executables across the
environment. It forces file-based credential storage because the native macOS keychain entry would
otherwise be shared across instances. The launch environment removes ambient Google credentials,
so an instance cannot silently use another account or billing project. The agent also resolves
its user-global skill directories under that profile, so the profile links those two directories
back to the user's real `~/.gemini`; MCP servers, hooks, and rules there stay out of the profile.
See [profile isolation](../../apps/server/src/provider/antigravityAuthSupport.ts).

The [Antigravity installer](../../apps/server/src/provider/AntigravityInstallation.ts) outlives
client connections and provider-instance rebuilds. Releases are immutable, with an atomic pointer
selecting the version for new processes. Running processes hold leases on their version. Updates
and removal must respect those leases instead of replacing executables under a running agent.

### Pi RPC discovery

`apps/server/src/provider/pi/` contains Pi's discovery transport, session runtime, and adapter.
The driver is opt-in and requires Pi 0.82.0 or newer. Chat currently accepts only full-access
text sessions. Protected runtime modes, attachments, plan mode, and T3 MCP tools are unsupported.

Discovery owns a scoped `pi --mode rpc --no-session --no-extensions` process. It keeps stdin
open until replies to `get_state`, `get_available_models`, and `get_commands` arrive, correlates
each reply by request ID and command, and closes the process on success, failure, timeout, or
cancellation. Extension commands are consequently absent from this discovery mode.

The decoder preserves event payloads but rejects malformed response envelopes. Model IDs use
`provider/id` because different Pi model providers can expose the same ID. Discovery does not
prove authentication and a reasoning flag does not establish supported thinking levels, so
neither is inferred from the model list.

On Pi 0.85.0 and newer, a second scoped discovery process selects each available model and
queries `get_available_thinking_levels` in order. Automatic extensions remain disabled. Only
successful probes advertise `reasoningEffort`; native `openai-responses` and
`openai-codex-responses` APIs also advertise `serviceTier`. Older versions keep the preview's
basic controls. RPC model changes in the probed version affect the disposable session only.

The adapter uses the existing per-thread `ModelSelection.options` persistence, validates
reasoning through RPC, and reads back the effective state. The bundled extension reads a
session-owned options file in `before_provider_request` to add priority or remove the service
tier field. Reload reuses this file; restarting recreates it from the thread selection. No
global Pi settings or third-party extensions are written. Fast represents a request for
priority, not proof that the upstream service granted it. Other Pi extensions can modify
the request after T3's hook.

Run the focused baseline from the repository root with:

```sh
node node_modules/vite-plus/bin/vp test run apps/server/src/provider/Layers/ProviderInstanceRegistryLive.test.ts apps/server/src/provider/pi apps/server/src/textGeneration/PiTextGeneration.test.ts
```

`PiOptions.integration.test.ts` additionally runs the installed Pi CLI when
`T3_PI_TEST_CLI` names its JavaScript entry point. It uses disposable configuration and a
loopback HTTP fixture, including Codex's compressed request body, so it does not spend API
credits. It checks defaults, reasoning, priority, refusal, reload, resume, and return to normal
through both native Responses implementations. Real client and upstream account validation
remain separate from this transport test. `.t3-pi-test/` is ignored test state.

Each conversation owns one process and a temporary T3 extension file. Request IDs correlate RPC
replies independently of the event consumer, so extension dialogs do not block transport reads.
`agent_settled`, rather than `agent_end`, settles the T3 turn after retries and queued continuations.
An event-consumer barrier prevents the adapter from reporting reload success before processing an
extension error. A lost process fails pending requests; restarting uses the saved session file.

`/reload` invokes the bundled `t3-reload` extension command, which awaits Pi's `ctx.reload()`.
This reloads resources inside the existing Pi process. The adapter rejects reload while a turn
is active, verifies that the bridge loaded at startup, and preserves the session cursor.
Pi's `fork` command implements rollback. Text-generation helpers use isolated, non-persistent
sessions with tools and automatic extension, skill, and prompt-template loading disabled.

## Setup must not happen as a health-check side effect

Opening a provider session can start MCP servers, run hooks, or launch a login browser.
[Grok probes](../../apps/server/src/provider/Layers/GrokProvider.ts) avoid authentication and
session creation for this reason. Antigravity likewise reserves authenticated catalog sessions for
explicit setup or model refresh; background checks use initialization only.

[Antigravity sign-in](../../apps/server/src/provider/AntigravityAuth.ts) belongs to the initiating
T3 auth session. The client carries the return URL back to the environment because the provider's
loopback listener may be on another machine. Forward only the callback for the owned pending flow;
a successful callback HTTP request is not proof that provider authentication finished. The native
process owns token exchange and storage.

Antigravity sign-out closes admission to new processes and stops existing processes before clearing account
metadata. Otherwise a helper or resumed session could retain the old account. Cached model lists
do not establish current access, and an authoritative empty catalog must clear the old list.

Antigravity text-generation helpers deny tool requests, but native hooks and MCP configuration can
run before the prompt. They reject profiles with such configuration before launch. Prompt
instructions and tool denial do not create a native sandbox.
See [helper constraints](../../apps/server/src/textGeneration/AntigravityTextGeneration.ts).

## Provider updates run only through the owning installer

A one-click update is offered only when the resolved executable's path proves which installer owns
it. Homebrew and npm are proven by the real path (symlinks followed): a versioned keg or cask under
`brew --prefix`, or `<prefix>/lib/node_modules/<pkg>/` (Windows: the shim beside `node_modules`).
Native installer layouts and the global bin directories of pnpm, Bun, and Vite+ may match on either
the resolved path or its real target, since those installers place real files or their own symlinks
there. Anything unproven stays manual-only but still reports the version gap. npm updates pin
`--prefix` because the `npm` on `PATH` can belong to a different Node than the one that owns the
provider. Homebrew
compares against `brew info` since casks trail npm by hours; native installs share npm's version
train, so the registry stays authoritative for them.
See the [resolver](../../apps/server/src/provider/providerMaintenance.ts).

Ownership is cached per instance and re-read immediately before an update runs. The
[runner](../../apps/server/src/provider/providerMaintenanceRunner.ts) refuses when the lock key
changed since the advisory, and reports success only when the refreshed provider is still installed
with a readable, current version.

## Protocol traps

Codex async questions arrive as notifications and are answered with a new user message. There is
no pending RPC response to send. Blocking questions still use the request/response path. The
[adapter](../../apps/server/src/provider/Layers/CodexAdapter.ts) distinguishes them; the
[decider](../../apps/server/src/orchestration/decider.ts) records an async answer and its user
message together.

An async question can outlive the turn or a server restart. The engine reads that request's
durable activity before resolving it because the in-memory command snapshot omits old activities.
Do not infer that a request has disappeared merely because it is outside the recent window.

Capabilities must describe what the provider can actually do. Antigravity can capture workspace
checkpoints but cannot roll back its conversation. The [checkpoint boundary](./overview.md#turn-completion-and-checkpoints)
therefore rejects revert before touching files. Native permission and question option IDs must
also survive normalization; a display label is not necessarily a valid reply.

## Attachments and stored history

Attachments live outside the project workspace. [ProviderService](../../apps/server/src/provider/Layers/ProviderService.ts)
puts their environment-local paths in turn input and lets adapters choose native input formats.
A path in the prompt does not grant filesystem access. Keep provider sandbox and approval rules
in force; copying uploads into the project to bypass them changes that boundary.

File attachments introduced a replay compatibility limit. Image-only clients cannot decode
file-bearing messages, and an image-only server can fail the entire environment's startup when
replaying one such event. Rollouts and downgrades must account for persisted history as well as
current client support.

Model classification has its own [manifest constraints](./model-manifest.md). Assistant-reference
handling is documented under [citations](./assistant-citations.md).
