"""
Pipeline context objects for ind-bridge V4.

PipelineCtx — shared mutable state passed through every plugin invocation
              during a request, materialised per-identity.
StartupCtx  — passed to plugin `setup_listener` capability methods during
              identity-load (one-time per identity-with-listener).

V4 dispatch model
-----------------
Plugins declare a ``CAPABILITIES`` dict in code. The plugin loader cross-
references config-declared placements (slots) against the plugin's capability
table. The pipeline assembler walks the resolved cascade and produces a flat
ordered list of ``(slot, plugin, resolved_config)`` tuples — that's the
identity's pipeline. The executor walks the tuples and invokes the matching
capability method on each plugin.

Capability methods (each capability has its own method on the plugin module):

  setup_listener(ctx: StartupCtx, config: dict) -> None | RouteHandle
      Called once at identity-load when ``listener`` is on a plugin in
      ``identity.plugins``. Materialises an inbound entry point.

  apply_outbound_params(ctx: PipelineCtx, config: dict) -> PipelineCtx
      Configures the outbound call (headers, model, auth, url) before the
      resource is hit. Fires for plugins declaring ``outbound_params``.

  modify_context(ctx: PipelineCtx, config: dict) -> PipelineCtx
      Inbound body modification. Fires for plugins declaring ``context_modify``
      in ``*.context.plugins`` slots.

  modify_response(ctx: PipelineCtx, config: dict) -> PipelineCtx
      Outbound body modification. Fires for plugins declaring
      ``response_modify`` in ``*.response.plugins`` slots.

  produce_response(ctx: PipelineCtx, config: dict) -> PipelineCtx
      Plugin IS the resource — terminates the pipeline, no outbound call.
      Fires for plugins declaring ``produce_response`` in ``resource.plugins``.

See ``~/Documents/ind-v4-brainstorm.md`` (*The Plugin Capability Contract*)
for the full contract, and ``CLAUDE.md`` for the cheat-sheet.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import FastAPI
    from types import ModuleType
    from .dev_trace import Trace


# ---------------------------------------------------------------------------
# PipelineCtx sub-objects
# ---------------------------------------------------------------------------

@dataclass
class RequestInfo:
    """The inbound request — both original and the working copy plugins modify."""

    original_messages: list[dict]
    """Raw messages from the client. NEVER mutated after construction.
    Session / memory plugins use this to see what the user actually said,
    regardless of what other plugins have done to the working copy."""

    messages: list[dict]
    """Working copy. Plugins mutate this freely. This is what gets forwarded
    to the resource (or short-circuited by `produce_response`)."""

    model: str
    """Model string from the client request body. May be overridden by
    `outbound_params` plugins via the cascade."""

    stream: bool
    """Whether the client requested a streaming response."""

    tools: list[dict]
    """Tool definitions for the outbound request. Plugins add to this list to
    inject bridge-native tools alongside any tools the client sent."""

    raw_body: dict
    """Full original request body — read-only reference. Use this to pass
    fields to the backend that the pipeline doesn't inspect."""


@dataclass
class IdentityInfo:
    """Resolved identity from config — who is making this request."""

    key: str
    """Config key, e.g. "harness_alice"."""

    name: str | None = None
    """Cascade-merged from `context.name` (identity overrides role).
    Becomes <caller>name</caller> via `bridge_sign.populate_caller`."""

    trust: str | None = None
    """Cascade-merged from `context.trust` (identity overrides role) —
    e.g. "trusted", "agent", "system"."""

    additional: dict = field(default_factory=dict)
    """Cascade-merged from `context.additional` — role keys provide
    defaults, identity keys overlay (identity wins per key). Flattens
    into XML attributes on <caller>."""


@dataclass
class RoleInfo:
    """Resolved role — what capabilities and resources this identity uses."""

    key: str
    """Config key, e.g. "alice_main"."""

    resource_key: str
    """The resource this role routes to."""

    session_key: str | None = None
    """The session this role uses, or None if the resource owns the session
    (or the pipeline is stateless). Sessions are optional in V4."""


@dataclass
class ResourceInfo:
    """Resolved resource — where the pipeline terminates."""

    key: str
    """Config key, e.g. "openrouter"."""

    endpoint_url: str | None = None
    """Populated by the `outbound_params` capability of the resource's
    transport plugin (e.g. `openai-protocol`). If still None after assembly
    AND no `produce_response` plugin is on the resource, the pipeline returns
    503 with a clear message at request time."""

    endpoint_token: str | None = None
    """Bearer token for the backend endpoint. Populated by the same plugin
    that sets `endpoint_url`."""

    is_terminal: bool = False
    """True iff a plugin declaring `produce_response` is wired onto this
    resource. The executor short-circuits outbound transport when True —
    no `outbound_params` plugins fire, no network call happens, the plugin's
    response IS the response."""


# ---------------------------------------------------------------------------
# PipelineCtx — the main per-request context object
# ---------------------------------------------------------------------------

@dataclass
class PipelineCtx:
    """
    Shared mutable state for a single request pipeline pass.

    Materialised per-identity (D-001). The executor walks `pipeline_tuples`
    and invokes the matching capability method on each plugin, passing this
    ctx in and receiving it back (mutated).

    For plugins: receive ctx, optionally modify it, return it. Returning None
    is treated as "pass through unchanged" (defensive — most plugins should
    return ctx).
    """

    # ── Resolved identity chain (set during pipeline assembly) ──────────────
    identity: IdentityInfo
    role: RoleInfo
    resource: ResourceInfo

    # ── The request ─────────────────────────────────────────────────────────
    request: RequestInfo

    # ── The assembled pipeline (D-001) ──────────────────────────────────────
    pipeline_tuples: list[tuple[str, "ModuleType", dict]] = field(default_factory=list)
    """
    The flat ordered list the executor walks. Each tuple is:
        (slot_name, plugin_module, resolved_config)

    where `slot_name` is one of:
        "identity.plugins" / "role.plugins" / "session.plugins" /
        "resource.plugins" / "identity.context.plugins" /
        "role.context.plugins" / "session.context.plugins" /
        "identity.response.plugins" / "role.response.plugins" /
        "session.response.plugins"

    Built by pipeline_assembler from the resolved cascade. The executor
    dispatches based on slot family → capability method:
        *.plugins (top-level)         → apply_outbound_params  /
                                        produce_response (resource only) /
                                        setup_listener (identity only, at load)
        *.context.plugins             → modify_context
        *.response.plugins            → modify_response
    """

    # ── Plugin output surfaces ──────────────────────────────────────────────
    bridge_context: dict[str, Any] = field(default_factory=dict)
    """
    Context contributions assembled into the signed <bridge_context> XML
    block by core (not by a plugin) and injected before the resource sees
    the request. See `bridge_context.py` (TODO).

    Keys become XML tags:
        ctx.bridge_context["current_time"] = "Saturday, 3rd May 2026"
        → <current_time>Saturday, 3rd May 2026</current_time>

    Order is insertion order. Whole block is HMAC-signed by core when
    BRIDGE_SIGN_SECRET is set.
    """

    plugin_data: dict[str, Any] = field(default_factory=dict)
    """
    Free-form plugin-to-plugin communication namespace. Core never reads
    this; it exists purely for plugins to coordinate. Convention: namespace
    your keys, e.g. ``ctx.plugin_data["memory_recall.results"] = [...]``.
    """

    # ── Resolved environment ────────────────────────────────────────────────
    timezone: str = "UTC"
    """IANA timezone string resolved from the cascade. Plugins should read
    this rather than re-resolving from config, unless they have explicit
    plugin-level timezone config."""

    max_tool_laps: int = 1
    """Runaway guardrail for the bridge's agentic tool loop (the
    ``handle_tool_calls`` intercept loop in pipeline_executor). The number of
    re-call laps the bridge resolves bridge-native tools in-band before giving
    up. It is NOT a feature leash — the never-leak guard already makes "the
    model never stops" safe — it's purely a cost/latency runaway bound.

    ``0`` means **uncapped**: the loop runs until the model stops calling tools
    (``finish_reason != tool_calls``). Resolved at ctx-build time top-down from
    the cascade (``identity → role → session → resource → server``), falling
    back to the ``BRIDGE_HANDLE_TOOL_CALLS_MAX_LAPS`` env var, then to 1. A
    bare scalar config key (``max_tool_laps:``), not a plugin — same shape as
    ``timezone``."""

    headers: dict[str, str] = field(default_factory=dict)
    """Sanitised inbound headers, lowercased keys. Hop-by-hop headers
    stripped at intake."""

    # ── Backend response (populated post-resource, pre-response.* slots) ────
    response: dict | None = None
    """
    Set by the executor after the resource step (network call OR
    `produce_response` short-circuit). Shape:
        {"role": "assistant", "content": "<text>"}

    May be None if response parsing failed. Plugins running in
    `*.response.plugins` slots should defend against None.
    """

    # ── Diagnostics ─────────────────────────────────────────────────────────
    slots_visited: list[str] = field(default_factory=list)
    """
    Append-only list of slot names visited by the executor, in order.
    Diagnostic only — useful for tracing pipeline flow when debugging.
    Not for plugin coordination (use plugin_data for that).
    """

    dev_trace: "Trace | None" = None
    """Per-request dev-trace handle when ``BRIDGE_DEV_TRACE=1`` is set.
    Plugins may call ``dev_trace.event(ctx.dev_trace, "annotate", ...)``
    to leave breadcrumbs that appear inline with the executor's own
    events in the trace log. None when tracing is disabled (the
    overwhelming default), so ``event()`` calls become no-ops."""


# ---------------------------------------------------------------------------
# StartupCtx — passed to setup_listener during identity-load
# ---------------------------------------------------------------------------

@dataclass
class StartupCtx:
    """
    Context for the `setup_listener` capability — invoked once per identity
    that has a listener-capability plugin on its `identity.plugins` slot.

    Listener plugins call ``ctx.app.add_api_route(...)`` (or any other
    framework-specific mechanism) to materialise their entry point.

    Per the Plugin Capability Contract: ``listener`` is only valid on
    ``identity.plugins``. Listener setup is per-identity, not global.
    """

    app: "FastAPI"
    """The FastAPI application instance. Listener plugins register routes
    here. Core only registers /health; everything else is plugin-driven."""

    server_cfg: dict
    """Full server: config block (env vars expanded). Plugins can read
    server-level defaults from here via the cascade."""

    identity_key: str
    """The identity this setup_listener call is for. Plugins should scope
    their route registration so that identity-specific config is honoured
    (e.g. a per-identity URL prefix, or a per-identity auth check)."""

    identity_cfg: dict
    """The resolved (cascade-merged) config for this identity, including
    `plugins:`, `context:`, `response:`, `role:`, `token:`, etc. Listener
    plugins typically read their own slot config here."""
