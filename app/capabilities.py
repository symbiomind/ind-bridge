"""
Capability contract validator for ind-bridge V4.

This module is the *trust boundary* between "config says X" and "X actually
happens." It cross-references every plugin reference in the loaded config
against each plugin's declared ``CAPABILITIES`` table, plus walks the
config tree itself looking for ill-formed slots (typos, V3 leftovers,
shapes that don't exist in V4).

Per **D-001** (capability contract), **D-002** (`produce_response`), and
**D-003** (identity is passive; the contract is final), the validator is
read-only: it produces a structured ``ValidationReport`` and a friendly
human-readable summary. Callers (``server.py``) decide what to log and
whether to act on warnings.

Outcomes per plugin placement:

    ok          — plugin loaded; CAPABILITIES has a capability whose valid-
                  slot list contains this slot. Usable in pipeline assembly.
    misplaced   — plugin loaded; declares CAPABILITIES; but no capability
                  in its table lists this slot as valid. Will be skipped.
    uncapable   — plugin loaded but exports no CAPABILITIES dict (stub /
                  malformed plugin).
    legacy_v3   — plugin loaded; declares only V3-style SUPPORTED_HOOKS.
                  Pending V4 port.
    unknown     — plugin name in config, not in registry. Probably not
                  yet ported, possibly a typo.

See ``CLAUDE.md`` for the architecture cheat-sheet and
``~/Documents/ind-v4-brainstorm.md`` for the full spec.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from . import config, plugin_loader

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# THE CANONICAL CONTRACT — single source of truth
# ---------------------------------------------------------------------------

KNOWN_CAPABILITIES: dict[str, list[str]] = {
    "listener":          ["identity.plugins"],
    "background":        ["identity.plugins"],
    "outbound_params":   ["identity.plugins", "role.plugins",
                          "session.plugins", "resource.plugins"],
    "context_modify":    ["identity.context.plugins",
                          "role.context.plugins"],
    "outbound_normalize": ["identity.context.plugins",
                          "role.context.plugins"],
    "response_modify":   ["identity.response.plugins",
                          "role.response.plugins"],
    "produce_response":  ["resource.plugins"],
    "post_response":     ["identity.plugins", "role.plugins",
                          "session.plugins",
                          "identity.context.plugins",
                          "role.context.plugins"],
    "handle_tool_calls": ["identity.plugins", "role.plugins"],
}
"""The capabilities a V4 plugin may declare and the slots in which
each capability is valid. Nine valid slot strings total (per D-006 —
sessions have only `session.plugins`, not session.context.plugins or
session.response.plugins; sessions are resource-shaped, not role-shaped).
This is the canonical contract from the brainstorm; any other module
that needs to reason about valid placements imports from here.

`background` (D-011) is the *spawn-a-loop-at-startup* capability — the
generic cousin of `listener`. Where `listener` materialises an HTTP route,
`background` materialises a long-running asyncio task that runs OUTSIDE the
request cycle (a polling daemon, a cron-like ticker). Core calls the
plugin's ``start_background(StartupCtx, config)`` once during lifespan
startup, schedules the returned coroutine on the event loop, and tracks the
task so lifespan shutdown can cancel it cleanly. Like `listener` it is
per-identity (only valid on `identity.plugins`) — the identity that wires
the plugin is the one whose loop spawns. First consumer: ``memory_enricher``
(background label enrichment). Future consumers ride the same seam:
cron-triggered identities (Agent_Dream), the bridge originating its own
messages. The V3 ``server.startup`` hook did NOT survive the rewrite; this
is its V4-shaped, capability-dispatched replacement.

`post_response` (D-007) is the *observe-after-delivery* capability: fires
in a background task after the response leaves the bridge for the client,
on streaming AND non-streaming paths. Plugins receive the assembled
assistant turn (text content, tool_calls, reasoning_content, finish_reason)
and must not block, must not raise. Used for memory storage, audit logging,
metrics emission, follow-up nudges — anything that needs to *see* what
was sent without delaying the user.

`post_response` is *also* permitted in `*.context.plugins` slots, not only
`*.plugins`. This lets a plugin pairing `context_modify` (recall) and
`post_response` (store) — e.g. ``conversational_memory`` — be declared
ONCE under `*.context.plugins` with both capabilities reading the same
config dict. Lifecycle is unchanged: `post_response` still fires after
delivery, fire-and-forget. Only the config-surface unifies — operator UX
wins UX questions. Validator still warns loud about a `post_response`-only
plugin (no `context_modify`) placed in a context slot — a context slot
implies inbound work, and a plugin that only observes after delivery
probably wants `*.plugins`.

`post_response` is *also* permitted in `session.plugins` (D-010). A session
plugin's whole job is conversation state — and *saving the turn is conversation
state*. Pairing `outbound_params` (load history, rebuild messages, stamp
``session_state``) with `post_response` (append the closed turn after delivery)
lets a session plugin — e.g. ``basic_session`` — live WHOLLY in one
``sessions.<name>.plugins`` block rather than being split across
`session.plugins` (load) and `role.plugins` (save). Same operator-UX-wins
rationale as the `*.context.plugins` pairing above. The executor already
collects observers by capability table, not slot string
(``_collect_observer_tuples`` / ``_is_post_response_tuple``), so a
`post_response` plugin in `session.plugins` fires with no executor change.

`handle_tool_calls` (D-008) is the *intercept-before-delivery* capability:
fires after the resource step assembles a frame, claims tool_calls by
name (the plugin's module-level ``OWNED_TOOLS`` list), executes them
locally, and the executor re-calls the resource so the agent reacts in
their own voice. Pre-delivery (blocks bytes); may re-call upstream
(capped, default 1 lap via ``BRIDGE_HANDLE_TOOL_CALLS_MAX_LAPS``).
When wired, the executor forces the upstream call non-streaming so the
plugins see an assembled frame; client-side streaming is preserved by
SSE re-emit (Q-K resolved-by-design).

See D-008 for the four-category executor model: **modify** (`response_modify`,
buffers + delivers), **intercept** (`handle_tool_calls`, buffers + may
re-call), **observe** (`post_response`, fires after delivery), and
**passthrough** (no plugins, raw bytes flow).

`outbound_normalize` (D-012) is the *normalize-the-frame-before-EVERY-send*
capability — the home of `quirks_mode` / provider-compatibility shimming.
Where `context_modify` runs ONCE on the inbound pass, `outbound_normalize`
runs on EVERY resource call — the inbound call AND every `handle_tool_calls`
intercept re-call AND any future bridge-originated call — because the executor
invokes it inside `_execute_resource_step` (the single chokepoint all resource
calls funnel through). This is why a strict provider (Moonshot) 400'd on the
intercept re-call even with the reasoning fix wired: the fix was a
`context_modify` (inbound-only); the re-call bypassed it. As a
normalize-before-send capability it covers all paths. Method:
`normalize_outbound(ctx, config)`, operating on `ctx.request.messages`. MUST be
idempotent (runs on inbound AND re-calls). Same valid slots as `context_modify`
(it's wired in a context block). First consumer: `quirks_mode` (reasoning
re-attach + key-drift mirror + trailing-orphan close)."""


CANONICAL_SLOTS: frozenset[str] = frozenset(
    slot for slots in KNOWN_CAPABILITIES.values() for slot in slots
)
"""Flat set of every slot string that exists in V4. Anything in config
outside this set at a `*.plugins` level is ill-formed."""


NO_FAN_OUT_CAPABILITIES: frozenset[str] = frozenset({"listener"})
"""Capabilities EXCLUDED from capability fan-out (D-?? — capability fan-out).

Most multi-capability plugins benefit from being declared once and auto-wired
into their other capability slots. But ``listener`` is an ENTRY-POINT capability:
``_materialise_listeners`` (server.py) deliberately reads the RAW identity
``plugins`` block to decide which identities open an HTTP route. Fanning it out
would (a) make the report claim listeners that materialisation won't actually
open — an honesty regression — and (b) silently turn every identity sharing a
role into a listener, which is exactly the "no listeners until an identity
declares one" V4 invariant we must not erode. So a plugin's ``listener``
capability is NEVER inferred; it must be declared on the identity that wants it.

``background`` is intentionally NOT here: ``_spawn_background_tasks`` resolves the
cascade, so an inferred background placement IS honoured (a role's MCP server map
spawns discovery for every identity on it — the whole point of the fan-out)."""


# Known non-plugin keys at each cascade level. A sub-key on an identity /
# role / session / resource / server block that's neither in this set nor
# a plugins-style slot is ill-formed config.
KNOWN_KEYS_SERVER: frozenset[str] = frozenset(
    {"timezone", "resources", "sessions", "roles", "identities", "plugins"}
)
KNOWN_KEYS_RESOURCE: frozenset[str] = frozenset(
    {"plugins", "endpoint_url", "token", "timeout"}
    # V3 nested plugins under `endpoint:` — V4 wants them directly under
    # `plugins:` per D-006. We deliberately don't list `endpoint` here so
    # operators with V3-shape configs get a loud warning.
    # `endpoint_url` and `token` are kept (used by simpler resource
    # configs that want to set those at the resource level directly).
    # `timeout` is per-resource seconds (float-coerced at use site, V4
    # fail-loud-at-use idiom). Consumed by resource consumers — today
    # `conversational_memory` uses it for both recall and store calls.
)
KNOWN_KEYS_SESSION: frozenset[str] = frozenset({"plugins"})
KNOWN_KEYS_ROLE: frozenset[str] = frozenset(
    {"resource", "session", "plugins", "context", "response"}
)
KNOWN_KEYS_IDENTITY: frozenset[str] = frozenset(
    {
        "role", "roles", "token", "session",
        "bridge_agent", "display_name", "trust", "inbound_identity",
        "plugins", "context", "response",
    }
)
KNOWN_KEYS_CONTEXT_BLOCK: frozenset[str] = frozenset(
    {"name", "trust", "additional", "timezone", "plugins"}
)
KNOWN_KEYS_RESPONSE_BLOCK: frozenset[str] = frozenset({"plugins"})


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------

@dataclass
class Placement:
    """One plugin reference at one slot in the config."""

    identity_key: str
    """The identity in whose pipeline this placement participates. Always
    an identity key — even role/session/resource-level placements are
    reported once per identity that uses them, because pipelines are
    materialised per-identity (D-001)."""

    cascade_level: str
    """Where in the cascade the plugin was declared. One of:
    'identity' / 'role' / 'session' / 'resource'. Used to construct the
    full slot string and to display in warnings."""

    cascade_key: str
    """The config key at that cascade level (e.g. 'my_role' for
    role, 'openrouter' for resource). Useful for warnings."""

    slot: str
    """One of the 10 canonical slot strings (e.g. 'role.context.plugins').
    May be an ill-formed slot string if cascade_level + sub-block was bad,
    but in that case the placement isn't reported here — see
    ConfigShapeIssue."""

    plugin_name: str
    plugin_config: dict

    outcome: str
    """'ok' | 'misplaced' | 'uncapable' | 'legacy_v3' | 'unknown'"""

    matched_capabilities: list[str] = field(default_factory=list)
    """Capabilities of the plugin whose valid-slot list includes this slot.
    Empty for misplaced/uncapable/legacy_v3/unknown."""

    valid_slots_for_plugin: list[str] = field(default_factory=list)
    """Aggregated list of every slot the plugin COULD legitimately appear
    at. Used in 'misplaced' warnings to suggest where to put it instead."""

    synthetic: bool = False
    """True iff this placement was AUTO-INFERRED by capability fan-out rather
    than declared by the operator. A multi-capability plugin (mcp_client,
    agent_tools) declared once is fanned out to its OTHER capability slots so
    the operator doesn't have to type empty `{}` ceremony markers. The
    inferred placement names the operator's REAL declaration site in
    cascade_level/cascade_key (so warnings point at real config), but is
    tagged here so report_summary stays honest about what was typed vs
    inferred. Downstream (assembler, merge_plugin_configs) treats synthetic
    and declared placements identically — the cascade is re-walked for config
    regardless."""


@dataclass
class ConfigShapeIssue:
    """An ill-formed config slot or unknown sub-key."""

    path: str
    """Dotted path, e.g. 'resources.openrouter.context'."""

    issue: str
    """Human-readable description of what's wrong and what's expected."""


@dataclass
class ValidationReport:
    placements: list[Placement] = field(default_factory=list)
    config_issues: list[ConfigShapeIssue] = field(default_factory=list)
    identities_with_listeners: list[str] = field(default_factory=list)
    resources_with_produce_response: list[str] = field(default_factory=list)

    def has_warnings(self) -> bool:
        """True iff any placement is non-ok or any config issue exists."""
        return bool(self.config_issues) or any(
            p.outcome != "ok" for p in self.placements
        )

    def get(self, outcome: str) -> list[Placement]:
        """All placements with the given outcome."""
        return [p for p in self.placements if p.outcome == outcome]

    def counts(self) -> dict[str, int]:
        """Count of placements per outcome (always returns all 5 keys)."""
        result = {k: 0 for k in ("ok", "misplaced", "uncapable",
                                 "legacy_v3", "unknown")}
        for p in self.placements:
            result[p.outcome] = result.get(p.outcome, 0) + 1
        return result


# ---------------------------------------------------------------------------
# Plugin classification — given a plugin name + slot, what's the outcome?
# ---------------------------------------------------------------------------

def _classify_placement(plugin_name: str, slot: str) -> tuple[str, list[str], list[str]]:
    """
    Returns (outcome, matched_capabilities, valid_slots_for_plugin).

    Pure inspection — no logging, no side effects.
    """
    plugin = plugin_loader.get_plugin(plugin_name)
    if plugin is None:
        return ("unknown", [], [])

    caps = plugin_loader.get_capabilities(plugin_name)
    if caps is None:
        # Plugin loaded but no CAPABILITIES dict
        legacy_hooks = getattr(plugin, "SUPPORTED_HOOKS", None)
        if legacy_hooks is not None:
            return ("legacy_v3", [], [])
        return ("uncapable", [], [])

    # Aggregate every slot the plugin could legitimately appear at —
    # restricted to KNOWN canonical capabilities, so a plugin declaring
    # `my_made_up_thing` doesn't pollute the suggestion list.
    valid_slots: list[str] = []
    matched: list[str] = []
    for cap_name, declared_slots in caps.items():
        if cap_name not in KNOWN_CAPABILITIES:
            continue  # plugin declared a non-canonical capability; ignore
        if not isinstance(declared_slots, list):
            continue  # malformed; treat as no slots
        # Intersect what the plugin declared with what's spec-valid for
        # that capability — guards against typo'd slot strings.
        canonical = KNOWN_CAPABILITIES[cap_name]
        actually_valid = [s for s in declared_slots if s in canonical]
        valid_slots.extend(actually_valid)
        if slot in actually_valid:
            matched.append(cap_name)

    valid_slots = sorted(set(valid_slots))

    if matched:
        return ("ok", sorted(set(matched)), valid_slots)
    if valid_slots:
        return ("misplaced", [], valid_slots)
    # Plugin has CAPABILITIES dict but no canonical capability → uncapable
    return ("uncapable", [], [])


# ---------------------------------------------------------------------------
# Per-identity placement enumeration
# ---------------------------------------------------------------------------

def _plugins_at_slot(cfg_block: dict | None, slot_path_keys: list[str]) -> dict:
    """Walk a config block by a list of keys and return the dict at the
    end (or {} if any step is missing/non-dict). Defensive helper."""
    cur: Any = cfg_block or {}
    for k in slot_path_keys:
        if not isinstance(cur, dict):
            return {}
        cur = cur.get(k)
        if cur is None:
            return {}
    return cur if isinstance(cur, dict) else {}


def _enumerate_placements_for_identity(identity_key: str) -> list[Placement]:
    """
    Walk the cascade for one identity and produce one Placement per
    plugin reference at any of the 10 canonical slots. Does NOT do
    cascade-merging — each placement is reported at its declaration
    cascade level, independently of others.
    """
    identity_cfg = config.resolve_identity(identity_key) or {}
    role_key = config.get_identity_role_key(identity_key)
    role_cfg = config.resolve_role(role_key) if role_key else None
    role_cfg = role_cfg or {}
    session_key = role_cfg.get("session")
    session_cfg = config.resolve_session(session_key) if session_key else None
    session_cfg = session_cfg or {}
    resource_key = role_cfg.get("resource")
    resource_cfg = config.resolve_resource(resource_key) if resource_key else None
    resource_cfg = resource_cfg or {}

    placements: list[Placement] = []

    # The 8 canonical slots, each tied to (cascade_level, cascade_key, cfg_block, path-keys).
    # Sessions and resources have ONLY `plugins:` — no context/response sub-blocks (per D-006).
    slots_to_walk = [
        ("identity.plugins",          "identity", identity_key,    identity_cfg, ["plugins"]),
        ("role.plugins",              "role",     role_key or "?", role_cfg,     ["plugins"]),
        ("session.plugins",           "session",  session_key or "?", session_cfg, ["plugins"]),
        ("resource.plugins",          "resource", resource_key or "?", resource_cfg, ["plugins"]),
        ("identity.context.plugins",  "identity", identity_key,    identity_cfg, ["context", "plugins"]),
        ("role.context.plugins",      "role",     role_key or "?", role_cfg,     ["context", "plugins"]),
        ("identity.response.plugins", "identity", identity_key,    identity_cfg, ["response", "plugins"]),
        ("role.response.plugins",     "role",     role_key or "?", role_cfg,     ["response", "plugins"]),
    ]

    for slot, level, key, cfg_block, path in slots_to_walk:
        if level == "role" and not role_key:
            continue  # no role at all — nothing to walk
        if level == "session" and not session_key:
            continue  # no session declared — slot doesn't apply
        if level == "resource" and not resource_key:
            continue  # no resource — pipeline will return 503; nothing to validate
        plugins_block = _plugins_at_slot(cfg_block, path)
        for plugin_name, plugin_config in plugins_block.items():
            if plugin_config is None:
                plugin_config = {}
            elif not isinstance(plugin_config, dict):
                plugin_config = {}
            outcome, matched, valid = _classify_placement(plugin_name, slot)
            placements.append(Placement(
                identity_key=identity_key,
                cascade_level=level,
                cascade_key=key,
                slot=slot,
                plugin_name=plugin_name,
                plugin_config=plugin_config,
                outcome=outcome,
                matched_capabilities=matched,
                valid_slots_for_plugin=valid,
            ))

    # Capability fan-out: a multi-capability plugin declared ONCE is auto-wired
    # into its OTHER capability slots, so the operator doesn't type empty `{}`
    # ceremony markers (e.g. mcp_client declared at role.plugins also gets its
    # context_modify and background slots inferred). Appends synthetic
    # placements for capabilities not already satisfied by a declared one.
    placements.extend(_fan_out_placements(
        identity_key, placements, role_key, session_key, resource_key,
    ))

    return placements


# ---------------------------------------------------------------------------
# Capability fan-out — synthesize the placements the operator didn't type
# ---------------------------------------------------------------------------

def _reachable_slots(
    role_key: str | None,
    session_key: str | None,
    resource_key: str | None,
) -> set[str]:
    """The set of canonical slots whose cascade level EXISTS for this identity.
    Mirrors the skip-gates in the declared-placement walk above: a fan-out
    target slot is only valid if the level it lives at is present in the
    cascade (no role → no role.* slots; no session → no session.plugins; etc.).
    Identity-level slots are always reachable (the identity exists by
    construction)."""
    reachable = {
        "identity.plugins",
        "identity.context.plugins",
        "identity.response.plugins",
    }
    if role_key:
        reachable |= {
            "role.plugins",
            "role.context.plugins",
            "role.response.plugins",
        }
    if session_key:
        reachable.add("session.plugins")
    if resource_key:
        reachable.add("resource.plugins")
    return reachable


def _fan_out_placements(
    identity_key: str,
    declared: list[Placement],
    role_key: str | None,
    session_key: str | None,
    resource_key: str | None,
) -> list[Placement]:
    """Synthesize the fanned-out placements for one identity.

    **Per-capability, not per-slot** (the load-bearing correctness rule): a
    plugin's capability may be valid at several slots (``handle_tool_calls`` →
    identity.plugins + role.plugins). We ensure each capability the plugin
    declares is represented at EXACTLY ONE reachable slot. If a capability is
    already satisfied by an operator-declared OK placement, we add nothing for
    it — otherwise multi-slot capabilities would get duplicate tuples and the
    executor would double-inject / double-dispatch.

    The synthetic placement names the operator's real declaration site in
    cascade_level/cascade_key (so warnings point at real config) and is tagged
    ``synthetic=True``. Its slot choice is cosmetic — ``merge_plugin_configs``
    re-walks ALL cascade levels for the slot family regardless, so the
    override/cascade semantics are preserved no matter which slot we name.
    """
    reachable = _reachable_slots(role_key, session_key, resource_key)

    # Group declared OK placements per plugin: which capabilities are already
    # satisfied, and the operator's declaration site to point warnings at.
    satisfied_caps: dict[str, set[str]] = {}
    decl_site: dict[str, tuple[str, str]] = {}  # plugin → (cascade_level, cascade_key)
    for p in declared:
        if p.outcome != "ok":
            continue
        satisfied_caps.setdefault(p.plugin_name, set()).update(p.matched_capabilities)
        decl_site.setdefault(p.plugin_name, (p.cascade_level, p.cascade_key))

    synthetic: list[Placement] = []
    for plugin_name, covered in satisfied_caps.items():
        caps = plugin_loader.get_capabilities(plugin_name)
        if not caps:
            continue
        level, key = decl_site[plugin_name]
        for cap_name, declared_slots in caps.items():
            if cap_name not in KNOWN_CAPABILITIES:
                continue  # non-canonical capability — ignore
            if cap_name in NO_FAN_OUT_CAPABILITIES:
                continue  # entry-point capability — must be declared explicitly
            if cap_name in covered:
                continue  # already satisfied by a declared placement — skip
            if not isinstance(declared_slots, list):
                continue
            # Candidate slots: spec-valid for this capability, declared by the
            # plugin, and reachable in this identity's cascade. Pick the
            # highest-priority by _SLOT_ORDER-ish preference (identity first).
            canonical = KNOWN_CAPABILITIES[cap_name]
            candidates = [
                s for s in declared_slots
                if s in canonical and s in reachable
            ]
            if not candidates:
                continue
            target = _pick_fan_out_slot(candidates)
            # SCOPED to this ONE capability — a synthetic placement created to
            # satisfy `cap_name` must claim ONLY `cap_name`, even if other
            # capabilities are also valid at `target`. _classify_placement
            # returns ALL matching caps for a slot (e.g. identity.plugins is
            # valid for BOTH background AND handle_tool_calls), which would
            # re-introduce an already-/separately-handled capability at a second
            # slot → double-dispatch. The slot is just a carrier; the placement
            # represents exactly the capability we're fanning out for.
            valid = _classify_placement(plugin_name, target)[2]
            synthetic.append(Placement(
                identity_key=identity_key,
                cascade_level=level,
                cascade_key=key,
                slot=target,
                plugin_name=plugin_name,
                plugin_config={},
                outcome="ok",
                matched_capabilities=[cap_name],
                valid_slots_for_plugin=valid,
                synthetic=True,
            ))
            covered.add(cap_name)  # this specific capability now covered

    return synthetic


def _pick_fan_out_slot(candidates: list[str]) -> str:
    """Pick one slot for a fanned-out capability. Cosmetic (affects only
    warning text — config is re-merged across all levels regardless), but we
    prefer identity-level over role-level so the inferred slot reads as the
    most-specific home. Deterministic for stable test/report output."""
    order = {
        "identity.plugins": 0,
        "identity.context.plugins": 1,
        "identity.response.plugins": 2,
        "role.plugins": 3,
        "role.context.plugins": 4,
        "role.response.plugins": 5,
        "session.plugins": 6,
        "resource.plugins": 7,
    }
    return min(candidates, key=lambda s: order.get(s, 99))


# ---------------------------------------------------------------------------
# Config-shape validation — catch typos and ill-formed slots
# ---------------------------------------------------------------------------

def _check_unknown_keys(path: str, block: dict, known: frozenset[str]) -> list[ConfigShapeIssue]:
    """Compare keys in `block` against `known` set; emit issues for each
    unrecognised key."""
    if not isinstance(block, dict):
        return []
    issues: list[ConfigShapeIssue] = []
    for k in block.keys():
        if k not in known:
            issues.append(ConfigShapeIssue(
                path=f"{path}.{k}",
                issue=(
                    f"Unknown key '{k}' at {path}. "
                    f"Recognised keys here: {sorted(known)}. "
                    f"Plugins declared under unknown keys will not fire."
                ),
            ))
    return issues


def _check_config_shape() -> list[ConfigShapeIssue]:
    """Walk the entire server config and report ill-formed slots / typos.

    Catches the most common operator mistakes:
      - resources.<name>.context.plugins (resources have no context)
      - resources.<name>.response.plugins (resources have no response)
      - server.context.plugins / server.response.plugins (server has neither)
      - typo'd top-level keys (e.g. plugzins:)
    """
    server_cfg = config.get_server_cfg()
    issues: list[ConfigShapeIssue] = []

    # Server top-level
    issues.extend(_check_unknown_keys("server", server_cfg, KNOWN_KEYS_SERVER))

    # Resources
    for name, body in (server_cfg.get("resources", {}) or {}).items():
        issues.extend(_check_unknown_keys(
            f"resources.{name}", body or {}, KNOWN_KEYS_RESOURCE
        ))

    # Sessions
    for name, body in (server_cfg.get("sessions", {}) or {}).items():
        issues.extend(_check_unknown_keys(
            f"sessions.{name}", body or {}, KNOWN_KEYS_SESSION
        ))

    # Roles
    for name, body in (server_cfg.get("roles", {}) or {}).items():
        issues.extend(_check_unknown_keys(
            f"roles.{name}", body or {}, KNOWN_KEYS_ROLE
        ))
        # Recurse into role.context and role.response (only `plugins:` allowed)
        issues.extend(_check_unknown_keys(
            f"roles.{name}.context",
            (body or {}).get("context", {}) or {},
            KNOWN_KEYS_CONTEXT_BLOCK,
        ))
        issues.extend(_check_unknown_keys(
            f"roles.{name}.response",
            (body or {}).get("response", {}) or {},
            KNOWN_KEYS_RESPONSE_BLOCK,
        ))

    # Identities
    for name, body in (server_cfg.get("identities", {}) or {}).items():
        issues.extend(_check_unknown_keys(
            f"identities.{name}", body or {}, KNOWN_KEYS_IDENTITY
        ))
        issues.extend(_check_unknown_keys(
            f"identities.{name}.context",
            (body or {}).get("context", {}) or {},
            KNOWN_KEYS_CONTEXT_BLOCK,
        ))
        issues.extend(_check_unknown_keys(
            f"identities.{name}.response",
            (body or {}).get("response", {}) or {},
            KNOWN_KEYS_RESPONSE_BLOCK,
        ))

    return issues


# ---------------------------------------------------------------------------
# Orchestrator + summary formatter
# ---------------------------------------------------------------------------

def validate_all() -> ValidationReport:
    """
    Run the full validator across all identities and the config tree.
    Read-only; never raises (returns an empty-ish report on bad state).

    Caller (server.py) decides how to log / act on the result.
    """
    report = ValidationReport()

    if not config.is_config_loaded():
        return report

    # Per-identity placement walk
    for identity_key in config.list_identities():
        report.placements.extend(_enumerate_placements_for_identity(identity_key))

    # Config-shape pass
    report.config_issues = _check_config_shape()

    # Derived fields: which identities have at least one OK listener-capable
    # placement on identity.plugins, and which resources have a produce_response
    # plugin on resource.plugins.
    listener_identities: set[str] = set()
    terminal_resources: set[str] = set()
    for p in report.placements:
        if p.outcome != "ok":
            continue
        if "listener" in p.matched_capabilities and p.slot == "identity.plugins":
            listener_identities.add(p.identity_key)
        if "produce_response" in p.matched_capabilities and p.slot == "resource.plugins":
            terminal_resources.add(p.cascade_key)

    report.identities_with_listeners = sorted(listener_identities)
    report.resources_with_produce_response = sorted(terminal_resources)

    # Resource-shape conflict detection (per D-006 pre-flight clarification):
    # a resource is one shape — either terminal (produce_response) OR
    # transport (outbound_params). Mixing both is operator confusion, not
    # legitimate fallback config. Surface as a config-shape issue.
    report.config_issues.extend(_check_resource_shape_conflicts(report))

    # D-008 invariants for handle_tool_calls plugins:
    #   1. plugin must declare a non-empty OWNED_TOOLS list at module level
    #   2. no two plugins on the same identity may claim the same tool name
    report.config_issues.extend(_check_handle_tool_calls_invariants(report))
    report.config_issues.extend(_check_owned_tools_collisions(report))

    # Identity-aware-pipeline invariant: any identity wiring context/response
    # plugins MUST declare context.name somewhere in its cascade. "Identity
    # Not Decided" means identity must be decidable — context-aware plugins
    # need someone to be aware OF.
    report.config_issues.extend(_check_caller_identity_required(report))

    # Reachability invariant: a tokenised identity that declares no listener of
    # its own is a misconfig — a token does not imply an HTTP listener, and
    # riding another identity's shared route is now rejected at request time.
    report.config_issues.extend(_check_tokenised_without_listener(report))

    return report


def _check_resource_shape_conflicts(report: ValidationReport) -> list[ConfigShapeIssue]:
    """Walk OK placements at resource.plugins and flag resources that have
    BOTH `produce_response` plugins AND `outbound_params` plugins. These
    conflict architecturally — `produce_response` says 'I AM the resource',
    `outbound_params` configures outbound calls. A resource has one shape;
    the operator has to pick.
    """
    by_resource: dict[str, dict[str, list[str]]] = {}
    for p in report.placements:
        if p.outcome != "ok" or p.slot != "resource.plugins":
            continue
        bucket = by_resource.setdefault(p.cascade_key, {"pr": [], "op": []})
        if "produce_response" in p.matched_capabilities:
            bucket["pr"].append(p.plugin_name)
        if "outbound_params" in p.matched_capabilities:
            bucket["op"].append(p.plugin_name)

    issues: list[ConfigShapeIssue] = []
    for resource_key, plugins in by_resource.items():
        if plugins["pr"] and plugins["op"]:
            issues.append(ConfigShapeIssue(
                path=f"resources.{resource_key}",
                issue=(
                    f"Resource '{resource_key}' has both 'produce_response' "
                    f"plugins ({plugins['pr']}) AND 'outbound_params' plugins "
                    f"({plugins['op']}) on its plugins: block. These conflict "
                    f"— 'produce_response' says 'I AM the resource', "
                    f"'outbound_params' configures outbound calls. A resource "
                    f"has one shape; pick either terminal-plugin or "
                    f"transport-plugin for this resource."
                ),
            ))
    return issues


def _check_handle_tool_calls_invariants(report: ValidationReport) -> list[ConfigShapeIssue]:
    """D-008 invariant: a plugin declaring ``handle_tool_calls`` must
    also expose a non-empty module-level ``OWNED_TOOLS: list[str]``
    declaration. Without it the plugin can never claim a tool_call and
    will never fire — that's a config-shape ill-formedness, not silent
    permissiveness.

    Walks OK placements with the capability; one ConfigShapeIssue per
    distinct plugin (deduplicated — the same plugin with bad
    OWNED_TOOLS placed at multiple identities reports once)."""
    issues: list[ConfigShapeIssue] = []
    seen_plugins: set[str] = set()
    for p in report.placements:
        if p.outcome != "ok":
            continue
        if "handle_tool_calls" not in p.matched_capabilities:
            continue
        if p.plugin_name in seen_plugins:
            continue
        plugin = plugin_loader.get_plugin(p.plugin_name)
        if plugin is None:
            continue  # unknown — already a separate placement issue
        owned = getattr(plugin, "OWNED_TOOLS", None)
        if callable(owned):
            # Dynamic OWNED_TOOLS (e.g. mcp_client discovers its tools from
            # MCP servers at startup, AFTER validation). The presence of the
            # callable satisfies the D-008 non-empty invariant; the concrete
            # tool names don't exist yet, so there's nothing more to check here.
            continue
        if owned is None or not isinstance(owned, list) or not owned:
            seen_plugins.add(p.plugin_name)
            issues.append(ConfigShapeIssue(
                path=f"plugins.{p.plugin_name}",
                issue=(
                    f"Plugin '{p.plugin_name}' declares 'handle_tool_calls' "
                    f"capability but no module-level OWNED_TOOLS list "
                    f"(or it's empty / wrong type). It can never claim a "
                    f"tool_call and will never fire. Fix: add "
                    f"`OWNED_TOOLS = [\"tool_name_1\", ...]` at module "
                    f"level alongside the CAPABILITIES dict."
                ),
            ))
    return issues


def _check_owned_tools_collisions(report: ValidationReport) -> list[ConfigShapeIssue]:
    """D-008 invariant: two plugins on the same identity may not both
    declare the same tool name in OWNED_TOOLS. Cross-identity collisions
    are non-issues per D-001 (pipelines are per-identity).

    Walks OK placements with ``handle_tool_calls``, builds a per-identity
    {tool_name: [plugin_names]} map, emits one ConfigShapeIssue per
    colliding tool name."""
    # Per-identity per-plugin: dedupe placements (same plugin can show
    # at multiple cascade levels for one identity).
    seen: set[tuple[str, str]] = set()  # (identity_key, plugin_name)
    by_identity: dict[str, dict[str, list[str]]] = {}  # identity → tool → plugins
    for p in report.placements:
        if p.outcome != "ok":
            continue
        if "handle_tool_calls" not in p.matched_capabilities:
            continue
        key = (p.identity_key, p.plugin_name)
        if key in seen:
            continue
        seen.add(key)
        plugin = plugin_loader.get_plugin(p.plugin_name)
        if plugin is None:
            continue
        owned = getattr(plugin, "OWNED_TOOLS", None)
        # A callable (dynamic) OWNED_TOOLS can't be collision-checked at
        # startup — the discovered names don't exist yet. Skipped here (the
        # isinstance check below catches it); cross-plugin collisions among
        # dynamic tools are a runtime concern parked for future-us.
        if not isinstance(owned, list):
            continue
        identity_map = by_identity.setdefault(p.identity_key, {})
        for tool in owned:
            if not isinstance(tool, str) or not tool:
                continue
            identity_map.setdefault(tool, []).append(p.plugin_name)

    issues: list[ConfigShapeIssue] = []
    for identity_key, tools in by_identity.items():
        for tool_name, plugins in tools.items():
            if len(plugins) > 1:
                issues.append(ConfigShapeIssue(
                    path=f"identities.{identity_key}",
                    issue=(
                        f"Identity '{identity_key}' has multiple plugins "
                        f"claiming tool '{tool_name}' via OWNED_TOOLS: "
                        f"{plugins}. Tool-name collisions on one identity "
                        f"are ambiguous — the executor cannot decide which "
                        f"plugin handles the call. Rename the tool in one "
                        f"plugin or remove the collision. (D-008 invariant; "
                        f"cross-identity collisions are fine — pipelines "
                        f"are per-identity per D-001.)"
                    ),
                ))
    return issues


_IDENTITY_AWARE_SLOTS: frozenset[str] = frozenset({
    "identity.context.plugins",
    "role.context.plugins",
    "identity.response.plugins",
    "role.response.plugins",
})
"""Slot strings that fire identity-aware behaviour. Any plugin in any of
these slots needs to know WHO is calling — that's the whole reason
context/response plugins exist. If an identity wires any of these slots
without declaring ``context.name`` somewhere in its cascade, the pipeline
is ill-formed: there's no caller to be aware of."""


def _check_caller_identity_required(report: ValidationReport) -> list[ConfigShapeIssue]:
    """Identity-aware-pipeline invariant.

    "Identity Not Decided" means identity must be *decidable somewhere*.
    Any identity wiring at least one ``ok`` placement in a context/response
    slot MUST have ``context.name`` declared at identity, role, or server
    level (the only levels that have context blocks per D-006 + the server
    fallback for identity-aware metadata).

    Failure → one ConfigShapeIssue per affected identity, naming the
    affected plugins and where to declare the missing field.

    The check uses raw config (no cascade-merge) because the cascade
    already encodes "highest-priority declaration wins" — we just need to
    find ANY level that declares ``context.name``. The server-level
    fallback is read from the raw server config, since cascade.py doesn't
    expose a server.context block today (server-level is .env-shaped, not
    name/trust-shaped) — but if an operator does declare server.context.name
    we accept it as honest intent.

    Identities the executor would already 503 (no resource AND no terminal
    plugin) are still reported here — the check is about *config shape*,
    not *runtime reachability*. An identity with both problems gets two
    warnings; that's fine, fail loud about all of them.
    """
    # Step 1: identity → set of plugin names placed in identity-aware slots
    identity_aware_plugins: dict[str, list[str]] = {}
    for p in report.placements:
        if p.outcome != "ok":
            continue
        if p.slot not in _IDENTITY_AWARE_SLOTS:
            continue
        identity_aware_plugins.setdefault(p.identity_key, []).append(p.plugin_name)

    if not identity_aware_plugins:
        return []

    issues: list[ConfigShapeIssue] = []
    server_cfg = config.get_server_cfg()
    server_context_name = (
        (server_cfg.get("context") or {}).get("name")
        if isinstance(server_cfg.get("context"), dict)
        else None
    )

    for identity_key, plugins in identity_aware_plugins.items():
        if _has_caller_name(identity_key, server_context_name):
            continue
        # Dedup plugin names while preserving first-seen order
        seen: set[str] = set()
        unique_plugins: list[str] = []
        for name in plugins:
            if name in seen:
                continue
            seen.add(name)
            unique_plugins.append(name)
        issues.append(ConfigShapeIssue(
            path=f"identities.{identity_key}",
            issue=(
                f"Identity '{identity_key}' wires context/response plugins "
                f"({unique_plugins}) but no 'context.name' is declared in its "
                f"cascade (identity, role, or server). Identity-aware plugins "
                f"need someone to be aware of. Declare 'context.name' at "
                f"identities.{identity_key}.context.name (most specific) or "
                f"on the role's context block. Identity will be marked "
                f"unusable; pipeline returns 503 on call."
            ),
        ))
    return issues


def _has_caller_name(identity_key: str, server_context_name: str | None) -> bool:
    """True iff ``context.name`` is declared at identity or role level for
    this identity, or at server level as a fallback. Walks raw config —
    cascade resolution isn't needed for a presence check."""
    identity_cfg = config.resolve_identity(identity_key) or {}
    identity_ctx = identity_cfg.get("context")
    if isinstance(identity_ctx, dict) and identity_ctx.get("name"):
        return True

    role_key = config.get_identity_role_key(identity_key)
    if role_key:
        role_cfg = config.resolve_role(role_key) or {}
        role_ctx = role_cfg.get("context")
        if isinstance(role_ctx, dict) and role_ctx.get("name"):
            return True

    if server_context_name:
        return True

    return False


def _check_tokenised_without_listener(report: ValidationReport) -> list[ConfigShapeIssue]:
    """Reachability invariant: a tokenised identity must declare its own listener.

    "The bridge does nothing by default — if I missed it in config, the identity
    should not work." A token does NOT imply an HTTP listener (D-003 / Q-B: a
    trigger is just a listener plugin, declared on the identity — there is no
    `trigger:` field, and no special-casing). An identity that carries a `token:`
    but declares no listener-capable plugin is only reachable by riding another
    identity's shared route — which is now rejected at request time. Either way
    it's a misconfig: warn loud at boot.

    Presence of ``token:`` is read from raw config (the operator's declared
    intent) rather than the env-expanded token map — that keeps the check
    honest even if the env var is unset at boot.

    Internal identities (no ``token:``) are intentionally not HTTP-reachable
    (mesh / cron / future internal listeners) — skipped, not flagged.
    """
    with_listeners = set(report.identities_with_listeners)
    issues: list[ConfigShapeIssue] = []

    for identity_key in config.list_identities():
        identity_cfg = config.resolve_identity(identity_key) or {}
        if not identity_cfg.get("token"):
            continue  # internal identity — not HTTP-reachable by design
        if identity_key in with_listeners:
            continue  # declares its own listener — honest and reachable

        issues.append(ConfigShapeIssue(
            path=f"identities.{identity_key}",
            issue=(
                f"Identity '{identity_key}' has a token but declares no listener. "
                f"A token does not imply an HTTP listener — this identity is NOT "
                f"reachable on its own (riding another identity's shared route is "
                f"rejected at request time). Declare a listener plugin "
                f"(e.g. OpenAI-Protocol on identities.{identity_key}.plugins) if it "
                f"should be reachable over HTTP, or remove the token if it "
                f"shouldn't."
            ),
        ))

    return issues


def report_summary(report: ValidationReport) -> str:
    """Format the report as a multi-line human-readable banner for
    logger.info. Always produced, even when everything is ok."""
    lines: list[str] = []
    sep = "─" * 60
    lines.append(sep)
    lines.append("Capability validation report:")

    counts = report.counts()
    total = sum(counts.values())
    lines.append(
        f"  Placements: {total} total "
        f"({counts['ok']} ok, "
        f"{counts['misplaced']} misplaced, "
        f"{counts['unknown']} unknown, "
        f"{counts['legacy_v3']} legacy_v3, "
        f"{counts['uncapable']} uncapable)"
    )

    def _fmt_placement(p: Placement) -> str:
        tag = " (inferred)" if p.synthetic else ""
        return (
            f"    - '{p.plugin_name}' at {p.slot}{tag} "
            f"(in {p.cascade_level} '{p.cascade_key}', identity '{p.identity_key}')"
        )

    # Inferred-by-fan-out placements: a multi-capability plugin declared once
    # is auto-wired into its other capability slots. List them (deduped per
    # plugin+slot to avoid N-identities-sharing-a-role noise) so the banner
    # stays honest about what the operator typed vs what the bridge fanned out.
    inferred = [p for p in report.placements if p.synthetic and p.outcome == "ok"]
    if inferred:
        lines.append(
            f"  INFERRED BY FAN-OUT ({len(inferred)}; auto-wired from a single "
            f"declaration — operator didn't type these):"
        )
        seen_i: set[tuple[str, str]] = set()
        for p in inferred:
            key = (p.plugin_name, p.slot)
            if key in seen_i:
                continue
            seen_i.add(key)
            caps = ", ".join(p.matched_capabilities) or "?"
            lines.append(
                f"    - '{p.plugin_name}' → {p.slot} ({caps}) "
                f"[from {p.cascade_level} '{p.cascade_key}' declaration]"
            )

    misplaced = report.get("misplaced")
    if misplaced:
        lines.append(f"  MISPLACED ({len(misplaced)}):")
        for p in misplaced:
            lines.append(_fmt_placement(p))
            lines.append(
                f"      → valid slots for '{p.plugin_name}': "
                f"{p.valid_slots_for_plugin}"
            )

    unknown = report.get("unknown")
    if unknown:
        lines.append(
            f"  UNKNOWN ({len(unknown)}; not loaded — possibly not yet ported to V4):"
        )
        # Dedup to one line per (plugin_name, slot) pair to avoid the same
        # unknown plugin filling 20 lines for 20 identities sharing a role.
        seen: set[tuple[str, str]] = set()
        for p in unknown:
            key = (p.plugin_name, p.slot)
            if key in seen:
                continue
            seen.add(key)
            lines.append(_fmt_placement(p))

    legacy = report.get("legacy_v3")
    if legacy:
        lines.append(f"  LEGACY V3 SHAPE ({len(legacy)}; pending V4 port):")
        seen_l: set[tuple[str, str]] = set()
        for p in legacy:
            key = (p.plugin_name, p.slot)
            if key in seen_l:
                continue
            seen_l.add(key)
            lines.append(_fmt_placement(p))

    uncapable = report.get("uncapable")
    if uncapable:
        lines.append(f"  UNCAPABLE ({len(uncapable)}; loaded but declares no canonical capability):")
        seen_u: set[tuple[str, str]] = set()
        for p in uncapable:
            key = (p.plugin_name, p.slot)
            if key in seen_u:
                continue
            seen_u.add(key)
            lines.append(_fmt_placement(p))

    if report.config_issues:
        lines.append(f"  CONFIG SHAPE ISSUES ({len(report.config_issues)}):")
        for issue in report.config_issues:
            lines.append(f"    - {issue.issue}")

    lines.append(
        f"  Identities with active listeners: "
        f"{report.identities_with_listeners or '(none)'}"
    )
    lines.append(
        f"  Resources with produce_response: "
        f"{report.resources_with_produce_response or '(none)'}"
    )
    lines.append(sep)

    return "\n".join(lines)
