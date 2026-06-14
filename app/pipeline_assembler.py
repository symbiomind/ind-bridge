"""
Pipeline assembler for ind-bridge V4.

Turns a per-identity ``ResolvedCascade`` + the validator's ``ValidationReport``
into a flat ordered list of ``PipelineTuple`` — the per-identity firing
order the executor walks for every request.

A ``PipelineTuple`` is::

    (slot_string, plugin_module, resolved_config)

The slot string is one of the eight canonical V4 slots (per D-006).
Slot order is fixed at the module level (`_SLOT_ORDER`) — identity-first
both directions per yesterday's "river flows one way" decision. Within a
slot, plugins fire in config-declaration order.

Per **D-001**: pipelines are materialised per-identity, on demand. This
module is the "on demand" half — given a cascade and a validation report,
build the tuple list. Per session pre-flight: caching happens at startup
and lives in this module's ``_PIPELINES`` dict; the executor calls
``get_pipeline(identity_key)``.

Per **D-002**: a resource with a ``produce_response`` plugin is terminal —
the pipeline doesn't include outbound_params tuples for transport. Per
the pre-flight clarification: the assembler doesn't try to be clever
about this — if config legitimately wires both onto the same resource,
the validator already warns, and the assembler just emits whatever's
declared. The executor decides what to do at the resource step.

Per **D-006**: eight canonical slots. Sessions and resources have only
``plugins:`` (no context/response sub-blocks).

See ``CLAUDE.md`` for the architecture cheat-sheet.
"""

from __future__ import annotations

import logging
from types import ModuleType

from . import cascade as cascade_mod
from . import config, plugin_loader
from .capabilities import Placement, ValidationReport
from .capabilities import validate_all as _validate_all

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------

PipelineTuple = tuple[str, ModuleType, dict]
"""(slot_string, plugin_module, resolved_config). Three-tuples because the
executor walks them by slot family and dispatches to capability methods on
the plugin module with the resolved config dict."""


# ---------------------------------------------------------------------------
# Slot order — "identity first always, both directions"
# ---------------------------------------------------------------------------

_SLOT_ORDER: tuple[str, ...] = (
    # Inbound / outbound-config phase: identity → role → session → resource
    "identity.plugins",
    "role.plugins",
    "session.plugins",
    "resource.plugins",
    # Inbound body modification (context.* slots) — only identity + role
    "identity.context.plugins",
    "role.context.plugins",
    # Outbound body modification (response.* slots) — only identity + role
    "identity.response.plugins",
    "role.response.plugins",
)
"""Eight canonical slots in firing order. The executor splits the walk:
context.* slots fire on the way in (after the inbound bridge_sign verify
and before the resource step); the resource step fires; response.* slots
fire on the way out (after the resource step and before outbound
bridge_sign assemble-and-sign). The top-level *.plugins slots fire as
*outbound_params* contributions (configuration-of-the-call), or — for
identity.plugins specifically with the ``listener`` capability —
``setup_listener`` was called once at startup; runtime is no-op for the
listener capability after that."""


# ---------------------------------------------------------------------------
# Module-level pipeline cache
# ---------------------------------------------------------------------------

_PIPELINES: dict[str, list[PipelineTuple]] = {}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def assemble_all() -> dict[str, list[PipelineTuple]]:
    """Walk every identity, build its pipeline tuple list, populate the
    module-level cache, return the cache.

    Called once at server startup (after ``capabilities.validate_all()``
    has produced its report). Caches per-identity pipelines for the
    executor to retrieve on every request.

    A future config-reload mechanism (open question Q-K) would call this
    again after re-reading config; for now it's startup-only.
    """
    global _PIPELINES
    _PIPELINES = {}

    if not config.is_config_loaded():
        return _PIPELINES

    report = _validate_all()
    for identity_key in config.list_identities():
        _PIPELINES[identity_key] = assemble_for_identity(identity_key, report)

    return _PIPELINES


def assemble_for_identity(
    identity_key: str,
    report: ValidationReport,
) -> list[PipelineTuple]:
    """Build the ordered tuple list for one identity.

    Inputs:
      - ``identity_key`` — the identity to assemble for
      - ``report`` — the full validation report (we filter to this
        identity's ``ok`` placements)

    Process:
      1. Filter the validator's placements to ``identity_key`` ∩ ``ok``
      2. Resolve the cascade for the identity
      3. Group placements by (slot, plugin_name) — same plugin can appear
         at multiple cascade levels; we emit one tuple per (slot, plugin)
         with deep-merged config across cascade levels
      4. Sort by ``_SLOT_ORDER``, preserving declaration order within slot
      5. For each (slot, plugin_name), look up the plugin module and use
         ``cascade.merge_plugin_configs`` to get the merged config
      6. Emit the ``(slot, plugin_module, config)`` tuple

    Returns the list. If anything is missing (cascade resolution fails,
    plugin module missing despite ``ok`` outcome — shouldn't happen but
    defend), the affected tuple is skipped with a warning and assembly
    continues.
    """
    cascade = cascade_mod.resolve_cascade(identity_key)
    if cascade is None:
        return []

    # Filter validator output to this identity's OK placements
    ok_placements = [
        p for p in report.placements
        if p.identity_key == identity_key and p.outcome == "ok"
    ]

    # Group by (slot, plugin_name) — same plugin at multiple cascade levels
    # in the same slot family produces ONE tuple with deep-merged config (D-001).
    # We use insertion order to preserve declaration order within each slot.
    seen: set[tuple[str, str]] = set()
    grouped: list[tuple[str, str]] = []  # (slot, plugin_name) in declaration order
    for p in ok_placements:
        key = (p.slot, p.plugin_name)
        if key in seen:
            continue
        seen.add(key)
        grouped.append(key)

    # Sort the grouped list by _SLOT_ORDER. Within a slot, preserve
    # declaration order (achieved by stable sort on the slot key only).
    slot_index = {s: i for i, s in enumerate(_SLOT_ORDER)}
    grouped.sort(key=lambda sp: slot_index.get(sp[0], len(_SLOT_ORDER)))

    # Build the tuples
    tuples: list[PipelineTuple] = []
    for slot, plugin_name in grouped:
        plugin = plugin_loader.get_plugin(plugin_name)
        if plugin is None:
            # Validator said this was OK so the plugin SHOULD be loaded.
            # Defend in depth — log and skip.
            logger.warning(
                f"Pipeline assembly: plugin '{plugin_name}' classified 'ok' "
                f"by validator but missing from registry. Skipping placement "
                f"at {slot} for identity '{identity_key}'."
            )
            continue

        slot_family = _slot_family(slot)
        merged_config = cascade_mod.merge_plugin_configs(
            cascade, plugin_name, slot_family
        )

        # Plugin-disable tombstone: a falsy declaration (`plugin: false` /
        # `disabled` / `off` / `0`) at the winning cascade level yeets the
        # plugin from this identity's pipeline. Emit no tuple. Symmetric —
        # a higher level can re-enable with a real config dict (handled in
        # merge_plugin_configs). Disable-at-role, re-enable-at-identity.
        if merged_config is cascade_mod.DISABLED:
            logger.info(
                f"Pipeline assembly: identity '{identity_key}' DISABLED plugin "
                f"'{plugin_name}' at '{slot}' (config tombstone) — skipping tuple."
            )
            continue

        # conversational_memory requires agent_alias (it anchors the memory pool,
        # the dedup cache, and stored attribution). Checked HERE because this is
        # where the cascade-MERGED config is available — the per-placement
        # validator can't see it (alias may be role-level while conv_mem is
        # declared at identity-level). Loud warning, but the identity still
        # assembles: fail-loud-not-fail-to-start. The plugin's request-time
        # guards are the actual safety net (skip recall/store rather than corrupt
        # a wrong pool/cache).
        if plugin_name == "conversational_memory" and not merged_config.get("agent_alias"):
            logger.warning(
                f"Pipeline assembly: identity '{identity_key}' wires "
                f"conversational_memory at '{slot}' with NO resolvable 'agent_alias' "
                f"(after cascade merge). agent_alias is REQUIRED — it anchors the "
                f"memory pool, dedup cache, and stored attribution. recall/store will "
                f"be SKIPPED at request time until it's set on the role or identity."
            )

        tuples.append((slot, plugin, merged_config))

    return tuples


def get_pipeline(identity_key: str) -> list[PipelineTuple] | None:
    """Retrieve a cached pipeline for one identity. Returns None if no
    pipeline was assembled for this identity (unknown identity, or
    ``assemble_all()`` hasn't been called yet)."""
    return _PIPELINES.get(identity_key)


def all_pipelines() -> dict[str, list[PipelineTuple]]:
    """Read-only access to the full pipeline cache. Useful for diagnostics."""
    return dict(_PIPELINES)


def has_terminal_resource(identity_key: str) -> bool:
    """True iff the identity's pipeline has a ``produce_response`` plugin
    on its resource (i.e. the resource is Eliza-shaped, file-shape, or
    similar). Diagnostic helper for the executor's resource-step branch."""
    pipeline = _PIPELINES.get(identity_key, [])
    for slot, plugin, _config in pipeline:
        if slot != "resource.plugins":
            continue
        caps = plugin_loader.get_capabilities(plugin.__name__.rsplit(".", 1)[-1])
        # Note: plugin.__name__ is e.g. 'plugins.builtin.openai-protocol' —
        # we strip to last segment for the capability lookup. The loader
        # registers by short name.
        if caps and "produce_response" in caps:
            return True
    return False


def has_pre_delivery_plugins(pipeline: list[PipelineTuple]) -> bool:
    """True iff any tuple's plugin declares a capability that fires
    *before* the response leaves the bridge for the client — namely
    ``response_modify`` or ``handle_tool_calls`` (D-008).

    Pre-delivery plugins operate on assembled frames; the executor
    forces the upstream call non-streaming when this returns True so
    the assembled frame is available for the plugins to inspect/modify.

    Capability-driven (not slot-string-matched) — `response_modify`
    and `handle_tool_calls` share slots with other capabilities (e.g.
    `outbound_params`, `post_response`), so we have to consult the
    capability table per-plugin.
    """
    for _slot, plugin, _config in pipeline:
        caps = plugin_loader.get_capabilities(
            plugin.__name__.rsplit(".", 1)[-1]
        ) or {}
        if "response_modify" in caps or "handle_tool_calls" in caps:
            return True
    return False


def has_observers(pipeline: list[PipelineTuple]) -> bool:
    """True iff any tuple's plugin declares ``post_response`` (D-007).

    Observers fire *after* the response is delivered to the client —
    BackgroundTask on streaming, ``asyncio.create_task`` on
    non-streaming. They never block bytes; they only observe the
    assembled assistant turn.
    """
    for _slot, plugin, _config in pipeline:
        caps = plugin_loader.get_capabilities(
            plugin.__name__.rsplit(".", 1)[-1]
        ) or {}
        if "post_response" in caps:
            return True
    return False


def has_intercepts(pipeline: list[PipelineTuple]) -> bool:
    """True iff any tuple's plugin declares ``handle_tool_calls`` (D-008).

    Intercepts are a subset of pre-delivery plugins — they specifically
    claim tool_calls by name and may trigger a re-call of the upstream
    resource. Helper for the executor's intercept-dispatch branch.
    """
    for _slot, plugin, _config in pipeline:
        caps = plugin_loader.get_capabilities(
            plugin.__name__.rsplit(".", 1)[-1]
        ) or {}
        if "handle_tool_calls" in caps:
            return True
    return False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _slot_family(slot: str) -> str:
    """Map a full slot string to its sub-block family name for cascade
    config lookup.

      identity.plugins / role.plugins / session.plugins / resource.plugins
        → "plugins"
      identity.context.plugins / role.context.plugins
        → "context.plugins"
      identity.response.plugins / role.response.plugins
        → "response.plugins"
    """
    if ".context.plugins" in slot:
        return "context.plugins"
    if ".response.plugins" in slot:
        return "response.plugins"
    return "plugins"
