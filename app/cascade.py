"""
Cascade resolver for ind-bridge V4.

Walks an identity's config chain and produces a ``ResolvedCascade`` —
a structured per-level view that the pipeline assembler then turns into
the per-identity tuple list.

V4 has four cascade levels (per D-006):

    identity > role > session > resource > server > .env

Per-level config blocks are stored as raw dicts; cascade resolution does
NOT pre-merge them into a single flat dict here — the assembler handles
that step per-slot, because the same plugin can appear at multiple levels
in the same slot family and the assembler is the right place for that
merge. ``cascade.py``'s job is purely "find the right config blocks for
this identity"; the assembler's job is "combine them into firing tuples."

Deep merge for nested dicts; lists replace (do not merge). This matches
operator intuition and V3 behaviour. Identity wins over role wins over
session wins over resource wins over server.

See ``CLAUDE.md`` for the architecture cheat-sheet, ``D-001`` for the
per-identity-pipeline-materialisation decision, and ``D-006`` for the
four-concerns / sessions-as-peer shape.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from . import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ResolvedCascade — the structured per-level view
# ---------------------------------------------------------------------------

@dataclass
class ResolvedCascade:
    """Per-level config view for one identity. Pure data.

    The assembler walks these and produces the firing tuple list.
    Lookups that returned no config block are stored as ``None`` (not as
    empty dicts) so the assembler can distinguish "no session declared"
    from "session block exists but is empty."
    """

    identity_key: str
    """The identity this cascade was resolved for."""

    identity_cfg: dict
    """The identity's own config block (always present if the identity
    exists; empty dict if the identity is malformed)."""

    role_key: str | None
    """The role key referenced by the identity, or None if no role."""

    role_cfg: dict | None
    """The role's config block, or None if no role/missing role."""

    session_key: str | None
    """The session key referenced by the role, or None if no session."""

    session_cfg: dict | None
    """The session's config block, or None if no session/missing session.
    Sessions are optional in V4 (per D-001/D-006)."""

    resource_key: str | None
    """The resource key referenced by the role, or None if no resource."""

    resource_cfg: dict | None
    """The resource's config block, or None if no resource/missing resource.
    A pipeline with no resource AND no terminal `produce_response` plugin
    will return 503 at request time — but that's the executor's concern,
    not cascade resolution."""

    server_cfg: dict
    """The global server: block (lowest priority). Always populated when
    config is loaded; empty dict if not."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def resolve_cascade(identity_key: str) -> ResolvedCascade | None:
    """Resolve the cascade for one identity.

    Returns a ``ResolvedCascade`` describing the per-level config view, or
    ``None`` if the identity itself doesn't exist in config (caller should
    treat this as a programming error — validate identity existence before
    calling). Missing role / session / resource references at lower levels
    are NOT errors — they're recorded as ``None`` in the resolved cascade
    so downstream code can decide how to handle (the validator already
    warns about them at startup).
    """
    identity_cfg = config.resolve_identity(identity_key)
    if identity_cfg is None:
        logger.warning(
            f"resolve_cascade called for unknown identity '{identity_key}' — "
            f"returning None. (This is usually a caller bug.)"
        )
        return None

    role_key = config.get_identity_role_key(identity_key)
    role_cfg: dict | None = None
    if role_key:
        role_cfg = config.resolve_role(role_key)
        if role_cfg is None:
            logger.debug(
                f"Identity '{identity_key}' references unknown role "
                f"'{role_key}' — role-level cascade will be empty."
            )

    # session: declared on the role
    session_key: str | None = None
    session_cfg: dict | None = None
    if role_cfg:
        session_key = role_cfg.get("session")
        if session_key:
            session_cfg = config.resolve_session(session_key)
            if session_cfg is None:
                logger.debug(
                    f"Role '{role_key}' references unknown session "
                    f"'{session_key}' — session-level cascade will be empty."
                )

    # resource: declared on the role (sub-block of role.resource: <name>)
    resource_key: str | None = None
    resource_cfg: dict | None = None
    if role_cfg:
        resource_key = role_cfg.get("resource")
        if resource_key:
            resource_cfg = config.resolve_resource(resource_key)
            if resource_cfg is None:
                logger.debug(
                    f"Role '{role_key}' references unknown resource "
                    f"'{resource_key}' — resource-level cascade will be empty."
                )

    return ResolvedCascade(
        identity_key=identity_key,
        identity_cfg=identity_cfg,
        role_key=role_key,
        role_cfg=role_cfg,
        session_key=session_key,
        session_cfg=session_cfg,
        resource_key=resource_key,
        resource_cfg=resource_cfg,
        server_cfg=config.get_server_cfg(),
    )


def merge_plugin_configs(
    cascade: ResolvedCascade,
    plugin_name: str,
    slot_family: str,
) -> dict | _Disabled:
    """
    Find every cascade level that declares ``plugin_name`` in the given
    slot family and produce a deep-merged final config dict (identity wins
    over role wins over session wins over resource).

    ``slot_family`` is one of:
        "plugins"
        "context.plugins"
        "response.plugins"

    The slot family determines which sub-block of each cascade level we
    look in. For example, ``slot_family="context.plugins"`` walks
    ``identity.context.plugins.<plugin_name>`` and
    ``role.context.plugins.<plugin_name>`` (sessions and resources don't
    have context blocks per D-006, so those levels are skipped).

    Returns the merged config dict, or ``{}`` if the plugin isn't declared
    at any cascade level for this slot family. Caller is the assembler;
    this function is a pure helper.

    May also return the ``DISABLED`` tombstone when the winning cascade level
    declares the plugin with a falsy-disable value (``false`` / ``disabled`` /
    ``off`` / ``0``). The assembler checks ``is DISABLED`` and skips the tuple.
    """
    # Bottom-to-top so identity overrides come last (highest priority wins).
    levels: list[dict | _Disabled] = []

    if slot_family == "plugins":
        # All four cascade levels can have top-level plugins
        for cfg in (cascade.resource_cfg, cascade.session_cfg,
                    cascade.role_cfg, cascade.identity_cfg):
            if cfg:
                levels.append(_get_plugin_config(cfg, ["plugins"], plugin_name))
    elif slot_family == "context.plugins":
        # Only identity and role have context blocks (D-006)
        for cfg in (cascade.role_cfg, cascade.identity_cfg):
            if cfg:
                levels.append(_get_plugin_config(cfg, ["context", "plugins"], plugin_name))
    elif slot_family == "response.plugins":
        # Only identity and role have response blocks (D-006)
        for cfg in (cascade.role_cfg, cascade.identity_cfg):
            if cfg:
                levels.append(_get_plugin_config(cfg, ["response", "plugins"], plugin_name))
    else:
        logger.warning(
            f"merge_plugin_configs called with unknown slot family "
            f"'{slot_family}' — returning empty dict."
        )
        return {}

    # Deep-merge bottom-up: start with empty, apply each level in order.
    # The DISABLED tombstone is a real overriding value (highest priority wins
    # BOTH ways): a tombstone at a higher level clears any merged dict from
    # below (disable), and a real dict at a higher level clears a tombstone
    # from below (re-enable). Symmetry falls out of "last declaration wins".
    merged: dict | _Disabled = {}
    for layer in levels:
        if layer is DISABLED:
            merged = DISABLED                 # higher level disables — drop lower config
        elif layer:                           # a non-empty dict
            if merged is DISABLED:
                merged = dict(layer)          # higher level RE-ENABLES — drop the tombstone
            else:
                merged = _deep_merge(merged, layer)
        # an empty dict ({} — "not declared here") changes nothing
    return merged


# ---------------------------------------------------------------------------
# Plugin-disable tombstone
# ---------------------------------------------------------------------------

class _Disabled:
    """Sentinel marking a plugin as disabled at a cascade level.

    A plugin declared with a falsy value (``plugin: false`` / ``disabled`` /
    ``off`` / ``0``) is *yeeted* from that identity's pipeline — the assembler
    emits no tuple for it. Unlike a bare ``{}`` ("run me with empty config"),
    the tombstone means "do NOT run me at all."

    It's a real value the cascade resolves: highest priority wins, BOTH ways.
    A role can disable a plugin and an identity can re-enable it by declaring a
    real config dict (dict beats tombstone); equally an identity can disable a
    plugin the role enabled (tombstone beats dict). The deep-merge does this
    for free because the sentinel is a non-dict override like any other.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return "<plugin disabled>"


#: Singleton tombstone. Compare with ``is DISABLED``.
DISABLED = _Disabled()

#: Documented falsy spellings that disable a plugin. YAML coerces bare
#: ``off``/``no`` to bools, so the string forms mainly cover quoted values and
#: the explicit ``"disabled"`` spelling, which reads like English in config.
_DISABLE_STRINGS = frozenset({"false", "disabled", "off", "no", "none"})


def _is_disable_value(value: Any) -> bool:
    """True iff ``value`` is a falsy plugin declaration meaning "disable me".

    Recognises ``False``, ``0`` (and ``0.0``), and the strings ``false`` /
    ``disabled`` / ``off`` / ``no`` / ``none`` (case-insensitive). Notably does
    NOT treat ``None`` as disable — a bare ``plugin:`` (YAML null) stays
    "operator meant empty", consistent with ``_deep_merge``'s null-skip rule.
    Also does NOT treat ``true`` / ``1`` as disable (those remain "empty config").
    """
    if value is True:
        return False  # explicit guard: bool is an int subclass; true != 0
    if value is False:
        return True
    if isinstance(value, int) and value == 0:
        return True  # 0 (and 0.0 via int check below)
    if isinstance(value, float) and value == 0.0:
        return True
    if isinstance(value, str):
        return value.strip().lower() in _DISABLE_STRINGS
    return False


# ---------------------------------------------------------------------------
# Internal helpers — pure functions, no side effects
# ---------------------------------------------------------------------------

def _get_plugin_config(
    cfg_block: dict,
    sub_path: list[str],
    plugin_name: str,
) -> dict | _Disabled:
    """Walk ``cfg_block`` along ``sub_path`` and return the dict at
    ``[plugin_name]`` if present. Defensive against any step being None,
    not-a-dict, or missing. Returns empty dict for "not declared here."

    Returns the ``DISABLED`` tombstone when the plugin is declared with a
    falsy-disable value (``plugin: false`` / ``disabled`` / ``off`` / ``0``).
    """
    cur: Any = cfg_block
    for key in sub_path:
        if not isinstance(cur, dict):
            return {}
        cur = cur.get(key)
        if cur is None:
            return {}
    if not isinstance(cur, dict):
        return {}
    plugin_cfg = cur.get(plugin_name)
    if plugin_cfg is None:
        return {}
    if _is_disable_value(plugin_cfg):
        # `plugin: false` / `disabled` / `off` / `0` — yeet it from the
        # pipeline. A real tombstone, not "empty config": the assembler skips
        # the tuple entirely. Distinct from `true` below.
        return DISABLED
    if not isinstance(plugin_cfg, dict):
        # Plugin declared with some OTHER non-dict value (e.g. `myplugin: true`)
        # — treat as "declared with empty config" rather than crashing.
        return {}
    return plugin_cfg


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge ``override`` into ``base``, returning a new dict.

    Rules (matched to operator intuition + V3 behaviour):
      - Both dicts at a key → recurse into the sub-dicts
      - Override has the key with a non-dict value → override wins (replace)
      - Base has the key, override doesn't → base value preserved
      - Override has a key base doesn't → override key+value added
      - Lists at the same key → override REPLACES (no list-merging — too magic)
      - None on either side at the same key → treated as missing (other side wins)

    Pure function — does not mutate either input. Returns a new dict.
    """
    result = dict(base)  # shallow copy; we'll deepen recursively as needed
    for key, override_val in override.items():
        if override_val is None:
            # `override.foo: null` should NOT clobber `base.foo: <something>`.
            # YAML produces None for explicit `key:` (no value) which is
            # almost always operator-meant-as-empty rather than meant-as-delete.
            continue
        base_val = result.get(key)
        if isinstance(base_val, dict) and isinstance(override_val, dict):
            result[key] = _deep_merge(base_val, override_val)
        else:
            # Non-dict override → replace. Includes lists (replace, not merge),
            # strings, ints, bools, etc.
            result[key] = override_val
    return result
