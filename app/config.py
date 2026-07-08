"""
Config loader for ind-bridge V4.

Parses ``config.yml`` into raw config blocks accessible by lookup. The four
clean concerns:

    resources   = WHERE   — outbound destination (or terminal `produce_response`)
    sessions    = HOW     — conversation management (optional)
    roles       = WHAT    — plugins, capabilities, context config
    identities  = WHO     — entry point: listener materialises here

V4 design decisions vs V3 (`mind-span-ce`)
------------------------------------------
- **Single role per identity.** The V3 ``roles: [list]`` shape is dropped;
  we warn loudly if encountered. Identities use ``role:`` (singular).
- **Internal identities are valid.** An identity without a ``token:`` is
  treated as internal (reachable via mesh / cron / future trigger types,
  not via HTTP bearer auth). Token map only contains tokenised identities;
  ``list_identities()`` returns all of them.
- **Lenient about unknown plugins.** Parsing does not validate plugin
  capability placements — that's the job of ``capabilities.py`` (TODO)
  during pipeline assembly. The parser just preserves config as raw dicts.
- **No role→resource validation at parse time.** Cascade resolution
  (``cascade.py``, TODO) handles that. The parser exposes lookup primitives;
  validation happens in a dedicated pass.
- **Fail loud, never fail to start.** Bad config (missing file, parse
  errors, structural issues) logs at appropriate levels and leaves the
  bridge in a degraded-but-running state. ``/health`` always works.

See ``CLAUDE.md`` for the architecture cheat-sheet and the project's
V4 design docs for the full spec.
"""

import logging
import os
import re

import yaml

logger = logging.getLogger(__name__)

CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config.yml")

_SERVER_CFG: dict = {}              # raw server: block, env vars expanded
_TOKEN_MAP: dict[str, str] = {}     # bearer_token → identity_key (tokenised identities only)
_config_loaded: bool = False


# ---------------------------------------------------------------------------
# Public API — load
# ---------------------------------------------------------------------------

def load_config() -> None:
    """
    Parse ``config.yml`` and populate module state. Called once at startup.

    Behaviour:
      - Missing file → INFO log, state remains empty (server serves /health).
      - Unparseable YAML → ERROR log, state remains empty.
      - Empty file or no ``server:`` block → WARNING, state remains empty.
      - Otherwise → state populated, INFO log with identity counts.

    Never raises. The bridge always starts.
    """
    global _config_loaded, _SERVER_CFG, _TOKEN_MAP

    if not os.path.exists(CONFIG_PATH):
        logger.info(
            f"No config file at {CONFIG_PATH} — "
            f"bridge will serve /health only. Create a config.yml to enable routing."
        )
        return

    try:
        with open(CONFIG_PATH, "r") as f:
            raw = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to parse config file {CONFIG_PATH}: {e}")
        return

    if not raw:
        logger.warning(f"Config file {CONFIG_PATH} is empty — serving /health only.")
        return

    try:
        raw = _expand_env_vars(raw)
        server = raw.get("server", {})
        if not server:
            logger.warning(
                f"Config file {CONFIG_PATH} has no 'server:' block — serving /health only."
            )
            return

        _SERVER_CFG = server
        _synthesize_internal_carriers(server)
        _TOKEN_MAP = _build_token_map(server)
        _config_loaded = True

        identities = server.get("identities", {}) or {}
        tokenised = len(_TOKEN_MAP)
        internal = len(identities) - tokenised
        logger.info(
            f"Config loaded: {len(identities)} identity/identities "
            f"({tokenised} tokenised, {internal} internal)."
        )
    except Exception as e:
        logger.error(f"Config structure error in {CONFIG_PATH}: {e}")


def is_config_loaded() -> bool:
    """True iff a valid config.yml has been parsed."""
    return _config_loaded


# ---------------------------------------------------------------------------
# Public API — lookups
# ---------------------------------------------------------------------------

def get_identity_key_for_token(token: str) -> str | None:
    """Returns the identity key for a bearer token, or None if not found.

    Only tokenised identities are in the token map; internal identities
    (no `token:` in config) are excluded by design.
    """
    return _TOKEN_MAP.get(token)


def get_server_cfg() -> dict:
    """Returns the full parsed `server:` block (env vars expanded)."""
    return _SERVER_CFG


def resolve_identity(key: str) -> dict | None:
    """Returns the raw identity config block for a given key, or None."""
    return _SERVER_CFG.get("identities", {}).get(key)


def resolve_role(key: str) -> dict | None:
    """Returns the raw role config block for a given key, or None."""
    return _SERVER_CFG.get("roles", {}).get(key)


def resolve_resource(key: str) -> dict | None:
    """Returns the raw resource config block for a given key, or None."""
    return _SERVER_CFG.get("resources", {}).get(key)


def resolve_session(key: str) -> dict | None:
    """Returns the raw session config block for a given key, or None.
    Sessions are optional in V4; many roles have ``session_key=None``."""
    return _SERVER_CFG.get("sessions", {}).get(key)


def list_identities() -> list[str]:
    """All identity keys in config order (tokenised + internal)."""
    return list((_SERVER_CFG.get("identities", {}) or {}).keys())


def list_roles() -> list[str]:
    """All role keys in config order."""
    return list((_SERVER_CFG.get("roles", {}) or {}).keys())


def list_resources() -> list[str]:
    """All resource keys in config order."""
    return list((_SERVER_CFG.get("resources", {}) or {}).keys())


def list_sessions() -> list[str]:
    """All session keys in config order."""
    return list((_SERVER_CFG.get("sessions", {}) or {}).keys())


def get_identity_role_key(identity_key: str) -> str | None:
    """
    Returns the role key referenced by an identity, or None.

    V4: only ``role:`` (singular) is supported. ``roles:`` (list) triggers
    a loud warning and only the first entry is used (defensive — V3 configs
    will eventually be migrated, but in the meantime we don't crash).
    """
    identity_cfg = resolve_identity(identity_key)
    if not identity_cfg:
        return None

    # V4: singular `role:` is the supported shape
    role = identity_cfg.get("role")
    if role:
        return role

    # V3 backwards-compat with a loud warning
    roles = identity_cfg.get("roles")
    if isinstance(roles, list) and roles:
        logger.warning(
            f"Identity '{identity_key}' uses V3 `roles: [list]` shape — "
            f"V4 supports single `role:` only. Using first entry '{roles[0]}'. "
            f"Update config to `role: {roles[0]}` to silence this warning."
        )
        return roles[0]

    return None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _expand_env_vars(obj):
    """Recursively expand ``${VAR}`` in string values. Missing vars → empty."""
    if isinstance(obj, str):
        return re.sub(r'\$\{(\w+)\}', lambda m: os.environ.get(m.group(1), ""), obj)
    elif isinstance(obj, dict):
        return {k: _expand_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_expand_env_vars(i) for i in obj]
    return obj


INTERNAL_CARRIER_PREFIX = "__bridge_internal__"
"""Prefix for identities the bridge synthesises for roles that are reachable
agent-to-agent (``bridge_messaging.agent_listing: true``) but have no operator-
declared identity pointing at them. Dispatch (``execute``) is keyed by identity;
a synthesised carrier gives such a role a token-less internal identity so its
pipeline can be materialised. These are never externally callable (no token,
no listener) — they exist purely as the in-process dispatch handle for the role."""


def _synthesize_internal_carriers(server: dict) -> None:
    """Inject a token-less internal identity for EVERY role listed for
    agent-to-agent messaging (``agent_listing: true``).

    THE TWO LANES (why this is unconditional). A message reaches a role two ways:
      • CHAT lane — ``HTTP client → operator identity (the WHO) → role``. The
        identity is load-bearing: it IS the caller (name/trust/overrides).
      • bridge_messaging lane — ``sending agent → recipient ROLE directly, with a
        CONSTRUCTED sovereign caller``. The caller is minted by the bridge; it must
        NOT borrow a pre-existing chat identity (that identity's own
        ``context.plugins`` — e.g. a ``conversational_memory: false`` tombstone —
        would cascade over the role and silently alter the delivered pipeline).

    Dispatch (``execute``) is keyed by identity, so the bridge_messaging lane still
    needs a vehicle. That vehicle is this NEUTRAL role-only carrier —
    ``__bridge_internal__<role>`` with ``{role: <role>}`` and nothing else — so a
    delivery materialises the role's composition PURELY, inheriting no human
    identity's overrides. The sovereign caller (name/trust/additional) is stamped
    on top in ``_build_ctx`` (see ``_bridge_caller``), so the bare carrier needs no
    ``context`` of its own.

    Minted for every agent-listed role REGARDLESS of whether operator identities
    also point at it — the chat lane and the delivery lane are independent, and the
    carrier is the delivery lane's handle. (Previously withheld when a role had an
    operator identity, which forced ``bridge_messaging`` to borrow the first such
    identity as the carrier — an ordering-dependent leak of that identity's
    ``context.plugins`` into deliveries.) Idempotent and non-destructive: an already
    -present carrier key is left untouched. Mutates ``server["identities"]`` in place.
    """
    roles = server.get("roles", {}) or {}
    identities = server.setdefault("identities", {}) or {}
    server["identities"] = identities  # ensure the dict is attached even if it was None

    for role_key, role_cfg in roles.items():
        if not isinstance(role_cfg, dict):
            continue
        # bridge_messaging's idiomatic single-block home is context.plugins (the
        # tool-inject slot, from which background/handle_tool_calls fan out). Read
        # THAT first, falling back to the legacy top-level plugins block so older
        # two-block configs still synthesise carriers. Mirrors the plugin's own
        # _role_bridge_messaging_cfg resolution (kept in sync by hand — config.py
        # must not import plugin code).
        ctx_plugins = (role_cfg.get("context") or {}).get("plugins") or {}
        bm = ctx_plugins.get("bridge_messaging")
        if not isinstance(bm, dict):
            bm = (role_cfg.get("plugins") or {}).get("bridge_messaging")
        agent_listed = isinstance(bm, dict) and bm.get("agent_listing")

        # A conversational_memory TASK worker (tasks: [label_enrichment, …]) is also
        # reached in-process via execute() — the enrichment loop fires the worker's
        # role through its carrier, exactly like a bridge_messaging delivery. So it
        # needs the same neutral role-only dispatch handle, even though it declares
        # no bridge_messaging block. Read tasks: the same context-first way.
        cm = ctx_plugins.get("conversational_memory")
        if not isinstance(cm, dict):
            cm = (role_cfg.get("plugins") or {}).get("conversational_memory")
        task_worker = isinstance(cm, dict) and bool(cm.get("tasks"))

        if not (agent_listed or task_worker):
            continue
        carrier_key = f"{INTERNAL_CARRIER_PREFIX}{role_key}"
        if carrier_key in identities:
            continue
        identities[carrier_key] = {"role": role_key}
        reason = "agent-listed" if agent_listed else "task-worker"
        logger.info(
            f"Synthesised internal carrier identity '{carrier_key}' for "
            f"{reason} role '{role_key}' (neutral in-process dispatch vehicle — "
            f"execute() never borrows a chat identity)."
        )


def _build_token_map(server: dict) -> dict[str, str]:
    """
    Build the bearer-token → identity_key map.

    V4 differences from V3:
      - Identities without a ``token:`` are NOT errors — they're internal-only
        (reachable via mesh, cron, or other future listener types). They're
        excluded from the token map but remain in ``server.identities``.
      - Token-level validation only: missing token, duplicate token, empty
        value. Role/resource validity is NOT checked here — that's the job
        of capability validation in a later pass.
    """
    identities = server.get("identities", {}) or {}

    token_map: dict[str, str] = {}
    seen_tokens: dict[str, str] = {}  # token → identity_key (for duplicate detection)

    for identity_key, identity_cfg in identities.items():
        if not identity_cfg:
            logger.warning(
                f"Identity '{identity_key}' has no config block — pipeline disabled "
                f"for this identity. Will return 503 if reached."
            )
            continue

        token = identity_cfg.get("token")
        if not token:
            # Internal identity — valid in V4, just not externally callable via HTTP.
            logger.debug(
                f"Identity '{identity_key}' has no token — internal-only "
                f"(reachable via mesh/cron/internal triggers, not HTTP bearer auth)."
            )
            continue

        if token in seen_tokens:
            logger.warning(
                f"Identity '{identity_key}' has a duplicate token "
                f"(already registered to '{seen_tokens[token]}') — skipping."
            )
            continue

        seen_tokens[token] = identity_key
        token_map[token] = identity_key
        logger.debug(f"Registered token for identity '{identity_key}'")

    return token_map
