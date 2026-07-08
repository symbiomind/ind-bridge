"""
mcp_client — connect MCP servers, offer their tools to the agent as
bridge-native tools (V4).

The payoff of the ``bridge_native__`` namespace channel: an operator adds an
MCP (Model Context Protocol) server to config and the AI agent gains that
server's tools — file management, memory, search, whatever the server exposes —
with ZERO hand-written tool code.

Three capabilities, one plugin (the agent_tools shape, plus discovery):

  * ``background`` (D-011) — at startup, connect to each configured MCP server
    and ``list_tools()`` to DISCOVER what it offers. Fail-open: a server that's
    down/slow at boot is logged and skipped (its tools just aren't offered this
    run); the bridge always starts; other servers are unaffected.

  * ``context_modify`` — inject the discovered tool definitions into
    ``ctx.request.tools`` (the OpenAI function list — NOT ctx.bridge_context),
    exactly like agent_tools. The agent sees them as native callable tools.

  * ``handle_tool_calls`` (D-008) — when the agent calls one, route it to the
    right MCP server and execute it, returning the result as the tool-result
    string. Default is fresh-connection-per-call (like conversational_memory);
    a resource may opt into ``persistent: true`` to reuse ONE held session
    across calls (see the persistent-session pool below) — needed for
    gateway-fronted stdio servers whose containers otherwise cold-start per call.

Three-layer tool name the agent sees::

    bridge_native__  +  <server_key>__  +  <mcp_tool_name>
    e.g.  bridge_native__diary__store_memory

  * ``bridge_native__`` — the bridge-wide invariant (app/bridge_native.py),
    applied at injection / stripped at dispatch by CORE. Already built.
  * ``<server_key>__`` — THIS plugin's own sub-namespace (the per-server config
    key), so two MCP servers exposing the same tool name don't collide. We apply
    it when building names and ``split("__", 1)`` it back at dispatch to route.
  * ``<mcp_tool_name>`` — the server's real tool name, stored verbatim (never
    reconstructed by splitting, so a tool name containing ``__`` survives).

Dynamic ownership: because tools are DISCOVERED at startup (after the validator
runs), ``OWNED_TOOLS`` is a CALLABLE, not a static list. Core's
``_plugin_owned_tools`` calls it; the validator treats a callable as satisfying
the D-008 presence invariant.

Config (operator writes the server map ONCE at role.plugins)::

    resources:
      my_agent_diary_mcp: {endpoint_url: http://...:6005/mcp, token: ${SECRET_A}, timeout: 120}
    identities:
      my_agent: { plugins: { mcp_client: {} } }        # background discovery trigger
    roles:
      my_agent_role:
        plugins:
          mcp_client:                                  # server map + handle_tool_calls
            diary:        { resource: my_agent_diary_mcp }  # optional: tools: [allowlist]
            personal-mcp: { resource: personal_mcp }
        context:
          plugins:
            mcp_client: {}                             # inject tools

Constraints: a ``server_key`` must NOT contain ``__`` (it collides with the
sub-namespace separator — warn-and-skip).

Per-server ACL + param injection (policy layer, enforced at BOTH injection and
dispatch — a denied tool is neither offered NOR callable-from-history)::

    roles:
      my_agent_role:
        plugins:
          mcp_client:
            diary:
              resource: my_agent_diary_mcp
              allow: [retrieve_memories]     # whitelist (only these visible/callable)
              deny:  [store_memory]          # blacklist (beats allow → deny-wins)
              params:                         # gateway-level arg injection
                append:                       # SHALLOW overwrite of named keys
                  retrieve_memories:
                    source: "my_agent"        # stamped server-side, model can't override
                # replace:                    # discard model args, use these
                #   some_tool: { query: "fixed" }

  * ``allow`` — whitelist. If present, ONLY listed tools are exposed/callable.
    ``tools:`` is a back-compat ALIAS for ``allow`` (same semantics; unioned if
    both present).
  * ``deny`` — blacklist. Removes tools even from ``allow``. On overlap, deny
    wins (more-restrictive-wins, fail-safe).
  * ``params.append`` — per-tool dict; SHALLOW-overwrites the named top-level arg
    keys onto the call verbatim (no deep-merge, no list-concat), clobbering the
    model's value. The sovereign-stamp fix (e.g. forcing ``source``).
  * ``params.replace`` — per-tool dict; discards the model's args entirely and
    substitutes the operator's. append runs after replace (append wins overlap).
  * Values are injected verbatim — schema-agnostic; a wrong type is rejected by
    the MCP server and narrated as a tool-error (operator owns config).

Parked for future-us: reconnect/retry of a server that was down at boot (needs
more dogfooding/research); cross-plugin collisions among dynamic tools (can't be
validated at startup).
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
from typing import TYPE_CHECKING

from app import bridge_native

if TYPE_CHECKING:
    from app.context import PipelineCtx, StartupCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "background":        ["identity.plugins"],
    "context_modify":    ["identity.context.plugins", "role.context.plugins"],
    "handle_tool_calls": ["identity.plugins", "role.plugins"],
}

_NS_SEP = "__"  # sub-namespace separator (matches bridge_native's prefix style)

# ---------------------------------------------------------------------------
# Module state — populated ONCE by start_background at boot (lifespan step 6,
# discovery awaited inline before the coro returns), read thereafter by
# modify_context / handle_tool_calls / OWNED_TOOLS(). Single-writer-at-boot,
# many-reader-per-request — no lock needed (conv_mem's _resource_cache style).
# ---------------------------------------------------------------------------

# Registry is keyed by (resource_key, tool_name) — the RESOURCE, not the
# server_key alias. Two roles can name a server the same (`filesystem`) while
# pointing at DIFFERENT resources (`agent_a_filesystem` vs `filesystem`); the
# server_key is agent-facing presentation, the resource is the true identity, so
# routing/defs key on the resource to avoid cross-role collision (last-writer-wins
# bleed). The agent-facing wire name still uses server_key — built per-role at
# injection by mapping the role's server_key → resource_key.
_tool_defs: dict[tuple[str, str], dict] = {}
"""``(resource_key, tool_name)`` → OpenAI function def dict, built with the RAW
tool name (no server_key, no bridge_native prefix). modify_context applies the
``bridge_native__<server_key>__`` layer per-role at injection."""

_tool_routes: dict[tuple[str, str], tuple[dict, str]] = {}
"""``(resource_key, tool_name)`` → (conn dict, raw MCP tool name). The raw tool
name is stored verbatim so a ``__``-containing tool name is never reconstructed
by splitting — dispatch reads it straight from here."""


# ---------------------------------------------------------------------------
# OWNED_TOOLS — a CALLABLE (dynamic; discovered at startup)
# ---------------------------------------------------------------------------

def OWNED_TOOLS() -> list[str]:
    """Dynamic owned-tools list. Core's ``_plugin_owned_tools`` calls this when
    the attribute is callable; the D-008 validator treats 'callable present' as
    satisfying the non-empty presence invariant (discovery hasn't run at
    validation time).

    Claimable names are the AGENT-FACING ``server_key__tool`` forms (NO
    bridge_native prefix; core strips it before matching). The registry is keyed
    by resource (not server_key) and the wire name uses server_key, so we map
    each DISCOVERED resource back to EVERY server_key alias that points at it,
    across all roles, and emit ``server_key__tool`` for each. This is
    presence-only (does the bridge own this name?) — the per-role gate in
    modify_context/handle_tool_calls is what actually scopes a given identity.

    NOTE (regression guard): we invert resource→server_keys here rather than
    reading a flat ``{server_key: resource_key}`` map. Multiple roles can share a
    server_key (`filesystem` → agent_a_filesystem / agent_b_filesystem); a flat
    map collides on the key and would drop every alias but one — and if the
    survivor is an UNdiscovered/stale resource (e.g. a role still pointing
    `filesystem` at a now-deleted `filesystem` resource), the real discovered
    tools get skipped entirely, never claimed, and LEAK to the harness as "tool
    not found". Build resource→{server_keys} so every alias of every discovered
    resource is represented."""
    # resource_key -> set of server_key aliases that map onto it (across roles)
    resource_to_aliases: dict[str, set[str]] = {}
    for server_key, resource_key in _all_server_to_resource_pairs():
        resource_to_aliases.setdefault(resource_key, set()).add(server_key)

    names: set[str] = set()
    for (resource_key, tool) in _tool_defs:  # only DISCOVERED (resource, tool)
        for server_key in resource_to_aliases.get(resource_key, set()):
            names.add(_join_name(server_key, tool))

    # Tolerant claim for segment-less names (model dropped the <server_key>__
    # middle — e.g. `search` for `memory__search`). Claim the BARE name too, but
    # ONLY when it is globally unambiguous — exactly one resource owns a tool by
    # that name. This lets core's claim-matcher (strip_namespace(name) in owned)
    # recognize the malformed call as bridge-owned instead of misclassifying it
    # as harness-pending (which tripped the mixed-calls turn-loss). Dispatch
    # re-confirms sole ownership per-role via _resolve_single_owner, so this
    # presence-claim never misroutes.
    # Ambiguous bare names (>1 owning resource) are deliberately left out → they
    # fall to the synthetic 'no handler' path rather than a silent wrong guess.
    resources_by_tool: dict[str, set[str]] = {}
    for (resource_key, tool) in _tool_defs:
        resources_by_tool.setdefault(tool, set()).add(resource_key)
    for tool, resources in resources_by_tool.items():
        if len(resources) == 1:
            names.add(tool)

    return sorted(names)


# ---------------------------------------------------------------------------
# Namespacing helpers (server_key sub-layer; bridge_native is core's job)
# ---------------------------------------------------------------------------

def _join_name(server_key: str, tool_name: str) -> str:
    return f"{server_key}{_NS_SEP}{tool_name}"


def _split_name(clean_name: str) -> tuple[str, str] | None:
    """Split a ``server__tool`` name into (server_key, tool_name). Split ONCE,
    leftmost — server_key is the first segment, the rest is the tool name
    verbatim (so a tool name containing ``__`` survives). Returns None if there
    is no separator."""
    if _NS_SEP not in clean_name:
        return None
    server_key, tool_name = clean_name.split(_NS_SEP, 1)
    return server_key, tool_name


def _resolve_single_owner(bare_tool: str, server_to_resource: dict[str, str]) -> str | None:
    """Tolerant routing for a segment-less bridge name. Small models (9b,
    and some larger ones) sometimes emit ``bridge_native__search`` — DROPPING the
    middle ``<server_key>__`` segment — so ``_split_name`` returns None, the name
    can't be claimed or dispatched, and a genuinely-bridge call gets
    misclassified as harness-pending → the mixed-calls path fires and (pre-fix)
    the turn was lost.

    Given a BARE tool name (no ``__``) and THIS identity's
    ``{server_key: resource_key}`` map, return the sole ``server_key`` whose
    resource actually owns a tool of that name, IFF exactly one does. Zero or
    >1 owners → None (ambiguous; never guess across owners — enforcement, not
    prompting; the caller falls back to the synthetic 'no handler' path). The
    reconstructed ``server_key__tool`` then routes normally through the existing
    per-role gate + ACL + route lookup — no dispatch shortcut, same security."""
    if _NS_SEP in bare_tool:
        return None  # not segment-less; normal split applies
    owners = [
        server_key
        for server_key, resource_key in server_to_resource.items()
        if (resource_key, bare_tool) in _tool_defs
    ]
    return owners[0] if len(owners) == 1 else None


# ---------------------------------------------------------------------------
# Resource resolution (conv_mem / memory_enricher pattern)
# ---------------------------------------------------------------------------

def _resolve_conn(resource_key: str | None) -> dict | None:
    """Resolve an MCP-shape resource to ``{endpoint_url, token, timeout}``.
    Returns None (caller warns + skips) when missing or has no endpoint_url."""
    if not resource_key:
        return None
    from app import config as app_config
    cfg = app_config.resolve_resource(resource_key) or {}
    endpoint_url = cfg.get("endpoint_url")
    if not endpoint_url:
        return None
    timeout_raw = cfg.get("timeout")
    return {
        "endpoint_url": endpoint_url,
        "token": cfg.get("token") or "",
        "timeout": float(timeout_raw) if timeout_raw is not None else 120.0,
        # Opt-in (default off): keep ONE MCP session open to this resource and
        # reuse it across calls, instead of open-and-teardown per call. ONLY for
        # gateway-fronted STDIO servers (docker/git), where each new bridge
        # session forces the gateway to cold-`docker run` a fresh container (the
        # gateway's `longLived` keptClients cache is keyed PER SESSION, so a
        # fresh session every call = guaranteed cache miss = ~20s cold-start
        # every time). Plain long-running HTTP MCP servers (memory-mcp-ce) don't
        # benefit and should leave this off — the stateless path stays the
        # default. ``resource_key`` is carried so _call_tool can key the pool.
        "persistent": bool(cfg.get("persistent")),
        "resource_key": resource_key,
    }


# ---------------------------------------------------------------------------
# start_background — locate the server map, return the discovery coroutine
# ---------------------------------------------------------------------------

def start_background(ctx: "StartupCtx", config: dict):
    """Locate this identity's MCP server map and return the one-shot discovery
    coroutine for core to schedule. Returns None (no task) when no servers are
    configured — DLC-grace.

    CONFIG-LOCATION GOTCHA: ``_spawn_background_tasks`` hands us the
    ``identity.plugins.mcp_client`` block as ``config``, but the server map
    lives at ``role.plugins.mcp_client`` (and ``ctx.identity_cfg`` is the RAW
    identity block — NOT cascade-merged with the role). So we resolve the role
    ourselves. Fall back to ``config`` if the map was placed on the identity."""
    from app import config as app_config

    server_map = _find_server_map(ctx, config, app_config)
    if not server_map:
        logger.info(
            f"mcp_client: identity '{ctx.identity_key}' has no MCP servers "
            f"configured (role.plugins.mcp_client is empty) — discovery skipped."
        )
        return None

    return _discover_all(server_map)


def _find_server_map(ctx: "StartupCtx", config: dict, app_config) -> dict:
    """The server map is a dict of ``server_key -> {resource, tools?}``. Prefer
    the role's ``plugins.mcp_client`` block; fall back to the passed identity
    block. Entries whose value isn't a dict (e.g. a bare ``mcp_client: {}``
    marker) are ignored — they're the inject/handle wiring, not the map."""
    role_key = app_config.get_identity_role_key(ctx.identity_key)
    role_cfg = app_config.resolve_role(role_key) if role_key else None
    if role_cfg:
        candidate = (role_cfg.get("plugins") or {}).get("mcp_client")
        mapped = _only_server_entries(candidate)
        if mapped:
            return mapped
    # Fall back to the identity.plugins.mcp_client block we were handed.
    return _only_server_entries(config)


def _only_server_entries(block) -> dict:
    """Keep only ``server_key -> {dict}`` entries from a plugin config block."""
    if not isinstance(block, dict):
        return {}
    return {k: v for k, v in block.items() if isinstance(v, dict)}


def _role_server_keys(ctx: "PipelineCtx") -> set[str]:
    """The set of MCP server keys THIS role may use — resolved from the role's
    ``plugins.mcp_client`` server map. Discovery is module-GLOBAL (one shared
    `_tool_defs`/`_tool_routes` across all identities), so both injection
    (modify_context) and dispatch (handle_tool_calls) MUST gate on this set —
    otherwise a role configured for only 'filesystem' would be offered (and
    could route to) another identity's 'diary' tools. Exposure is
    per-cascade-level even though discovery is global.

    Returns an empty set if the role declares no server map (→ no MCP tools for
    this role)."""
    from app import config as app_config
    role_key = getattr(ctx.role, "key", None)
    role_cfg = app_config.resolve_role(role_key) if role_key else None
    if not role_cfg:
        return set()
    server_map = _only_server_entries((role_cfg.get("plugins") or {}).get("mcp_client"))
    return set(server_map.keys())


def _role_server_map(ctx: "PipelineCtx") -> dict[str, dict]:
    """THIS role's full ``server_key -> {resource, tools?}`` map (raw). Used to
    read per-server ``tools:`` allowlists at injection. Empty if none."""
    from app import config as app_config
    role_key = getattr(ctx.role, "key", None)
    role_cfg = app_config.resolve_role(role_key) if role_key else None
    if not role_cfg:
        return {}
    return _only_server_entries((role_cfg.get("plugins") or {}).get("mcp_client"))


def _role_server_to_resource(ctx: "PipelineCtx") -> dict[str, str]:
    """THIS role's ``server_key -> resource_key`` map (from
    ``role.plugins.mcp_client``). The registry is keyed by resource, so injection
    and dispatch use this to translate the agent-facing server_key alias into the
    resource whose discovered tools/route to use. Empty if the role declares no
    server map. (Server entries with no ``resource`` are skipped.)"""
    from app import config as app_config
    role_key = getattr(ctx.role, "key", None)
    role_cfg = app_config.resolve_role(role_key) if role_key else None
    if not role_cfg:
        return {}
    server_map = _only_server_entries((role_cfg.get("plugins") or {}).get("mcp_client"))
    out: dict[str, str] = {}
    for server_key, server_cfg in server_map.items():
        resource_key = server_cfg.get("resource")
        if resource_key:
            out[server_key] = resource_key
    return out


def _all_server_to_resource_pairs() -> list[tuple[str, str]]:
    """Every ``(server_key, resource_key)`` pair across all roles' mcp_client
    maps — as a LIST of pairs, NOT a dict. A dict keyed by server_key collides
    when multiple roles share a server_key (`filesystem` → three different
    per-agent resources) and silently drops all but one alias, which broke
    OWNED_TOOLS claim-matching (the dropped survivor could be a stale/undiscovered
    resource → filesystem tools never claimed → leaked to the harness). Keeping
    pairs preserves every alias→resource edge."""
    from app import config as app_config
    pairs: list[tuple[str, str]] = []
    try:
        roles = (app_config.get_server_cfg() or {}).get("roles") or {}
    except Exception:
        return pairs
    for role_cfg in roles.values():
        if not isinstance(role_cfg, dict):
            continue
        server_map = _only_server_entries((role_cfg.get("plugins") or {}).get("mcp_client"))
        for server_key, server_cfg in server_map.items():
            resource_key = server_cfg.get("resource")
            if resource_key:
                pairs.append((server_key, resource_key))
    return pairs


# ---------------------------------------------------------------------------
# Discovery (one-shot at boot, fail-open per server)
# ---------------------------------------------------------------------------

async def _discover_all(server_map: dict) -> None:
    """Connect to each server, list its tools, register defs + routes keyed by
    ``(resource_key, tool_name)``. One-shot: no persistent connection, no
    keep-alive loop (fresh connection per call at execution time). Fail-open: a
    server that errors is logged and skipped; the others still register.

    Resource-keyed so two roles' same-named servers (`filesystem`) pointing at
    DIFFERENT resources don't collide. DEDUPE by resource: this coroutine runs
    once per identity at boot, and several identities may map to the same
    resource — we only list a resource's tools ONCE (already-discovered →
    skip). The per-server ``tools:`` allowlist is NOT applied here (it's
    per-role/per-server presentation, applied at injection in modify_context);
    discovery registers everything the resource exposes.

    Parked for future-us: reconnect/retry of a server that was down at boot
    (needs more dogfooding/research). Discovery is one-shot here."""
    resources_ok = 0
    for server_key, server_cfg in server_map.items():
        if _NS_SEP in server_key:
            logger.warning(
                f"mcp_client: server key '{server_key}' contains '{_NS_SEP}', "
                f"which collides with the sub-namespace separator — skipping. "
                f"Rename the server key."
            )
            continue

        resource_key = server_cfg.get("resource")
        if not resource_key:
            logger.warning(
                f"mcp_client: server '{server_key}' has no 'resource' — skipping."
            )
            continue

        # Already discovered this resource (another identity/role got here first
        # this boot)? The registry is resource-keyed, so don't re-list it.
        if any(res == resource_key for (res, _tool) in _tool_defs):
            continue

        conn = _resolve_conn(resource_key)
        if conn is None:
            logger.warning(
                f"mcp_client: server '{server_key}' resource '{resource_key}' "
                f"unresolved (missing or no endpoint_url) — skipping this server."
            )
            continue

        try:
            tools = await _list_tools(conn)
        except Exception as e:
            logger.warning(
                f"mcp_client: discovery failed for resource '{resource_key}' "
                f"(server '{server_key}', {type(e).__name__}: {e}) — its tools "
                f"won't be offered this run. (Reconnect/retry parked for future-us.)"
            )
            continue

        count = 0
        for t in tools:
            raw_name = getattr(t, "name", None)
            if not raw_name:
                continue
            key = (resource_key, raw_name)
            # def built with the RAW tool name; modify_context applies the
            # bridge_native__<server_key>__ wire-name layer per-role.
            _tool_defs[key] = _build_openai_def(raw_name, t)
            _tool_routes[key] = (conn, raw_name)
            count += 1
        resources_ok += 1
        logger.info(
            f"mcp_client: resource '{resource_key}' (server '{server_key}') — "
            f"discovered {count} tool(s)."
        )

    logger.info(
        f"mcp_client: discovery complete — {len(_tool_defs)} tool(s) across "
        f"{resources_ok} resource(s) this pass."
    )


def _build_openai_def(clean_name: str, tool) -> dict:
    """Build an OpenAI function-tool def from an MCP Tool. ``inputSchema`` is a
    JSON Schema object — the same shape OpenAI's ``function.parameters`` wants —
    so it's used directly; empty/None substitutes a minimal object schema."""
    schema = getattr(tool, "inputSchema", None) or {"type": "object", "properties": {}}
    return {
        "type": "function",
        "function": {
            "name": clean_name,  # CLEAN — bridge_native applied at injection
            "description": getattr(tool, "description", "") or "",
            "parameters": schema,
        },
    }


# ---------------------------------------------------------------------------
# MCP calls — fresh connection per call (conversational_memory's pattern),
# OR a persistent reused session for gateway-fronted stdio resources (opt-in).
# ---------------------------------------------------------------------------

async def _list_tools(conn: dict) -> list:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    token = conn["token"]
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with streamablehttp_client(conn["endpoint_url"], headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.list_tools()
            return list(result.tools)


# --- Persistent session pool (opt-in; see _resolve_conn 'persistent') --------
#
# Goal: keep ONE MCP session open across calls so the gateway sees a STABLE
# session and its `longLived` container stays warm (cold-start paid once).
#
# WHY AN OWNER TASK (and not just an AsyncExitStack kept open across calls):
# ``ClientSession`` and ``streamablehttp_client`` open anyio task groups / cancel
# scopes BOUND TO THE TASK THAT ENTERED THEM. A bridge request runs in its own
# asyncio task; when that request's task ends, anyio tears down any scope entered
# inside it — the session's reader dies and the NEXT call (a different task) gets
# ``ClosedResourceError`` ("exit cancel scope in a different task than it was
# entered in"). So the contexts must be entered AND exited in ONE long-lived task.
#
# Pattern: each persistent resource gets a dedicated OWNER coroutine (an actor).
# It opens the session, initialises it, then loops pulling jobs off a queue,
# runs ``call_tool`` IN ITS OWN TASK, and resolves each job's future. Request
# tasks only ``put`` a job and ``await`` its future — they never touch the MCP
# contexts, so no cross-task scope violation. The owner is a DETACHED top-level
# task (tracked in ``_owner_tasks``), so it outlives the request that spawned it.

_session_owners: dict[str, "_SessionOwner"] = {}
_owner_tasks: dict[str, asyncio.Task] = {}
_pool_lock = asyncio.Lock()  # guards create/replace of a resource's owner


class _SessionOwner:
    """Actor owning one persistent MCP session. ``call`` is the only public
    entry: it's awaited by request tasks; the work runs in the owner task."""

    def __init__(self, resource_key: str, conn: dict):
        self.resource_key = resource_key
        self.conn = conn
        self._jobs: asyncio.Queue = asyncio.Queue()
        self._ready: asyncio.Future = asyncio.get_event_loop().create_future()
        self._closing = False

    async def call(self, raw_tool_name: str, args: dict):
        """Submit a tool call to the owner task and await its result. Raises if
        the session failed to open (caller drops + retries)."""
        await self._ready  # propagates open/initialise failure to the caller
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        await self._jobs.put((raw_tool_name, args, fut))
        return await fut

    async def run(self):
        """Owner-task body: open the session, signal ready, serve jobs until
        cancelled. ALL context enter/exit + call_tool happen in THIS task."""
        from mcp import ClientSession
        from mcp.client.streamable_http import streamablehttp_client

        token = self.conn["token"]
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        try:
            async with streamablehttp_client(self.conn["endpoint_url"], headers=headers) as (read, write, _):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    if not self._ready.done():
                        self._ready.set_result(True)
                    logger.info(
                        f"mcp_client: opened persistent session for resource "
                        f"'{self.resource_key}' (gateway container will stay warm)."
                    )
                    await self._serve(session)
        except asyncio.CancelledError:
            raise
        except BaseException as e:
            # Open/serve failed — wake any waiter on _ready, fail in-flight jobs.
            if not self._ready.done():
                self._ready.set_exception(e)
            self._fail_pending(e)
            logger.warning(
                f"mcp_client: persistent session owner for "
                f"'{self.resource_key}' exited ({type(e).__name__}: {e})."
            )

    async def _serve(self, session):
        while not self._closing:
            raw_tool_name, args, fut = await self._jobs.get()
            if fut.cancelled():
                continue
            try:
                result = await session.call_tool(raw_tool_name, arguments=args)
                if not fut.done():
                    fut.set_result(result)
            except asyncio.CancelledError:
                # Session is going down — fail this job and re-raise so the
                # owner unwinds its contexts in its OWN task (correct scope).
                if not fut.done():
                    fut.set_exception(asyncio.CancelledError())
                raise
            except BaseException as e:
                # A single call failed (e.g. gateway dropped). Fail THIS job and
                # exit the owner so the resource gets a fresh session next call.
                if not fut.done():
                    fut.set_exception(e)
                self._fail_pending(e)
                raise

    def _fail_pending(self, e: BaseException):
        while not self._jobs.empty():
            try:
                _t, _a, fut = self._jobs.get_nowait()
            except asyncio.QueueEmpty:
                break
            if not fut.done():
                fut.set_exception(e)


async def _get_owner(conn: dict) -> _SessionOwner:
    """Return the resource's session owner, spawning its detached task on first
    use (double-checked under _pool_lock)."""
    resource_key = conn["resource_key"]
    owner = _session_owners.get(resource_key)
    if owner is not None:
        return owner
    async with _pool_lock:
        owner = _session_owners.get(resource_key)
        if owner is None:
            owner = _SessionOwner(resource_key, conn)
            _session_owners[resource_key] = owner
            _owner_tasks[resource_key] = asyncio.ensure_future(owner.run())
    return owner


async def _drop_owner(resource_key: str) -> None:
    """Cancel and forget a resource's session owner (gateway restart / broken
    pipe / shutdown). The owner unwinds its MCP contexts in its OWN task on
    cancellation — the correct anyio scope. Best-effort."""
    owner = _session_owners.pop(resource_key, None)
    if owner is not None:
        owner._closing = True
    task = _owner_tasks.pop(resource_key, None)
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception) as e:
            logger.debug(f"mcp_client: owner '{resource_key}' teardown — {type(e).__name__}: {e}")


async def shutdown() -> None:
    """Cancel every persistent session owner. Wired from the bridge lifespan
    shutdown so the gateway's `--rm` containers don't leak when the bridge
    stops."""
    for resource_key in list(_session_owners.keys()):
        await _drop_owner(resource_key)


async def _call_tool_persistent(conn: dict, raw_tool_name: str, args: dict):
    """Submit a call to the resource's session owner. On a session error, drop
    the dead owner and retry ONCE with a fresh one — mirrors the gateway's own
    delete-on-error so a gateway restart doesn't permanently wedge the tools."""
    resource_key = conn["resource_key"]
    owner = await _get_owner(conn)
    try:
        return await owner.call(raw_tool_name, args)
    except Exception as e:
        logger.warning(
            f"mcp_client: persistent session for '{resource_key}' failed "
            f"({type(e).__name__}: {e}) — dropping and retrying once."
        )
        await _drop_owner(resource_key)
        owner = await _get_owner(conn)
        return await owner.call(raw_tool_name, args)


async def _call_tool(conn: dict, raw_tool_name: str, args: dict):
    if conn.get("persistent"):
        return await _call_tool_persistent(conn, raw_tool_name, args)

    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    token = conn["token"]
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with streamablehttp_client(conn["endpoint_url"], headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            return await session.call_tool(raw_tool_name, arguments=args)


def _stringify(result) -> str:
    """Extract text content from an MCP CallToolResult and hand it to the agent
    as the tool-result string. Unlike conv_mem (which json.loads to consume
    structured fields), we pass the raw text through — the agent reads it."""
    try:
        parts = []
        for item in getattr(result, "content", None) or []:
            text = getattr(item, "text", None)
            if text:
                parts.append(text)
        if parts:
            return "\n".join(parts)
    except Exception as e:
        logger.warning(f"mcp_client: result parse failed — {e}")
    return "[mcp_client: tool returned no text content]"


# ---------------------------------------------------------------------------
# context_modify — inject discovered tool defs (mirror agent_tools)
# ---------------------------------------------------------------------------

def modify_context(ctx: "PipelineCtx", config: dict) -> "PipelineCtx":
    """Inject the discovered MCP tools into ``ctx.request.tools``, applying the
    ``bridge_native__`` prefix at the boundary (same as agent_tools). Honours an
    optional ``tools:`` allowlist on this context block (accepts ``server__tool``
    or bare ``tool`` forms). De-dupes against client-supplied tools."""
    allow = config.get("tools")
    if allow is not None and not isinstance(allow, list):
        logger.warning(
            f"mcp_client: 'tools' allowlist must be a list; got "
            f"{type(allow).__name__}. Ignoring the allowlist."
        )
        allow = None

    # Per-role server→resource map: discovery is GLOBAL and resource-keyed, but a
    # role may only be offered tools from the servers IT configured, routed to
    # ITS OWN resources. Resolving server_key→resource here is what makes two
    # roles' same-named 'filesystem' servers inject from their OWN endpoints.
    server_to_resource = _role_server_to_resource(ctx)
    if not server_to_resource:
        return ctx  # this role declares no MCP servers — offer nothing

    # Per-server ACL (role.plugins.mcp_client.<server>.{allow,deny,tools}) —
    # applied here, not at discovery, because the registry is resource-keyed and
    # shared across roles. `tools:` is folded into `allow` by _tool_permitted.
    role_server_map = _role_server_map(ctx)

    existing = {
        t.get("function", {}).get("name")
        for t in ctx.request.tools
        if isinstance(t, dict)
    }
    to_add = []
    for server_key, resource_key in server_to_resource.items():
        server_cfg = role_server_map.get(server_key) or {}
        for (res, raw_tool), d in _tool_defs.items():
            if res != resource_key:
                continue
            wire_clean = _join_name(server_key, raw_tool)  # server_key__tool
            # per-server ACL (allow/deny + `tools:` alias, deny-wins)
            if not _tool_permitted(raw_tool, server_cfg):
                continue
            # context-level allowlist (`tools:` on this context block)
            if allow is not None and not _allowed(wire_clean, allow):
                continue
            namespaced = copy.deepcopy(d)  # never mutate the module registry
            namespaced["function"]["name"] = bridge_native.apply_namespace(wire_clean)
            if namespaced["function"]["name"] not in existing:
                to_add.append(namespaced)
                existing.add(namespaced["function"]["name"])

    if to_add:
        ctx.request.tools.extend(to_add)
        logger.info(
            f"mcp_client: injected {len(to_add)} tool(s) on identity "
            f"'{ctx.identity.key}': {[d['function']['name'] for d in to_add]}"
        )
    return ctx


def _allowed(clean_name: str, allow: list) -> bool:
    """An allowlist entry may be the full ``server__tool`` name or the bare
    tool name."""
    if clean_name in allow:
        return True
    split = _split_name(clean_name)
    return bool(split and split[1] in allow)


# ---------------------------------------------------------------------------
# Per-server ACL + param-injection policy (allow / deny / params)
#
# A per-role, per-server policy layer sitting between a bridge resident and an
# MCP server. Enforcement, not prompting: a small/untrusted model (e.g. a
# confabulating local model) cannot see a denied tool (injection seam) NOR call
# one that arrives from crafted history (dispatch seam), and cannot fill a
# sovereign argument the operator has stamped server-side (param injection).
#
# This is the sovereign-signal principle applied to MCP: the model may DESCRIBE
# itself in a tool call, but never DECLARE fields the operator reserves — same
# line as the bridge auto-stamping <caller> rather than trusting a send arg.
# ---------------------------------------------------------------------------

def _server_cfg_for_role(ctx: "PipelineCtx", server_key: str) -> dict:
    """THIS role's raw config block for one server_key
    (``role.plugins.mcp_client.<server_key>``), or ``{}`` if absent. Reuses
    ``_role_server_map`` so injection and dispatch read the SAME block."""
    return _role_server_map(ctx).get(server_key) or {}


def _permit_lists(server_cfg: dict) -> tuple[list | None, list]:
    """Extract (allow, deny) from a server block. ``tools:`` is a back-compat
    alias for ``allow`` (same whitelist semantics); if BOTH are present they're
    unioned (either whitelist entry admits the tool). Returns ``allow=None`` when
    no whitelist is declared (→ all tools pass the allow stage). Non-list values
    are ignored (warn) so a malformed policy fails OPEN on allow / CLOSED on deny
    is avoided — both just degrade to 'not declared'."""
    def _as_list(val, field):
        if val is None:
            return None
        if isinstance(val, list):
            return val
        logger.warning(
            f"mcp_client: '{field}' must be a list; got {type(val).__name__}. "
            f"Ignoring it."
        )
        return None

    allow = _as_list(server_cfg.get("allow"), "allow")
    tools_alias = _as_list(server_cfg.get("tools"), "tools")
    if allow is None:
        allow = tools_alias
    elif tools_alias is not None:
        allow = list(allow) + tools_alias  # union of both whitelists
    deny = _as_list(server_cfg.get("deny"), "deny") or []
    return allow, deny


def _tool_permitted(raw_tool: str, server_cfg: dict) -> bool:
    """Single source of truth for the per-server ACL, used by BOTH seams.
    ``deny`` beats ``allow`` on overlap (more-restrictive-wins, fail-safe).
    Matches on the RAW (bare) tool name — the per-server block names tools
    without the server_key prefix."""
    allow, deny = _permit_lists(server_cfg)
    if raw_tool in deny:
        return False
    if allow is not None and raw_tool not in allow:
        return False
    return True


def _apply_param_policy(args: dict, raw_tool: str, server_cfg: dict) -> dict:
    """Apply ``params.replace`` then ``params.append`` for one tool call.

    - ``replace[raw_tool]`` (if a dict) discards the model's args entirely and
      substitutes the operator's object.
    - ``append[raw_tool]`` (if a dict) SHALLOW-overwrites the named top-level
      keys onto the args (``dict.update`` — no deep-merge, no list-concat),
      clobbering whatever the model supplied. append runs AFTER replace, so it
      wins any overlapping key (the sovereign-stamp guarantee).

    Values are injected verbatim — no type coercion/validation (schema-agnostic;
    a wrong type is rejected by the MCP server and narrated as a tool-error).
    Returns the (possibly new) args dict."""
    params = server_cfg.get("params")
    if not isinstance(params, dict):
        return args

    replace = params.get("replace")
    if isinstance(replace, dict):
        repl = replace.get(raw_tool)
        if isinstance(repl, dict):
            args = copy.deepcopy(repl)

    append = params.get("append")
    if isinstance(append, dict):
        add = append.get(raw_tool)
        if isinstance(add, dict):
            args.update(copy.deepcopy(add))
    return args


# ---------------------------------------------------------------------------
# handle_tool_calls — route to the right MCP server and execute (D-008)
# ---------------------------------------------------------------------------

async def handle_tool_calls(ctx: "PipelineCtx", config: dict) -> str:
    """Execute a claimed MCP tool call. The claimed call's ``function.name`` has
    already had ``bridge_native__`` stripped by core, so it's ``server__tool``.
    We split off the server_key, route to that server's resource, and call the
    real MCP tool (fresh connection per call). Known-bad inputs return an error
    string; genuine MCP/network failures propagate to the executor's synthetic
    tool-error path so the agent narrates the failure."""
    tc = ctx.plugin_data.get("handle_tool_calls.claimed")
    if not isinstance(tc, dict):
        return "[mcp_client error: no claimed tool_call on ctx.plugin_data]"

    name = tc.get("function", {}).get("name")
    server_to_resource = _role_server_to_resource(ctx)
    split = _split_name(name)
    if split is None:
        # Segment-less name (model dropped the <server_key>__ middle). If exactly
        # one of this role's servers owns a tool by this bare name, route it there
        # (tolerant single-owner disambiguation — never guess across owners).
        sole_owner = _resolve_single_owner(name or "", server_to_resource)
        if sole_owner is None:
            return f"[mcp_client error: tool name '{name}' has no server sub-namespace]"
        logger.info(
            f"mcp_client: identity '{ctx.identity.key}' — segment-less tool "
            f"'{name}' resolved to sole owner '{sole_owner}' (tolerant routing)."
        )
        server_key, raw_tool = sole_owner, name
    else:
        server_key, raw_tool = split

    # Per-role server gate (security): discovery + routes are GLOBAL and
    # resource-keyed, so refuse to dispatch a call to a server THIS role didn't
    # configure — even if the call arrived from history or a crafted request.
    # Resolving server_key→resource here ALSO routes to the role's OWN resource:
    # two roles' same-named 'filesystem' servers each hit their own endpoint.
    resource_key = server_to_resource.get(server_key)
    if resource_key is None:
        logger.warning(
            f"mcp_client: identity '{ctx.identity.key}' tried to call '{name}' "
            f"but its role does not configure server '{server_key}' — refused."
        )
        return (
            f"[mcp_client error: tool '{name}' is not available to this identity "
            f"(server '{server_key}' not configured for this role)]"
        )

    # Per-server ACL gate (security): re-check allow/deny at DISPATCH, not just
    # at injection — a denied tool can arrive from crafted history even though it
    # was never offered. Same belt-and-suspenders posture as the server gate
    # above. Reads the SAME per-server block modify_context filtered on.
    server_cfg = _server_cfg_for_role(ctx, server_key)
    if not _tool_permitted(raw_tool, server_cfg):
        logger.warning(
            f"mcp_client: identity '{ctx.identity.key}' tried to call '{name}' "
            f"but tool '{raw_tool}' is denied by the server '{server_key}' policy "
            f"— refused."
        )
        return (
            f"[mcp_client error: tool '{name}' is not permitted for this identity "
            f"(blocked by the '{server_key}' allow/deny policy)]"
        )

    route = _tool_routes.get((resource_key, raw_tool))
    if route is None:
        return (
            f"[mcp_client error: no route for '{name}' → resource "
            f"'{resource_key}' (tool '{raw_tool}') — the resource may have been "
            f"down at startup discovery]"
        )
    conn, raw_tool_name = route

    raw_args = tc.get("function", {}).get("arguments") or "{}"
    try:
        args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
    except (json.JSONDecodeError, ValueError):
        return f"[mcp_client error: could not parse arguments for '{name}']"
    if not isinstance(args, dict):
        args = {}

    # Gateway-level param injection (replace then append) — stamp sovereign
    # fields the model shouldn't fill itself (e.g. `source`), overriding whatever
    # it supplied, BEFORE the call reaches the MCP server. See _apply_param_policy.
    args = _apply_param_policy(args, raw_tool, server_cfg)

    result = await _call_tool(conn, raw_tool_name, args)
    result_text = _stringify(result)
    logger.info(
        f"mcp_client: executed '{name}' on identity '{ctx.identity.key}' "
        f"→ '{result_text[:80]}{'...' if len(result_text) > 80 else ''}'"
    )
    return result_text
