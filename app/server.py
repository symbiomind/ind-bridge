"""
ind-bridge V4 — entry point.

Core owns exactly one route:

  GET /health — always 200, reports config_loaded status.

Every other route materialises during startup, when an identity declaring a
listener-capability plugin (e.g. ``OpenAI-Protocol``) gets its
``setup_listener`` capability method invoked. **No listener plugins on any
identity = no routes beyond /health.** That's not broken — that's the spec
working as designed:

    No identities = no bridge (latent, not idle)
    Identity with listener plugin = bridge opens that listener
    Otherwise = silence

See ``CLAUDE.md`` (project root) and ``~/Documents/ind-v4-brainstorm.md``
for the full contract. D-001 in ``~/Documents/ind-v4-decisions.md``
establishes ``setup_listener`` as the listener capability's entry point.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from . import capabilities, cascade as cascade_mod, config, pipeline_assembler, plugin_loader
from .context import StartupCtx

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# Background tasks spawned by `background`-capability plugins during startup.
# Tracked so lifespan shutdown can cancel them cleanly (V3's server.startup had
# no teardown; V4 does it right).
_background_tasks: list[asyncio.Task] = []


# ---------------------------------------------------------------------------
# Lifespan — wire identities to listeners (no-op until a listener plugin lands)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ─────────────────────────────────────────────────────────────
    logger.info("ind-bridge V4 starting up...")

    config.load_config()

    pkg_dir = os.path.dirname(__file__)
    builtin_dir = os.path.join(pkg_dir, "plugins", "_builtin")
    user_dir = os.path.join(pkg_dir, "plugins", "user")
    plugin_loader.load_plugins(builtin_dir, user_dir)

    if not os.getenv("BRIDGE_SIGN_SECRET"):
        logger.warning(
            "BRIDGE_SIGN_SECRET is not set — bridge_context blocks will be "
            "unsigned and inbound blocks cannot be verified. Set this env "
            "var to enable HMAC signing."
        )

    listeners_active = 0
    if config.is_config_loaded():
        # Validate the contract before materialising listeners — operators
        # see exactly what V4 thinks of their config, line by line.
        report = capabilities.validate_all()
        logger.info(capabilities.report_summary(report))

        # Assemble per-identity pipelines (D-001) — cached for the executor.
        # Builds tuple lists from the ok placements; unknown/misplaced ones
        # were already warned in the report above.
        pipelines = pipeline_assembler.assemble_all()
        total_tuples = sum(len(p) for p in pipelines.values())
        logger.info(
            f"Assembled {len(pipelines)} pipeline(s), "
            f"{total_tuples} tuple(s) total."
        )

        listeners_active = _materialise_listeners(app)
        _spawn_background_tasks(app)

    identity_count = len(config.list_identities()) if config.is_config_loaded() else 0
    logger.info(
        f"ind-bridge ready. {identity_count} identity/identities configured, "
        f"{listeners_active} listener plugin(s) active, "
        f"{len(_background_tasks)} background task(s) running."
    )

    yield
    # ── Shutdown ────────────────────────────────────────────────────────────
    # Cancel background tasks spawned by `background`-capability plugins, then
    # let plugins manage any other teardown.
    if _background_tasks:
        logger.info(f"Cancelling {len(_background_tasks)} background task(s)...")
        for task in _background_tasks:
            task.cancel()
        # Give them a moment to unwind; swallow the expected CancelledError.
        await asyncio.gather(*_background_tasks, return_exceptions=True)
        _background_tasks.clear()
    logger.info("ind-bridge shutting down.")


# ---------------------------------------------------------------------------
# Listener materialisation — V4-shaped, dispatched by capability not hook-point
# ---------------------------------------------------------------------------

def _materialise_listeners(app: FastAPI) -> int:
    """
    For each identity that declares a plugin in its ``plugins:`` block,
    check whether that plugin declares the ``listener`` capability with
    ``identity.plugins`` as a valid slot. If so, invoke the plugin's
    ``setup_listener(StartupCtx, plugin_config)`` method.

    Per D-001 and the Plugin Capability Contract: ``listener`` is only
    valid on ``identity.plugins``. Listener setup is per-identity, not
    server-wide. Until a plugin with the listener capability is loaded,
    this loop is a clean no-op.

    Returns the count of successful listener setups.
    """
    server_cfg = config.get_server_cfg()
    success_count = 0

    for identity_key in config.list_identities():
        identity_cfg = config.resolve_identity(identity_key) or {}
        identity_plugins = identity_cfg.get("plugins") or {}
        if not isinstance(identity_plugins, dict) or not identity_plugins:
            continue

        for plugin_name, plugin_config in identity_plugins.items():
            plugin = plugin_loader.get_plugin(plugin_name)
            if plugin is None:
                # Plugin referenced in config but not loaded — not a listener
                # problem per se, but worth a debug log. (Capability validator
                # will warn loudly when it lands.)
                logger.debug(
                    f"Identity '{identity_key}' references unloaded plugin "
                    f"'{plugin_name}' — skipping."
                )
                continue

            caps = plugin_loader.get_capabilities(plugin_name) or {}
            listener_slots = caps.get("listener") or []
            if "identity.plugins" not in listener_slots:
                # Plugin doesn't declare listener-capability for this slot.
                # Could be valid (e.g. it's outbound-only). Not our job here.
                continue

            setup_fn = getattr(plugin, "setup_listener", None)
            if setup_fn is None:
                logger.warning(
                    f"Plugin '{plugin_name}' declares 'listener' capability "
                    f"but exports no setup_listener() function — identity "
                    f"'{identity_key}' will not have a working listener."
                )
                continue

            startup_ctx = StartupCtx(
                app=app,
                server_cfg=server_cfg,
                identity_key=identity_key,
                identity_cfg=identity_cfg,
            )
            try:
                setup_fn(startup_ctx, plugin_config or {})
                logger.info(
                    f"Listener active: identity='{identity_key}' "
                    f"plugin='{plugin_name}'"
                )
                success_count += 1
            except Exception as e:
                logger.error(
                    f"Listener setup failed for identity '{identity_key}' "
                    f"plugin '{plugin_name}': {e!r} — identity disabled, "
                    f"bridge continues.",
                    exc_info=True,
                )

    return success_count


# ---------------------------------------------------------------------------
# Background task spawn — the `background` capability (D-011)
# ---------------------------------------------------------------------------

def _spawn_background_tasks(app: FastAPI) -> int:
    """
    For each identity that declares a plugin with the ``background`` capability
    on its ``identity.plugins`` slot, call the plugin's
    ``start_background(StartupCtx, config)``. If it returns a coroutine, schedule
    it on the running event loop and track the task so lifespan shutdown can
    cancel it cleanly.

    The generic cousin of ``_materialise_listeners``: ``listener`` registers an
    HTTP route, ``background`` spawns a long-running task that runs outside the
    request cycle (a polling daemon, a cron-like ticker). Per the capability
    contract, ``background`` is only valid on ``identity.plugins`` — per-identity,
    like listeners. The V3 ``server.startup`` hook's V4-shaped replacement.

    Returns the count of successful spawns. A plugin whose ``start_background``
    raises or returns no coroutine is logged and skipped — the bridge continues.
    """
    server_cfg = config.get_server_cfg()
    spawn_count = 0

    for identity_key in config.list_identities():
        identity_cfg = config.resolve_identity(identity_key) or {}

        # A background plugin may be declared on the identity OR inherited from
        # the role (capability fan-out: declare a plugin's server map once on
        # the role, every identity on that role spawns its discovery — no
        # per-identity `mcp_client: {}` marker needed). Union the cascade's
        # `plugins` family keyed by NAME so a plugin declared at BOTH levels
        # (back-compat configs) spawns exactly ONCE per identity, not twice.
        cascade = cascade_mod.resolve_cascade(identity_key)
        plugin_names: set[str] = set()
        if isinstance(identity_cfg.get("plugins"), dict):
            plugin_names |= set(identity_cfg["plugins"].keys())
        if cascade and cascade.role_cfg and isinstance(cascade.role_cfg.get("plugins"), dict):
            plugin_names |= set(cascade.role_cfg["plugins"].keys())
        if cascade and cascade.session_cfg and isinstance(cascade.session_cfg.get("plugins"), dict):
            plugin_names |= set(cascade.session_cfg["plugins"].keys())
        if cascade and cascade.resource_cfg and isinstance(cascade.resource_cfg.get("plugins"), dict):
            plugin_names |= set(cascade.resource_cfg["plugins"].keys())
        if not plugin_names:
            continue

        for plugin_name in sorted(plugin_names):
            plugin = plugin_loader.get_plugin(plugin_name)
            if plugin is None:
                continue

            caps = plugin_loader.get_capabilities(plugin_name) or {}
            if "identity.plugins" not in (caps.get("background") or []):
                continue

            start_fn = getattr(plugin, "start_background", None)
            if start_fn is None:
                logger.warning(
                    f"Plugin '{plugin_name}' declares 'background' capability "
                    f"but exports no start_background() function — identity "
                    f"'{identity_key}' will have no background task."
                )
                continue

            # Cascade-merged config so a future background plugin that READS its
            # config sees correct identity>role override behaviour. (mcp_client
            # ignores it — start_background/_find_server_map resolve the role
            # map themselves — but principle-of-least-surprise for the next one.)
            plugin_config = (
                cascade_mod.merge_plugin_configs(cascade, plugin_name, "plugins")
                if cascade else {}
            )

            # Plugin-disable tombstone: a falsy declaration at the winning
            # cascade level yeets this background plugin too (e.g. `cron: false`
            # on a specific identity). Don't spawn its loop.
            if plugin_config is cascade_mod.DISABLED:
                logger.info(
                    f"Background spawn: identity '{identity_key}' DISABLED plugin "
                    f"'{plugin_name}' (config tombstone) — not spawning."
                )
                continue

            startup_ctx = StartupCtx(
                app=app,
                server_cfg=server_cfg,
                identity_key=identity_key,
                identity_cfg=identity_cfg,
            )
            try:
                coro = start_fn(startup_ctx, plugin_config or {})
                if coro is None:
                    # Plugin chose not to spawn (e.g. config disabled it). Fine.
                    continue
                task = asyncio.ensure_future(coro)
                _background_tasks.append(task)
                logger.info(
                    f"Background task spawned: identity='{identity_key}' "
                    f"plugin='{plugin_name}'"
                )
                spawn_count += 1
            except Exception as e:
                logger.error(
                    f"Background spawn failed for identity '{identity_key}' "
                    f"plugin '{plugin_name}': {e!r} — task skipped, bridge "
                    f"continues.",
                    exc_info=True,
                )

    return spawn_count


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="ind-bridge",
    version="0.1.0-dev",
    lifespan=lifespan,
    # Most routes are added by listener plugins after startup, so the default
    # OpenAPI docs would only show /health and be misleading. Disabled.
    docs_url=None,
    redoc_url=None,
)


@app.get("/health")
async def health():
    """Always responds 200. Reports whether config.yml was loaded."""
    return {
        "status": "ok",
        "service": "ind-bridge",
        "version": "0.1.0-dev",
        "config_loaded": config.is_config_loaded(),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        "app.server:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 5005)),
        reload=False,
    )
