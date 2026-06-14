"""
Plugin loader for ind-bridge V4.

Scans plugin directories at startup and loads plugin modules into a registry.
Separated from dispatch — this module only loads, never invokes.

Plugin layout
-------------
    my_plugin/
      __init__.py       ← exports CAPABILITIES dict and capability methods
      requirements.txt  ← optional, auto-pip-installed at startup
      README.md         ← optional, documents the plugin's purpose and config

Load order
----------
  1. ``_builtin/`` plugins (alphabetical)
  2. ``user/`` plugins (alphabetical)

Builtins load first; user plugins with the same name as a builtin are warned
and skipped. (User plugins cannot silently shadow builtins — rename the dir.)

V4 capability contract
----------------------
Every plugin SHOULD export a ``CAPABILITIES`` dict mapping capability names
to lists of valid slot families:

    CAPABILITIES = {
        "listener":         ["identity.plugins"],
        "outbound_params":  ["identity.plugins", "role.plugins",
                             "session.plugins", "resource.plugins"],
        "context_modify":   ["identity.context.plugins",
                             "role.context.plugins",
                             "session.context.plugins"],
        "response_modify":  ["identity.response.plugins",
                             "role.response.plugins",
                             "session.response.plugins"],
        "produce_response": ["resource.plugins"],
    }

The loader captures CAPABILITIES at load time but does NOT validate
placements against it — that's the job of ``capabilities.py`` (TODO) during
pipeline assembly. The loader only WARNS if CAPABILITIES is absent or
malformed; the plugin still loads (so V3-shape plugins that haven't been
ported yet don't break the boot).

See ``CLAUDE.md`` for the architecture cheat-sheet.
"""

import importlib.util
import logging
import os
import subprocess
import sys
from types import ModuleType

logger = logging.getLogger(__name__)

_REGISTRY: dict[str, ModuleType] = {}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_plugins(builtin_dir: str, user_dir: str) -> None:
    """Load all plugins from both dirs into the registry. Called once at
    startup. Never raises — bad plugins log and skip."""
    _load_from_dir(builtin_dir, source="builtin")
    _load_from_dir(user_dir, source="user")
    loaded = sorted(_REGISTRY.keys())
    logger.info(
        f"Plugin registry: {len(_REGISTRY)} plugin(s) loaded: "
        f"{loaded if loaded else '(none)'}"
    )


def get_plugin(name: str) -> ModuleType | None:
    """Returns the loaded plugin module for the given name, or None."""
    return _REGISTRY.get(name)


def get_registry() -> dict[str, ModuleType]:
    """Returns the loaded plugin registry (read-only reference)."""
    return _REGISTRY


def get_capabilities(name: str) -> dict | None:
    """
    Returns the plugin's declared CAPABILITIES dict, or None if the plugin
    is not loaded or doesn't declare one.

    A plugin without CAPABILITIES cannot be validly placed in V4 — this is
    primarily a diagnostic helper. The capability validator (TODO) is the
    enforcement point.
    """
    plugin = _REGISTRY.get(name)
    if plugin is None:
        return None
    caps = getattr(plugin, "CAPABILITIES", None)
    if isinstance(caps, dict):
        return caps
    return None


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------

def _load_from_dir(plugin_dir: str, source: str) -> None:
    if not os.path.isdir(plugin_dir):
        logger.debug(f"Plugin dir '{plugin_dir}' not found — skipping {source}/.")
        return

    for entry in sorted(os.scandir(plugin_dir), key=lambda e: e.name):
        if not entry.is_dir():
            continue
        if entry.name.startswith("_") or entry.name.startswith("."):
            continue
        init_path = os.path.join(entry.path, "__init__.py")
        if not os.path.isfile(init_path):
            logger.debug(f"Skipping '{entry.name}' in {source}/ — no __init__.py")
            continue
        _load_plugin(entry.name, entry.path, init_path, source)


def _load_plugin(name: str, plugin_dir: str, init_path: str, source: str) -> None:
    if source == "user" and name in _REGISTRY:
        logger.warning(
            f"User plugin '{name}' conflicts with a builtin of the same name — "
            f"skipping user plugin. Rename your plugin directory to use a unique name."
        )
        return

    # Auto-install requirements.txt if present (preserved from V3 — useful)
    req_path = os.path.join(plugin_dir, "requirements.txt")
    if os.path.isfile(req_path):
        logger.info(f"Installing requirements for {source} plugin '{name}'...")
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "-r", req_path, "-q"],
                timeout=120,
            )
        except Exception as e:
            logger.error(
                f"Failed to install requirements for {source} plugin '{name}': {e} — "
                f"plugin may not work correctly."
            )

    try:
        spec = importlib.util.spec_from_file_location(
            f"plugins.{source}.{name}", init_path
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[f"plugins.{source}.{name}"] = module
        spec.loader.exec_module(module)
        _REGISTRY[name] = module
        _log_plugin_capabilities(name, module, source)
    except Exception as e:
        logger.error(
            f"Failed to load {source} plugin '{name}' from '{init_path}': {e}"
        )


def _log_plugin_capabilities(name: str, module: ModuleType, source: str) -> None:
    """Log what the plugin declares (or what's missing). Diagnostic only —
    the capability validator (TODO) is the actual enforcement point."""
    caps = getattr(module, "CAPABILITIES", None)
    legacy_hooks = getattr(module, "SUPPORTED_HOOKS", None)

    if isinstance(caps, dict) and caps:
        cap_names = sorted(caps.keys())
        logger.debug(
            f"Loaded {source} plugin '{name}' — capabilities: {cap_names}"
        )
    elif legacy_hooks is not None:
        # V3-shape plugin not yet ported — load it but flag clearly.
        logger.warning(
            f"Plugin '{name}' uses V3 SUPPORTED_HOOKS shape ({legacy_hooks}) — "
            f"V4 expects a CAPABILITIES dict. Plugin loaded but will not be "
            f"invokable until ported to the V4 capability contract."
        )
    else:
        logger.warning(
            f"Plugin '{name}' declares neither CAPABILITIES nor SUPPORTED_HOOKS — "
            f"plugin loaded but will not be invokable. Add a CAPABILITIES dict "
            f"per the V4 plugin contract (see CLAUDE.md)."
        )
