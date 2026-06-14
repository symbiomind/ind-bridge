"""bridge_native — the bridge-wide tool-namespacing invariant.

Bridge-native tools (tools the bridge offers the buddy via ``handle_tool_calls``
plugins and executes locally) are ALWAYS presented to the buddy with a hard
prefix so they can never collide with harness/client-supplied tools that arrive
in ``ctx.request.tools`` at request time. The startup validator can reject two
*bridge plugins* claiming the same tool name on one identity, but it cannot see
harness tools — they only exist per-request. This prefix is the runtime defence
the validator can't provide.

The prefix is applied AT THE BOUNDARY, never stored. Plugins keep clean internal
names (``OWNED_TOOLS = ["rng_message"]``, ``_TOOL_DEFINITIONS`` keyed clean):

  * ``agent_tools.modify_context`` prepends the prefix when injecting the tool
    definition into ``ctx.request.tools`` (the apply side).
  * ``pipeline_executor`` strips the prefix when matching a returned tool_call
    against a plugin's clean ``OWNED_TOOLS`` and before handing the claimed call
    to the plugin's clean-keyed handler (the strip side — one central seam every
    ``handle_tool_calls`` plugin inherits for free).

No opt-out, no config knob — this is a bridge-wide invariant.

Two-layer namespace: the bridge owns ONLY the ``bridge_native__`` layer. A plugin
MAY apply its own optional sub-prefix first (e.g. an internal clean name like
``yeah_baby_yeah`` → wire ``bridge_native__yeah_baby_yeah``). That sub-prefix is
the plugin's business; the boundary simply wraps whatever clean name it's handed.
``apply_namespace`` is idempotent so a plugin can pre-shape a name without the
boundary double-wrapping.
"""

from __future__ import annotations

BRIDGE_NATIVE_PREFIX = "bridge_native__"


def apply_namespace(name: str) -> str:
    """Prepend the invariant prefix. Idempotent — never double-prefixes.
    Non-str / empty inputs are returned unchanged (no crash)."""
    if not isinstance(name, str) or not name:
        return name
    if name.startswith(BRIDGE_NATIVE_PREFIX):
        return name
    return BRIDGE_NATIVE_PREFIX + name


def strip_namespace(name: str) -> str:
    """Remove the invariant prefix if present; a no-op otherwise.
    Non-str inputs are returned unchanged (no crash)."""
    if not isinstance(name, str):
        return name
    if name.startswith(BRIDGE_NATIVE_PREFIX):
        return name[len(BRIDGE_NATIVE_PREFIX):]
    return name


def is_namespaced(name: str) -> bool:
    """True iff ``name`` carries the bridge-native prefix."""
    return isinstance(name, str) and name.startswith(BRIDGE_NATIVE_PREFIX)
