"""
agent_tools — bundled smoke-test plugin for D-008's ``handle_tool_calls``
capability.

**This is the canonical V4 smoke test for the intercept mechanism.**
Lifted from V3's ``plugins/user/agent_tools/`` (where it was the
proof-of-concept for the V3 ``response.intercept`` hook), simplified
to the V4 capability shape. ``rng_message`` returns a random string
from a pool — no I/O, no auth, no signing, no inter-bridge HTTP.
The point is to prove the executor's intercept dispatch + re-call
loop works end-to-end before more complex consumers (``bridge_message``,
future ``agent_tools``-evolved sub-agent shapes, ``web_search``,
``eliza_as_tool``) are ported onto the same capability.

What this plugin does, when wired into a config:

  * ``modify_context`` — injects the ``rng_message`` tool definition
    into ``ctx.request.tools`` so the LLM is offered the tool. Honours
    a ``tools: [name, ...]`` config knob (lifted from V3) so operators
    can enable a subset.

  * ``handle_tool_calls`` — when the LLM calls ``rng_message``, the
    executor dispatches here; we pick a random message and return it
    as the tool result string. The executor splices the result and
    re-calls the upstream LLM, which narrates the random message in
    its own voice. Per V4, **the executor does the partitioning,
    splicing, and re-calling** — the plugin only executes its tool.

Wire it in (test config — do not commit to production config.yml):

    roles:
      smoketest_role:
        resource: openrouter
        plugins:
          OpenAI-Protocol: {model: anthropic/claude-sonnet-4-6}
          agent_tools: {}                  # handle_tool_calls slot
        context:
          plugins:
            agent_tools:
              tools: [rng_message]         # context_modify slot

Per the V4 capability contract:

  * ``handle_tool_calls`` lives at ``identity.plugins`` or ``role.plugins``.
  * ``context_modify`` lives at ``identity.context.plugins`` or
    ``role.context.plugins``.

The validator checks ``OWNED_TOOLS`` is present and non-empty (D-008
invariant); the loader cross-references slot placements against the
capability table. Misplaced declarations are warned-and-skipped, never
silently misbehaved.
"""

from __future__ import annotations

import copy
import json
import logging
import random
from typing import TYPE_CHECKING

from app import bridge_native

if TYPE_CHECKING:
    from app.context import PipelineCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "context_modify":     ["identity.context.plugins", "role.context.plugins"],
    "handle_tool_calls":  ["identity.plugins", "role.plugins"],
}


OWNED_TOOLS = ["rng_message"]
"""Tool names this plugin claims via the D-008 ``handle_tool_calls``
capability. Validator emits a config-shape issue if this is missing or
empty when the capability is declared. Tool-name collisions across
plugins on the same identity are also rejected at startup."""


# ---------------------------------------------------------------------------
# Tool registry (lifted from V3)
# ---------------------------------------------------------------------------

_RNG_MESSAGES = [
    "The pelican is watching. Act natural.",
    "Have you considered that the answer was banana all along?",
    "This message was randomly selected. It has no meaning. Or does it?",
    "Error 418: I'm a teapot. Just kidding. Maybe.",
    "The bridge says hello. The bridge is always watching.",
    "Statistically, this is the best random message. Trust the math.",
    "The answer is 42. You're welcome.",
    "Somewhere, an AI is very proud of this result.",
]


_TOOL_DEFINITIONS = {
    "rng_message": {
        "type": "function",
        "function": {
            "name": "rng_message",
            "description": (
                "Returns a random message from the bridge. Use when you "
                "want a surprise, need inspiration, or just feel like "
                "rolling the dice."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
}


_TOOL_HANDLERS = {
    "rng_message": lambda args: random.choice(_RNG_MESSAGES),
}


# ---------------------------------------------------------------------------
# context_modify — inject tool definitions into outbound request
# ---------------------------------------------------------------------------

def modify_context(ctx: "PipelineCtx", config: dict) -> "PipelineCtx":
    """Inject this plugin's tool definitions into ``ctx.request.tools``
    so the upstream LLM is offered them. Honours ``tools:`` in config:

        agent_tools:
          tools: [rng_message]

    Defaults to all OWNED_TOOLS when ``tools:`` is absent (operator
    didn't restrict). De-dupes against any tools the client already
    sent — never overwrites client-supplied tool definitions."""
    enabled = config.get("tools")
    if enabled is None:
        enabled = list(OWNED_TOOLS)
    elif not isinstance(enabled, list):
        logger.warning(
            f"agent_tools: 'tools' config must be a list of tool names; "
            f"got {type(enabled).__name__}. Skipping injection."
        )
        return ctx

    definitions = [
        _TOOL_DEFINITIONS[name]
        for name in enabled
        if name in _TOOL_DEFINITIONS
    ]
    if not definitions:
        return ctx

    existing_names = {
        t.get("function", {}).get("name")
        for t in ctx.request.tools
        if isinstance(t, dict)
    }
    # Prefix-then-dedup. The bridge-native prefix is a hard invariant applied
    # AT THE BOUNDARY (see app/bridge_native.py): we deep-copy the registry
    # definition (mutating the module-level _TOOL_DEFINITIONS in place would
    # permanently corrupt it), rewrite function.name to the prefixed wire name,
    # then dedup the PREFIXED name. Deduping post-prefix means a client tool
    # named literally `bridge_native__rng_message` is respected (skip injection),
    # while a client's bare `rng_message` correctly coexists as a different wire
    # name — it's a harness tool we don't own.
    to_add = []
    for d in definitions:
        namespaced = copy.deepcopy(d)
        namespaced["function"]["name"] = bridge_native.apply_namespace(
            d["function"]["name"]
        )
        if namespaced["function"]["name"] not in existing_names:
            to_add.append(namespaced)
    if to_add:
        ctx.request.tools.extend(to_add)
        logger.info(
            f"agent_tools: injected {len(to_add)} tool(s) on "
            f"identity '{ctx.identity.key}': "
            f"{[d['function']['name'] for d in to_add]}"
        )
    return ctx


# ---------------------------------------------------------------------------
# handle_tool_calls — execute claimed tool calls locally (D-008)
# ---------------------------------------------------------------------------

async def handle_tool_calls(ctx: "PipelineCtx", config: dict) -> str:
    """Executor calls this once per claimed tool_call. The claimed call
    is on ``ctx.plugin_data["handle_tool_calls.claimed"]``.

    V4 contract: return the tool result as a string (or a dict; the
    executor's normaliser handles both). The executor splices the
    result into ctx.request.messages alongside the assistant turn that
    called the tool, then re-calls the upstream resource so the agent
    reacts in their own voice.

    Plugin authors **don't** build tool messages, **don't** modify
    ctx.request.messages, **don't** re-call upstream. The executor
    owns all of that. Plugin authors execute their tool and return
    the result.
    """
    tc = ctx.plugin_data.get("handle_tool_calls.claimed")
    if not isinstance(tc, dict):
        return "[agent_tools error: no claimed tool_call on ctx.plugin_data]"

    name = tc.get("function", {}).get("name")
    handler = _TOOL_HANDLERS.get(name)
    if handler is None:
        return f"[agent_tools error: no handler for '{name}']"

    # Parse args (rng_message takes none, but defend in depth for future tools).
    raw_args = tc.get("function", {}).get("arguments") or "{}"
    try:
        args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
    except (json.JSONDecodeError, ValueError):
        args = {}

    try:
        result = handler(args)
    except Exception as e:
        # Plugin-level exceptions go up to the executor's _run_intercept_plugin
        # which wraps them in a synthetic error tool_result. Letting it propagate
        # is the right move — the executor logs with exc_info and the agent
        # narrates the failure. But we can also log a friendlier message here
        # while we have the tool name for context.
        logger.exception(
            f"agent_tools: handler for '{name}' raised on identity "
            f"'{ctx.identity.key}'"
        )
        raise

    result_text = str(result)
    logger.info(
        f"agent_tools: executed '{name}' on identity "
        f"'{ctx.identity.key}' → '{result_text[:80]}{'...' if len(result_text) > 80 else ''}'"
    )
    return result_text
