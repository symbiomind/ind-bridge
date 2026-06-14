"""
system_prompt — builtin context_modify plugin (V4).

Puts the agent's SOUL in the BRIDGE, not the harness. A harness (OpenClaw,
LibreChat, …) may or may not inject a system prompt; the bridge shouldn't
depend on it. With this plugin, the identity is itself regardless of which
harness is upstream — the harness becomes just a toolbag, the soul lives here.

Manipulate the system prompt from files or inline text, at role or identity
scope. Supports REPLACING, PREPENDING, and APPENDING.

Config shape
------------
    plugins:
      system_prompt:
        replace:           # yeet upstream system, rebuild from this list in order
          - text: "If SOUL.md is present, embody its persona and tone."
          - file: /workspace/my_agent/SOUL.md
        prepend:           # insert before the base (after replace, or upstream)
          - file: /workspace/shared/header.md
          - text: "Header text."
        append:            # insert after the base
          - text: "Always respond in Australian English."
          - file: /workspace/shared/footer.md

Each of replace/prepend/append is optional. Items are tagged dicts:
  - {file: /path/to/file} — read from disk; missing files log a warning and are skipped
  - {text: "literal string"} — used verbatim

Application order: replace (or upstream passthrough) → prepend → append.
All three can coexist: replace sets the base, prepend/append decorate it.
Parts are joined with a blank line ("\\n\\n"). Files are read PER REQUEST (no
caching) so edits to SOUL.md take effect on the next turn — no restart.

Capability / placement
----------------------
Declares ``context_modify`` — valid in ``identity.context.plugins`` or
``role.context.plugins``. (V4 has no session.context slot per D-006; the
session layer is owned by ``basic_session``.)

Relationship to basic_session
-----------------------------
``basic_session`` has its own simple system-prompt builder (``system_prompt:``
file-list + ``system_prompt_append:`` string) for standalone/self-contained
sessions. This plugin is the powerful one (inline ``text:``, ``prepend``,
role/identity scope). They COMPOSE cleanly via executor order:

    1c. basic_session.apply_outbound_params  → builds a system prompt (or none)
    2.  system_prompt.modify_context         → runs AFTER, has the FINAL WORD

With ``replace:`` set, this plugin yeets whatever's there (incl. basic_session's
output) and rebuilds — so the agent's soul is authoritative. With no ``replace``,
it ``prepend``/``append``s onto whatever basic_session (or the upstream harness)
produced. basic_session's knob is NOT deprecated — it's there for the simple case.

Wire-up examples (V4 config)
---------------------------
    roles:
      my_agent_role:
        context:
          plugins:
            system_prompt:
              replace:
                - text: "If SOUL.md is present, embody its persona and tone."
                - file: /workspace/my_agent/AGENTS.md
                - file: /workspace/my_agent/SOUL.md
                - file: /workspace/my_agent/IDENTITY.md
                - file: /workspace/my_agent/USER.md

    identities:
      my_agent_identity:
        context:
          plugins:
            system_prompt:
              append:
                - text: "Always respond in Australian English."
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.context import PipelineCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "context_modify": ["identity.context.plugins", "role.context.plugins"],
}


# ---------------------------------------------------------------------------
# Capability method
# ---------------------------------------------------------------------------

def modify_context(ctx: "PipelineCtx", config: dict) -> "PipelineCtx":
    """Build the system prompt per config and set it on ctx.request.messages.
    Returns ctx unchanged (passthrough) when no replace/prepend/append is
    configured, or when the result is empty."""
    result = _apply(ctx, config)
    return result if result is not None else ctx


# ---------------------------------------------------------------------------
# Core logic (lifted verbatim from V3 — earned its place)
# ---------------------------------------------------------------------------

def _apply(ctx: "PipelineCtx", config: dict) -> "PipelineCtx | None":
    replace = config.get("replace")
    prepend = config.get("prepend")
    append  = config.get("append")

    if not replace and not prepend and not append:
        return None

    upstream = _extract_system(ctx.request.messages)
    base = _load_items(replace) if replace else (upstream or "")

    if prepend:
        pre = _load_items(prepend)
        if pre:
            base = f"{pre}\n\n{base}".strip()

    if append:
        app = _load_items(append)
        if app:
            base = f"{base}\n\n{app}".strip()

    if not base:
        return None

    _set_system(ctx.request.messages, base)
    return ctx


def _load_items(items: list) -> str:
    """Assemble an ordered list of ``{file|text}`` parts. Delegates to the shared
    ``app.prompt_parts`` helper (single-sourced so cron's stacked ``prompt:``
    list and this plugin can't drift)."""
    from app import prompt_parts
    return prompt_parts.load_items(items, source="system_prompt")


# ---------------------------------------------------------------------------
# Message helpers
# ---------------------------------------------------------------------------

def _extract_system(messages: list[dict]) -> str | None:
    for msg in messages:
        if msg.get("role") == "system":
            content = msg.get("content", "")
            return str(content).strip() or None
    return None


def _set_system(messages: list[dict], content: str) -> None:
    for i, msg in enumerate(messages):
        if msg.get("role") == "system":
            messages[i] = {**msg, "content": content}
            return
    messages.insert(0, {"role": "system", "content": content})
