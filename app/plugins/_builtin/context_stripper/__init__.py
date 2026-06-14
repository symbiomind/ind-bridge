"""
context_stripper — builtin context_modify plugin (V4).

Strips client-injected baggage from ``ctx.request.messages`` before the
request leaves the bridge. Different harnesses send different cruft; this
plugin removes it so the resource (or session plugin) sees clean input.

V4 shape: a SET of independently-toggleable FILTERS
---------------------------------------------------
V3 had a single mutually-exclusive ``client_mode: <one-of>`` selector that
also doubled as ``IdentityInfo.client_mode``. V4 dropped ``client_mode`` from
the identity entirely (it was never WHO, it was always WHICH TRANSFORMS), so
the plugin config is now the only source — and it's a set of filters, not one
mode:

    context_stripper:
      librechat: {}          # present  → enabled. runs FIRST.
      openclaw:              # runs SECOND (chained after librechat's output)
        enabled: true        # default-on when the key is present;
                             # set false to keep-configured-but-dormant.

  * **Presence of a filter key = ON.** ``openclaw: {}`` enables it.
  * **``enabled: false``** is the explicit dormant override — keeps the block
    in config (and any future sub-options) without running it. Handy for
    toggling a filter without deleting its config.
  * **Order = YAML order.** Filters chain top-to-bottom as written; each
    filter's output feeds the next. Author controls the order, same rule as
    the plugin list itself. (e.g. ``librechat`` collapses 91→1, *then*
    ``openclaw`` strips the prefix off that surviving message.)
  * **Unknown filter name** (typo, or a filter not yet implemented) → logged
    as a warning and skipped; recognised filters still run. Fail loud, never
    fail to start.

This config shape deep-merges cleanly across the cascade: a role can set a
filter default and an identity can override it per-key
(``openclaw: {enabled: false}`` on the identity dormants the role's
``openclaw: {}``).

Filters
-------
``librechat`` — LibreChat sends its entire conversation history every turn.
                Keep only the last user message; the bridge (or the upstream
                provider) manages its own session context.

``openclaw``  — OpenClaw injects an untrusted-metadata prefix onto the current
                user turn (a ``Sender (untrusted metadata):`` JSON block + a
                ``[timestamp]`` line). Strip it from the last user message,
                leave history intact. Handles both plain-string and multi-part
                (list) message content.

                Note: as agents migrate fully onto signed ``<bridge_context>``
                (verified by core ``bridge_sign``), this prefix path becomes
                redundant — the metadata concern moves to the signed block.
                Kept for the non-bridge_context OpenClaw path.

Capability / placement
----------------------
Declares ``context_modify`` — valid in ``identity.context.plugins`` or
``role.context.plugins``. Wire it where the strip should happen; if you also
inject signed context, place ``context_stripper`` BEFORE the inject so the
injection lands in the surviving message.

    identities:
      my_agent:
        context:
          plugins:
            context_stripper:
              openclaw: {}
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from app.context import PipelineCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "context_modify": ["identity.context.plugins", "role.context.plugins"],
}


# ---------------------------------------------------------------------------
# Filter: librechat — collapse full history to the last user message
# ---------------------------------------------------------------------------

def _strip_librechat(messages: list, filter_cfg: dict) -> list:
    """LibreChat sends its entire conversation history every turn. Keep only
    the last user message — the bridge/provider owns session context."""
    for msg in reversed(messages):
        if msg.get("role") == "user":
            if len(messages) > 1:
                logger.debug(
                    f"context_stripper [librechat]: {len(messages)} messages → 1"
                )
            return [msg]
    logger.warning(
        "context_stripper [librechat]: no user message found, "
        "passing through unchanged"
    )
    return messages


# ---------------------------------------------------------------------------
# Filter: openclaw — strip the untrusted-metadata prefix off the last user turn
# ---------------------------------------------------------------------------

# Matches the openclaw metadata prefix on user turns:
#   Sender (untrusted metadata):\n```json\n{...}\n```\n\n[date line]\n
# Everything up to and including the date line (+ trailing whitespace) is stripped.
_OPENCLAW_PREFIX_RE = re.compile(
    r'^Sender \(untrusted metadata\):.*?```\s*\n\n\[[^\]]+\]\s*',
    re.DOTALL,
)


def _strip_openclaw_prefix(messages: list, filter_cfg: dict) -> list:
    """
    OpenClaw injects a metadata prefix onto the current user turn:

        Sender (untrusted metadata):
        ```json
        {"label": "openclaw-control-ui", "id": "openclaw-control-ui"}
        ```

        [Thu 2026-04-16 09:32 GMT+9:30] <actual message>

    Strip it from the last user message, leave everything else intact.
    Handles both plain string content and list content (multi-part messages).
    Returns the input unchanged if the prefix isn't found.
    """
    for i, msg in reversed(list(enumerate(messages))):
        if msg.get("role") != "user":
            continue
        content = msg.get("content") or ""

        if isinstance(content, str):
            cleaned, count = _OPENCLAW_PREFIX_RE.subn("", content, count=1)
            if count:
                logger.debug(
                    "context_stripper [openclaw]: stripped prefix from string content"
                )
                new_messages = list(messages)
                new_messages[i] = {**msg, "content": cleaned.lstrip("\n")}
                return new_messages
            return messages  # prefix not found — nothing to strip

        if isinstance(content, list):
            # Find the first text part and strip the prefix from it.
            new_parts = list(content)
            for j, part in enumerate(new_parts):
                if not isinstance(part, dict) or part.get("type") != "text":
                    continue
                text = part.get("text") or ""
                cleaned, count = _OPENCLAW_PREFIX_RE.subn("", text, count=1)
                if count:
                    logger.debug(
                        "context_stripper [openclaw]: stripped prefix from "
                        "list content part"
                    )
                    new_parts[j] = {**part, "text": cleaned.lstrip("\n")}
                    new_messages = list(messages)
                    new_messages[i] = {**msg, "content": new_parts}
                    return new_messages
                break  # first text part checked, prefix not found
            return messages

        return messages  # unknown content type — don't touch
    return messages


# ---------------------------------------------------------------------------
# Filter registry — add new filters here. Key = config name = run-by-name.
# ---------------------------------------------------------------------------

_FILTERS: dict[str, Callable[[list, dict], list]] = {
    "librechat": _strip_librechat,
    "openclaw": _strip_openclaw_prefix,
}


def _is_enabled(filter_cfg) -> bool:
    """A filter is enabled by presence of its key, unless it explicitly
    declares ``enabled: false``. A bare ``filter: {}`` (or ``filter:`` with
    null) is enabled."""
    if isinstance(filter_cfg, dict):
        return filter_cfg.get("enabled", True) is not False
    # Non-dict value (e.g. ``openclaw: true`` / null) → presence = on.
    return filter_cfg is not False


# ---------------------------------------------------------------------------
# Capability method
# ---------------------------------------------------------------------------

def modify_context(ctx: "PipelineCtx", config: dict) -> "PipelineCtx":
    """Run each enabled filter, in config (YAML) order, chaining outputs.

    config shape::

        { "librechat": {}, "openclaw": {"enabled": True} }

    Unknown filter names are warned-and-skipped. ``enabled: false`` filters
    are skipped silently (dormant). Empty/absent config is a no-op.
    """
    if not config:
        return ctx

    messages = ctx.request.messages
    if not messages:
        return ctx

    before_count = len(messages)
    applied: list[str] = []

    for name, filter_cfg in config.items():
        fn = _FILTERS.get(name)
        if fn is None:
            logger.warning(
                f"context_stripper: unknown filter '{name}' on identity "
                f"'{ctx.identity.key}' — skipping. Known filters: "
                f"{sorted(_FILTERS)}"
            )
            continue
        if not _is_enabled(filter_cfg):
            logger.debug(
                f"context_stripper: filter '{name}' disabled (enabled: false) "
                f"on identity '{ctx.identity.key}' — skipping."
            )
            continue
        messages = fn(messages, filter_cfg if isinstance(filter_cfg, dict) else {})
        applied.append(name)

    if applied:
        ctx.request.messages = messages
        logger.info(
            f"context_stripper {applied}: {before_count} messages → "
            f"{len(messages)} on identity '{ctx.identity.key}'"
        )
    return ctx
