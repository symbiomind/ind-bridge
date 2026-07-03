"""
file_resource — a terminal resource plugin that writes the inbound request to
a file and returns a fixed acknowledgement.

This is the first consumer of the ``produce_response`` capability: a resource
plugin that *is* the response source rather than configuring an outbound call.
When wired onto a resource, the executor short-circuits all outbound transport
— no network call is made. The plugin writes the inbound request to a file and
returns a static reply (default ``"message received"``).

Use it to terminate a pipeline at the filesystem instead of an AI backend, or
as an inspectable sink: because the bridge assembles and signs the
``<bridge_context>`` block into the request *before* the resource step, a full
capture records the exact envelope an AI backend would have received.

Two independent knobs control what is written and how:

  * ``capture`` — WHAT to write.
      - ``message`` (default): the latest ``role: "user"`` message text.
      - ``full``: the entire assembled outbound ``messages`` list, including the
        signed ``<bridge_context>`` block.
  * ``format`` — HOW to serialise it.
      - ``text`` (default): plain text.
      - ``json``: ``json.dumps(..., indent=2)``.
      - ``markdown``: human-readable role/content sections.

Wire it in::

    resources:
      log_sink:
        plugins:
          file_resource:
            path: /data/messages.log   # required
            append: true               # default true; false truncates each turn
            reply: "message received"  # optional; default "message received"
            capture: full              # message (default) | full
            format: json               # text (default) | json | markdown

    roles:
      log_role:
        resource: log_sink

Per the V4 capability contract, ``produce_response`` is valid only on
``resource.plugins``. A resource may not declare both ``produce_response`` and
an ``outbound_params`` transport plugin — the validator rejects that as a
terminal-vs-transport conflict.
"""

from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.context import PipelineCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "produce_response": ["resource.plugins"],
}


_DEFAULT_REPLY = "message received"
_VALID_CAPTURE = ("message", "full")
_VALID_FORMAT = ("text", "json", "markdown")


def _message_text(message: dict) -> str:
    """Text of a single message. ``content`` may be a string or a list of
    content parts (multimodal); for a list we concatenate the ``text`` parts."""
    content = message.get("content") if isinstance(message, dict) else None
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            part.get("text", "")
            for part in content
            if isinstance(part, dict) and part.get("type") == "text"
        )
    return ""


def _latest_user_text(messages: list[dict]) -> str:
    """Text of the last ``role: "user"`` message in the working copy, or ""."""
    for msg in reversed(messages or []):
        if isinstance(msg, dict) and msg.get("role") == "user":
            return _message_text(msg)
    return ""


def _serialise(messages: list[dict], capture: str, fmt: str) -> str:
    """Render the captured payload to a string per the ``capture``/``format``
    knobs. ``capture: message`` reduces to the latest user text; ``full`` keeps
    the whole assembled messages list (including the signed <bridge_context>)."""
    if capture == "full":
        if fmt == "json":
            return json.dumps(messages, indent=2, ensure_ascii=False)
        if fmt == "markdown":
            blocks = []
            for msg in messages or []:
                role = msg.get("role", "?") if isinstance(msg, dict) else "?"
                blocks.append(f"### {role}\n\n{_message_text(msg)}")
            return "\n\n".join(blocks)
        # text: role-prefixed lines, faithful but plain
        return "\n".join(
            f"{(msg.get('role', '?') if isinstance(msg, dict) else '?')}: {_message_text(msg)}"
            for msg in (messages or [])
        )

    # capture == "message"
    text = _latest_user_text(messages)
    if fmt == "json":
        return json.dumps(text, ensure_ascii=False)
    if fmt == "markdown":
        return f"### user\n\n{text}"
    return text


def produce_response(ctx: "PipelineCtx", config: dict) -> "PipelineCtx":
    """Write the inbound request to ``config['path']`` and set a fixed reply.

    Terminal — no outbound call. The executor wraps ``ctx.response`` in an
    OpenAI-shaped envelope. File I/O failures are logged but never fail the
    turn; the acknowledgement is still returned so callers get a response.
    """
    reply = config.get("reply", _DEFAULT_REPLY)
    path = config.get("path")
    if not path:
        logger.warning(
            "file_resource: no 'path' configured for resource "
            f"'{ctx.resource.key}' — nothing written, returning reply only."
        )
        ctx.response = {"role": "assistant", "content": reply}
        return ctx

    capture = config.get("capture", "message")
    if capture not in _VALID_CAPTURE:
        logger.warning(
            f"file_resource: unknown capture '{capture}' for resource "
            f"'{ctx.resource.key}' — defaulting to 'message'. "
            f"Valid: {', '.join(_VALID_CAPTURE)}."
        )
        capture = "message"

    fmt = config.get("format", "text")
    if fmt not in _VALID_FORMAT:
        logger.warning(
            f"file_resource: unknown format '{fmt}' for resource "
            f"'{ctx.resource.key}' — defaulting to 'text'. "
            f"Valid: {', '.join(_VALID_FORMAT)}."
        )
        fmt = "text"

    append = config.get("append", True)
    mode = "a" if append else "w"
    payload = _serialise(ctx.request.messages, capture, fmt)

    try:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, mode, encoding="utf-8") as fh:
            fh.write(payload + "\n")
        logger.info(
            f"file_resource: wrote {len(payload)} char(s) to '{path}' "
            f"(mode={mode!r}, capture={capture!r}, format={fmt!r}) "
            f"for resource '{ctx.resource.key}'."
        )
    except OSError as e:
        logger.error(
            f"file_resource: failed to write to '{path}' for resource "
            f"'{ctx.resource.key}': {e}"
        )

    ctx.response = {"role": "assistant", "content": reply}
    return ctx
