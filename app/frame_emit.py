"""
Assistant-turn → SSE chunks emitter for ind-bridge V4.

Pure module — no I/O, no httpx, no FastAPI. The mechanical *dual* of
``app.stream_reconstruct.reconstruct_from_chunks``: given an assembled
OpenAI-shape response (either the verbatim upstream dict, or the flat
shape produced by the reconstructor), yield a sequence of SSE
``data: {...}`` event bytes that a client streaming consumer would
accept as a normal chat-completions stream.

Why this exists (D-008): when pre-delivery plugins (``response_modify``
or ``handle_tool_calls``) are wired on a pipeline whose client requested
streaming, the executor forces the upstream call to non-streaming so
the assembled frame is available to the plugins. After the plugins
finish, the client still expects SSE — this module is what re-emits
the post-pre-delivery frame as a stream.

Companion to ``stream_reconstruct.py``: same SSE format, opposite
direction. Format constants (event prefix, terminator, JSON envelope
shape) are pinned in ``_DATA_PREFIX`` / ``_DONE_LINE`` here and mirror
the parser's expectations there.

Pre-delivery plugins operate on assembled frames (the GgWave principle
applied to LLM streaming): the medium is the transmission, the message
is the assembled response. This module hides the transport choice
(stream vs non-stream) from the client — the client never sees that
the upstream was non-stream.

Contract:

  Input — a dict in either of two shapes:

    A. **Upstream-verbatim** (what ``_do_http_call_nonstream`` puts on
       ``ctx.response["_full_response"]``)::

          {
            "id": "...", "object": "chat.completion", "created": ...,
            "model": "...", "choices": [
              {"index": 0,
               "message": {"role": "assistant",
                           "content": "...",
                           "tool_calls": [...]?,
                           "reasoning_content": "..."?},
               "finish_reason": "stop|tool_calls|length|..."}
            ],
            "usage": {...}?
          }

    B. **Flat reconstructed** (what ``stream_reconstruct`` returns)::

          {
            "role": "assistant",
            "content": "...",
            "tool_calls": [...] | None,
            "reasoning_content": "..." | None,
            "finish_reason": "stop|tool_calls|...",
            ...
          }

  Output — async generator yielding ``bytes``. Each yielded value is
  one SSE event already terminated with ``\\n\\n``. The final yield
  is always ``data: [DONE]\\n\\n``.

  Emission strategy: full content as a single delta (clients tolerate
  this — chunked emission would be cosmetic only and adds latency).
  Tool_calls and reasoning_content each in their own delta. A final
  chunk carries the ``finish_reason``. ``[DONE]`` terminator.

  An optional ``model``/``id`` echo is included on each chunk's
  envelope so tools that key off these fields don't choke. When the
  input doesn't carry these (Shape B), defaults are synthesised.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, AsyncIterator

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SSE format constants — mirror stream_reconstruct's parser expectations
# ---------------------------------------------------------------------------

_DATA_PREFIX = "data: "
_EVENT_TERMINATOR = "\n\n"
_DONE_LINE = b"data: [DONE]\n\n"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def emit_frame_as_sse(frame: dict) -> AsyncIterator[bytes]:
    """Yield SSE-event bytes representing the assembled assistant turn.

    Tolerant of either input shape (see module docstring). Async
    generator so it plugs into FastAPI's ``StreamingResponse(...)``
    directly without an intermediate buffer.

    Defensive: a missing/malformed input field becomes an empty/None
    delta, never an exception. The point of this module is to *always*
    produce a valid SSE stream from whatever ctx.response holds —
    partial output beats blowing up the client connection.
    """
    msg = _extract_message(frame)
    finish_reason = _extract_finish_reason(frame)
    completion_id = _extract_id(frame)
    model = _extract_model(frame)
    created = _extract_created(frame)

    base_envelope = {
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
    }

    role_delta = {"role": "assistant"}
    yield _format_chunk(base_envelope, delta=role_delta, finish_reason=None)

    # Reasoning — emit whichever key(s) the frame carries (provider key drift:
    # Moonshot-direct → reasoning_content; OpenRouter → reasoning + structured
    # reasoning_details). Relay under the original key, never normalise, so the
    # client sees the agent think on the buffered re-emit path too (parity with
    # the live stream_intercept path).
    reasoning = msg.get("reasoning_content")
    if isinstance(reasoning, str) and reasoning:
        yield _format_chunk(
            base_envelope,
            delta={"reasoning_content": reasoning},
            finish_reason=None,
        )
    reasoning_alt = msg.get("reasoning")
    if isinstance(reasoning_alt, str) and reasoning_alt:
        yield _format_chunk(
            base_envelope,
            delta={"reasoning": reasoning_alt},
            finish_reason=None,
        )
    reasoning_details = msg.get("reasoning_details")
    if isinstance(reasoning_details, list) and reasoning_details:
        yield _format_chunk(
            base_envelope,
            delta={"reasoning_details": reasoning_details},
            finish_reason=None,
        )

    content = msg.get("content")
    if isinstance(content, str) and content:
        yield _format_chunk(
            base_envelope,
            delta={"content": content},
            finish_reason=None,
        )

    tool_calls = msg.get("tool_calls")
    if isinstance(tool_calls, list) and tool_calls:
        yield _format_chunk(
            base_envelope,
            delta={"tool_calls": tool_calls},
            finish_reason=None,
        )

    yield _format_chunk(base_envelope, delta={}, finish_reason=finish_reason)
    yield _DONE_LINE


# ---------------------------------------------------------------------------
# Shape extraction — handle both upstream-verbatim and flat-reconstructed
# ---------------------------------------------------------------------------

def _extract_message(frame: dict) -> dict:
    """Pull the assistant message dict out of either input shape.

    Shape A: ``frame["choices"][0]["message"]``.
    Shape B: ``frame`` itself (already flat).
    Returns an empty dict if neither shape has anything usable —
    callers treat absent fields as empty/None.
    """
    if not isinstance(frame, dict):
        return {}
    choices = frame.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            msg = first.get("message")
            if isinstance(msg, dict):
                return msg
    # Flat shape
    if frame.get("role") == "assistant" or "content" in frame:
        return frame
    return {}


def _extract_finish_reason(frame: dict) -> str | None:
    """Pull finish_reason from either input shape."""
    if not isinstance(frame, dict):
        return None
    choices = frame.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            fr = first.get("finish_reason")
            if isinstance(fr, str):
                return fr
    fr = frame.get("finish_reason")
    return fr if isinstance(fr, str) else None


def _extract_id(frame: dict) -> str:
    """Use upstream-provided id if present, otherwise synthesise one.
    Synthesised ids are prefixed so an operator can grep them out of
    logs and see which turns came through the re-emit path."""
    if isinstance(frame, dict):
        candidate = frame.get("id")
        if isinstance(candidate, str) and candidate:
            return candidate
    return f"ind-bridge-emit-{uuid.uuid4().hex[:24]}"


def _extract_model(frame: dict) -> str:
    if isinstance(frame, dict):
        candidate = frame.get("model")
        if isinstance(candidate, str) and candidate:
            return candidate
    return "ind-bridge"


def _extract_created(frame: dict) -> int:
    if isinstance(frame, dict):
        candidate = frame.get("created")
        if isinstance(candidate, int):
            return candidate
    return int(time.time())


# ---------------------------------------------------------------------------
# Internals — format one SSE event
# ---------------------------------------------------------------------------

def _format_chunk(
    base_envelope: dict,
    *,
    delta: dict,
    finish_reason: str | None,
) -> bytes:
    """Build one SSE ``data: {...}\\n\\n`` event."""
    envelope: dict[str, Any] = dict(base_envelope)
    envelope["choices"] = [{
        "index": 0,
        "delta": delta,
        "finish_reason": finish_reason,
    }]
    try:
        payload = json.dumps(envelope, ensure_ascii=False, default=str)
    except Exception as e:
        logger.warning(
            f"frame_emit: JSON serialisation failed ({type(e).__name__}: {e!r}); "
            f"falling back to a minimal error event."
        )
        payload = json.dumps({
            "id": base_envelope.get("id", "ind-bridge-emit-error"),
            "object": "chat.completion.chunk",
            "choices": [{"index": 0, "delta": {}, "finish_reason": "error"}],
        })
    return f"{_DATA_PREFIX}{payload}{_EVENT_TERMINATOR}".encode("utf-8")
