"""
SSE → assistant turn reconstructor for ind-bridge V4.

Pure module — no I/O, no httpx, no FastAPI. Given a list of OpenAI-shaped
streaming SSE chunk bytes (as captured by the executor's stream tee),
returns a single assistant turn dict suitable for handing to a
``post_response`` plugin (D-007).

Constraints:

  - Accumulate ``delta.content`` by string concatenation (additive).
  - Accumulate ``delta.tool_calls`` by ``index`` — each tool_call's
    ``function.arguments`` concatenates across chunks; ``id`` and
    ``function.name`` may arrive on first chunk only OR be re-sent
    by some providers — last-non-empty wins.
  - Accumulate ``delta.reasoning_content`` like ``content`` — must
    round-trip cleanly so follow-up requests with the reconstructed
    turn don't 400 against providers that require it in the message history.
  - ``finish_reason`` is verbatim from upstream — no semantic interpretation.
    Different providers report differently (``stop`` / ``tool_calls`` /
    ``length`` / ``end_turn`` / ``function_call``); plugins decide policy.
  - Be tolerant of provider drift: full-tool_call-on-first-chunk,
    finish_reason on its own chunk, finish_reason on last content chunk —
    all valid shapes; the reconstructor handles each.
  - No JSON parsing of ``tool_calls.function.arguments`` — preserved as
    the concatenated string. Plugins that need parsed args parse them.

Shape returned::

    {
        "role": "assistant",
        "content": str,                      # accumulated; "" if none
        "tool_calls": list[dict] | None,     # None if none seen
        "reasoning_content": str | None,     # Moonshot-direct key; None if none seen
        "reasoning": str,                    # OpenRouter key; present only if seen
        "reasoning_details": list,           # structured; present only if seen
        "finish_reason": str | None,         # last non-null seen, or None
        "raw_chunks": list[bytes],           # echo of input for plugins that want it
    }

Reasoning key drift: providers report reasoning under different keys — Moonshot
direct uses ``reasoning_content``; Moonshot-via-OpenRouter uses ``reasoning`` (+
``reasoning_details``). We accumulate all of them and surface whatever was sent,
so no provider's reasoning is silently dropped (the prior code only watched
``reasoning_content``, erasing OpenRouter-streamed reasoning).
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def reconstruct_from_chunks(chunks: list[bytes]) -> dict:
    """Turn a list of OpenAI streaming SSE chunk bytes into one assistant
    turn dict. See module docstring for the full contract.

    Defensive against malformed chunks: bad SSE lines are logged at debug
    and skipped — never raises. The point of this function is to produce
    *something useful* from whatever the upstream sent; partial data is
    better than an exception in a background task.
    """
    content_parts: list[str] = []
    reasoning_parts: list[str] = []
    # Reasoning arrives under different keys per provider (key drift): Moonshot
    # (via OpenRouter) streams `reasoning` (+ `reasoning_details`), Moonshot-direct
    # streams `reasoning_content`. We accumulate whichever string keys appear and
    # preserve `reasoning_details` (structured) verbatim, so no provider's
    # reasoning is silently dropped on the streamed path.
    reasoning_alt_parts: list[str] = []
    reasoning_details: list[Any] = []
    finish_reason: str | None = None

    # Tool calls accumulate by `index`. Provider quirks:
    #   - `id` and `function.name` may arrive only on the first chunk for an
    #     index, OR be re-sent on every chunk. We use last-non-empty-wins so
    #     either pattern produces the same final value.
    #   - `function.arguments` is the only field that *concatenates* — every
    #     chunk's contribution is appended to the running string.
    tool_calls_by_index: dict[int, dict[str, Any]] = {}

    for chunk in chunks:
        for event in _iter_sse_events(chunk):
            choice = _first_choice(event)
            if choice is None:
                continue

            delta = choice.get("delta") or {}
            if not isinstance(delta, dict):
                continue

            # Content
            c = delta.get("content")
            if isinstance(c, str) and c:
                content_parts.append(c)

            # Reasoning — key varies by provider (drift). Accumulate both the
            # Moonshot-direct key (`reasoning_content`) and the OpenRouter key
            # (`reasoning`), plus structured `reasoning_details`. The bridge used
            # to look only for `reasoning_content`, silently dropping Moonshot-via-
            # OpenRouter reasoning (which streams under `reasoning`) — that loss is
            # what broke reasoning re-attach on tool-call round-trips.
            rc = delta.get("reasoning_content")
            if isinstance(rc, str) and rc:
                reasoning_parts.append(rc)
            r_alt = delta.get("reasoning")
            if isinstance(r_alt, str) and r_alt:
                reasoning_alt_parts.append(r_alt)
            rd = delta.get("reasoning_details")
            if isinstance(rd, list):
                reasoning_details.extend(rd)

            # Tool calls
            tc_list = delta.get("tool_calls")
            if isinstance(tc_list, list):
                for tc in tc_list:
                    if isinstance(tc, dict):
                        _merge_tool_call(tool_calls_by_index, tc)

            # finish_reason — last non-null wins
            fr = choice.get("finish_reason")
            if isinstance(fr, str) and fr:
                finish_reason = fr

    # Assemble
    tool_calls: list[dict] | None
    if tool_calls_by_index:
        # Sort by index so the order matches what the upstream emitted
        tool_calls = [
            tool_calls_by_index[i]
            for i in sorted(tool_calls_by_index.keys())
        ]
    else:
        tool_calls = None

    reasoning_content = "".join(reasoning_parts) if reasoning_parts else None
    reasoning = "".join(reasoning_alt_parts) if reasoning_alt_parts else None

    turn = {
        "role": "assistant",
        "content": "".join(content_parts),
        "tool_calls": tool_calls,
        # Moonshot-direct key (kept for back-compat with existing consumers).
        "reasoning_content": reasoning_content,
        "finish_reason": finish_reason,
        "raw_chunks": list(chunks),
    }
    # Surface the OpenRouter-style keys only when present, so consumers that
    # round-trip rich fields (e.g. reasoning re-attach) see whatever the provider
    # actually sent without the key drift erasing it.
    if reasoning is not None:
        turn["reasoning"] = reasoning
    if reasoning_details:
        turn["reasoning_details"] = reasoning_details
    return turn


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _iter_sse_events(chunk: bytes):
    """Yield the parsed JSON object for each ``data: {...}`` line in chunk.

    SSE chunks may carry zero, one, or many events. ``[DONE]`` and
    keep-alive comments (lines starting with ``:``) are skipped. Non-JSON
    payloads after ``data:`` are logged and skipped — providers
    occasionally send half-formed chunks at stream boundaries.
    """
    if not chunk:
        return
    try:
        text = chunk.decode("utf-8", errors="replace")
    except Exception as e:
        logger.debug(f"stream_reconstruct: chunk decode failed: {e!r}")
        return

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith(":"):
            continue  # SSE comment / keep-alive
        if not line.startswith("data:"):
            continue
        payload = line[len("data:"):].strip()
        if not payload:
            continue
        if payload == "[DONE]":
            continue
        try:
            yield json.loads(payload)
        except (ValueError, json.JSONDecodeError) as e:
            logger.debug(
                f"stream_reconstruct: skipping non-JSON SSE payload "
                f"({type(e).__name__}): {payload[:120]!r}"
            )
            continue


def _first_choice(event: Any) -> dict | None:
    """Pull ``choices[0]`` from an SSE event JSON object, defending
    against shape drift. Returns None if absent/malformed."""
    if not isinstance(event, dict):
        return None
    choices = event.get("choices")
    if not isinstance(choices, list) or not choices:
        return None
    first = choices[0]
    if not isinstance(first, dict):
        return None
    return first


def _merge_tool_call(by_index: dict[int, dict[str, Any]], delta_tc: dict) -> None:
    """Merge one tool_call delta into the running by-index accumulator.

    Rules:
      - `index` is required to identify which tool_call this is (OpenAI
        guarantees it on streaming tool_calls). If absent, default to 0
        and warn — some providers omit it when there's only one call.
      - `function.arguments` is *concatenated*; everything else is
        last-non-empty-wins (handles both "sent once" and "re-sent every
        chunk" provider patterns).
      - `type` defaults to "function" if absent (current OpenAI spec
        only defines that one).
    """
    index = delta_tc.get("index")
    if not isinstance(index, int):
        # Defensive: some providers omit `index` when there's only one
        # tool_call. Bucket them all at 0 — same effect as "single call".
        index = 0

    bucket = by_index.setdefault(index, {
        "index": index,
        "id": "",
        "type": "function",
        "function": {"name": "", "arguments": ""},
    })

    # id — last-non-empty
    new_id = delta_tc.get("id")
    if isinstance(new_id, str) and new_id:
        bucket["id"] = new_id

    # type — last-non-empty
    new_type = delta_tc.get("type")
    if isinstance(new_type, str) and new_type:
        bucket["type"] = new_type

    fn_delta = delta_tc.get("function")
    if isinstance(fn_delta, dict):
        bucket_fn = bucket["function"]

        # name — last-non-empty (re-sent harmlessly on some providers)
        new_name = fn_delta.get("name")
        if isinstance(new_name, str) and new_name:
            bucket_fn["name"] = new_name

        # arguments — CONCATENATE (the streaming-fragment field)
        new_args = fn_delta.get("arguments")
        if isinstance(new_args, str) and new_args:
            bucket_fn["arguments"] = (bucket_fn.get("arguments") or "") + new_args
