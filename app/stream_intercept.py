"""
Streaming-while-intercepting path for ind-bridge — "the pause IS the tool".

The third executor path (the first two: buffer-all when pre-delivery wired, and
raw passthrough when not). This one streams a bridge-owned tool loop to the
client AS IT RUNS, so a capable agent doing many tool laps over minutes/hours
feels alive instead of a dead connection that times out.

The insight: in an OpenAI SSE stream, ``content`` deltas
and ``tool_calls`` deltas are SEPARATE fields. So we can:

  * forward ``content`` deltas to the client LIVE (the agent's narration — and
    that flow of real bytes IS a natural keepalive, so the client never times
    out → the cancellation turn-loss gap can't open),
  * accumulate ``tool_calls`` deltas SILENTLY (the client never sees bridge-
    native calls it couldn't run anyway),
  * suppress each lap's ``finish_reason`` / ``[DONE]``,
  * at each lap's close: if it's a bridge tool_call, execute it, splice the
    result, re-call upstream → next lap. The client just sees a PAUSE, then
    more narration. The whole tool round-trip lives inside the pause.

The client experiences ONE continuous assistant message:
``content → pause → content → pause → … → stop``. Bridge-native plumbing is
never visible. Harness tool_calls (claimed by no intercept plugin) and the
final text turn are forwarded with the REAL finish_reason + ``[DONE]``.

Reuses the executor's intercept helpers (claim/execute/splice/neutralize) and
``stream_reconstruct`` / ``frame_emit`` — this module is wiring, not new tool
logic.
"""

from __future__ import annotations

import json
import logging
from typing import Any, AsyncIterator, Callable

import httpx

from . import bridge_native, frame_emit, stream_reconstruct

logger = logging.getLogger(__name__)


# SSE framing (mirror frame_emit / stream_reconstruct constants).
_DONE_LINE = b"data: [DONE]\n\n"


# Client-facing delta keys we relay LIVE during a lap. ``content`` is the
# agent's narration; the reasoning keys are the agent *thinking* (harnesses that
# render reasoning should see it stream, not just appear after the tool pause).
# Provider key drift: Moonshot-direct streams `reasoning_content`; OpenRouter
# streams `reasoning` (+ structured `reasoning_details`). We relay under whatever
# key the provider used — never normalise (mirrors _wrap_as_full_response).
# tool_calls are DELIBERATELY excluded: bridge-native calls must never leak to
# the harness, and they're accumulated separately by reconstructing the lap.
# Order matters: reasoning keys BEFORE content, so within a chunk that carries
# both, the client sees the thinking delta first then the narration (mirrors
# frame_emit; keeps the reasoning→content render seam clean).
#
# `reasoning_details` is NOT relayed to the client: providers (Kimi via
# OpenRouter) send it alongside `reasoning` carrying the SAME text per chunk.
# Relaying both = the reasoning text arrives TWICE and as two chunks per token,
# which fragments the client render into word-below-word. The client renders
# from the `reasoning`/`reasoning_content` string; the structured array is only
# needed for the WIRE round-trip, which travels via lap_chunks/reconstruction —
# not this live relay.
_CLIENT_DELTA_KEYS = ("reasoning_content", "reasoning", "content")


def _tidy_reasoning_active(ctx: Any, pipeline: list) -> bool:
    """True if the quirks_mode `tidy_reasoning_whitespace` quirk is enabled for
    this identity — so the live relay tidies reasoning deltas to match what gets
    stored. Reads the quirks_mode tuple's resolved config off the pipeline; any
    import/shape hiccup → False (relay verbatim, never crash the stream)."""
    try:
        from .plugins._builtin.quirks_mode import _enabled as _quirk_enabled
        for tup in pipeline:
            # pipeline tuples are (slot, plugin, config); find quirks_mode
            plugin = tup[1] if len(tup) > 1 else None
            cfg = tup[2] if len(tup) > 2 else {}
            mod = getattr(plugin, "__name__", "") or getattr(
                getattr(plugin, "__class__", None), "__module__", ""
            )
            if "quirks_mode" in str(mod) and _quirk_enabled(cfg, "tidy_reasoning_whitespace"):
                return True
    except Exception:
        return False
    return False


def _tidy_reasoning_delta(delta: dict) -> "dict | None":
    """Tidy a single reasoning delta for the live client relay: a newline-only
    delta is dropped (return None — Kimi's standalone `\\n` tokens), and a delta
    carrying newlines has them stripped. Content deltas pass through untouched.
    Mirrors quirks_mode.tidy_reasoning_text at the per-token granularity the live
    stream can act on."""
    for key in ("reasoning_content", "reasoning"):
        val = delta.get(key)
        if isinstance(val, str) and "\n" in val:
            stripped = val.replace("\n", "")
            if not stripped:
                return None  # newline-only token → drop from the live stream
            return {key: stripped}
    return delta


def _should_relay(delta: dict, content_started: bool) -> bool:
    """Decide whether a single-key client delta reaches the glass this turn.

    Content always relays. Reasoning relays only BEFORE the turn's first content
    delta — once content has started, resumed reasoning is dropped from the
    client stream (clients render reasoning once, before content; resumed
    reasoning makes them fold later content into the reasoning box). The dropped
    reasoning is still in lap_chunks, so it's reconstructed and stored — the
    agent's full thinking survives; only the client view is trimmed."""
    if "content" in delta:
        return True
    return not content_started


def _client_chunk(delta: dict, model: str | None) -> bytes:
    """A single OpenAI streaming chunk carrying a client-facing delta. Used to
    relay upstream content/reasoning verbatim (we re-emit rather than pass the
    raw upstream chunk so per-lap stitching stays under our control — one
    stream, our framing, no upstream ``[DONE]`` leaking between laps)."""
    payload = {
        "object": "chat.completion.chunk",
        "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
    }
    if model:
        payload["model"] = model
    return f"data: {json.dumps(payload)}\n\n".encode("utf-8")


def _content_chunk(text: str, model: str | None) -> bytes:
    """A single OpenAI streaming chunk carrying a content delta. Used for error
    relay and the legacy call sites that only emit content."""
    return _client_chunk({"content": text}, model)


def _iter_client_deltas(chunk: bytes) -> list[dict]:
    """Pull client-facing delta dicts (``content`` + reasoning keys) out of one
    raw SSE chunk, preserving the provider's key per delta. Returns [] for
    tool_call-only chunks, ``[DONE]``, finish_reason-only chunks, etc.
    Tool_call deltas are deliberately NOT surfaced here — bridge-native calls
    must never leak to the harness, and they're accumulated separately by
    reconstructing the whole lap."""
    out: list[dict] = []
    for raw_line in chunk.split(b"\n"):
        line = raw_line.strip()
        if not line.startswith(b"data:"):
            continue
        data = line[len(b"data:"):].strip()
        if not data or data == b"[DONE]":
            continue
        try:
            event = json.loads(data)
        except (ValueError, TypeError):
            continue
        choices = event.get("choices") or []
        if not choices or not isinstance(choices[0], dict):
            continue
        delta = choices[0].get("delta") or {}
        # One delta PER KEY (never combine content + reasoning in a single
        # chunk). Some clients (LibreChat) close the content block when a
        # reasoning key arrives in the same/next delta, which fragments the
        # render into one-token-per-line. Emitting single-key chunks in a stable
        # order (reasoning keys first, then content — mirrors frame_emit) keeps
        # the client's delta accumulator clean across the reasoning→content seam.
        for key in _CLIENT_DELTA_KEYS:
            val = delta.get(key)
            # All client-relayed keys are strings (content / reasoning text).
            # reasoning_details is excluded — see _CLIENT_DELTA_KEYS note.
            if isinstance(val, str) and val:
                out.append({key: val})
    return out


def _wrap_as_full_response(reconstructed: dict, model: str | None) -> dict:
    """Wrap a flat ``stream_reconstruct`` dict into the upstream-verbatim
    ``choices[0].message`` shape the executor's intercept helpers read off
    ``ctx.response["_full_response"]``."""
    message: dict[str, Any] = {
        "role": "assistant",
        "content": reconstructed.get("content", "") or "",
    }
    tcs = reconstructed.get("tool_calls")
    if tcs:
        message["tool_calls"] = tcs
    # Preserve reasoning under whatever key(s) the provider streamed (drift).
    for k in ("reasoning_content", "reasoning", "reasoning_details"):
        v = reconstructed.get(k)
        if v:
            message[k] = v
    return {
        "object": "chat.completion",
        "model": model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": reconstructed.get("finish_reason"),
        }],
    }


async def stream_with_intercepts(
    ctx: Any,
    pipeline: list,
    *,
    build_request: Callable[[Any], tuple[str, dict, dict, float]],
    intercept_tuples: list,
    plugin_owned_tools: Callable[[Any], list[str]],
    tool_call_name: Callable[[dict], "str | None"],
    run_intercept_plugin: Callable[..., Any],
    build_assistant_turn: Callable[[Any], dict],
    max_tool_laps: int,
    on_complete: "Callable[[], Any] | None" = None,
):
    """Return a FastAPI StreamingResponse generator that runs the bridge-owned
    tool loop while streaming content to the client.

    The executor passes its own helpers in (build_request, the intercept
    helpers, the lap cap) so this module stays free of executor internals and
    circular imports. ``ctx.request.stream`` MUST be True coming in; we drive
    upstream streaming ourselves per lap.

    ``on_complete`` (async, no args) runs once after the stream closes — AFTER
    ``ctx.response`` holds the final assembled turn — so the executor can fire
    its post_response observers (basic_session save, conversational_memory).
    This is how the streaming path persists the full exchange (the
    ``intercept.loop_ran`` breadcrumb is set per-lap; the save reads it). Runs
    in the generator's ``finally`` so it fires even if the client disconnects
    mid-stream — sealing the cancellation turn-loss gap on this path too.
    """
    model = ctx.request.model
    uncapped = max_tool_laps <= 0

    async def generator() -> AsyncIterator[bytes]:
        lap = 0
        # Clients render reasoning ONCE per message (reasoning* → content*), not
        # resumed after content. A multi-lap turn is really reason→talk→tool→
        # reason→reply, so relaying every lap's reasoning makes LibreChat fold
        # later content into the reasoning box (content bleed, broken lines). We
        # stream reasoning live only UNTIL the first content delta of the WHOLE
        # turn; after that, reasoning is captured/stored internally but not sent
        # to the glass. One clean reasoning block at the top, then narration —
        # matching what a raw OpenRouter connection looks like in the client.
        content_started = False
        # tidy_reasoning_whitespace quirk: collapse Kimi's stray reasoning
        # newlines in the LIVE relay too (storage is tidied separately by
        # quirks_mode.observe_response), so client + stored agree.
        tidy_reasoning = _tidy_reasoning_active(ctx, pipeline)
        client = httpx.AsyncClient()
        try:
            while True:
                url, body, headers, timeout = build_request(ctx)
                body["stream"] = True  # we always stream upstream on this path
                client.timeout = httpx.Timeout(timeout)

                lap_chunks: list[bytes] = []
                # ── stream this lap: forward content, stash everything ──────
                async with client.stream("POST", url, json=body, headers=headers) as up:
                    if up.status_code >= 400:
                        err_body = await up.aread()
                        snippet = err_body[:200].decode("utf-8", errors="replace")
                        logger.warning(
                            f"stream_intercept upstream error: identity="
                            f"'{ctx.identity.key}' status={up.status_code} "
                            f"body={snippet!r}"
                        )
                        yield _content_chunk(
                            f"[upstream {up.status_code}: {snippet}]", model
                        )
                        yield _DONE_LINE
                        return
                    # Iterate by LINE, not raw bytes. ``aiter_bytes`` yields
                    # arbitrary byte slices that split SSE events mid-JSON;
                    # parsing those per-slice drops/mangles the split event
                    # (root cause of the per-word newline + duplication in stored
                    # reasoning — proven via a raw-OpenRouter curl, 2026-06-23).
                    # ``aiter_lines`` reassembles complete lines for us, so each
                    # ``data: {...}`` event is whole before we parse it.
                    async for line in up.aiter_lines():
                        if not line:
                            continue
                        chunk = (line + "\n").encode("utf-8")
                        lap_chunks.append(chunk)
                        for delta in _iter_client_deltas(chunk):
                            if not _should_relay(delta, content_started):
                                continue
                            if "content" in delta:
                                content_started = True
                            elif tidy_reasoning:
                                delta = _tidy_reasoning_delta(delta)
                                if delta is None:
                                    continue  # newline-only reasoning token — drop
                            yield _client_chunk(delta, model)

                # ── lap closed: reconstruct the assembled frame ─────────────
                recon = stream_reconstruct.reconstruct_from_chunks(lap_chunks)
                full = _wrap_as_full_response(recon, model)
                ctx.response = {
                    "role": "assistant",
                    "content": recon.get("content", "") or "",
                    "_full_response": full,
                }
                finish_reason = recon.get("finish_reason")
                tool_calls = recon.get("tool_calls") or []

                # No tool_calls → this lap is the final text turn. Close out.
                if finish_reason != "tool_calls" or not tool_calls:
                    yield _terminal_chunk(finish_reason or "stop", model)
                    yield _DONE_LINE
                    ctx.plugin_data["stream_intercept.closed"] = True
                    return

                # Bucket tool_calls by claiming intercept plugin (mirror the
                # executor's _dispatch_intercepts claim logic exactly).
                unhandled: set[int] = set(range(len(tool_calls)))
                plugin_claims: list[tuple[Any, dict, str, list[dict]]] = []
                for slot, plugin, plugin_config in intercept_tuples:
                    owned = plugin_owned_tools(plugin)
                    if not owned:
                        continue
                    claimed = []
                    for i, tc in enumerate(tool_calls):
                        name = tool_call_name(tc)
                        if name and bridge_native.strip_namespace(name) in owned and i in unhandled:
                            claimed.append(tc)
                            unhandled.discard(i)
                    if claimed:
                        plugin_claims.append((plugin, plugin_config, slot, claimed))

                # Mixed bridge+harness OR nothing-claimed → we can't transparently
                # intercept. Forward the assembled frame (minus any bridge-native
                # calls) as the real reply + DONE. (Never leak bridge_native to the
                # client — strip those, emit whatever remains as a normal frame.)
                if unhandled or not plugin_claims:
                    async for sse in _emit_assembled_frame(
                        full, intercept_tuples, plugin_owned_tools, tool_call_name
                    ):
                        yield sse
                    yield _DONE_LINE
                    ctx.plugin_data["stream_intercept.closed"] = True
                    return

                # Lap cap (finite) → stop looping; deliver what we have.
                if not uncapped and lap >= max_tool_laps:
                    logger.warning(
                        f"stream_intercept: identity='{ctx.identity.key}' "
                        f"max_tool_laps={max_tool_laps} reached; closing."
                    )
                    yield _terminal_chunk("stop", model)
                    yield _DONE_LINE
                    ctx.plugin_data["stream_intercept.closed"] = True
                    return

                # ── execute claimed tools (THE PAUSE), splice, loop ─────────
                assistant_turn = build_assistant_turn(ctx)
                tool_result_messages: list[dict] = []
                for plugin, plugin_config, slot, claimed in plugin_claims:
                    results = await run_intercept_plugin(
                        plugin, plugin_config, ctx, slot, claimed,
                    )
                    tool_result_messages.extend(results)

                ctx.request.messages = [
                    *ctx.request.messages,
                    assistant_turn,
                    *tool_result_messages,
                ]
                ctx.response = None
                # Breadcrumb for the save side (same flag the buffered path sets)
                # so basic_session stores the full exchange.
                ctx.plugin_data["intercept.loop_ran"] = True
                lap += 1
                # loop → next upstream call → client sees more content after the pause
        except httpx.HTTPError as e:
            logger.error(f"stream_intercept network error: {type(e).__name__}: {e!r}")
            yield _content_chunk(f"[network error: {type(e).__name__}]", model)
            yield _DONE_LINE
        finally:
            await client.aclose()
            # Persist the turn — runs even on client disconnect (the finally),
            # so a cancelled long loop still saves what executed. ctx.response
            # holds the last assembled lap; the intercept.loop_ran breadcrumb
            # (set per-lap) tells basic_session to store the full exchange.
            if on_complete is not None:
                try:
                    await on_complete()
                except Exception:
                    logger.error(
                        "stream_intercept on_complete (save) raised", exc_info=True
                    )

    return generator


def _terminal_chunk(finish_reason: str, model: str | None) -> bytes:
    """An empty-delta chunk carrying the final finish_reason — closes the
    single stitched assistant message."""
    payload = {
        "object": "chat.completion.chunk",
        "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}],
    }
    if model:
        payload["model"] = model
    return f"data: {json.dumps(payload)}\n\n".encode("utf-8")


async def _emit_assembled_frame(full, intercept_tuples, plugin_owned_tools, tool_call_name):
    """Strip bridge-native tool_calls from an assembled frame (never-leak
    invariant) then emit it as SSE chunks via frame_emit. Used on the
    mixed/unclaimed branch where we hand the turn back to the client."""
    owned_all: set[str] = set()
    for _slot, plugin, _cfg in intercept_tuples:
        owned_all.update(plugin_owned_tools(plugin))
    choices = full.get("choices") or []
    if choices and isinstance(choices[0], dict):
        msg = choices[0].get("message") or {}
        tcs = msg.get("tool_calls")
        if isinstance(tcs, list) and tcs:
            kept = [tc for tc in tcs if not (
                (n := tool_call_name(tc)) and bridge_native.strip_namespace(n) in owned_all
            )]
            if kept:
                msg["tool_calls"] = kept
            else:
                msg.pop("tool_calls", None)
                choices[0]["finish_reason"] = "stop"
    async for sse in frame_emit.emit_frame_as_sse(full):
        yield sse
