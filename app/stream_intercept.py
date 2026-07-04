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

import asyncio
import json
import logging
import os
from typing import Any, AsyncIterator, Callable

import httpx

from . import bridge_native, frame_emit, stream_reconstruct

logger = logging.getLogger(__name__)


# Hard references to in-flight save tasks (on_complete run under cancellation).
# A task with no reference can be garbage-collected before it finishes, so we
# hold it here and drop it in the done-callback — this is what makes a save
# started during client-disconnect actually complete. Module-level so it
# survives the generator frame being torn down.
_SAVE_TASKS: set = set()


def _on_save_done(task) -> None:
    """Done-callback for a backgrounded on_complete save: log the outcome (so a
    cancelled turn's persistence is VISIBLE, not inferred) and drop the hard ref."""
    _SAVE_TASKS.discard(task)
    if task.cancelled():
        logger.warning("stream_intercept: background save task was CANCELLED — turn may not have persisted")
        return
    exc = task.exception()
    if exc is not None:
        logger.error(f"stream_intercept: background save task raised: {exc!r}", exc_info=exc)


# SSE framing (mirror frame_emit / stream_reconstruct constants).
_DONE_LINE = b"data: [DONE]\n\n"

# Keepalive heartbeat interval (seconds) during a bridge tool pause. A slow tool
# (docker cold-start ~20s) must not let the client/proxy read-timeout on a silent
# gap. Overridable via env for tuning against a specific client's timeout.
try:
    _KEEPALIVE_INTERVAL_S = float(os.getenv("BRIDGE_STREAM_KEEPALIVE_S", "5"))
except (TypeError, ValueError):
    _KEEPALIVE_INTERVAL_S = 5.0


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

    async def _run_on_complete(fn, ctx) -> None:
        """Run the save (on_complete) with a visibility log on either side, so a
        cancelled-mid-flight turn leaves a clear trail: we can SEE the save ran to
        completion rather than inferring it. Runs as the shielded task in the
        generator's finally."""
        logger.debug(
            f"stream_intercept: identity='{ctx.identity.key}' running on_complete "
            f"(persist turn); loop_ran={ctx.plugin_data.get('intercept.loop_ran')}"
        )
        await fn()
        logger.info(
            f"stream_intercept: identity='{ctx.identity.key}' on_complete finished "
            f"(turn persisted)."
        )

    async def generator() -> AsyncIterator[bytes]:
        lap = 0
        # TRANSPARENT PIPE: this generator now forwards the upstream stream to the
        # client VERBATIM (content, reasoning, reasoning_details, harness tool_calls)
        # and only swallows a BRIDGE-NATIVE tool_call (+ the upstream per-lap [DONE]).
        # The old re-mint machinery (_iter_client_deltas / _should_relay /
        # content_started gate / _tidy_reasoning_delta live-tidy) is no longer on the
        # hot path — raw passthrough carries the provider's own bytes, so there is
        # nothing to re-frame or tidy (storage-side tidy, if wired, is separate). The
        # helper functions remain (still unit-tested; used by the assembled-frame
        # fallback lineage) but the live relay does not call them.
        client = httpx.AsyncClient()
        try:
            while True:
                url, body, headers, timeout = build_request(ctx)
                body["stream"] = True  # we always stream upstream on this path
                client.timeout = httpx.Timeout(timeout)

                lap_chunks: list[bytes] = []
                # ── stream this lap: TRANSPARENT PASSTHROUGH ────────────────
                # The bridge is a transparent pipe: forward the upstream stream
                # to the client VERBATIM (content, reasoning, reasoning_details,
                # harness tool_calls — everything) and touch nothing. Only a
                # BRIDGE-NATIVE tool_call breaks transparency (Stage 2 tripwire:
                # swallow it + pause). We work at whole-SSE-LINE granularity —
                # byte-perfect forward per line, but a complete line so the peek
                # (Stage 2) sees whole `data: {...}` events, never a mid-JSON
                # slice (the aiter_bytes-split corruption, proven 2026-06-23).
                # `lap_chunks` tees the same whole lines for reconstruction
                # (storage) — identical framing to what the client received.
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
                    # `swallowing` trips the instant a BRIDGE-NATIVE tool_call
                    # line appears; from then on this lap's lines are buffered
                    # silently (tee'd for reconstruction) but NOT forwarded — so no
                    # bridge_native__ token leaks and the client sees a pause (the
                    # tool runs, then the next lap streams). Content/reasoning and
                    # HARNESS tool_calls before the trip already streamed verbatim.
                    swallowing = False
                    sse_buf = b""
                    async for raw in up.aiter_bytes():
                        sse_buf += raw
                        # Split into COMPLETE lines; keep the trailing partial in
                        # sse_buf until its newline arrives (whole-event invariant).
                        while b"\n" in sse_buf:
                            line, sse_buf = sse_buf.split(b"\n", 1)
                            line_bytes = line + b"\n"
                            lap_chunks.append(line_bytes)  # tee for reconstruction
                            if swallowing:
                                continue  # already tripped — buffer silently
                            # THE TRIPWIRE: inspect BEFORE forwarding. A bridge-
                            # native tool_call line is swallowed (and everything
                            # after it this lap) — never yielded.
                            if _line_has_bridge_native_toolcall(line_bytes):
                                swallowing = True
                                continue
                            # Swallow the UPSTREAM's own `[DONE]` — this generator
                            # owns the single stitched terminal/DONE for the WHOLE
                            # turn (a turn may span multiple laps; an upstream
                            # per-lap [DONE] must not leak mid-turn). Everything
                            # else forwards VERBATIM.
                            if _is_done_line(line_bytes):
                                continue
                            yield line_bytes
                    # Flush any trailing partial (rare; upstream usually ends on \n).
                    if sse_buf:
                        lap_chunks.append(sse_buf)
                        if not swallowing and not _is_done_line(sse_buf) \
                                and not _line_has_bridge_native_toolcall(sse_buf):
                            yield sse_buf

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

                # TRANSPARENT CLOSE: if we swallowed NOTHING this lap, the whole
                # lap already streamed to the client verbatim — content, reasoning,
                # AND any HARNESS tool_calls + the upstream's finish_reason. There
                # is nothing to intercept (no bridge-native call tripped the wire),
                # so we just emit the single stitched [DONE] and return. This is the
                # common path (pure text OR harness-tool turns): the bridge is a
                # clean pipe, the reconstruction/claim/intercept machinery below is
                # reached ONLY when a bridge-native call was swallowed. (Guard: if
                # the upstream never sent a finish_reason chunk — rare drift —
                # synthesise one so the client still gets a clean close.)
                if not swallowing:
                    if not _lap_forwarded_finish_reason(lap_chunks):
                        yield _terminal_chunk(finish_reason or "stop", model)
                    yield _DONE_LINE
                    ctx.plugin_data["stream_intercept.closed"] = True
                    return

                # We swallowed a bridge-native tool_call this lap. If reconstruction
                # somehow found no tool_calls (defensive — shouldn't happen after a
                # trip), close cleanly rather than loop forever.
                if not tool_calls:
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
                # The tool run can be SLOW (a docker cold-start ~20s). We swallowed
                # the bridge-native call, so nothing is streaming — a silent gap
                # this long risks a client/proxy read-timeout. Run the tools as a
                # background task and emit a KEEPALIVE heartbeat every
                # _KEEPALIVE_INTERVAL_S until it completes. The heartbeat is an
                # empty-delta chunk (no content/reasoning) — every OpenAI client
                # accepts it silently; it injects nothing into the render, just
                # keeps the connection warm. (If the model streamed preamble content
                # before the call, that already served as keepalive; this covers the
                # no-preamble case.)
                assistant_turn = build_assistant_turn(ctx)

                async def _run_all_claimed() -> list[dict]:
                    out: list[dict] = []
                    for plugin, plugin_config, slot, claimed in plugin_claims:
                        results = await run_intercept_plugin(
                            plugin, plugin_config, ctx, slot, claimed,
                        )
                        out.extend(results)
                    return out

                tool_task = asyncio.ensure_future(_run_all_claimed())
                while not tool_task.done():
                    try:
                        await asyncio.wait_for(
                            asyncio.shield(tool_task), timeout=_KEEPALIVE_INTERVAL_S
                        )
                    except asyncio.TimeoutError:
                        yield _keepalive_chunk(model)  # heartbeat; loop again
                tool_result_messages: list[dict] = tool_task.result()

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
        except asyncio.CancelledError:
            # The client disconnected (or the request scope was torn down). This is
            # NOT an error and NOT ours to swallow — re-raise so the task unwinds
            # cleanly. We do NOT fabricate a closing turn (pipe, not author): the
            # finally still fires and persists whatever REALLY completed. Logged at
            # INFO so it's visible but not alarming (distinguishes a real disconnect
            # from the DEBUG-level mcp-session teardown cancels).
            logger.info(
                f"stream_intercept: identity='{ctx.identity.key}' stream cancelled "
                f"mid-flight (client disconnect / scope teardown) at lap {lap} — "
                f"re-raising; finally will persist what completed."
            )
            raise
        except Exception as e:
            # A tool/plugin raised, or an unexpected fault. Intercept plugins are
            # CONTRACTED to return synthetic error tool_results, not raise (D-008) —
            # but if one breaks that contract we must not leave the client with a
            # truncated stream and no [DONE]. Emit a REAL error frame (this failure
            # actually happened — transparent, not fabricated) and close cleanly.
            logger.error(
                f"stream_intercept: identity='{ctx.identity.key}' unexpected fault "
                f"at lap {lap}: {type(e).__name__}: {e!r}",
                exc_info=True,
            )
            yield _content_chunk(f"[bridge error: {type(e).__name__}]", model)
            yield _DONE_LINE
        finally:
            await client.aclose()
            # Persist the turn — runs even on client disconnect (the finally),
            # so a cancelled long loop still saves what executed. ctx.response
            # holds the last assembled lap; the intercept.loop_ran breadcrumb
            # (set per-lap) tells basic_session to store the full exchange.
            #
            # SHIELDED: on the cancellation path this finally runs INSIDE a
            # cancelling scope, so a bare `await on_complete()` can itself be
            # re-cancelled before the save completes — silently losing the turn the
            # comment above promises to keep. asyncio.shield lets the save run to
            # completion even as the surrounding task unwinds. (shield still
            # propagates the outer CancelledError afterward, so unwind is unaffected.)
            if on_complete is not None:
                # Create the save as a REAL task first and keep a hard reference —
                # a detached task can be GC'd mid-flight, so we register it and only
                # drop the ref in its done-callback. This is what makes the save
                # actually LAND under cancellation, not just look shielded.
                save_task = asyncio.ensure_future(_run_on_complete(on_complete, ctx))
                _SAVE_TASKS.add(save_task)
                save_task.add_done_callback(_on_save_done)
                try:
                    await asyncio.shield(save_task)
                except asyncio.CancelledError:
                    # Our await was cancelled (outer scope tearing down), but the
                    # shielded save_task keeps running on the still-live loop until
                    # done (the done-callback logs its outcome + drops the ref).
                    # Re-raise for clean unwind; the save is NOT lost.
                    logger.info(
                        f"stream_intercept: identity='{ctx.identity.key}' save "
                        f"shielded through cancellation — completing in background."
                    )
                    raise
                except Exception:
                    logger.error(
                        "stream_intercept on_complete (save) raised", exc_info=True
                    )

    return generator


def _is_done_line(line: bytes) -> bool:
    """True if this SSE line is the terminal ``data: [DONE]`` sentinel. We swallow
    the upstream's per-lap [DONE] on the passthrough path — the generator owns the
    single stitched [DONE] for the whole (possibly multi-lap) turn."""
    s = line.strip()
    return s == b"data: [DONE]" or s == b"[DONE]"


def _line_has_bridge_native_toolcall(line: bytes) -> bool:
    """THE TRIPWIRE. True if this SSE line carries a ``tool_calls`` delta whose
    ``function.name`` is bridge-native (``bridge_native__*``). The tool name arrives
    on the FIRST chunk for a tool_call index (per the stream_reconstruct provider
    contract), so the very first bridge-native tool_call line trips this — and the
    caller stops forwarding BEFORE yielding it, so no ``bridge_native__`` token ever
    reaches the client. HARNESS tool_calls (any other name) return False → they pass
    through verbatim (the harness runs its own loop). Tolerant: a malformed line, a
    tool_call with no name yet (args-only continuation), or a non-tool line → False."""
    s = line.strip()
    if not s.startswith(b"data:"):
        return False
    payload = s[len(b"data:"):].strip()
    if not payload or payload == b"[DONE]":
        return False
    try:
        event = json.loads(payload)
    except (ValueError, TypeError):
        return False
    choices = event.get("choices") or []
    if not choices or not isinstance(choices[0], dict):
        return False
    delta = choices[0].get("delta") or {}
    tcs = delta.get("tool_calls")
    if not isinstance(tcs, list):
        return False
    for tc in tcs:
        if not isinstance(tc, dict):
            continue
        name = ((tc.get("function") or {}).get("name")) or ""
        if name and bridge_native.is_namespaced(name):
            return True
    return False


def _lap_forwarded_finish_reason(lap_chunks: list[bytes]) -> bool:
    """True if this lap's upstream stream already carried a non-null
    ``finish_reason`` chunk (which we forwarded VERBATIM). Lets the close path
    avoid synthesising a duplicate terminal chunk. Tolerant parser: scans the
    tee'd lines for any ``data:`` event whose choices[0].finish_reason is set."""
    for line in lap_chunks:
        s = line.strip()
        if not s.startswith(b"data:"):
            continue
        payload = s[len(b"data:"):].strip()
        if not payload or payload == b"[DONE]":
            continue
        try:
            event = json.loads(payload)
        except (ValueError, TypeError):
            continue
        choices = event.get("choices") or []
        if choices and isinstance(choices[0], dict):
            if choices[0].get("finish_reason"):
                return True
    return False


def _keepalive_chunk(model: str | None) -> bytes:
    """An empty-delta heartbeat chunk emitted during a slow bridge-tool pause.
    Carries no content/reasoning and a null finish_reason — every OpenAI client
    accepts it silently (it injects nothing into the render), it just keeps the
    connection from read-timing-out while the tool runs."""
    payload = {
        "object": "chat.completion.chunk",
        "choices": [{"index": 0, "delta": {}, "finish_reason": None}],
    }
    if model:
        payload["model"] = model
    return f"data: {json.dumps(payload)}\n\n".encode("utf-8")


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
            # Drop bridge-owned tool_calls (never-leak). Bridge-owned = matches
            # OWNED_TOOLS OR carries the bridge_native__ prefix at all — the prefix
            # check catches a MALFORMED bridge-native call (bare `bridge_native__server`
            # with no method) that OWNED_TOOLS membership alone would miss. Genuine
            # HARNESS tool_calls (no bridge prefix) are kept and passed through.
            kept = [tc for tc in tcs if not (
                (n := tool_call_name(tc)) and (
                    bridge_native.strip_namespace(n) in owned_all
                    or bridge_native.is_namespaced(n)
                )
            )]
            if kept:
                msg["tool_calls"] = kept
            else:
                msg.pop("tool_calls", None)
                choices[0]["finish_reason"] = "stop"
    async for sse in frame_emit.emit_frame_as_sse(full):
        yield sse
