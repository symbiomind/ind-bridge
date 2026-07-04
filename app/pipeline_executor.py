"""
Pipeline executor for ind-bridge V4.

Per-request walker. Listener plugins call ``execute(identity_key, body, headers)``
from their HTTP route handlers; the executor:

  1. Builds a ``PipelineCtx`` from the inbound request
  2. Calls ``bridge_sign.verify_inbound(ctx)`` (D-005 — core, not plugin)
  3. Walks the assembled pipeline tuples in slot order:
       a. ``*.context.plugins`` slots → ``plugin.modify_context(ctx, config)``
       b. top-level ``*.plugins`` (non-resource) → ``plugin.apply_outbound_params(ctx, config)``
       c. resource step (terminal ``produce_response`` OR ``outbound_params`` + actual HTTP)
       d. ``*.response.plugins`` slots → ``plugin.modify_response(ctx, config)``
  4. Calls ``bridge_sign.assemble_and_sign(ctx)`` (D-005 — core, not plugin)
  5. Returns the response dict

Plugin authors implement capability methods directly on their plugin
module — no decorators, no registration calls. The executor uses
``getattr()`` to find them and ``asyncio.iscoroutinefunction()`` to know
whether to ``await``. This means a plugin can be a pure-sync module and
still work in the async pipeline; async plugins get awaited.

Per **D-001**: dispatch is by capability method, not hook-point string.
Per **D-002**: a resource with ``produce_response`` short-circuits — no
HTTP call. Per **D-005**: bridge_sign wraps the walk, never as tuples.
Per **D-006**: only 8 canonical slots — ``session.context.plugins`` and
``session.response.plugins`` don't exist.

See ``CLAUDE.md`` for the architecture cheat-sheet,
``~/Documents/ind-v4-decisions.md`` for the decisions referenced above.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import time
from typing import Any, AsyncIterator

import httpx
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask

from . import (
    bridge_native, bridge_sign, config, dev_trace, frame_emit,
    pipeline_assembler, plugin_loader, stream_intercept, stream_reconstruct,
)
from .context import (
    IdentityInfo, PipelineCtx, RequestInfo, ResourceInfo, RoleInfo,
)

logger = logging.getLogger(__name__)


# Slot family classification ---------------------------------------------------

_INBOUND_CONTEXT_SLOTS = frozenset({
    "identity.context.plugins",
    "role.context.plugins",
})
_SESSION_SLOTS = frozenset({
    "session.plugins",
})
# session.plugins fires earlier (step 1c) so session plugins can stamp
# authoritative state (e.g. ctx.plugin_data["session_state"]) BEFORE
# context.plugins read it. Per D-009: sessions are upstream state,
# context plugins are state-based contributors. Step 3 walks only the
# non-session outbound_params slots to avoid double-firing.
_NON_SESSION_OUTBOUND_PARAMS_SLOTS = frozenset({
    "identity.plugins",
    "role.plugins",
})
_OUTBOUND_PARAMS_SLOTS = _NON_SESSION_OUTBOUND_PARAMS_SLOTS | _SESSION_SLOTS
# Kept as the union for any external readers (capabilities validator,
# tests) that ask "what slots can outbound_params plugins live in?" — the
# capability contract is unchanged, only the executor's firing order is.
# resource.plugins is handled specially (terminal vs transport)
_RESPONSE_SLOTS = frozenset({
    "identity.response.plugins",
    "role.response.plugins",
})


def _is_post_response_tuple(plugin: Any) -> bool:
    """True iff this plugin declares the `post_response` capability.

    Per D-007: post_response plugins live at `identity.plugins` or
    `role.plugins` — the same slots `outbound_params` uses, so slot-string
    matching alone isn't enough. We have to ask the capability table."""
    caps = plugin_loader.get_capabilities(_short_name(plugin)) or {}
    return "post_response" in caps


def _has_response_modify(pipeline: list) -> bool:
    """True iff any tuple is a response_modify plugin (lives in a response
    slot AND declares the capability). The streaming-intercept path can't be
    used when response_modify is wired — that plugin needs the whole buffered
    frame to rewrite it, which is incompatible with forwarding content live."""
    for slot, plugin, _cfg in pipeline:
        if slot in _RESPONSE_SLOTS:
            caps = plugin_loader.get_capabilities(_short_name(plugin)) or {}
            if "response_modify" in caps:
                return True
    return False


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def execute(
    identity_key: str, raw_body: dict, headers: dict[str, str]
) -> "dict | StreamingResponse":
    """Run one request through ``identity_key``'s assembled pipeline.

    Returns either an OpenAI-shaped response dict (non-streaming, or
    `produce_response` terminal path) or a FastAPI ``StreamingResponse``
    (streaming pass-through to upstream). The listener plugin's route
    handler returns whatever this returns — FastAPI handles both cases.

    On error, raises — listener plugins decide how to surface errors.
    """
    pipeline = pipeline_assembler.get_pipeline(identity_key)
    if pipeline is None:
        raise PipelineNotReady(
            f"No pipeline assembled for identity '{identity_key}'. "
            f"Either the identity is unknown, or assembly hasn't run."
        )

    ctx = _build_ctx(identity_key, raw_body, headers)
    ctx.dev_trace = dev_trace.begin_request(ctx)

    # Trace: cached pipeline summary (cascade resolution + tuple list happen
    # at startup; here we just print what was assembled for this identity).
    if ctx.dev_trace is not None:
        cascade_str = (
            f"identity={identity_key} → role={ctx.role.key or '(none)'} → "
            f"session={ctx.role.session_key or '(none)'} → "
            f"resource={ctx.resource.key or '(none)'}"
        )
        dev_trace.event(
            ctx.dev_trace, "assembly",
            cascade=cascade_str,
            tuples=[(slot, _short_name(plugin)) for slot, plugin, _ in pipeline],
        )

    # Capture the client's stream preference BEFORE any plugin or core step
    # may flip ``ctx.request.stream`` to False. When pre-delivery plugins
    # are wired we force the upstream call non-streaming so plugins see an
    # assembled frame; the client still gets SSE re-emitted at the end if
    # they asked for streaming. (D-008 / Q-K resolved-by-design.)
    client_requested_stream = bool(ctx.request.stream)

    try:
        # 1. Core: verify inbound bridge_context (D-005)
        before_block = _extract_bridge_block(ctx.request.messages)
        ctx = bridge_sign.verify_inbound(ctx)
        ctx.slots_visited.append("[core] bridge_sign.verify_inbound")
        if ctx.dev_trace is not None:
            after_block = _extract_bridge_block(ctx.request.messages)
            if before_block is None:
                result = "absent (no <bridge_context> block in any message)"
                detail = None
            elif not os.getenv("BRIDGE_SIGN_SECRET"):
                result = "skipped (BRIDGE_SIGN_SECRET unset — dev mode)"
                detail = before_block
            elif after_block and 'trust="untrusted"' in after_block and 'Unknown' in after_block:
                result = "stripped (verification failed; replaced with bare untrusted marker)"
                detail = (
                    f"original block (now discarded):\n{before_block}\n\n"
                    f"replacement:\n{after_block}"
                )
            else:
                result = "verified (signature OK)"
                detail = after_block
            dev_trace.event(
                ctx.dev_trace, "verify_inbound",
                result=result, detail=detail,
            )

        # 1b. Core: populate <caller> from cascade-merged identity context.
        #     Inserts ctx.bridge_context["_raw_caller"] iff a name is set.
        #     Runs before context.plugins so other contributions stack
        #     after the caller in the assembled block (insertion order).
        ctx = bridge_sign.populate_caller(ctx)
        ctx.slots_visited.append("[core] bridge_sign.populate_caller")
        if ctx.dev_trace is not None:
            caller_xml = ctx.bridge_context.get("_raw_caller")
            dev_trace.event(
                ctx.dev_trace, "populate_caller",
                result=("emitted" if caller_xml else "skipped (no name)"),
                detail=caller_xml,
            )

        # 1c. session.plugins slot — sessions establish authoritative state
        #     (e.g. ctx.plugin_data["session_state"], history loaded from
        #     a store) BEFORE context.plugins fire. Per D-009: sessions
        #     are upstream state; context plugins are contributors based
        #     on that state. Order matters for any context.plugin that
        #     consults session_state (e.g. conversational_memory's L2/L3
        #     wakeup cascades, when those land).
        index = 0
        for slot, plugin, plugin_config in pipeline:
            if slot not in _SESSION_SLOTS:
                continue
            index += 1
            ctx = await _dispatch(
                plugin, "apply_outbound_params", ctx, plugin_config, slot,
                tuple_index=index,
            )

        # 1d. Core: stamp ctx.plugin_data["session_state"] via message-shape
        #     inference IFF no session plugin filled it. Session plugins are
        #     authoritative when wired (D-009); core inference is the fallback
        #     so downstream context plugins always see *some* signal.
        if "session_state" not in ctx.plugin_data:
            from app import session_freshness
            ctx.plugin_data["session_state"] = session_freshness.infer_from_messages(
                ctx.request.original_messages
            )
            logger.debug(
                f"session_state inferred (no session plugin stamped): "
                f"{ctx.plugin_data['session_state']}"
            )

        # 2. context.plugins slots — inbound body modification (plugins populate
        #    ctx.bridge_context here; may read ctx.plugin_data["session_state"])
        for slot, plugin, plugin_config in pipeline:
            if slot not in _INBOUND_CONTEXT_SLOTS:
                continue
            index += 1
            ctx = await _dispatch(
                plugin, "modify_context", ctx, plugin_config, slot, tuple_index=index,
            )

        # 3. top-level *.plugins (non-resource, non-session) — outbound_params
        #    contributions. session.plugins already fired in step 1c.
        for slot, plugin, plugin_config in pipeline:
            if slot not in _NON_SESSION_OUTBOUND_PARAMS_SLOTS:
                continue
            index += 1
            ctx = await _dispatch(
                plugin, "apply_outbound_params", ctx, plugin_config, slot,
                tuple_index=index,
            )

        # 4. Core: assemble and sign outbound bridge_context BEFORE the resource
        #    is hit. This injects the signed XML block into ctx.request.messages
        #    so the upstream LLM sees it. Must happen before _execute_resource_step.
        ctx = bridge_sign.assemble_and_sign(ctx)
        ctx.slots_visited.append("[core] bridge_sign.assemble_and_sign")
        _trace_assemble_and_sign(ctx)

        # 5. Pre-delivery branch decision (D-008).
        #
        # If any plugin in the assembled pipeline declares response_modify or
        # handle_tool_calls, those plugins must see an *assembled frame* —
        # tool_calls and finish_reason are frame-level properties, you can't
        # decide them mid-stream. So we force the upstream call non-streaming
        # for the duration of the resource step. The client's stream
        # preference was captured at request entry (`client_requested_stream`)
        # and honoured at the end of the pipeline via SSE re-emit if needed.
        #
        # This is the buffer-when-intercepting rule, applied uniformly to
        # both `response_modify` (modify) and `handle_tool_calls` (intercept)
        # — the two pre-delivery categories of the four-category executor
        # model. The third (post_response observe) and fourth (passthrough)
        # categories don't trigger buffering.
        pre_delivery_wired = pipeline_assembler.has_pre_delivery_plugins(pipeline)

        # ── THE THIRD PATH: stream-while-intercepting ("the pause IS the tool")
        # When the client wants streaming AND the only pre-delivery work is
        # handle_tool_calls intercepts (NOT response_modify, which needs the
        # whole buffered frame), we can stream the bridge-owned tool loop live:
        # forward the agent's content deltas as a natural keepalive, swallow
        # bridge-native tool_calls, run them between laps (the client sees a
        # pause), and stitch to one [DONE]. Kills the long-loop client timeout
        # (→ the cancellation turn-loss gap) and makes invisible bridge tools
        # legible. Opt-in via BRIDGE_STREAM_INTERCEPTS=1 during dogfooding.
        if (
            ctx.request.stream
            and pipeline_assembler.has_intercepts(pipeline)
            and not _has_response_modify(pipeline)
            and os.getenv("BRIDGE_STREAM_INTERCEPTS") == "1"
        ):
            logger.info(
                f"Identity '{identity_key}': streaming-intercept path "
                f"(content forwarded live as keepalive; bridge tool loop "
                f"runs in the pauses; stitched to one stream)."
            )
            ctx.slots_visited.append("[core] streaming-intercept path")

            # After the stream closes (ctx.response = final lap), fire the
            # post_response observers (basic_session save, conversational_memory)
            # so the streaming path persists the full exchange like the others,
            # THEN flush the dev_trace. This path returns a StreamingResponse
            # immediately (below) and never reaches the end_request calls on the
            # other branches, so without this flush the streaming-intercept path
            # produces NO trace file — the trace is built in memory and lost when
            # the request ends. on_complete fires in the generator's finally
            # (even on client disconnect), so the trace is flushed AFTER the
            # observers' events are recorded — mirroring the pass-through path,
            # which closes its trace inside _dispatch_observers_after_stream.
            async def _save_after_stream() -> None:
                for slot, plugin, plugin_config in _collect_observer_tuples(pipeline):
                    await _run_observer(plugin, plugin_config, ctx, slot)
                dev_trace.end_request(
                    ctx.dev_trace, status="ok-stream-intercept",
                    response_summary="streaming-intercept path; observers fired after stream close",
                )

            gen = await stream_intercept.stream_with_intercepts(
                ctx, pipeline,
                build_request=_build_outbound_request,
                intercept_tuples=_collect_intercept_tuples(pipeline),
                plugin_owned_tools=_plugin_owned_tools,
                tool_call_name=_tool_call_name,
                run_intercept_plugin=_run_intercept_plugin,
                build_assistant_turn=_build_assistant_turn_from_response,
                max_tool_laps=ctx.max_tool_laps,
                on_complete=_save_after_stream,
            )
            return StreamingResponse(gen(), media_type="text/event-stream")

        if pre_delivery_wired and ctx.request.stream:
            logger.info(
                f"Identity '{identity_key}': pre-delivery plugins wired; "
                f"forcing upstream non-streaming. Client-side stream "
                f"preference ({client_requested_stream}) preserved via "
                f"SSE re-emit after pre-delivery plugins finish."
            )
            ctx.request.stream = False
            ctx.slots_visited.append(
                "[core] pre-delivery wired — upstream forced non-stream"
            )

        # 6. resource step — terminal (produce_response), streaming HTTP, or
        #    non-streaming HTTP. Returns either ctx (non-stream/terminal) OR a
        #    StreamingResponse (stream pass-through).
        resource_result = await _execute_resource_step(ctx, pipeline)
        if isinstance(resource_result, StreamingResponse):
            # Pass-through streaming path — only reachable when no
            # pre-delivery plugins are wired (we forced stream=False above
            # otherwise). post_response observers DO run via the
            # BackgroundTask wired inside _do_http_call_stream.
            observer_count = len(_collect_observer_tuples(pipeline))
            ctx.slots_visited.append(
                f"[core] streaming pass-through "
                f"({observer_count} post_response observer(s) "
                f"scheduled as BackgroundTask)"
            )
            if observer_count == 0:
                # No observers wired — close the trace now; the stream is
                # the whole story for this request.
                dev_trace.end_request(
                    ctx.dev_trace, status="stream-passthrough",
                    response_summary="passthrough stream; no observers wired",
                )
            # Else: trace is closed inside _dispatch_observers_after_stream
            # so the post_response event lands in the trace file.
            return resource_result
        ctx = resource_result  # ctx was returned (non-stream/terminal)

        # 7. handle_tool_calls intercept dispatch (D-008). Stub for now —
        #    fills in once the capability machinery lands. Reached only on
        #    the buffered (non-streaming-upstream) path because intercepts
        #    require an assembled frame.
        ctx = await _dispatch_intercepts(ctx, pipeline)

        # 8. response.plugins slots — outbound body modification. Reached
        #    on either the buffered-pre-delivery path or the no-pre-delivery
        #    non-streaming path. (Streaming pass-through returned above.)
        for slot, plugin, plugin_config in pipeline:
            if slot not in _RESPONSE_SLOTS:
                continue
            index += 1
            ctx = await _dispatch(
                plugin, "modify_response", ctx, plugin_config, slot,
                tuple_index=index,
            )

        # 9. post_response observers (D-007) — fire-and-forget background
        #    dispatch on the running event loop. Observers see the assembled
        #    assistant turn but don't delay the response. Exceptions logged
        #    and swallowed inside _run_observer.
        _dispatch_observers_nonstream(ctx, pipeline)

        # Final response: ctx.response was populated by produce_response or by
        # _do_http_call_nonstream. If still None, something went wrong.
        if ctx.response is None:
            logger.error(
                f"Pipeline for identity '{identity_key}' completed but "
                f"ctx.response is None. slots_visited: {ctx.slots_visited}"
            )
            dev_trace.end_request(
                ctx.dev_trace, status="error",
                response_summary="ctx.response is None — no resource produced output",
            )
            raise PipelineNotReady(
                f"Pipeline completed without producing a response. "
                f"Likely no resource was wired (or the resource plugin failed)."
            )

        result = _ctx_response_to_openai(ctx)

        # 10. SSE re-emit when client wanted streaming AND we forced
        #     non-streaming upstream (pre-delivery plugins were wired). The
        #     client never sees that the upstream was non-stream — we hand
        #     them an SSE stream of the assembled (and possibly modified
        #     and/or intercepted) frame. Honours the V3 invisibility
        #     property: pre-delivery is operationally invisible to the
        #     client.
        if pre_delivery_wired and client_requested_stream:
            ctx.slots_visited.append(
                "[core] SSE re-emit — buffered upstream → streamed client"
            )
            dev_trace.end_request(
                ctx.dev_trace, status="ok-restreamed",
                response_summary=(
                    f"buffered upstream re-emitted as SSE; "
                    f"final_bytes={len(_safe_serialise(result))}"
                ),
            )
            return StreamingResponse(
                frame_emit.emit_frame_as_sse(result),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",  # nginx — disable buffering for SSE
                },
            )

        result_bytes = len(_safe_serialise(result))
        dev_trace.end_request(
            ctx.dev_trace, status="ok",
            response_summary=f"response_bytes={result_bytes}",
        )
        return result
    except Exception as e:
        dev_trace.emit_exception(ctx.dev_trace, where="execute", exc=e)
        dev_trace.end_request(ctx.dev_trace, status="error")
        raise


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class PipelineNotReady(RuntimeError):
    """Raised when execute() cannot proceed — bad identity, no pipeline,
    no resource configured, etc. Listener plugins should map this to 503."""


class PipelineFailure(RuntimeError):
    """Raised when a plugin during execution raises. Wraps the underlying
    exception with identity/plugin/slot context for diagnostics."""


# ---------------------------------------------------------------------------
# Build initial PipelineCtx from inbound request
# ---------------------------------------------------------------------------

def _build_ctx(identity_key: str, raw_body: dict, headers: dict[str, str]) -> PipelineCtx:
    """Construct a fresh PipelineCtx from the inbound request data.

    Resolves identity/role/resource keys from config; populates the
    context-sugar fields (name, trust, additional) cascade-merged from
    `identities.<id>.context` over `roles.<role>.context` — identity wins
    per field, role provides defaults.
    """
    identity_cfg = config.resolve_identity(identity_key) or {}
    role_key = config.get_identity_role_key(identity_key) or ""
    role_cfg = config.resolve_role(role_key) or {}
    resource_key = role_cfg.get("resource") or ""
    session_key = role_cfg.get("session")
    resource_cfg = config.resolve_resource(resource_key) or {}
    session_cfg = (config.resolve_session(session_key) or {}) if session_key else {}
    server_cfg = config.get_server_cfg() or {}

    # context: sugar fields — cascade-merged, identity overrides role.
    # Role provides defaults (e.g. roles.my_agent.context.name: "Guest");
    # identity overrides per-field. `additional` is a per-key overlay so role
    # keys survive when identity adds its own.
    identity_ctx = identity_cfg.get("context") or {}
    role_ctx = role_cfg.get("context") or {}
    if not isinstance(identity_ctx, dict):
        identity_ctx = {}
    if not isinstance(role_ctx, dict):
        role_ctx = {}

    name = identity_ctx.get("name") or role_ctx.get("name")
    trust = identity_ctx.get("trust") or role_ctx.get("trust")

    role_additional = role_ctx.get("additional") or {}
    identity_additional = identity_ctx.get("additional") or {}
    if not isinstance(role_additional, dict):
        role_additional = {}
    if not isinstance(identity_additional, dict):
        identity_additional = {}
    additional = {**role_additional, **identity_additional}  # identity wins per key

    # Internally-originated callers (e.g. the cron plugin firing a scheduled turn)
    # may stamp per-turn `additional` via a reserved `_cron_additional` body key.
    # It layers OVER role+identity additional (job wins on a key collision) and is
    # popped here so it never reaches the upstream model provider. Signed into
    # <bridge_context> downstream by populate_caller like any other additional.
    cron_additional = raw_body.pop("_cron_additional", None) if isinstance(raw_body, dict) else None

    # Internally-originated callers may ALSO override the caller's name + trust via
    # reserved `_bridge_caller` / `_bridge_trust` body keys. This is how an in-process
    # sender (bridge_messaging) delivers a turn AS a synthetic caller through a carrier
    # identity's pipeline: it drives execute() on the target role's carrier identity,
    # but overrides the WHO so the signed <caller> reflects the real sender, not the
    # carrier. SOVEREIGN by construction — these keys originate inside the bridge (an
    # external HTTP body that set them never reaches here as a "trusted" caller because
    # the listener builds raw_body from client JSON; bridge_messaging injects them
    # in-process). Popped before the upstream call so they never leak to the provider;
    # signed into <bridge_context> downstream by populate_caller.
    bridge_no_tools = False
    bridge_caller = None
    bridge_storage = None
    if isinstance(raw_body, dict):
        bridge_caller = raw_body.pop("_bridge_caller", None)
        bridge_trust = raw_body.pop("_bridge_trust", None)
        bridge_no_tools = bool(raw_body.pop("_bridge_no_tools", False))
        # Signed turn-handling marker (bridge_messaging sets "false" for an
        # ephemeral turn). A property of the TURN, so it becomes an attribute on
        # the <bridge_context> envelope (not the <caller>), folded into the
        # signature by assemble_and_sign. Popped so it never reaches the provider.
        bridge_storage = raw_body.pop("_bridge_storage", None)
        if isinstance(bridge_caller, str) and bridge_caller:
            name = bridge_caller          # synthetic caller overrides carrier name
        if isinstance(bridge_trust, str) and bridge_trust:
            trust = bridge_trust          # e.g. "bridge_messaging"

    # SOVEREIGN ADDITIONAL (whitelist, not merge). When a synthetic caller is set
    # (_bridge_caller present), the WHO is wholly the bridge's, not the carrier's —
    # the name and trust are *replaced* above, and the caller ATTRIBUTES must follow
    # the same rule. The carrier identity's own `additional` (its curl/harness
    # decoration, e.g. information="Claude Code dropping in to say hi") must NOT ride
    # along onto an agent-to-agent <caller>. So we DROP the carrier base entirely and
    # rebuild from only the bridge-intended kv in `_cron_additional` (which already
    # carries the bridge `tool`/`storage` signals + any explicit `additional` arg the
    # sender passed). Normal turns (no _bridge_caller) keep the historical MERGE so
    # cron/identity decoration still layers as before.
    if isinstance(bridge_caller, str) and bridge_caller:
        additional = dict(cron_additional) if isinstance(cron_additional, dict) else {}
    elif isinstance(cron_additional, dict):
        additional = {**additional, **cron_additional}  # job wins per key

    identity = IdentityInfo(
        key=identity_key,
        name=name,
        trust=trust,
        additional=additional,
    )
    role = RoleInfo(
        key=role_key,
        resource_key=resource_key,
        session_key=session_key,
    )
    resource = ResourceInfo(key=resource_key)

    # Build RequestInfo from the raw body
    messages = raw_body.get("messages") or []
    request = RequestInfo(
        original_messages=list(messages),
        messages=list(messages),
        model=raw_body.get("model", ""),
        stream=bool(raw_body.get("stream", False)),
        tools=list(raw_body.get("tools") or []),
        raw_body=raw_body,
    )

    # TEMP DEBUG (passthrough investigation 2026-06-24): log the RAW inbound client
    # tool names — what LibreChat/OpenClaw actually forwards in raw_body["tools"],
    # BEFORE any plugin (mcp_client) injects bridge-native tools. This is the ground
    # truth for "do non-bridge-native client tools pass through?". Remove after the
    # personal-mcp passthrough test.
    _inbound_tool_names = [
        (t.get("function") or {}).get("name")
        for t in request.tools
        if isinstance(t, dict)
    ]
    logger.info(
        f"[INBOUND TOOLS] identity={identity_key!r} count={len(request.tools)} "
        f"names={_inbound_tool_names}"
    )

    # TEMP DEBUG (fold/leak investigation 2026-06-24): log the inbound MESSAGE
    # roles the client (LibreChat) replayed to us — specifically whether it sent
    # back any assistant(tool_calls) or role:tool turns. If it does, LibreChat is
    # keeping its OWN tool history and the in_tool_loop verbatim path fires
    # (fold-bypassed). Remove after the investigation.
    _inbound_msgs = request.messages or []
    _role_counts: dict = {}
    _asst_with_tc = 0
    for _m in _inbound_msgs:
        if not isinstance(_m, dict):
            continue
        _role_counts[_m.get("role")] = _role_counts.get(_m.get("role"), 0) + 1
        if _m.get("role") == "assistant" and _m.get("tool_calls"):
            _asst_with_tc += 1
    logger.info(
        f"[INBOUND MSGS] identity={identity_key!r} roles={_role_counts} "
        f"assistant_with_tool_calls={_asst_with_tc} "
        f"role_tool={_role_counts.get('tool', 0)}"
    )

    max_tool_laps = _resolve_max_tool_laps(
        identity_cfg, role_cfg, session_cfg, resource_cfg, server_cfg,
    )

    ctx = PipelineCtx(
        identity=identity,
        role=role,
        resource=resource,
        request=request,
        headers={k.lower(): v for k, v in (headers or {}).items()},
        max_tool_laps=max_tool_laps,
    )
    # Delivery turns (bridge_messaging) carry _bridge_no_tools: the recipient REPLIES
    # in words, with no tools, regardless of what context plugins inject. Stashed here;
    # enforced at the resource-call gate (_do_http_call_*). Ordering-proof.
    if bridge_no_tools:
        ctx.plugin_data["bridge_messaging.no_tools"] = True
    # Stash the signed storage marker onto the bridge_context so assemble_and_sign
    # renders + signs it as a <bridge_context> opening-tag attribute. Only when a
    # synthetic bridge caller is present (an in-process bridge_messaging delivery)
    # — same sovereignty as _bridge_caller/_bridge_trust: originates inside the
    # bridge, never from an external body claiming to be trusted.
    if isinstance(bridge_caller, str) and bridge_caller and isinstance(bridge_storage, str) and bridge_storage:
        ctx.bridge_context["_attr_storage"] = bridge_storage
    return ctx


# ---------------------------------------------------------------------------
# Resource step — terminal vs transport
# ---------------------------------------------------------------------------

async def _execute_resource_step(
    ctx: PipelineCtx, pipeline: list
) -> "PipelineCtx | StreamingResponse":
    """Walk the resource.plugins tuples. Three possible outcomes:

      - If any plugin declares ``produce_response``: call it and return ctx
        (terminal — no HTTP).
      - If ``ctx.request.stream`` is True: run outbound_params plugins to
        configure, then call ``_do_http_call_stream`` which returns a
        FastAPI ``StreamingResponse`` for pass-through SSE.
      - Otherwise: run outbound_params plugins, call ``_do_http_call_nonstream``
        which populates ``ctx.response`` and returns ctx.

    Before ANY of the above, run `outbound_normalize` plugins (D-012) on
    `ctx.request.messages`. This is the single chokepoint all resource calls
    funnel through — the inbound call AND every handle_tool_calls intercept
    re-call — so frame-normalization (quirks_mode / provider-compat shimming)
    covers bridge-owned tool loops, not just the inbound pass. These plugins MUST
    be idempotent (this runs on every lap).
    """
    for slot, plugin, plugin_config in _collect_outbound_normalize_tuples(pipeline):
        ctx = await _dispatch(
            plugin, "normalize_outbound", ctx, plugin_config,
            f"{slot} (outbound_normalize)",
        )

    resource_tuples = [t for t in pipeline if t[0] == "resource.plugins"]

    # Find a produce_response plugin if any
    terminal_plugin = None
    terminal_config: dict = {}
    transport_plugins: list[tuple[Any, dict]] = []

    for _slot, plugin, plugin_config in resource_tuples:
        plugin_short = _short_name(plugin)
        caps = plugin_loader.get_capabilities(plugin_short) or {}
        if "produce_response" in caps:
            terminal_plugin = plugin
            terminal_config = plugin_config
            ctx.resource.is_terminal = True
            break  # First produce_response wins; conflicts already warned by validator
        if "outbound_params" in caps:
            transport_plugins.append((plugin, plugin_config))

    # Resource-step tuples don't share the request-wide index counter —
    # their slot labels ("resource.plugins (terminal)" / "(transport)")
    # already disambiguate them in the trace.
    if terminal_plugin is not None:
        # Terminal — call produce_response, no HTTP
        dev_trace.event(
            ctx.dev_trace, "resource_step",
            decision="terminal (produce_response)",
            outbound_plugins=[_short_name(terminal_plugin)],
        )
        ctx = await _dispatch(
            terminal_plugin, "produce_response", ctx, terminal_config,
            "resource.plugins (terminal)",
        )
        return ctx

    # Transport path — run outbound_params plugins to configure the call
    for plugin, plugin_config in transport_plugins:
        ctx = await _dispatch(
            plugin, "apply_outbound_params", ctx, plugin_config,
            "resource.plugins (transport)",
        )

    # If no plugin populated the endpoint, we can't make a call — 503-shaped error
    if not ctx.resource.endpoint_url:
        raise PipelineNotReady(
            f"Resource '{ctx.resource.key}' has no endpoint_url after "
            f"outbound_params dispatch. Wire a transport plugin "
            f"(e.g. OpenAI-Protocol) on the resource."
        )

    decision = "stream" if ctx.request.stream else "nonstream"
    dev_trace.event(
        ctx.dev_trace, "resource_step",
        decision=decision,
        endpoint=ctx.resource.endpoint_url,
        model=ctx.request.model or "(unset)",
        outbound_plugins=[_short_name(p) for p, _ in transport_plugins],
    )

    # Stream or non-stream based on what the client requested
    if ctx.request.stream:
        return await _do_http_call_stream(ctx, pipeline)
    ctx = await _do_http_call_nonstream(ctx)
    return ctx


def _build_outbound_request(ctx: PipelineCtx) -> tuple[str, dict, dict, float]:
    """Construct the (url, body, headers, timeout) tuple for the outbound
    HTTP call. Shared between streaming and non-streaming paths so the
    request shape is identical regardless of stream mode."""
    url = ctx.resource.endpoint_url
    if not url.endswith("/chat/completions"):
        # Allow plugins to set either the base URL (.../v1) or the full
        # endpoint (.../v1/chat/completions). Be forgiving.
        url = url.rstrip("/") + "/chat/completions"

    body = dict(ctx.request.raw_body)
    # Use possibly-mutated messages from the working copy (post-context-modify
    # and post-bridge_sign-injection)
    body["messages"] = ctx.request.messages
    # bridge_messaging delivery turn → NO tools upstream, whatever context plugins
    # injected. The recipient replies in words. Ordering-proof: this is the final gate.
    if ctx.plugin_data.get("bridge_messaging.no_tools"):
        body.pop("tools", None)
        body.pop("tool_choice", None)
    elif ctx.request.tools:
        body["tools"] = ctx.request.tools
    if ctx.request.model:
        body["model"] = ctx.request.model
    body["stream"] = ctx.request.stream  # honour what the client asked for

    headers: dict[str, str] = {"Content-Type": "application/json"}
    if ctx.resource.endpoint_token:
        headers["Authorization"] = f"Bearer {ctx.resource.endpoint_token}"
    extra_headers = ctx.plugin_data.get("openai-protocol.headers", {})
    if isinstance(extra_headers, dict):
        headers.update(extra_headers)

    timeout = float(os.getenv("OUTBOUND_TIMEOUT_SECONDS", "300"))
    return url, body, headers, timeout


async def _do_http_call_nonstream(ctx: PipelineCtx) -> PipelineCtx:
    """Non-streaming outbound: POST, parse JSON, populate ctx.response.

    Used when ``ctx.request.stream`` is False, OR when the resource is
    terminal (produce_response — but in that case this function isn't
    called at all; the resource step short-circuits earlier).
    """
    url, body, headers, timeout = _build_outbound_request(ctx)

    logger.info(
        f"Outbound call (non-stream): identity='{ctx.identity.key}' "
        f"resource='{ctx.resource.key}' url='{url}' "
        f"model='{body.get('model')}'"
    )

    dev_trace.event(
        ctx.dev_trace, "upstream_request",
        method="POST", url=url, headers=headers, body=body,
    )
    started = time.monotonic()

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            response = await client.post(url, json=body, headers=headers)
        except httpx.HTTPError as e:
            duration_ms = int((time.monotonic() - started) * 1000)
            dev_trace.event(
                ctx.dev_trace, "upstream_response",
                status=f"network-error: {type(e).__name__}",
                duration_ms=duration_ms,
            )
            raise PipelineFailure(
                f"HTTP error calling {url}: {type(e).__name__}: {e!r}"
            ) from e

    duration_ms = int((time.monotonic() - started) * 1000)

    if response.status_code >= 400:
        dev_trace.event(
            ctx.dev_trace, "upstream_response",
            status=f"{response.status_code} {response.reason_phrase}",
            duration_ms=duration_ms,
            stream_summary=f"error body (truncated): {response.text[:500]!r}",
        )
        raise PipelineFailure(
            f"Resource '{ctx.resource.key}' returned HTTP {response.status_code}: "
            f"{response.text[:500]}"
        )

    try:
        response_body = response.json()
    except ValueError as e:
        dev_trace.event(
            ctx.dev_trace, "upstream_response",
            status=f"{response.status_code} (non-JSON body)",
            duration_ms=duration_ms,
            stream_summary=f"raw body (truncated): {response.text[:500]!r}",
        )
        raise PipelineFailure(
            f"Resource '{ctx.resource.key}' returned non-JSON: "
            f"{response.text[:500]!r}"
        ) from e

    dev_trace.event(
        ctx.dev_trace, "upstream_response",
        status=f"{response.status_code} {response.reason_phrase}",
        duration_ms=duration_ms,
        body=response_body,
    )

    # Extract the assistant turn from the OpenAI-shaped response
    choices = response_body.get("choices") or []
    if not choices:
        ctx.response = None
    else:
        first = choices[0]
        message = first.get("message") or {}
        ctx.response = {
            "role": "assistant",
            "content": message.get("content", ""),
            # Preserve full envelope for plugins/listener that want it
            "_full_response": response_body,
        }

    return ctx


async def _do_http_call_stream(
    ctx: PipelineCtx, pipeline: list | None = None
) -> StreamingResponse:
    """Streaming outbound: forward upstream's SSE chunks through to the
    client as they arrive. True pass-through — bytes flow client-ward
    immediately, no buffering. Upstream errors surface as the SSE stream
    closes (with the upstream HTTP status code if non-200).

    Response.* slots are NOT applied on this path (the executor logs a
    warning at its caller if any are wired). The OpenAI-shaped response
    structure is preserved verbatim from upstream — anything in the
    chunks (choices, finish_reason, usage, custom fields) passes through
    unmodified, honouring the "passthrough by default" principle.

    **post_response handling (D-007).** If `pipeline` contains any tuple
    whose plugin declares the `post_response` capability, we tee chunks
    into a list as they pass through. After the stream closes (and the
    client has received `[DONE]`), a starlette BackgroundTask reconstructs
    the assistant turn from the tee'd chunks and dispatches each
    observer. Observers run *after* the body is fully delivered — never
    delaying the user.
    """
    url, body, headers, timeout = _build_outbound_request(ctx)

    has_observers = bool(
        pipeline is not None and _collect_observer_tuples(pipeline)
    )

    logger.info(
        f"Outbound call (stream): identity='{ctx.identity.key}' "
        f"resource='{ctx.resource.key}' url='{url}' "
        f"model='{body.get('model')}' "
        f"observers={'yes' if has_observers else 'no'}"
    )

    dev_trace.event(
        ctx.dev_trace, "upstream_request",
        method="POST", url=url, headers=headers, body=body,
    )
    dev_trace.event(
        ctx.dev_trace, "upstream_response",
        status="(streaming — see container logs for upstream HTTP status)",
        duration_ms=0,
        stream_summary=(
            "bytes pass through to client"
            + ("; chunks tee'd for post_response observers" if has_observers else "")
        ),
    )

    # Shared bucket for the tee. The generator appends; the BackgroundTask
    # reads after the generator is exhausted. Same async loop, no locking
    # required (asyncio is cooperative, the generator and the background
    # task don't overlap).
    teed_chunks: list[bytes] = []

    async def stream_generator() -> AsyncIterator[bytes]:
        client = httpx.AsyncClient(timeout=timeout)
        try:
            async with client.stream(
                "POST", url, json=body, headers=headers
            ) as upstream:
                if upstream.status_code >= 400:
                    # Buffer the error body so the client gets a useful message
                    error_body = await upstream.aread()
                    snippet = error_body[:500].decode("utf-8", errors="replace")
                    logger.warning(
                        f"Upstream stream error: identity='{ctx.identity.key}' "
                        f"status={upstream.status_code} body={snippet!r}"
                    )
                    # Emit the upstream error as a single SSE event so the client
                    # sees something instead of an empty stream. Format follows
                    # OpenAI conventions for error events.
                    #
                    # Build the payload with json.dumps, NOT a hand-rolled
                    # f-string: upstream error bodies routinely contain nested,
                    # already-escaped JSON (e.g. Moonshot's `"raw":"{\"error\"…`).
                    # The old `.replace('"',"'")` left bare backslashes (`\'`),
                    # producing invalid JSON the client choked on ("Bad escaped
                    # character at position N"). json.dumps escapes backslashes,
                    # quotes, and control chars correctly for ANY upstream garbage.
                    err_payload = {
                        "error": {
                            "message": f"upstream {upstream.status_code}: {snippet[:200]}",
                            "type": "upstream_error",
                        }
                    }
                    err_chunk = f"data: {json.dumps(err_payload)}\n\n".encode("utf-8")
                    yield err_chunk
                    yield b"data: [DONE]\n\n"
                    return

                # The client gets verbatim upstream bytes (passthrough
                # invariant). The TEE, however, must hold COMPLETE SSE lines:
                # aiter_bytes splits events mid-JSON, and reconstructing those
                # per-slice corrupts the STORED turn (per-word newlines +
                # duplicated reasoning — proven via raw-OpenRouter curl,
                # 2026-06-23). So we forward raw bytes but buffer them into whole
                # lines for the tee.
                sse_buf = b""
                async for chunk in upstream.aiter_bytes():
                    yield chunk  # verbatim to client — never reframe the wire
                    if has_observers:
                        sse_buf += chunk
                        while b"\n" in sse_buf:
                            line, sse_buf = sse_buf.split(b"\n", 1)
                            if line:
                                teed_chunks.append(line + b"\n")
                if has_observers and sse_buf:
                    teed_chunks.append(sse_buf)  # flush any trailing partial line
        except httpx.HTTPError as e:
            logger.error(
                f"Streaming HTTP error to {url}: {type(e).__name__}: {e!r}"
            )
            err_payload = {
                "error": {
                    "message": f"network error: {type(e).__name__}",
                    "type": "network_error",
                }
            }
            err_chunk = f"data: {json.dumps(err_payload)}\n\n".encode("utf-8")
            yield err_chunk
            yield b"data: [DONE]\n\n"
        finally:
            await client.aclose()

    background = None
    if has_observers:
        # BackgroundTask runs after the response body has been fully sent
        # to the client. It calls our async function with the captured
        # chunk list — by that point teed_chunks is fully populated.
        async def _observe_after_send():
            await _dispatch_observers_after_stream(ctx, pipeline, teed_chunks)
        background = BackgroundTask(_observe_after_send)

    return StreamingResponse(
        stream_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # nginx — disable buffering for SSE
        },
        background=background,
    )


# ---------------------------------------------------------------------------
# Dispatch helper — invoke a capability method on a plugin
# ---------------------------------------------------------------------------

async def _dispatch(
    plugin: Any,
    method_name: str,
    ctx: PipelineCtx,
    plugin_config: dict,
    slot_label: str,
    *,
    tuple_index: int = 0,
) -> PipelineCtx:
    """Look up ``method_name`` on the plugin module; invoke; await if
    coroutine. Updates ``ctx.slots_visited`` for diagnostics. Wraps any
    plugin exception in PipelineFailure with identity/plugin/slot context.

    If the plugin doesn't export the method (shouldn't happen if the
    validator classified the placement as 'ok', but defend), log and skip.
    """
    plugin_short = _short_name(plugin)
    fn = getattr(plugin, method_name, None)
    if fn is None:
        logger.warning(
            f"Plugin '{plugin_short}' has no '{method_name}' method "
            f"despite a placement at {slot_label} requiring it. Skipping."
        )
        if ctx.dev_trace is not None:
            dev_trace.event(
                ctx.dev_trace, "tuple_start",
                index=tuple_index, slot=slot_label, plugin=plugin_short,
                method=method_name, config=plugin_config,
            )
            dev_trace.event(
                ctx.dev_trace, "tuple_end",
                result="skipped", duration_ms=0,
                message=f"plugin has no '{method_name}' method",
            )
        return ctx

    ctx.slots_visited.append(f"{slot_label}:{plugin_short}.{method_name}")

    if ctx.dev_trace is not None:
        dev_trace.event(
            ctx.dev_trace, "tuple_start",
            index=tuple_index, slot=slot_label, plugin=plugin_short,
            method=method_name, config=plugin_config,
        )
    started = time.monotonic()

    try:
        if inspect.iscoroutinefunction(fn) or asyncio.iscoroutinefunction(fn):
            result = await fn(ctx, plugin_config)
        else:
            result = fn(ctx, plugin_config)
    except Exception as e:
        duration_ms = int((time.monotonic() - started) * 1000)
        if ctx.dev_trace is not None:
            dev_trace.event(
                ctx.dev_trace, "tuple_end",
                result="error", duration_ms=duration_ms,
                message=f"{type(e).__name__}: {e!r}",
            )
        raise PipelineFailure(
            f"Plugin '{plugin_short}'.{method_name} at {slot_label} "
            f"(identity '{ctx.identity.key}') raised: "
            f"{type(e).__name__}: {e!r}"
        ) from e

    duration_ms = int((time.monotonic() - started) * 1000)

    # Plugins should return ctx; treat None as "pass through unchanged"
    if result is None:
        if ctx.dev_trace is not None:
            dev_trace.event(
                ctx.dev_trace, "tuple_end",
                result="ok", duration_ms=duration_ms,
                message="returned None (passthrough)",
            )
        return ctx
    if not isinstance(result, PipelineCtx):
        logger.warning(
            f"Plugin '{plugin_short}'.{method_name} returned "
            f"{type(result).__name__}, not PipelineCtx. Treating as "
            f"pass-through."
        )
        if ctx.dev_trace is not None:
            dev_trace.event(
                ctx.dev_trace, "tuple_end",
                result="ok", duration_ms=duration_ms,
                message=f"returned {type(result).__name__}, not PipelineCtx (passthrough)",
            )
        return ctx

    if result.dev_trace is not None:
        dev_trace.event(
            result.dev_trace, "tuple_end",
            result="ok", duration_ms=duration_ms,
        )
    return result


# ---------------------------------------------------------------------------
# post_response — observer dispatch (D-007)
# ---------------------------------------------------------------------------

def _collect_observer_tuples(pipeline: list) -> list[tuple[str, Any, dict]]:
    """Filter the assembled pipeline down to tuples whose plugin declares
    `post_response`. The slot strings (`identity.plugins`, `role.plugins`)
    overlap with `outbound_params`, so we have to consult the capability
    table per-plugin — a tuple in `role.plugins` may be contributing
    `outbound_params`, `post_response`, both, or neither.

    **Deduped by plugin** (see _collect_intercept_tuples): a multi-capability
    plugin can occupy several slots (capability fan-out), but its
    `post_response` must fire ONCE per request — observing/storing the same
    turn twice would e.g. double-write a memory."""
    return _dedupe_by_plugin(
        (slot, plugin, cfg)
        for slot, plugin, cfg in pipeline
        if _is_post_response_tuple(plugin)
    )


def _collect_outbound_normalize_tuples(pipeline: list) -> list[tuple[str, Any, dict]]:
    """Filter the assembled pipeline down to tuples whose plugin declares
    `outbound_normalize` (D-012). These run inside `_execute_resource_step`
    before EVERY resource call — the inbound call AND every handle_tool_calls
    intercept re-call — so a frame-fix (quirks_mode) covers bridge-owned tool
    loops, not just the inbound pass. A plugin in a context slot may declare
    `outbound_normalize`, `context_modify`, `post_response`, any combination, or
    none — consult the capability table per plugin. Preserves pipeline order.

    **Deduped by plugin** (see _collect_intercept_tuples): normalize must be
    idempotent anyway, but a fan-out plugin occupying several slots should still
    normalize once per call, not once per slot."""
    out = []
    for slot, plugin, plugin_config in pipeline:
        caps = plugin_loader.get_capabilities(_short_name(plugin)) or {}
        if "outbound_normalize" in caps:
            out.append((slot, plugin, plugin_config))
    return _dedupe_by_plugin(out)


def _dedupe_by_plugin(tuples) -> list[tuple[str, Any, dict]]:
    """Keep the first tuple per plugin short-name, preserving order. A
    multi-capability plugin fanned out across slots appears as one tuple per
    slot in the assembled pipeline; capability-dispatch collectors that run
    side effects (intercept / observe / normalize) must act on it once per
    request, not once per slot. Config is cascade-merged identically across the
    plugin's slots, so first-wins is behaviour-neutral."""
    seen: set[str] = set()
    out: list[tuple[str, Any, dict]] = []
    for slot, plugin, plugin_config in tuples:
        name = _short_name(plugin)
        if name in seen:
            continue
        seen.add(name)
        out.append((slot, plugin, plugin_config))
    return out


async def _run_observer(plugin: Any, plugin_config: dict, ctx: PipelineCtx, slot: str) -> None:
    """Run one post_response plugin's `observe_response` method. Logs and
    swallows any exception — observers must NEVER surface to the client.
    The request handler has already returned by the time this runs.
    """
    plugin_short = _short_name(plugin)
    fn = getattr(plugin, "observe_response", None)
    if fn is None:
        logger.warning(
            f"Plugin '{plugin_short}' declares 'post_response' capability "
            f"but has no observe_response() method — skipping."
        )
        return
    try:
        if inspect.iscoroutinefunction(fn) or asyncio.iscoroutinefunction(fn):
            await fn(ctx, plugin_config)
        else:
            fn(ctx, plugin_config)
    except Exception as e:
        # Observers must never raise into the request path. Log loudly
        # so the operator notices, but don't propagate.
        logger.error(
            f"post_response observer '{plugin_short}' at {slot} raised "
            f"(identity '{ctx.identity.key}'): {type(e).__name__}: {e!r}",
            exc_info=True,
        )


def _dispatch_observers_nonstream(ctx: PipelineCtx, pipeline: list) -> None:
    """Schedule observer dispatches on the running event loop, then return
    immediately so the response can ship to the client.

    Used on the non-streaming path. ctx.response is already populated by
    either produce_response or _do_http_call_nonstream, so observers see
    the assembled assistant turn directly."""
    observers = _collect_observer_tuples(pipeline)
    if not observers:
        return
    if ctx.dev_trace is not None:
        dev_trace.event(
            ctx.dev_trace, "post_response",
            mode="non-stream",
            observer_count=len(observers),
            observers=[_short_name(p) for _, p, _ in observers],
        )
    for slot, plugin, plugin_config in observers:
        # asyncio.create_task fires the coroutine immediately on the
        # current event loop. The request handler returns before these
        # complete; FastAPI/uvicorn don't await them. Pure fire-and-forget.
        asyncio.create_task(
            _run_observer(plugin, plugin_config, ctx, slot)
        )


async def _dispatch_observers_after_stream(
    ctx: PipelineCtx, pipeline: list, chunks: list[bytes]
) -> None:
    """Reconstruct the assistant turn from teed SSE chunks, populate
    ctx.response, then run each post_response observer.

    Runs as a starlette BackgroundTask — i.e. *after* the StreamingResponse
    body has been fully delivered to the client. The client has already
    received [DONE]; observer latency is invisible.

    Why this is one async function (not per-observer create_task) on the
    streaming path: we already have a "the body is sent, now do this"
    callback hook (BackgroundTask). Inside it we await each observer
    serially. They could be parallelised with asyncio.gather, but storing
    a memory pair is fast enough that serial keeps the code simple and
    error attribution clear. Revisit if a slow observer arrives.
    """
    observers = _collect_observer_tuples(pipeline)
    if not observers:
        # Belt-and-braces: caller already checked, but stay defensive.
        dev_trace.end_request(
            ctx.dev_trace, status="stream-passthrough",
            response_summary="no observers wired",
        )
        return

    try:
        assembled = stream_reconstruct.reconstruct_from_chunks(chunks)
    except Exception as e:
        logger.error(
            f"post_response: reconstruct failed for identity "
            f"'{ctx.identity.key}': {type(e).__name__}: {e!r}",
            exc_info=True,
        )
        dev_trace.end_request(
            ctx.dev_trace, status="error",
            response_summary=f"post_response reconstruct failed: {type(e).__name__}",
        )
        return

    # Populate ctx.response with the reconstructed turn so observers see
    # it the same way they would on the non-streaming path. Preserve the
    # full reconstructed dict on _full_response for plugins that need
    # tool_calls / reasoning_content / finish_reason.
    ctx.response = {
        "role": "assistant",
        "content": assembled.get("content", ""),
        "_full_response": assembled,
    }

    if ctx.dev_trace is not None:
        dev_trace.event(
            ctx.dev_trace, "post_response",
            mode="stream",
            observer_count=len(observers),
            observers=[_short_name(p) for _, p, _ in observers],
            assembled_finish_reason=assembled.get("finish_reason"),
            assembled_content_len=len(assembled.get("content") or ""),
            assembled_tool_calls=(
                len(assembled.get("tool_calls") or [])
                if assembled.get("tool_calls") else 0
            ),
        )

    for slot, plugin, plugin_config in observers:
        await _run_observer(plugin, plugin_config, ctx, slot)

    # Close the trace now that all observers have completed. The trace
    # file thus captures the full request lifecycle (verify → assemble →
    # stream → reconstruct → observe).
    dev_trace.end_request(
        ctx.dev_trace, status="stream-passthrough",
        response_summary=(
            f"response.* skipped on stream; "
            f"{len(observers)} post_response observer(s) ran "
            f"(finish_reason={assembled.get('finish_reason') or '(none)'})"
        ),
    )


# ---------------------------------------------------------------------------
# handle_tool_calls — intercept dispatch (D-008)
# ---------------------------------------------------------------------------

async def _dispatch_intercepts(ctx: PipelineCtx, pipeline: list) -> PipelineCtx:
    """Walk plugins declaring ``handle_tool_calls`` (D-008). Each plugin
    claims tool_calls by name (its module-level ``OWNED_TOOLS`` list);
    if a plugin returns a modified ctx with rewritten messages, the
    executor re-runs the resource step so the agent reacts in their own
    voice, and keeps re-calling for as long as the model keeps calling
    tools — up to ``ctx.max_tool_laps`` laps (a cascade-resolved runaway
    guardrail; ``0`` means uncapped, run until ``finish_reason != tool_calls``).
    This is the bridge acting as its own agentic harness for bridge-native
    tools. The cap is a cost/latency bound, NOT a feature leash — the
    never-leak guard makes an unbounded model safe.

    Reached only after the resource step on the buffered (non-streaming
    upstream) path — pre-delivery plugins require an assembled frame.

    On the no-handle_tool_calls-wired path (or when the assembled frame
    has no tool_calls), this is a no-op.
    """
    intercept_tuples = _collect_intercept_tuples(pipeline)
    if not intercept_tuples:
        return ctx

    # Runaway guardrail, cascade-resolved at ctx-build time (identity → role →
    # session → resource → server → env → 1). 0 == uncapped: run until the
    # model stops calling tools. The never-leak guard makes uncapped safe.
    max_laps = ctx.max_tool_laps
    uncapped = max_laps <= 0

    lap = 0
    while True:
        # Read the assembled assistant message + tool_calls + finish_reason
        # off ctx.response["_full_response"] (populated by
        # _do_http_call_nonstream OR a previous re-call iteration).
        finish_reason, tool_calls, _message = _extract_intercept_inputs(ctx)
        if finish_reason != "tool_calls" or not tool_calls:
            return ctx

        # Bucket the tool_calls by which plugin claims them. Plugins that
        # don't claim anything are skipped. Tool_calls claimed by no
        # plugin are "harness tools" — current behaviour mirrors V3:
        # mixed bridge+harness tool_calls log a warning and pass through
        # unchanged (Q-M parks the upgrade to splice bridge results
        # alongside harness tool_calls).
        claimed_any = False
        unhandled_indices: set[int] = set(range(len(tool_calls)))
        plugin_claims: list[tuple[Any, dict, str, list[dict]]] = []
        for slot, plugin, plugin_config in intercept_tuples:
            owned = _plugin_owned_tools(plugin)
            if not owned:
                continue
            claimed = []
            for i, tc in enumerate(tool_calls):
                name = _tool_call_name(tc)
                # Strip the bridge-native prefix before the OWNED_TOOLS match:
                # the agent returns the prefixed wire name (e.g.
                # bridge_native__rng_message) but OWNED_TOOLS is clean
                # (rng_message). strip_namespace is a no-op for non-prefixed
                # names, so harness tools still fall through to the pure/mixed
                # passthrough branches below. This single edit is the central
                # seam every handle_tool_calls plugin inherits.
                if name and bridge_native.strip_namespace(name) in owned and i in unhandled_indices:
                    claimed.append(tc)
                    unhandled_indices.discard(i)
            if claimed:
                claimed_any = True
                plugin_claims.append((plugin, plugin_config, slot, claimed))

        if not claimed_any:
            # Nothing an intercept plugin claimed. This is EITHER genuine harness
            # tool_calls (pass through, the harness runs them) OR a MALFORMED
            # bridge-native call a model emitted (e.g. a bare `bridge_native__filesystem`
            # with no `__method`) that matched no OWNED_TOOLS. Neutralize first: strip any
            # bridge_native__-prefixed call (never-leak by prefix — see
            # _neutralize_bridge_tool_calls). If that empties the tool_calls it flips
            # finish_reason → "stop", turning a would-be DANGLING tool_call turn into a
            # CLEAN TEXT turn — so basic_session can SAVE the exchange (session-truth:
            # a buddy that ran tools then fizzled on garbage is still truth; store it
            # coherently, never as a broken frame, never as a void). Genuine harness
            # tool_calls (no bridge prefix) survive and pass through unchanged.
            _neutralize_bridge_tool_calls(ctx, intercept_tuples)
            # Breadcrumb: this turn DID run a bridge tool loop (the queries that
            # resolved before the fizzle), so basic_session persists the FULL exchange
            # (user + resolved tool turns + partial text), not just the final text.
            ctx.plugin_data["intercept.loop_ran"] = True
            return ctx

        if unhandled_indices:
            # Mixed bridge + harness tool_calls. V3 fall-back per the plan:
            # log + pass through unchanged (lose bridge result). Q-M
            # parks the splicing upgrade.
            unhandled_names = [
                _tool_call_name(tool_calls[i]) or "(anonymous)"
                for i in sorted(unhandled_indices)
            ]
            logger.warning(
                f"Identity '{ctx.identity.key}' — mixed bridge+harness "
                f"tool_calls (bridge-claimed by intercept plugins, "
                f"harness-pending: {unhandled_names}). Neutralizing the bridge "
                f"tool_calls (they must never reach the harness) and passing "
                f"the harness tool_calls through. (Q-M parks splicing bridge "
                f"results alongside harness tool_calls.)"
            )
            _neutralize_bridge_tool_calls(ctx, intercept_tuples)
            return ctx

        # All claimed by intercept plugins — execute them, splice results,
        # and re-call the resource step. The runaway guardrail only fires for a
        # finite cap; an uncapped identity (max_tool_laps: 0) runs until the
        # model stops calling tools — the never-leak guard still protects the
        # mixed-frame case above, so uncapped is safe.
        if not uncapped and lap >= max_laps:
            logger.warning(
                f"Identity '{ctx.identity.key}' — handle_tool_calls "
                f"max_tool_laps={max_laps} reached and upstream STILL returned "
                f"tool_calls. Neutralizing unresolved bridge tool_calls "
                f"(they must never reach the harness) and delivering. Raise "
                f"this identity's max_tool_laps (or set 0 for uncapped) to "
                f"allow more laps."
            )
            _neutralize_bridge_tool_calls(ctx, intercept_tuples)
            return ctx

        # Build the assistant turn that called the tools (preserve
        # tool_calls, content, reasoning_content).
        assistant_turn = _build_assistant_turn_from_response(ctx)

        # Walk plugin claims; each plugin handles its claimed tool_calls
        # and returns tool_result messages to splice in. Errors become
        # synthetic tool_result messages so the agent narrates the failure.
        tool_result_messages: list[dict] = []
        for plugin, plugin_config, slot, claimed in plugin_claims:
            results = await _run_intercept_plugin(
                plugin, plugin_config, ctx, slot, claimed,
            )
            tool_result_messages.extend(results)

        # Splice into messages and clear the response so the re-call
        # populates a fresh one.
        ctx.request.messages = [
            *ctx.request.messages,
            assistant_turn,
            *tool_result_messages,
        ]
        ctx.response = None

        # Breadcrumb for the save side (basic_session.observe_response): a
        # bridge-owned tool loop actually executed at least one lap, so the
        # spliced [assistant(tool_calls), tool(result)…] turns now live on
        # ctx.request.messages. Without this flag the save path can't tell a
        # bridge-owned loop from a plain turn and stores ONLY the final reply
        # (the agent's history becomes a "theatre script, not a lab notebook").
        # The flag lets basic_session take its existing full-exchange storage
        # path (the one harness-owned loops already use), preserving the real
        # tool_calls + results + the agent's mid-loop narration.
        ctx.plugin_data["intercept.loop_ran"] = True

        # Re-call the resource step. ctx.request.stream is already False
        # from the pre-delivery branch decision; keep it that way.
        cap_label = "∞" if uncapped else str(max_laps + 1)
        if ctx.dev_trace is not None:
            dev_trace.event(
                ctx.dev_trace, "intercept_recall",
                lap=lap + 1, max_laps=("uncapped" if uncapped else max_laps),
                claimed_tool_count=sum(len(c) for _, _, _, c in plugin_claims),
                claimants=[_short_name(p) for p, _, _, _ in plugin_claims],
            )
        ctx.slots_visited.append(
            f"[core] intercept lap {lap + 1}/{cap_label} "
            f"({sum(len(c) for _, _, _, c in plugin_claims)} tool_call(s) "
            f"executed; re-calling resource)"
        )
        resource_result = await _execute_resource_step(ctx, pipeline)
        if isinstance(resource_result, StreamingResponse):
            # Should not happen — we forced ctx.request.stream = False.
            # Defend in depth: log and break.
            logger.error(
                f"Identity '{ctx.identity.key}' — intercept re-call "
                f"returned StreamingResponse despite stream=False. This "
                f"is a bug. Aborting intercept loop."
            )
            return ctx
        ctx = resource_result
        lap += 1

    return ctx


def _collect_intercept_tuples(pipeline: list) -> list[tuple[str, Any, dict]]:
    """Filter the assembled pipeline down to tuples whose plugin declares
    `handle_tool_calls`. Slot-string overlap with `outbound_params` and
    `post_response` means we have to consult the capability table per-plugin.

    **Deduped by plugin.** A multi-capability plugin can legitimately occupy
    several slots in the assembled pipeline (e.g. mcp_client appears at
    role.plugins for handle_tool_calls AND identity.plugins for its background
    capability AND a context slot for context_modify — capability fan-out
    materialises a tuple per slot). But its `handle_tool_calls` must fire at
    most ONCE per request — dispatching the same plugin N times for one claimed
    tool_call would execute the tool N times. We keep the FIRST tuple per
    plugin (its config is cascade-merged identically across slots, so which one
    we keep doesn't change behaviour)."""
    return _dedupe_by_plugin(
        (slot, plugin, plugin_config)
        for slot, plugin, plugin_config in pipeline
        if _is_intercept_tuple(plugin)
    )


def _is_intercept_tuple(plugin: Any) -> bool:
    """True iff this plugin declares the `handle_tool_calls` capability."""
    caps = plugin_loader.get_capabilities(_short_name(plugin)) or {}
    return "handle_tool_calls" in caps


def _plugin_owned_tools(plugin: Any) -> list[str]:
    """Pull the module-level ``OWNED_TOOLS`` declaration off a plugin.
    Returns empty list when missing/malformed — validator emits a
    warning at startup so the operator is told.

    ``OWNED_TOOLS`` may be a static list (agent_tools) OR a callable
    (mcp_client, whose tools are discovered from MCP servers at startup
    and so can't be a module-level literal). When callable, call it to
    get the current list — discovery has run by request time."""
    owned = getattr(plugin, "OWNED_TOOLS", None)
    if callable(owned):
        try:
            owned = owned()
        except Exception:
            logger.warning(
                f"OWNED_TOOLS callable on {_short_name(plugin)} raised; "
                f"treating as empty",
                exc_info=True,
            )
            return []
    if not isinstance(owned, list):
        return []
    return [t for t in owned if isinstance(t, str) and t]


def _coerce_laps(raw: object) -> int | None:
    """Parse a ``max_tool_laps`` value into a non-negative int, or None if it
    isn't a usable integer. ``0`` is preserved (it means *uncapped*) — only
    negative values are clamped (to 0 / uncapped, since a negative bound is
    meaningless and 'no bound' is the closest honest reading)."""
    try:
        n = int(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return max(0, n)


def _resolve_max_tool_laps(
    identity_cfg: dict,
    role_cfg: dict,
    session_cfg: dict,
    resource_cfg: dict,
    server_cfg: dict,
) -> int:
    """Resolve the agentic-tool-loop runaway guardrail top-down.

    ``max_tool_laps`` is a bare scalar config key (like ``timezone``), not a
    plugin. Cascade order matches the rest of the bridge:
    ``identity > role > session > resource > server > .env``. First level that
    declares an integer wins; ``0`` is a valid winning value meaning *uncapped*
    (run until ``finish_reason != tool_calls``). If no level declares it, fall
    back to the legacy ``BRIDGE_HANDLE_TOOL_CALLS_MAX_LAPS`` env var, then to 1.

    This is a runaway/cost guardrail, NOT a feature leash — the never-leak
    guard (``_neutralize_bridge_tool_calls``) already makes an unbounded model
    safe by stripping unresolved bridge tool_calls before delivery."""
    for cfg in (identity_cfg, role_cfg, session_cfg, resource_cfg, server_cfg):
        if not isinstance(cfg, dict) or "max_tool_laps" not in cfg:
            continue
        n = _coerce_laps(cfg.get("max_tool_laps"))
        if n is not None:
            return n
    # Legacy env fallback (absolute bottom of the cascade).
    env_n = _coerce_laps(os.getenv("BRIDGE_HANDLE_TOOL_CALLS_MAX_LAPS"))
    if env_n is not None:
        return env_n
    return 1


def _extract_intercept_inputs(
    ctx: PipelineCtx,
) -> tuple[str | None, list[dict], dict]:
    """Read finish_reason + tool_calls + assistant message off ctx.response.

    The resource step populates ``ctx.response = {"role": "assistant",
    "content": ..., "_full_response": <upstream JSON>}``. The upstream
    JSON has ``choices[0].message.{content, tool_calls, ...}`` and
    ``choices[0].finish_reason``. Returns (finish_reason, tool_calls,
    message_dict). Empty/None defaults if anything is malformed."""
    response = ctx.response or {}
    full = response.get("_full_response") or {}
    choices = full.get("choices") or []
    if not choices or not isinstance(choices[0], dict):
        return (None, [], {})
    first = choices[0]
    finish_reason = first.get("finish_reason") if isinstance(first.get("finish_reason"), str) else None
    message = first.get("message") if isinstance(first.get("message"), dict) else {}
    tool_calls = message.get("tool_calls") if isinstance(message.get("tool_calls"), list) else []
    return (finish_reason, tool_calls, message)


def _neutralize_bridge_tool_calls(ctx: PipelineCtx, intercept_tuples: list) -> None:
    """Never-leak invariant: a ``bridge_native__`` tool_call must NEVER be
    delivered to the harness — the harness can't run it ("tool not found").

    When the intercept loop gives up (lap budget exhausted, or a mixed
    bridge+harness response it can't splice), this strips any unresolved
    BRIDGE-owned tool_calls out of the assembled response in place, leaving
    only harness tool_calls (if any). If no tool_calls remain, ``finish_reason``
    is flipped to ``stop`` so the harness sees a clean text turn rather than a
    dangling ``tool_calls`` finish with an empty array. Bridge-owned is decided
    the same way the claim-match does: strip the prefix, check OWNED_TOOLS.

    Mutates ctx.response["_full_response"].choices[0].message.tool_calls and
    .finish_reason. No-op if there's no response / no tool_calls."""
    response = ctx.response or {}
    full = response.get("_full_response") or {}
    choices = full.get("choices") or []
    if not choices or not isinstance(choices[0], dict):
        return
    message = choices[0].get("message")
    if not isinstance(message, dict):
        return
    tool_calls = message.get("tool_calls")
    if not isinstance(tool_calls, list) or not tool_calls:
        return

    # Build the set of bridge-owned (clean) tool names across all intercept plugins.
    owned_all: set[str] = set()
    for _slot, plugin, _cfg in intercept_tuples:
        owned_all.update(_plugin_owned_tools(plugin))

    kept = []
    dropped = []
    for tc in tool_calls:
        name = _tool_call_name(tc)
        # Bridge-owned-must-strip if EITHER (a) the stripped name matches a plugin's
        # OWNED_TOOLS, OR (b) it carries the bridge_native__ prefix AT ALL. (b) is the
        # never-leak-by-PREFIX guard: a MALFORMED bridge-native call (e.g. a bare
        # `bridge_native__filesystem` with no `__method`, which a small model may emit)
        # strips to `filesystem` — NOT a full OWNED_TOOLS entry — so (a) alone would let
        # it fall through to the harness as if it were a harness tool. It isn't: a
        # bridge_native__-prefixed call must NEVER reach the harness, malformed or not.
        # (a) still preserves genuine HARNESS tool_calls (no bridge_native__ prefix).
        if name and (bridge_native.strip_namespace(name) in owned_all
                     or bridge_native.is_namespaced(name)):
            dropped.append(name)
        else:
            kept.append(tc)

    if not dropped:
        return  # nothing bridge-owned to strip

    message["tool_calls"] = kept
    if not kept:
        # No harness tool_calls remain — deliver as a plain text turn.
        message.pop("tool_calls", None)
        choices[0]["finish_reason"] = "stop"
    logger.info(
        f"Identity '{ctx.identity.key}' — neutralized {len(dropped)} unresolved "
        f"bridge tool_call(s) so they don't reach the harness: {dropped}"
        + (f"; {len(kept)} harness tool_call(s) preserved" if kept else "")
    )


def _build_assistant_turn_from_response(ctx: PipelineCtx) -> dict:
    """Build the assistant message dict that gets spliced into messages
    before the tool_results, so the re-call sees: [..., assistant_turn,
    tool_result_1, tool_result_2, ...]. Preserves tool_calls, content,
    reasoning_content (Moonshot fix)."""
    _fr, _tcs, message = _extract_intercept_inputs(ctx)
    turn: dict[str, Any] = {"role": "assistant"}
    content = message.get("content")
    if isinstance(content, str):
        turn["content"] = content
    else:
        turn["content"] = ""
    tcs = message.get("tool_calls")
    if isinstance(tcs, list):
        turn["tool_calls"] = tcs
    # Reasoning key drift: Moonshot DIRECT streams `reasoning_content`; Moonshot
    # via OpenRouter streams `reasoning` (+ `reasoning_details`). Moonshot ENFORCES
    # `reasoning_content` on a round-tripped assistant(tool_calls) turn — so we must
    # mirror `reasoning` → `reasoning_content` when only the OpenRouter key is
    # present, exactly as V3 did. Without this, the re-call 400s
    # ("reasoning_content is missing in assistant tool call message").
    rc = message.get("reasoning_content")
    r_alt = message.get("reasoning")
    if isinstance(rc, str) and rc:
        turn["reasoning_content"] = rc
    elif isinstance(r_alt, str) and r_alt:
        turn["reasoning_content"] = r_alt
    rd = message.get("reasoning_details")
    if rd:
        turn["reasoning_details"] = rd
    return turn


def _tool_call_name(tc: dict) -> str | None:
    """Pull the function name off a tool_call dict, defending against
    shape drift. Returns None for malformed entries."""
    if not isinstance(tc, dict):
        return None
    fn = tc.get("function")
    if not isinstance(fn, dict):
        return None
    name = fn.get("name")
    return name if isinstance(name, str) and name else None


def _claimed_with_clean_name(tc: dict) -> dict:
    """Return a shallow copy of the claimed tool_call with the bridge_native__
    prefix stripped from function.name, so the plugin's clean OWNED_TOOLS-keyed
    handler lookup resolves. The ORIGINAL tc (prefixed) must be preserved for
    the assistant turn and tool result message the agent sees in history — so we
    never mutate it; we hand the plugin this normalised copy only. No-op (returns
    the original) for non-prefixed names or malformed entries."""
    if not isinstance(tc, dict):
        return tc
    fn = tc.get("function")
    if not isinstance(fn, dict):
        return tc
    name = fn.get("name")
    if not bridge_native.is_namespaced(name):
        return tc
    return {**tc, "function": {**fn, "name": bridge_native.strip_namespace(name)}}


async def _run_intercept_plugin(
    plugin: Any,
    plugin_config: dict,
    ctx: PipelineCtx,
    slot: str,
    claimed: list[dict],
) -> list[dict]:
    """Invoke a single intercept plugin's ``handle_tool_calls`` method
    once per claimed tool_call. Returns a list of tool_result messages
    suitable for splicing into ``ctx.request.messages``.

    The plugin's method receives (ctx, config) and reads claimed
    tool_calls off ``ctx.plugin_data["handle_tool_calls.claimed"]``
    (set here per call). The plugin returns either a string (the tool
    result content) or a dict ``{"content": str, "extra": ...}`` (for
    plugins that want to set extra fields on the tool message).

    Plugin exceptions become synthetic error tool_result messages so
    the agent narrates the failure in their own voice — never 500s the
    client. Plugin-tool-failed is operationally a tool returning an
    error string, not infrastructure failure.
    """
    plugin_short = _short_name(plugin)
    fn = getattr(plugin, "handle_tool_calls", None)
    results: list[dict] = []
    if fn is None:
        # Validator should have caught this. Defend in depth.
        for tc in claimed:
            results.append(_synthetic_tool_error(
                tc,
                f"plugin '{plugin_short}' declares handle_tool_calls "
                f"but has no handle_tool_calls() method",
            ))
        return results

    for tc in claimed:
        # Hand the plugin a copy with the clean name so its OWNED_TOOLS-keyed
        # handler lookup resolves; the original prefixed tc is kept for the
        # assistant turn + tool result message (history fidelity).
        ctx.plugin_data["handle_tool_calls.claimed"] = _claimed_with_clean_name(tc)
        if ctx.dev_trace is not None:
            dev_trace.event(
                ctx.dev_trace, "intercept_tuple_start",
                slot=slot, plugin=plugin_short,
                tool_name=_tool_call_name(tc) or "(anonymous)",
                tool_call_id=tc.get("id") if isinstance(tc, dict) else None,
            )
        started = time.monotonic()
        try:
            if inspect.iscoroutinefunction(fn) or asyncio.iscoroutinefunction(fn):
                result = await fn(ctx, plugin_config)
            else:
                result = fn(ctx, plugin_config)
        except Exception as e:
            duration_ms = int((time.monotonic() - started) * 1000)
            logger.error(
                f"intercept plugin '{plugin_short}' raised on tool_call "
                f"'{_tool_call_name(tc) or '?'}' "
                f"(identity '{ctx.identity.key}'): "
                f"{type(e).__name__}: {e!r}",
                exc_info=True,
            )
            if ctx.dev_trace is not None:
                dev_trace.event(
                    ctx.dev_trace, "intercept_tuple_end",
                    slot=slot, plugin=plugin_short,
                    result="error", duration_ms=duration_ms,
                    message=f"{type(e).__name__}: {e!r}",
                )
            results.append(_synthetic_tool_error(
                tc, f"{type(e).__name__}: {e}",
            ))
            continue

        duration_ms = int((time.monotonic() - started) * 1000)
        # Plugin returned: either a string (tool content) or dict.
        content_str = _normalise_tool_result_content(result)
        results.append(_make_tool_message(tc, content_str))
        if ctx.dev_trace is not None:
            dev_trace.event(
                ctx.dev_trace, "intercept_tuple_end",
                slot=slot, plugin=plugin_short,
                result="ok", duration_ms=duration_ms,
                content_len=len(content_str),
            )

    # Clear the per-call slot so subsequent plugins don't see stale data.
    ctx.plugin_data.pop("handle_tool_calls.claimed", None)
    return results


def _normalise_tool_result_content(result: Any) -> str:
    """Coerce a plugin's return value into a string suitable for the
    tool message's ``content`` field. None/empty → empty string. Dict →
    json. Everything else → str()."""
    if result is None:
        return ""
    if isinstance(result, str):
        return result
    if isinstance(result, dict):
        explicit = result.get("content")
        if isinstance(explicit, str):
            return explicit
        try:
            return json.dumps(result, default=str)
        except Exception:
            return str(result)
    return str(result)


def _make_tool_message(tc: dict, content: str) -> dict:
    """Build a {role: tool, tool_call_id, name, content} dict from the
    original tool_call entry."""
    return {
        "role": "tool",
        "tool_call_id": tc.get("id") if isinstance(tc, dict) else None,
        "name": _tool_call_name(tc) or "",
        "content": content,
    }


def _synthetic_tool_error(tc: dict, message: str) -> dict:
    """Build a tool message representing a local failure, so the agent
    can narrate it. Mirrors the V3 error-string convention."""
    return _make_tool_message(tc, f"Error: {message}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _short_name(plugin) -> str:
    """Plugin module's __name__ is e.g. 'plugins.builtin.openai-protocol' —
    strip to last segment, which is the registry key."""
    return plugin.__name__.rsplit(".", 1)[-1]


def _safe_serialise(obj: Any) -> bytes:
    """Best-effort JSON serialisation for byte-counting in the trace
    footer. Returns empty bytes on failure — never raises."""
    try:
        return json.dumps(obj, default=str).encode("utf-8")
    except Exception:
        return b""


def _trace_assemble_and_sign(ctx: PipelineCtx) -> None:
    """Emit the assemble_and_sign event for the trace, surfacing the
    bridge_context dict and (when injected into messages) the produced
    XML block. The block lives in ctx.request.messages — we extract it
    from the last user message's content to display."""
    if ctx.dev_trace is None:
        return
    block = _extract_bridge_block(ctx.request.messages)
    dev_trace.event(
        ctx.dev_trace, "assemble_and_sign",
        bridge_context=dict(ctx.bridge_context),
        block=block,
        injected=block is not None,
    )


def _extract_bridge_block(messages: list[dict]) -> str | None:
    """Pull the <bridge_context>...</bridge_context> XML block out of
    the most recent message that contains one. Returns None if no
    block is present (e.g. ctx.bridge_context was empty so nothing
    got assembled)."""
    import re
    pattern = re.compile(
        r"<bridge_context\b[^>]*>.*?</bridge_context>", re.DOTALL
    )
    for msg in reversed(messages):
        content = msg.get("content", "")
        if isinstance(content, str):
            m = pattern.search(content)
            if m:
                return m.group(0)
        elif isinstance(content, list):
            for part in content:
                if isinstance(part, dict) and part.get("type") == "text":
                    m = pattern.search(part.get("text", ""))
                    if m:
                        return m.group(0)
    return None


def _ctx_response_to_openai(ctx: PipelineCtx) -> dict:
    """Convert the populated ctx.response dict into the final OpenAI-shaped
    response body to send back to the client.

    If the response carries a `_full_response` (set by `_do_http_call`),
    return that — it's the verbatim upstream response, which preserves
    finish_reason, usage, model, etc. For terminal/produce_response paths,
    construct a minimal OpenAI-shaped envelope around the response."""
    full = ctx.response.get("_full_response") if ctx.response else None
    if full is not None:
        return full

    # produce_response path — wrap in a minimal OpenAI-shaped envelope
    return {
        "id": "ind-bridge-local",
        "object": "chat.completion",
        "model": ctx.request.model or ctx.resource.key or "ind-bridge",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": (ctx.response or {}).get("content", ""),
            },
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }
