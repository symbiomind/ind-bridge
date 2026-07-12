"""
basic_session — builtin session-management plugin (V4).

The bridge owns the conversation. Each turn, ``basic_session``:

  1. **Loads** the full history from a JSON file on disk.
  2. **Rebuilds** ``ctx.request.messages`` as ``[system?] + send-window + latest-user``
     — taking only the *last* user turn from inbound, because the bridge owns the
     session and upstream (OpenClaw, LibreChat, …) is stateless from our view.
  3. **Stamps** the authoritative ``ctx.plugin_data["session_state"]`` (D-009 contract)
     so the wakeup cascade fires on *truth* (this plugin owns the file; it KNOWS
     whether the session is fresh) rather than bridge-core's message-shape inference.
  4. After the reply ships, **saves** the new user+assistant pair to disk — storing
     *rich* turns (``reasoning_content`` / ``tool_calls`` preserved when present) so
     replay to picky providers (Moonshot et al.) survives the round-trip.

Two knobs, one honest meaning each:

  * **Storage is always full.** A session file is text; disk is cheap. We never trim
    what we keep. (The wakeup/recency cascade can therefore reach *past* the send
    window into deep history.)
  * **``max_turns`` is the SEND window** — how many user/agent *pairs* go upstream each
    turn. Defaults bounded (so a fresh wire-up can't blow context + wallet on
    "hello, I'm back"). Set ``false``/``0`` to send everything, knowingly.

Reset is OFF by default. A fresh session happens only when the file is gone/empty —
**deleting the session file is the always-available hard reset.** Opt into a soft
boundary with ``session_reset``:

  * ``daily`` — a background scheduler (``start_background``) archives the session
    file at the configured wall-clock ``at`` time (renames it to a dated copy
    beside it; nothing is destroyed). The next read sees no file and takes the
    always-on fresh path, so the wake cascade fires for the *human's* first turn of
    the day — not for whatever cron/heartbeat happened to write the file overnight.
    This is event-driven on purpose: a read-time mtime heuristic could be raced by a
    mid-day write that bumped the file past the boundary and ate the reset.
  * ``manual`` — resets on an explicit ``x-session-reset`` header.

This plugin lives WHOLLY in one ``sessions.<name>.plugins`` block (D-010 lets
``post_response`` sit in ``session.plugins`` alongside ``outbound_params``):

    sessions:
      my_session:
        plugins:
          basic_session:
            max_turns: 20                    # SEND window in PAIRS; false/0 = send all. Storage always full.
            send_window: "36h"               # optional TIME-based SEND window ("36h"/"2d"/"90m";
                                             #   off unless set). Forwards only turns from the last
                                             #   N; older ones (and any pre-feature turn with no
                                             #   `ts`) drop off the WIRE, stay on disk. Composes with
                                             #   max_turns — TIGHTEST wins. Turns get a save-time `ts`
                                             #   (bridge-internal, stripped before upstream). Ideal for
                                             #   a CONTINUOUS buddy (session_reset: never) — a rolling
                                             #   horizon instead of a daily wipe.
            data_dir: data/basic_session     # parent dir (default as shown); each
                                             # session is stored under its own subfolder:
                                             # <data_dir>/<session_key>/<session_key>.json
            system_prompt:                   # optional file list — replaces upstream system
              - /workspace/my_agent/SOUL.md
            system_prompt_append: "..."      # optional string appended to whatever system is used
            session_reset:                   # optional; OFF by default
              mode: never                    # never (default) | daily | manual
              at: "04:00"                    # daily only — boundary in server tz
            degrade_tool_history: false      # opt-in: fold OLD tool exchanges to
                                             # assistant prose on SEND (storage stays full).
                                             # Keeps the most-recent closed exchange raw.
            partitions:                      # optional: split ONE session block into
                                             # several files keyed by CALLER (the signed
                                             # <caller> = context.name). Unset = one
                                             # shared file (default, unchanged).
              team_a:                        #   named group → <base>__group__team_a.json
                - alice
                - bob
              default: shared                # shared (one file) | caller (per-caller:
                                             #   <base>__user__<caller>.json). A caller with
                                             #   no context.name can only land in shared.

    roles:
      my_agent:
        session: my_session
        ...

Session key is the NAMED session block (``role.session: <name>`` → the
``sessions.<name>`` key). Sessions are shared by name-and-reference (D-006), so
multiple identities on one role/session share one history file — e.g. a
``*_librechat`` and a ``*_harness_b`` identity pointing at the same role give the
agent ONE continuous conversation across harness switches.

Capabilities / lifecycle:
  * ``outbound_params`` (``apply_outbound_params``) — fires step 1c, before context
    plugins, so the ``session_state`` stamp is visible to conversational_memory's
    wakeup cascade.
  * ``post_response`` (``observe_response``) — fires after delivery, fire-and-forget;
    sees only the final, *closed* assistant turn.
  * ``background`` (``start_background``) — spawned once at startup per identity that
    wires this plugin; schedules the ``session_reset.mode: daily`` archive (no task
    for ``manual``/``never``).

Tool-calling, two ownerships:
  * **Bridge-owned** tools (``handle_tool_calls`` plugins) — the executor owns the
    loop between load and save; the session never sees intermediate laps.
  * **Harness-owned** tools (LibreChat / OpenClaw run their own MCP loop OUTSIDE the
    bridge) — the harness re-hits us mid-loop with the closed thread
    ``[user, assistant(tool_calls), tool(result), …]``. ``apply_outbound_params``
    detects this (tail after the last user turn carries tool/assistant-tool_calls
    messages) and splices the tail through VERBATIM, or the model never sees the tool
    ran and re-calls it forever. When the loop closes, ``observe_response`` stores the
    FULL exchange so the agent's memory records that it ran the tool.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, time as dt_time, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.context import PipelineCtx, StartupCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "outbound_params": ["session.plugins"],   # load + rebuild + stamp session_state
    "post_response":   ["session.plugins"],   # save the closed turn, after delivery (D-010)
    "background":      ["session.plugins"],   # schedule the daily reset-archive (mode: daily)
}


_DEFAULT_MAX_TURNS = 20
_DEFAULT_SEND_WINDOW = None  # time-based send window OFF unless a session opts in
_DEFAULT_DATA_DIR = "data/basic_session"

# ---------------------------------------------------------------------------
# Per-session-file save lock (concurrency safety — step 1 of the session queue).
#
# The session file is shared BY DESIGN (D-006: sessions are top-level named
# blocks referenced by name; multiple identities/harnesses point at one file).
# So one session file has THREE concurrent writers on the single event loop:
#   * HTTP harness turns (OpenAI-Protocol listener),
#   * cron heartbeats (cron._fire → execute()),
#   * bridge_messaging deliveries (_handle_send → execute() on the recipient).
# All three funnel through observe_response, whose load→append→save is a
# non-atomic read-modify-write. Interleaved at any await point → last-writer-wins
# clobber = LOST TURNS — several callers talking over one agent at once.
#
# Fix: serialize the save per RESOLVED SESSION FILE PATH — NOT per identity or
# per session-name, because partitions + bridge_messaging's caller override can
# route the same carrier to DIFFERENT partition files (_partition_suffix); the
# file path is the true contention unit, and keying on it avoids over-serializing
# unrelated partitions/sessions. Locks are created lazily (double-checked under
# _lock_registry_lock) so each asyncio.Lock is bound to the running loop, never
# at import (conv_mem's _resource_cache style). This is the NARROW seam: it
# guards only the save's read-modify-write, so concurrent LLM calls to one
# session are NOT throttled — only the disk write is serialized.
# ---------------------------------------------------------------------------
_save_locks: dict[str, asyncio.Lock] = {}
_lock_registry_lock = asyncio.Lock()  # guards the create-check of _save_locks


async def _get_save_lock(session_file: str) -> asyncio.Lock:
    """Return the per-file save lock, lazily creating it (double-checked) so the
    ``asyncio.Lock`` is bound to the running event loop, not import time."""
    lock = _save_locks.get(session_file)
    if lock is None:
        async with _lock_registry_lock:
            lock = _save_locks.get(session_file)
            if lock is None:
                lock = asyncio.Lock()
                _save_locks[session_file] = lock
    return lock

# Bridge-internal per-turn field: Unix epoch stamped on SAVE. NOT part of the
# OpenAI protocol — stripped before the upstream call so it never reaches a
# provider. It's a REUSABLE primitive (the time-based send window is its first
# consumer, not its owner): future time-aware features read the same field.
_TS_KEY = "ts"

# Header the harness can send to force a manual reset (session_reset.mode: manual).
_MANUAL_RESET_HEADER = "x-session-reset"

# Rich assistant fields preserved on save so replay survives picky providers.
# Reasoning key varies by provider: Moonshot → reasoning_content; OpenRouter →
# reasoning. tool_calls is preserved for completeness, though the loop-not-closed
# guard skips saving turns that still carry one.
#
# NOTE: `reasoning_details` (OpenRouter's per-token structured array) is
# DELIBERATELY NOT stored. OpenRouter sends reasoning twice — the concatenated
# `reasoning` string AND a per-token array — for provider compatibility. We only
# need the completed artifact once (OpenAI-protocol storage), and nothing reads
# the array structure back; it just bloats session files to 300k+ lines. The
# WIRE/streaming paths still relay the array; the drop is storage-only.
_RICH_KEYS = ("reasoning_content", "reasoning", "tool_calls")


# ---------------------------------------------------------------------------
# outbound_params — load history, rebuild messages, stamp session_state
# ---------------------------------------------------------------------------

def apply_outbound_params(ctx: "PipelineCtx", config: dict) -> "PipelineCtx":
    """Load the session, rebuild the outbound message list, and stamp the
    authoritative ``session_state`` so the wakeup cascade fires on ground
    truth. Fires at executor step 1c (before context plugins)."""
    session_key = _resolve_session_key(ctx, config)
    session_file = _session_file(session_key, config)

    history = _load_history(session_file)
    max_turns = _resolve_max_turns(config)

    # ── Freshness: is this a new session? ──────────────────────────────────
    # Empty/missing file is always fresh (delete-the-file = hard reset). A
    # configured soft reset can also flip a non-empty session to fresh.
    if not history:
        is_new, reason = True, "fresh_file"
    else:
        is_new, reason = _check_reset(ctx, session_file, config)

    ctx.plugin_data["session_state"] = {
        "is_new": is_new,
        "owner": "session_plugin:basic_session",
        "reason": reason,
    }

    # ── Rebuild ctx.request.messages = [system?] + send-window + tail ───────
    inbound = ctx.request.messages
    upstream_system = _extract_system(inbound)
    system_content = _build_system_prompt(upstream_system, config)

    # The send window trims what we forward upstream — NOT the stored file.
    # Two composable windows, TIGHTEST WINS (both are just successive trims):
    #   1. time  — drop turns older than `send_window` (or with no `ts`).
    #   2. count — keep the last `max_turns` pairs.
    # Both stay tool-group-safe so windowing never orphans a tool message from
    # its parent (a leading orphan `tool` = a strict-provider 400).
    send_window = _resolve_send_window(config)
    sent_history = _time_window(history, send_window)
    max_messages = None if max_turns is None else max_turns * 2
    sent_history = _tool_group_safe_window(sent_history, max_messages)

    # WIRE-SAFETY: never send a content-less, tool_call-less assistant turn. A
    # strict provider (Moonshot) 400s: "the message at position N with role
    # 'assistant' must not be empty". Such turns EXIST on disk on purpose — the
    # save-loss fix persists the empty final lap so the tool exchange around it
    # survives (a Moonshot quirk: it sometimes closes a tool-follow-up lap with
    # nothing at all). Storage keeps them (they hold the turn's place in the
    # lab-notebook); the WIRE must not carry them. Same "full storage, filtered
    # wire" rule as the windows above. An assistant turn WITH tool_calls but empty
    # content is LEGITIMATE (a bare tool call) and is kept.
    sent_history = _drop_empty_assistant_turns(sent_history)

    # Optional: fold OLD tool exchanges to assistant prose on the wire (storage
    # untouched). Keeps the most-recent closed exchange raw so the agent can
    # still reason from fresh tool results; older groups collapse to one
    # assistant message with paired <tool type="call|result" name="…"> tags so
    # the toolbelt-breadcrumb survives but old (possibly harness-truncated)
    # plumbing stops bloating the wire turn-over-turn.
    if _resolve_degrade(config):
        sent_history = _degrade_tool_history(sent_history, keep_recent=1)

    # Harness tool-loop detection. The harness (LibreChat, etc.) owns the tools
    # and runs the loop OUTSIDE the bridge, re-hitting us with the closed thread
    # [user, assistant(tool_calls), tool(result), …] after the last user turn.
    # We must pass that whole tail through verbatim, or the model never sees the
    # tool already ran and re-calls it forever.
    last_user_idx = _last_user_index(inbound)
    tail = inbound[last_user_idx:] if last_user_idx is not None else []
    in_tool_loop = _contains_tool_loop_messages(tail)

    new_messages: list[dict] = []
    if system_content:
        new_messages.append({"role": "system", "content": system_content})

    if in_tool_loop:
        # TRANSPARENCY (Option A, DESIGN-transparent-harness-tool-loops.md).
        # The HARNESS owns this tool loop and re-hit us with a coherent, complete
        # thread. The bridge's job here is a CLEAN PIPE: prepend our system, then
        # forward the harness's own messages — do NOT Frankenstein
        # [stored_history] + [spliced tail]. The old splice substituted the
        # bridge's prose store (0 tool turns) for the part of the thread BEFORE
        # the last user turn, which orphaned tool groups whose call sat in the
        # tail but whose result sat in the windowed-out store (or vice-versa).
        # That orphan is exactly the trailing-unanswered-tool_call a strict
        # provider (Moonshot) 400s on — a wound the bridge inflicted by NOT being
        # transparent. Passing the inbound through keeps every tool group intact
        # and sidesteps LibreChat's non-unique ``:0`` tool_call_ids (no slicing →
        # no pairing to get wrong). We strip the harness's own system message
        # (we supply ours / bridge_context); assemble_and_sign still injects into
        # the LAST user message, which the full inbound still carries.
        new_messages.extend(_strip_system_messages(inbound))
        # loop_tail (for the save side) stays the user→tool tail, unchanged.
        ctx.plugin_data["basic_session.in_tool_loop"] = True
        ctx.plugin_data["basic_session.loop_tail"] = tail
        logger.info(
            f"basic_session: '{session_key}' — harness tool-loop, "
            f"passing {len(inbound)} inbound message(s) through transparently "
            f"(clean pipe; no stored-history substitution)"
        )
    else:
        # Strip the bridge-internal `ts` so it never rides upstream to a provider
        # (only stored turns carry it; inbound/latest_user come clean from the
        # harness). Storage keeps `ts`; the wire does not.
        new_messages.extend(_strip_ts(sent_history))
        # SEND path: preserve multi-part content (image_url etc.) verbatim so the
        # model actually receives the image. (Storage still flattens — line ~312.)
        latest_user = _extract_latest_user_message(inbound, flatten=False)
        if latest_user is not None:
            new_messages.append(latest_user)

    ctx.request.messages = new_messages

    # Stash for the save side (observe_response).
    ctx.plugin_data["basic_session.key"] = session_key
    ctx.plugin_data["basic_session.file"] = session_file

    logger.info(
        f"basic_session: '{session_key}' — loaded {len(history) // 2} turn(s), "
        f"sending {len(sent_history) // 2} (window={'all' if max_turns is None else max_turns}), "
        f"is_new={is_new} ({reason})"
    )
    return ctx


# ---------------------------------------------------------------------------
# post_response — append the closed turn to the stored history (after delivery)
# ---------------------------------------------------------------------------

async def observe_response(ctx: "PipelineCtx", config: dict) -> None:
    """Append the closed turn to the stored history. Fires fire-and-forget
    after the response has shipped (D-007 observer); sees only the final,
    closed assistant turn. Stores *rich* assistant fields (reasoning /
    tool_calls) so replay survives picky providers. When a harness tool-loop
    closes, stores the FULL exchange (user + tool turns + final text) so the
    agent's memory records that it actually ran the tool.

    ASYNC + LOCKED: the load→append→save below is serialized per session FILE
    (``_get_save_lock``) so concurrent writers to one shared session (HTTP turn +
    cron heartbeat + bridge_messaging delivery) can't clobber each other's turns
    (last-writer-wins → lost history). The lock spans the whole read-modify-write
    — the load must be inside it too, or a flow that loaded before another saved
    would still append onto a stale base and overwrite. Narrow seam: only the
    save is serialized; the upstream LLM call already ran, so callers aren't
    throttled. ``_run_observer`` already awaits coroutine observers, so no
    executor change is needed."""
    session_file = ctx.plugin_data.get("basic_session.file")
    session_key = ctx.plugin_data.get("basic_session.key", "?")
    if not session_file:
        return  # apply_outbound_params didn't run (or session not wired) — nothing to save.

    # Did a BRIDGE-OWNED tool loop already run and CLOSE this turn? Only the
    # bridge-owned breadcrumb (intercept.loop_ran) means "the bridge ran the loop
    # and there is NO further lap coming to save it" — so we MUST persist now,
    # even if the final assistant turn is empty or (edge case) still carries
    # tool_calls. This is the streaming-intercept save-loss fix: that path can
    # close after the tool executed but before a clean text lap lands, leaving
    # ctx.response None or tool_call-bearing → the guards below would skip and the
    # whole turn (the call + result) would VANISH (e.g. an output:async
    # bridge_messaging_send turn — proven 2026-07-11).
    #
    # NOT in_tool_loop: that's HARNESS-owned — the harness runs the loop and WILL
    # re-hit the bridge with the next lap, so skipping a mid-harness-loop turn
    # that still carries tool_calls is correct (a later lap saves it). Conflating
    # the two would wrongly persist a mid-harness-loop turn twice.
    bridge_loop_closed = bool(ctx.plugin_data.get("intercept.loop_ran"))

    assistant_turn = _build_rich_assistant_turn(ctx.response)
    if assistant_turn is None:
        if bridge_loop_closed:
            # Loop ran but produced no final text turn (stream closed after the
            # tool). Persist the exchange with a placeholder final turn so the
            # tool call + result are not lost.
            assistant_turn = {"role": "assistant", "content": ""}
        else:
            return

    # Loop-not-closed guard: an assistant turn still carrying tool_calls means
    # the agent wants a tool, not to talk. Skip the save ONLY when the loop has
    # NOT already run — then the next lap comes back through and we save when it
    # closes with text. If the loop ALREADY ran (breadcrumb set), we persist now
    # (stripping the trailing tool_calls off the final turn — the exchange is
    # stored from the working-copy tail, not this dangling call).
    if assistant_turn.get("tool_calls"):
        if bridge_loop_closed:
            assistant_turn = {
                k: v for k, v in assistant_turn.items() if k != "tool_calls"
            }
            if not assistant_turn.get("content"):
                assistant_turn["content"] = ""
        else:
            logger.info(
                f"basic_session: '{session_key}' — assistant turn carries tool_calls, "
                f"skipping save (waiting for loop to close)"
            )
            return

    # Serialize the read-modify-write per session FILE so concurrent writers
    # (HTTP turn + cron heartbeat + bridge_messaging delivery all sharing one
    # session) can't clobber each other. The load is INSIDE the lock on purpose:
    # a flow that loaded before another saved would otherwise append onto a stale
    # base and overwrite the other's turn. See _get_save_lock.
    lock = await _get_save_lock(session_file)
    async with lock:
        _append_closed_turn(ctx, session_file, session_key, assistant_turn)


def _append_closed_turn(
    ctx: "PipelineCtx", session_file: str, session_key: str, assistant_turn: dict
) -> None:
    """Load history, append the closed turn (plain pair OR full tool-loop
    exchange), and save. MUST run under the per-file save lock (called only from
    observe_response) — it is a read-modify-write on the shared session file."""
    # We store the WORKING-copy user turn — i.e. WITH the bridge_context the
    # bridge injected this turn (caller + that turn's recalled memories). This is
    # what the agent actually saw, so replaying it is the agent's faithful memory.
    # Safe and not bloat:
    #   1. bridge_sign.verify_inbound only verifies the LAST user message and
    #      returns — never re-walks history — so a stored signed block is never
    #      re-verified/stripped on replay. No stale-signature downgrade.
    #   2. conversational_memory keeps a per-session seen-list (shown.json) and
    #      never re-injects a memory shown this session. So the recall in stored
    #      history is the ONLY copy — storing clean would lose it (a memory gap).
    history = _load_history(session_file)

    # Two ways a tool-loop tail ends up on the working copy at save time:
    #   * in_tool_loop      — HARNESS-owned: the inbound request already carried
    #                         [user, assistant(tool_calls), tool(result)…] (set at
    #                         load by _contains_tool_loop_messages).
    #   * intercept.loop_ran — BRIDGE-owned: the executor's handle_tool_calls /
    #                         mcp_client loop spliced the tool turns onto
    #                         ctx.request.messages mid-request (set in
    #                         pipeline_executor._dispatch_intercepts).
    # Either way the working-copy tail is [user, assistant(tool_calls),
    # tool(result)…] and we persist the FULL exchange — the real tool_calls +
    # results + the agent's mid-loop narration — so its history is a lab
    # notebook, not a theatre script. _loop_exchange_to_store is agnostic to who
    # owned the loop (it just walks the tail from the last user turn).
    in_tool_loop = ctx.plugin_data.get("basic_session.in_tool_loop")
    bridge_loop_ran = ctx.plugin_data.get("intercept.loop_ran")
    if in_tool_loop or bridge_loop_ran:
        # Tool-loop just CLOSED (assistant_turn is final text). Persist the full
        # exchange so the agent remembers running the tool:
        #   user + assistant(tool_calls) + tool(result)… + final assistant(text)
        # The user turn + the tool turns come from the working-copy tail
        # (= ctx.request.messages, which carries the bridge_context on its user
        # turn from assemble_and_sign). The final text turn comes from ctx.response.
        loop_messages = _loop_exchange_to_store(ctx.request.messages)
        if not loop_messages:
            return
        # Stamp the whole exchange with one save-time `ts` (a tool-loop is one
        # moment). Existing turns already in `history` keep their own stamp.
        now = int(time.time())
        for turn in loop_messages:
            _stamp(turn, now)
        _stamp(assistant_turn, now)
        history.extend(loop_messages)
        history.append(assistant_turn)
        _save_history(session_file, history)
        owner = "harness" if in_tool_loop else "bridge"
        logger.info(
            f"basic_session: '{session_key}' — {owner}-owned tool-loop closed, "
            f"saved full exchange ({len(loop_messages)} tool message(s) + reply), "
            f"history now {len(history)} message(s)"
        )
        return

    # Plain turn: user + assistant pair.
    user_turn = _extract_latest_user_message(ctx.request.messages)
    if user_turn is None:
        return
    # Stamp the pair with one save-time `ts` (a REUSABLE per-turn primitive;
    # the send window reads it, but it's not window-only).
    now = int(time.time())
    _stamp(user_turn, now)
    _stamp(assistant_turn, now)
    history.append(user_turn)
    history.append(assistant_turn)
    # Storage is always full — no trim on save.
    _save_history(session_file, history)
    logger.info(
        f"basic_session: '{session_key}' — saved, history now {len(history) // 2} turn(s)"
    )


# ---------------------------------------------------------------------------
# background — schedule the daily reset-archive (mode: daily)
# ---------------------------------------------------------------------------

def start_background(ctx: "StartupCtx", config: dict):
    """Schedule the daily session-reset archive for ``session_reset.mode: daily``.

    Returns a runner coroutine (cron-style sleeper loop) that, at the configured
    wall-clock boundary, renames the session file to a dated archive beside it,
    labelled with the DAY THAT JUST ENDED (not the boundary morning) so the
    filename date matches the content — e.g. a 04:00 reset on 2026-06-15 renames
    ``agent.json`` → ``agent.2026-06-14T0400.json`` (June 14's turns). The next read sees no file
    → the always-on ``not history → fresh_file`` path stamps ``is_new`` →
    conversational_memory wipes shown-state and fires the wakeup cascade.

    Returns ``None`` (no task spawned) for ``manual`` / ``never`` / no reset — those
    modes don't need a scheduler. Core (`server._spawn_background_tasks`) calls this
    once per identity that wires basic_session on a slot, with the cascade-merged
    ``basic_session`` config.
    """
    reset_cfg = (config or {}).get("session_reset") or {}
    mode = (reset_cfg.get("mode") or "never").lower()
    if mode != "daily":
        return None  # only `daily` needs an out-of-band scheduler

    # Resolve the session file path at startup from the cascade (no request ctx
    # here). basic_session's key IS the named session block — resolve_cascade
    # gives us the identity's role→session name.
    from app import cascade as cascade_mod

    cascade = cascade_mod.resolve_cascade(ctx.identity_key)
    session_key = cascade.session_key if cascade else None
    if not session_key:
        logger.warning(
            f"basic_session: identity '{ctx.identity_key}' has session_reset.mode=daily "
            f"but no resolvable session name — no reset scheduled."
        )
        return None
    session_file = _session_file(_sanitise_key(session_key), config)

    # Boundary 'HH:MM' → a daily cron expression in the resolved tz.
    # Resolution order (most specific wins): a `timezone:` on this session_reset
    # block → an identity-level hint → the global `server.timezone` → UTC. Any
    # plugin that needs a tz follows this same ladder, so a future bridge with
    # per-session timezones (different sessions on different clocks) just sets
    # `session_reset.timezone` and the bridge honours it — it doesn't care why.
    at = _parse_hhmm(reset_cfg.get("at", "04:00"))
    cron_expr = f"{at.minute} {at.hour} * * *"
    tz_name = (
        reset_cfg.get("timezone")
        or _identity_timezone(ctx.identity_cfg)
        or (ctx.server_cfg or {}).get("timezone")
        or "UTC"
    )

    # Validate the expr loud at startup, like cron does.
    try:
        from croniter import croniter
        if not croniter.is_valid(cron_expr):
            raise ValueError("invalid cron expression")
    except ImportError:
        logger.error(
            "basic_session: the 'croniter' package is not installed — daily reset "
            "disabled. It's declared in the bridge's main requirements.txt."
        )
        return None
    except Exception as e:
        logger.warning(
            f"basic_session: identity '{ctx.identity_key}' bad daily-reset boundary "
            f"'{reset_cfg.get('at')}' ({e}) — no reset scheduled."
        )
        return None

    logger.info(
        f"basic_session: identity '{ctx.identity_key}' daily reset-archive scheduled "
        f"'{cron_expr}' [{tz_name}] for session '{session_key}'"
    )
    return _run_reset_loop(ctx.identity_key, session_key, session_file, cron_expr, tz_name)


async def _run_reset_loop(
    identity_key: str, session_key: str, session_file: str, cron_expr: str, tz_name: str
) -> None:
    """Sleep until the next boundary, archive the session file, repeat. One bad
    fire is logged and swallowed; the loop continues. CancelledError re-raises
    for clean shutdown (mirrors cron's runner)."""
    from croniter import croniter

    tz = _resolve_tz(tz_name)
    while True:
        now = datetime.now(tz)
        nxt = croniter(cron_expr, now).get_next(datetime)
        delay = max(0.0, (nxt - now).total_seconds())
        logger.debug(
            f"basic_session: reset for '{session_key}' next fire at {nxt.isoformat()} "
            f"(in {delay:.0f}s)"
        )
        try:
            await asyncio.sleep(delay)
            _archive_session_family(session_key, session_file, tz)
        except asyncio.CancelledError:
            logger.info(
                f"basic_session: reset loop for '{session_key}' cancelled — shutting down"
            )
            raise
        except Exception as e:
            logger.error(
                f"basic_session: reset for '{session_key}' (identity '{identity_key}') "
                f"tick error — {e!r} — loop continues.",
                exc_info=True,
            )


def _archive_session_family(session_key: str, session_file: str, tz) -> None:
    """Archive the base session file AND any partition siblings.

    basic_session stays basic: rather than teach the reset loop about the
    ``partitions`` config, we archive the sessions we FIND. Partition files live
    in their own subfolders (``_session_file`` nests ``<data_dir>/<key>/<key>.json``
    and partition keys are ``<base>__group__X`` / ``<base>__user__Y``), so we glob
    sibling folders whose name starts with the base key and archive each one's
    live ``.json``. Handles caller-mode's unpredictable per-caller names for free,
    and stays correct if a config is later removed — we act on the filesystem, not
    the config. No partitions configured → the glob finds exactly the base folder.

    ``session_file`` is ``<data_dir>/<key>/<key>.json``; its grandparent is
    ``data_dir``, and its parent-folder basename is the (sanitised) key."""
    session_dir = os.path.dirname(session_file)        # <data_dir>/<key>
    data_dir = os.path.dirname(session_dir)            # <data_dir>
    base_folder = os.path.basename(session_dir)        # <key> (== session_key)

    # Sibling folders: the base key itself + any "<base>__..." partition folders.
    archived_any = False
    try:
        entries = sorted(os.listdir(data_dir)) if os.path.isdir(data_dir) else []
    except OSError:
        entries = []
    for name in entries:
        if name != base_folder and not name.startswith(f"{base_folder}__"):
            continue
        live = os.path.join(data_dir, name, f"{name}.json")
        if os.path.exists(live):
            _archive_session_file(name, live, tz)
            archived_any = True

    if not archived_any:
        logger.info(
            f"basic_session: reset for '{session_key}' — no session files to "
            f"archive (already fresh / nothing spoken yet); skipping."
        )


def _archive_session_file(session_key: str, session_file: str, tz) -> None:
    """Rename the live session file to a dated archive beside it. No file = the
    reset already effectively happened (a hard delete, or nothing spoken yet);
    nothing to do."""
    if not os.path.exists(session_file):
        logger.info(
            f"basic_session: reset for '{session_key}' — no session file to archive "
            f"(already fresh); skipping."
        )
        return
    # Label the archive with the DAY THAT JUST ENDED, not the boundary morning.
    # The reset fires at ~04:00, but the conversation inside is yesterday's day —
    # so we back up to the previous calendar day. This keeps the filename date
    # honest (a file named 2026-07-01 holds July 1's turns) for humans and any
    # date-eyeballing downstream. The time portion stays the boundary wall-clock.
    now = datetime.now(tz)
    stamp = f"{(now - timedelta(days=1)).strftime('%Y-%m-%d')}T{now.strftime('%H%M')}"
    root, ext = os.path.splitext(session_file)
    archive_path = f"{root}.{stamp}{ext}"
    # Avoid clobbering a same-minute archive (e.g. a manual fire + the scheduled one).
    if os.path.exists(archive_path):
        archive_path = f"{root}.{stamp}.{int(now.timestamp())}{ext}"
    os.rename(session_file, archive_path)
    logger.info(
        f"basic_session: reset for '{session_key}' — archived session to "
        f"'{os.path.basename(archive_path)}'; next read is a fresh session."
    )


def _identity_timezone(identity_cfg: dict | None) -> str | None:
    """Best-effort identity-level tz hint — a `timezone:` directly on the identity,
    or under context.additional. Mirrors cron's `_identity_timezone`. We don't have
    ctx.timezone at startup (that's a PipelineCtx field), so this is one rung of the
    ladder: session_reset.timezone → identity hint → server.timezone → UTC."""
    if not isinstance(identity_cfg, dict):
        return None
    tz = identity_cfg.get("timezone")
    if tz:
        return str(tz)
    context = identity_cfg.get("context") or {}
    additional = context.get("additional") or {}
    tz = additional.get("timezone")
    return str(tz) if tz else None


# ---------------------------------------------------------------------------
# Reset policy — never (default) | daily | manual
# ---------------------------------------------------------------------------
#
# `daily` is EVENT-driven, not read-driven. A background scheduler
# (`start_background`, below) archives the session file at the configured
# wall-clock boundary; the next read then hits the always-on empty-file path
# (`not history → fresh_file`) and conversational_memory wakes the cascade off
# `session_state.is_new`. This deliberately replaced the old mtime-comparison
# heuristic: a cron/heartbeat turn that writes the session file mid-day used to
# bump the mtime past the boundary and silently consume the reset before the
# human's morning turn. An event can't be raced that way — the archive either
# happened or it didn't. (The gap is a feature: the boundary is a ritual the
# agent's pre-gap reflection can prepare for.) `_check_reset` therefore only
# handles `manual` now; `daily`/`never` are continuations here.

def _check_reset(ctx: "PipelineCtx", session_file: str, config: dict) -> tuple[bool, str]:
    """For a non-empty session, decide whether a configured soft reset makes
    it fresh on THIS read. Returns (is_new, reason). Only ``manual`` resets on
    a read (via header); ``daily`` is handled out-of-band by the archive
    scheduler, and ``never`` (default) is always a continuation."""
    reset_cfg = config.get("session_reset") or {}
    mode = (reset_cfg.get("mode") or "never").lower()

    if mode == "manual":
        flag = ctx.headers.get(_MANUAL_RESET_HEADER, "")
        if str(flag).strip().lower() in ("1", "true", "yes", "reset"):
            return True, "manual_reset"
        return False, "continuation"

    return False, "continuation"


# ---------------------------------------------------------------------------
# System prompt builder (lifted from V3 — earned its place)
# ---------------------------------------------------------------------------

def _build_system_prompt(upstream_system: str | None, config: dict) -> str | None:
    files = config.get("system_prompt")
    append = config.get("system_prompt_append")

    if files:
        # Replace upstream entirely — concatenate files in order.
        parts = []
        for path in files:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if content:
                        parts.append(content)
            except FileNotFoundError:
                logger.warning(f"basic_session: system_prompt file not found: '{path}'")
            except Exception as e:
                logger.warning(f"basic_session: could not read '{path}' — {e}")
        base = "\n\n".join(parts) if parts else (upstream_system or "")
    else:
        base = upstream_system or ""

    if append:
        base = f"{base}\n\n{append}".strip() if base else append

    return base or None


# ---------------------------------------------------------------------------
# Session key + file helpers
# ---------------------------------------------------------------------------

def _resolve_session_key(ctx: "PipelineCtx", config: dict) -> str:
    """The session key IS the named session block (``role.session: <name>``),
    optionally narrowed by a ``partitions`` map.

    A session is a top-level named block in V4, shared by name-and-reference —
    multiple identities can point at one role/session and MUST share its history
    (D-006 / CLAUDE.md). So the base key is the session's own name, not the
    caller's: two identities on one role (e.g. ``*_librechat`` + ``*_openclaw``)
    resolve to the SAME file → cross-harness conversation continuity.

    ``basic_session`` only loads from a ``sessions.<name>`` block (its CAPABILITIES
    restrict it to ``session.plugins``), so ``ctx.role.session_key`` is always set
    when this runs — a session that isn't defined doesn't exist, and this code
    wouldn't be running. A missing key means broken state, so we raise rather than
    silently writing to a surprise file.

    **Partitions** (optional) split one session block into several files keyed by
    CALLER — the signed ``<caller>`` name (``ctx.identity.name``, the value that
    lands in the tamper-proof ``<caller>`` tag). With no ``partitions`` key the
    base key is returned unchanged (zero behaviour change — the bridge does nothing
    until configured). See ``_partition_suffix`` for the match rules."""
    if not ctx.role.session_key:
        raise RuntimeError(
            "basic_session: no session name on ctx.role.session_key — basic_session "
            "must be wired inside a sessions.<name> block. This should be unreachable "
            f"(identity={ctx.identity.key!r}, role={ctx.role.key!r})."
        )
    base = _sanitise_key(ctx.role.session_key)
    suffix = _partition_suffix(ctx.identity.name, config)
    return f"{base}{suffix}" if suffix else base


def _partition_suffix(caller: str | None, config: dict) -> str:
    """Resolve the file-name suffix for a caller under a session's ``partitions``
    map. Returns ``""`` (the shared base file) when partitions are unset or the
    caller falls through to ``shared``.

    Config shape::

        partitions:
          team_a:               # named group → suffix "__group__team_a"
            - alice
            - bob
          default: shared       # shared (one file) | caller (per-caller isolation)

    Match rules:
      - Named group whose member list contains the caller (case-insensitive).
        First match wins by config declaration order.
      - No named match + ``default: caller`` → suffix "__user__<caller>".
      - No named match + ``default: shared`` (or unset) → "" (base file).
      - No caller name at all (no ``context.name``) → cannot match a group →
        falls to ``default``. Partitions make ``context.name`` load-bearing for
        session routing; a nameless caller can only ever land in the shared file.

    Distinct ``__group__`` / ``__user__`` namespaces keep a group literally named
    like a caller from colliding with that caller's per-caller file."""
    partitions = config.get("partitions")
    if not isinstance(partitions, dict) or not partitions:
        return ""

    default = str(partitions.get("default", "shared")).lower()
    if default not in ("shared", "caller"):
        logger.warning(
            f"basic_session: partitions.default={default!r} is not "
            f"'shared' or 'caller' — treating as 'shared'."
        )
        default = "shared"

    norm_caller = caller.lower() if caller else None

    # Named-group match: first group (config order) whose members include caller.
    matched: str | None = None
    if norm_caller is not None:
        for group, members in partitions.items():
            if group == "default":
                continue
            if not isinstance(members, list):
                continue
            member_set = {str(m).lower() for m in members}
            if norm_caller in member_set:
                if matched is not None:
                    # One connection = one AI = one session. A caller in two
                    # groups is a config error, not a feature. Shout, keep first.
                    logger.warning(
                        f"basic_session: caller {caller!r} appears in multiple "
                        f"partitions (matched '{matched}', also in '{group}') — "
                        f"a caller belongs to ONE partition. Using first match "
                        f"'{matched}'; fix the config."
                    )
                    continue
                matched = group
    if matched is not None:
        return f"__group__{_sanitise_key(matched)}"

    if default == "caller" and caller:
        return f"__user__{_sanitise_key(caller)}"
    return ""


def _sanitise_key(key: str) -> str:
    """Strip characters unsafe for filenames."""
    return re.sub(r"[^\w\-:.]", "_", key)


def _session_file(session_key: str, config: dict) -> str:
    """Path to a session's live file, nested under its OWN subfolder:
    ``<data_dir>/<session_key>/<session_key>.json``. The folder boundary makes
    each agent's session (live file + dated archives + .bak siblings, which all
    land beside it) a clean per-agent mount target. ``session_key`` is already
    ``_sanitise_key``'d at both call sites, so it's filesystem-safe as a dir name."""
    data_dir = config.get("data_dir", _DEFAULT_DATA_DIR)
    session_dir = os.path.join(data_dir, session_key)
    os.makedirs(session_dir, exist_ok=True)
    return os.path.join(session_dir, f"{session_key}.json")


def _resolve_max_turns(config: dict) -> int | None:
    """The SEND window in user/agent pairs. None == unlimited. Storage is
    unaffected — this only governs what we forward upstream."""
    raw = config.get("max_turns", _DEFAULT_MAX_TURNS)
    if raw is False or raw is None or raw == 0:
        return None
    return int(raw)


_DURATION_UNITS = {"m": 60, "h": 3600, "d": 86400}  # minutes / hours / days


def _parse_duration(raw) -> float | None:
    """Parse a duration string like ``"36h"`` / ``"2d"`` / ``"90m"`` / ``"0.5h"``
    into SECONDS. Returns None (no window) for a falsy/unset value, and
    warn+None for a malformed one (fail-loud-not-fail — a typo disables the
    window rather than crashing the session)."""
    if raw is None or raw is False or raw == "":
        return None
    s = str(raw).strip().lower()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([mhd])", s)
    if not m:
        logger.warning(
            f"basic_session: send_window '{raw}' is not a valid duration "
            f"(expected e.g. '36h', '2d', '90m') — no time window applied."
        )
        return None
    value, unit = float(m.group(1)), m.group(2)
    return value * _DURATION_UNITS[unit]


def _resolve_send_window(config: dict) -> float | None:
    """The time-based SEND window in SECONDS (None == off). Turns older than
    ``now - seconds`` (or with no ``ts``) drop off the wire. Storage is
    unaffected. Composes with ``max_turns`` — tightest wins."""
    return _parse_duration(config.get("send_window", _DEFAULT_SEND_WINDOW))


# ---------------------------------------------------------------------------
# History I/O (lifted from V3)
# ---------------------------------------------------------------------------

def _load_history(path: str) -> list[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning(f"basic_session: could not load '{path}' — {e}")
    return []


def _save_history(path: str, history: list[dict]) -> None:
    """Atomically persist history: write a temp file beside the target, then
    ``os.replace`` it into place. os.replace is atomic on POSIX, so a reader
    (``_load_history``) never sees a half-written file — which would parse-fail
    and be silently swallowed as an EMPTY session (catastrophic: the whole
    conversation reads as gone). Belt-and-suspenders alongside the per-file save
    lock: the lock serializes bridge-internal writers, the atomic rename also
    guards a reader mid-write and any writer that crashes partway."""
    tmp_path = f"{path}.tmp.{os.getpid()}"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
    except Exception as e:
        logger.error(f"basic_session: could not save '{path}' — {e}")
        # Don't leave a stray temp file behind on failure.
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass


def _stamp(turn: dict, now: int | None = None) -> dict:
    """Stamp a turn dict with the bridge-internal ``ts`` (Unix epoch) on SAVE,
    unless it already carries one. Idempotent and mutate-in-place (returns the
    same dict for chaining). A REUSABLE primitive — the send window is its first
    reader; keep it general, don't couple it to windowing."""
    if isinstance(turn, dict) and _TS_KEY not in turn:
        turn[_TS_KEY] = int(time.time()) if now is None else now
    return turn


def _strip_ts(messages: list[dict]) -> list[dict]:
    """Return copies of ``messages`` with the bridge-internal ``ts`` removed, so
    the field never rides upstream to a provider. Only copies dicts that carry
    ``ts`` (untouched ones pass through by reference — cheap)."""
    out: list[dict] = []
    for m in messages:
        if isinstance(m, dict) and _TS_KEY in m:
            m = {k: v for k, v in m.items() if k != _TS_KEY}
        out.append(m)
    return out


# ---------------------------------------------------------------------------
# Message extraction / assembly
# ---------------------------------------------------------------------------

def _extract_latest_user_message(
    messages: list[dict], flatten: bool = True
) -> dict | None:
    """Return the last user message as a clean ``{"role": "user", "content": ...}`` dict.

    ``flatten=True`` (default, used on the SAVE path): multi-part vision content is
    collapsed to its text parts — storage stays text-only for now (the full
    multimodal-session shape is a separate, parked design question). Returns None
    if there's no text to store.

    ``flatten=False`` (used on the SEND/wire path): multi-part content is preserved
    VERBATIM so image_url / other parts reach the model. Without this the wire
    rebuild would silently drop the image — the agent receives text only and
    "sees nothing" (the LibreChat/curl image leak, diagnosed 2026-06-21: the bridge
    RECEIVED the image but this extractor flattened it off the wire)."""
    for msg in reversed(messages):
        if msg.get("role") == "user":
            content = msg.get("content", "")
            if isinstance(content, list):
                if not flatten:
                    # Preserve the multi-part content verbatim (keeps image_url etc.).
                    # Empty only if there are literally no parts.
                    return {"role": "user", "content": content} if content else None
                parts = [p.get("text", "") for p in content if p.get("type") == "text"]
                text = " ".join(parts).strip()
            else:
                text = str(content).strip()
            return {"role": "user", "content": text} if text else None
    return None


def _extract_system(messages: list[dict]) -> str | None:
    for msg in messages:
        if msg.get("role") == "system":
            content = msg.get("content", "")
            return str(content).strip() or None
    return None


def _strip_system_messages(messages: list[dict]) -> list[dict]:
    """Return ``messages`` without any ``system`` turns. Used by the transparent
    harness-tool-loop path: the bridge supplies its OWN system prompt (built from
    config + bridge_context), so the harness's system message must not leak a
    second one into the forwarded thread. Non-system turns pass through verbatim
    (tool groups intact)."""
    return [m for m in messages if m.get("role") != "system"]


# ---------------------------------------------------------------------------
# Harness tool-loop detection (the harness owns the tools, runs its own loop)
# ---------------------------------------------------------------------------

def _last_user_index(messages: list[dict]) -> int | None:
    """Index of the last ``user`` message, or None if there isn't one."""
    for i in range(len(messages) - 1, -1, -1):
        if messages[i].get("role") == "user":
            return i
    return None


def _contains_tool_loop_messages(tail: list[dict]) -> bool:
    """True if the slice carries harness tool-loop state — a ``tool`` result
    message, or an ``assistant`` turn with ``tool_calls``. When the harness
    (LibreChat, OpenClaw, …) owns the tools and runs the loop OUTSIDE the
    bridge, it re-hits us with the closed thread so the model can continue.
    Replacing that tail with just the clean user text would erase the model's
    own tool_calls + the results → it re-emits the same call → infinite loop."""
    for msg in tail:
        role = msg.get("role")
        if role == "tool":
            return True
        if role == "assistant" and msg.get("tool_calls"):
            return True
    return False


def _loop_exchange_to_store(messages: list[dict]) -> list[dict]:
    """Pull the harness tool-loop exchange to persist from the working copy:
    the last user turn (verbatim — it carries the bridge_context injected this
    turn) plus the harness ``assistant(tool_calls)`` and ``tool(result)`` turns
    that follow it. Returned verbatim so the stored group stays well-formed
    (tool messages keep their parent assistant turn). Does NOT include the final
    assistant(text) reply — the caller appends that from ctx.response. Empty if
    there's no user turn."""
    idx = _last_user_index(messages)
    if idx is None:
        return []
    # Everything from the last user turn onward EXCEPT any trailing assistant
    # text turn the resource step may have left on the working copy (the real
    # final reply is rebuilt from ctx.response by the caller). Tool-loop tails
    # from the harness end at a tool result, so this is usually the whole slice.
    tail = messages[idx:]
    out: list[dict] = []
    for msg in tail:
        # Keep user, assistant(tool_calls), and tool messages verbatim.
        role = msg.get("role")
        if role == "assistant" and not msg.get("tool_calls"):
            continue  # skip any stray plain-text assistant turn; reply comes from ctx.response
        out.append(_strip_stored_reasoning_details(msg))
    return out


def _strip_stored_reasoning_details(msg: dict) -> dict:
    """Drop the per-token `reasoning_details` array from a turn before storage.
    The spliced assistant(tool_calls) turns on the working copy carry it because
    the executor put it there for the WIRE re-call (see
    pipeline_executor._build_assistant_turn_from_response). Storage only needs
    the completed `reasoning`/`reasoning_content` string artifact — the array is
    bloat nothing reads back (same rule as _RICH_KEYS / the final turn). Returns
    a shallow copy so the wire copy in ctx.request.messages stays untouched."""
    if not isinstance(msg, dict) or "reasoning_details" not in msg:
        return msg
    cleaned = dict(msg)
    # Preserve the thinking text if no string key survived (array-only edge case).
    if not cleaned.get("reasoning") and not cleaned.get("reasoning_content"):
        collapsed = _collapse_reasoning_details(cleaned.get("reasoning_details"))
        if collapsed:
            cleaned["reasoning"] = collapsed
    cleaned.pop("reasoning_details", None)
    return cleaned


def _tool_group_safe_window(history: list[dict], max_messages: int) -> list[dict]:
    """Trim ``history`` to the last ``max_messages`` WITHOUT orphaning a tool
    group. A ``tool`` message must be preceded by the ``assistant`` turn whose
    ``tool_calls`` spawned it, or providers 400. A naive ``[-max_messages:]``
    slice can land the head boundary in the middle of an
    ``assistant(tool_calls) → tool(result)…`` group, leaving a leading orphan
    ``tool``. If the first kept message is a ``tool``, walk the boundary back to
    include its parent assistant turn.

    Runs BEFORE ``_degrade_tool_history`` so the fold operates on a
    well-formed window."""
    if max_messages is None or len(history) <= max_messages:
        return history
    start = len(history) - max_messages
    # Back the boundary up over any leading orphan tool messages, then over the
    # assistant(tool_calls) parent that precedes them.
    while start > 0 and history[start].get("role") == "tool":
        start -= 1
    return history[start:]


def _drop_empty_assistant_turns(messages: list[dict]) -> list[dict]:
    """Drop assistant turns that carry NEITHER content NOR tool_calls — the WIRE
    can't take them (Moonshot 400: "the message at position N with role
    'assistant' must not be empty"). Wire-only: storage keeps them.

    WHY THEY EXIST ON DISK: the streaming save-loss fix persists a turn whose
    bridge-owned tool loop closed without a final text lap (a Moonshot quirk — it
    sometimes returns a completely empty close-out lap after a tool result). We
    store an empty placeholder so the tool call + result around it are NOT lost.
    That's right for the lab-notebook; it's poison on the wire.

    TOOL-GROUP SAFE BY CONSTRUCTION: an assistant turn WITH ``tool_calls`` is
    KEPT (a bare tool call with empty content is legitimate and is the parent of
    the ``tool`` messages that follow). We only drop turns with no tool_calls, and
    those never have tool children — so dropping one can never orphan a ``tool``
    message. Reasoning-only turns (content empty but reasoning present) are also
    dropped from the wire: the provider still sees an empty assistant message."""
    out: list[dict] = []
    for m in messages:
        if (
            isinstance(m, dict)
            and m.get("role") == "assistant"
            and not m.get("tool_calls")
        ):
            content = m.get("content")
            if content is None or (isinstance(content, str) and not content.strip()):
                continue  # empty + no tool_calls → never goes upstream
        out.append(m)
    return out


def _time_window(history: list[dict], seconds: float | None) -> list[dict]:
    """Drop turns older than ``now - seconds`` from the SEND view (storage
    untouched). ``seconds is None`` → no time window (pass through).

    UN-STAMPED = OLD: a turn with no ``ts`` is by definition pre-feature, so it
    falls OUTSIDE any window — the absence of a timestamp is the back-compat
    signal (no migration needed). When a session first gets a ``send_window``,
    its existing un-stamped turns drop off the wire (still on disk) and the
    window rebuilds rolling as new stamped turns land.

    Tool-group-safe: after the age cut, if the first kept message is a ``tool``,
    walk the boundary back to include its parent ``assistant(tool_calls)`` (same
    invariant as ``_tool_group_safe_window``) so a strict provider never 400s on
    a leading orphan tool result."""
    if seconds is None or not history:
        return history
    cutoff = time.time() - seconds
    # First index that is IN-window (stamped and fresh enough). Scan from the
    # front; everything before `start` is old/un-stamped and drops.
    start = 0
    while start < len(history):
        ts = history[start].get(_TS_KEY)
        if isinstance(ts, (int, float)) and ts >= cutoff:
            break
        start += 1
    if start >= len(history):
        return []  # nothing fresh enough
    # Don't leave a leading orphan `tool`: back up over any tool messages and
    # their parent assistant(tool_calls) so the kept slice stays well-formed.
    while start > 0 and history[start].get("role") == "tool":
        start -= 1
    return history[start:]


# ---------------------------------------------------------------------------
# degrade_tool_history — wire-only fold of OLD tool exchanges to assistant prose
# ---------------------------------------------------------------------------
#
# Storage stays rich; the wire gets a windowed, optionally-folded view. Same
# shape as max_turns (full storage, windowed wire), one rung up. The fold turns
# OLD assistant(tool_calls) + tool(result) groups into a SINGLE assistant
# message carrying paired <tool type="call|result" name="…"> tags — the agent
# still infers its own toolbelt from past calls, while the wire stops dragging
# old (possibly harness-truncated) plumbing turn-over-turn.
#
# Invariants (load-bearing, see TestDegradeToolHistory):
#   * Fold names ONLY the bare tool ``name`` from the OpenAI tool_calls payload.
#     NEVER harness/MCP-server/source labels. Agents are harness-portable; the
#     fold MUST NOT leak which harness or server produced the turn.
#   * ``truncated="harness"`` is the ONLY hint about lossiness, set when the
#     result content contains ``[truncated:`` (harness left its marker). Honest
#     without naming names — applies to LibreChat, OpenClaw, anything future.
#   * Storage on disk is NEVER touched by this code path.

def _resolve_degrade(config: dict) -> bool:
    return bool(config.get("degrade_tool_history", False))


def _degrade_tool_history(messages: list[dict], keep_recent: int = 1) -> list[dict]:
    """Fold OLD ``assistant(tool_calls)`` + ``tool(result)…`` groups in
    ``messages`` into one ``role:assistant`` prose message each, with paired
    ``<tool type="call|result" name="…">`` tags. Keeps the most-recent
    ``keep_recent`` groups verbatim. Wire-only — caller is responsible for
    never persisting the result."""
    groups = _find_tool_groups(messages)
    if len(groups) <= keep_recent:
        return messages
    fold_groups = groups[:-keep_recent] if keep_recent > 0 else groups
    fold_starts = {g[0]: g for g in fold_groups}

    out: list[dict] = []
    skip_until = -1
    for i, msg in enumerate(messages):
        if i < skip_until:
            continue
        if i in fold_starts:
            start, end = fold_starts[i]
            out.append(_fold_group(messages[start:end]))
            skip_until = end
            continue
        out.append(msg)
    return out


def _find_tool_groups(messages: list[dict]) -> list[tuple[int, int]]:
    """Locate ``(start, end)`` spans for each ``assistant(tool_calls)`` +
    following ``tool`` messages group. End is exclusive."""
    groups: list[tuple[int, int]] = []
    i = 0
    n = len(messages)
    while i < n:
        msg = messages[i]
        if msg.get("role") == "assistant" and msg.get("tool_calls"):
            j = i + 1
            while j < n and messages[j].get("role") == "tool":
                j += 1
            groups.append((i, j))
            i = j
        else:
            i += 1
    return groups


def _fold_group(group: list[dict]) -> dict:
    """Build the single assistant prose message that replaces a tool exchange
    group. Multi-call assistant turns (parallel ``tool_calls``) fold to ONE
    assistant message with multiple ``<tool>`` tag pairs inside."""
    assistant_msg = group[0]
    tool_results = {
        m.get("tool_call_id"): m
        for m in group[1:]
        if m.get("role") == "tool"
    }

    parts: list[str] = []
    for tc in assistant_msg.get("tool_calls") or []:
        fn = tc.get("function") or {}
        name = fn.get("name") or "unknown"
        args = fn.get("arguments", "")
        # Args arrive as a JSON string per OpenAI protocol; re-pretty for
        # cleanliness, fall back to raw on parse failure (never strip).
        try:
            args_clean = json.dumps(json.loads(args), ensure_ascii=False)
        except (json.JSONDecodeError, TypeError, ValueError):
            args_clean = args if isinstance(args, str) else json.dumps(args, ensure_ascii=False)
        parts.append(
            f'<tool type="call" name="{_escape_attr(name)}">{args_clean}</tool>'
        )

        result_msg = tool_results.get(tc.get("id"))
        if result_msg is not None:
            content = result_msg.get("content", "")
            content_str = (
                content if isinstance(content, str)
                else json.dumps(content, ensure_ascii=False)
            )
            truncated_attr = (
                ' truncated="harness"' if "[truncated:" in content_str else ""
            )
            parts.append(
                f'<tool type="result" name="{_escape_attr(name)}"{truncated_attr}>'
                f'{content_str}</tool>'
            )

    return {"role": "assistant", "content": "\n".join(parts)}


def _escape_attr(value: str) -> str:
    return (
        str(value)
        .replace('&', '&amp;')
        .replace('"', '&quot;')
        .replace('<', '&lt;')
    )


def _collapse_reasoning_details(details: object) -> str | None:
    """Join an OpenRouter `reasoning_details` array down to its text. Used only
    when the provider sent the structured array but no reasoning string, so the
    thinking text survives without storing thousands of per-token entries."""
    if not isinstance(details, list):
        return None
    parts = [
        entry.get("text")
        for entry in details
        if isinstance(entry, dict) and isinstance(entry.get("text"), str)
    ]
    joined = "".join(p for p in parts if p)
    return joined or None


def _build_rich_assistant_turn(response: dict | None) -> dict | None:
    """Build the assistant turn to persist from ctx.response. Stores rich
    fields (reasoning_content, tool_calls) when the provider returned them so
    replay survives picky backends. Returns None if there's nothing usable."""
    if not isinstance(response, dict):
        return None

    # Prefer the full upstream message (carries reasoning_content / tool_calls);
    # fall back to the flattened content the executor put on ctx.response.
    message = _full_response_message(response)
    if message is None:
        content = response.get("content")
        message = {"role": "assistant", "content": content} if content is not None else None
    if message is None:
        return None

    turn: dict = {"role": "assistant", "content": message.get("content")}
    # Preserve rich fields verbatim so replay survives picky providers. Reasoning
    # arrives under different keys per provider — Moonshot uses `reasoning_content`,
    # OpenRouter uses `reasoning`. Keep whatever was sent; don't normalise
    # (replaying the same key the provider gave us is the safe bet). The per-token
    # `reasoning_details` array is intentionally excluded from _RICH_KEYS (see note
    # there) — storage keeps the completed string artifact only.
    for rich_key in _RICH_KEYS:
        val = message.get(rich_key)
        if val:
            turn[rich_key] = val

    # Edge case: provider sent ONLY the structured array (no reasoning string).
    # Collapse it to a joined string so no thinking text is lost, then leave the
    # array unstored. (OpenRouter normally sends both, so this rarely fires.)
    if not turn.get("reasoning") and not turn.get("reasoning_content"):
        collapsed = _collapse_reasoning_details(message.get("reasoning_details"))
        if collapsed:
            turn["reasoning"] = collapsed

    # Nothing to say AND no tool call — not worth persisting.
    if not turn.get("content") and not turn.get("tool_calls"):
        return None
    return turn


def _full_response_message(response: dict) -> dict | None:
    """Dig the assistant message out of the preserved upstream envelope."""
    full = response.get("_full_response")
    if not isinstance(full, dict):
        return None
    choices = full.get("choices") or []
    if not choices:
        return None
    message = choices[0].get("message")
    return message if isinstance(message, dict) else None


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def _parse_hhmm(value: str) -> dt_time:
    """Parse 'HH:MM' → time. Falls back to 04:00 on malformed input."""
    try:
        hh, mm = str(value).split(":", 1)
        return dt_time(hour=int(hh), minute=int(mm))
    except (ValueError, AttributeError):
        logger.warning(f"basic_session: bad session_reset.at '{value}', using 04:00")
        return dt_time(hour=4, minute=0)


def _resolve_tz(timezone: str):
    """Resolve an IANA tz string to a tzinfo; fall back to system local."""
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(timezone)
    except Exception:
        logger.warning(f"basic_session: unknown timezone '{timezone}', using local")
        return datetime.now().astimezone().tzinfo
