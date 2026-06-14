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
boundary with ``session_reset`` (``daily`` rolls at a configured time; ``manual``
resets on an explicit header). A soft reset does NOT delete the file — it stamps
``is_new`` for the wake cascade and starts a fresh send-window; trim stays governed by
the (non-existent) storage limit, i.e. nothing is lost.

This plugin lives WHOLLY in one ``sessions.<name>.plugins`` block (D-010 lets
``post_response`` sit in ``session.plugins`` alongside ``outbound_params``):

    sessions:
      my_session:
        plugins:
          basic_session:
            max_turns: 20                    # SEND window; false/0 = send all. Storage is always full.
            data_dir: data/basic_session     # session file location (default as shown)
            system_prompt:                   # optional file list — replaces upstream system
              - /workspace/my_agent/SOUL.md
            system_prompt_append: "..."      # optional string appended to whatever system is used
            session_reset:                   # optional; OFF by default
              mode: never                    # never (default) | daily | manual
              at: "04:00"                    # daily only — boundary in server tz
            degrade_tool_history: false      # opt-in: fold OLD tool exchanges to
                                             # assistant prose on SEND (storage stays full).
                                             # Keeps the most-recent closed exchange raw.

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

Tool-calling, two ownerships:
  * **Bridge-owned** tools (``handle_tool_calls`` plugins) — the executor owns the
    loop between load and save; the session never sees intermediate laps.
  * **Harness-owned** tools (LibreChat / OpenClaw run their own MCP loop OUTSIDE the
    bridge) — the harness re-hits us mid-loop with the closed thread
    ``[user, assistant(tool_calls), tool(result), …]``. ``apply_outbound_params``
    detects this (tail after the last user turn carries tool/assistant-tool_calls
    messages) and splices the tail through VERBATIM, or the model never sees the tool
    ran and re-calls it forever. When the loop closes, ``observe_response`` stores the
    FULL exchange so the buddy's memory records that it ran the tool.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, time as dt_time, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.context import PipelineCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "outbound_params": ["session.plugins"],   # load + rebuild + stamp session_state
    "post_response":   ["session.plugins"],   # save the closed turn, after delivery (D-010)
}


_DEFAULT_MAX_TURNS = 20
_DEFAULT_DATA_DIR = "data/basic_session"

# Header the harness can send to force a manual reset (session_reset.mode: manual).
_MANUAL_RESET_HEADER = "x-session-reset"

# Rich assistant fields preserved on save so replay survives picky providers.
# Reasoning key varies by provider: Moonshot → reasoning_content; OpenRouter →
# reasoning (+ reasoning_details). tool_calls is preserved for completeness, though
# the loop-not-closed guard skips saving turns that still carry one.
_RICH_KEYS = ("reasoning_content", "reasoning", "reasoning_details", "tool_calls")


# ---------------------------------------------------------------------------
# outbound_params — load history, rebuild messages, stamp session_state
# ---------------------------------------------------------------------------

def apply_outbound_params(ctx: "PipelineCtx", config: dict) -> "PipelineCtx":
    """Load the session, rebuild the outbound message list, and stamp the
    authoritative ``session_state`` so the wakeup cascade fires on ground
    truth. Fires at executor step 1c (before context plugins)."""
    session_key = _resolve_session_key(ctx)
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
    # Tool-group-safe so windowing never orphans a tool message from its parent.
    max_messages = None if max_turns is None else max_turns * 2
    sent_history = _tool_group_safe_window(history, max_messages)

    # Optional: fold OLD tool exchanges to assistant prose on the wire (storage
    # untouched). Keeps the most-recent closed exchange raw so the buddy can
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
    new_messages.extend(sent_history)

    if in_tool_loop:
        # Splice the entire user→tool tail through verbatim so the model sees
        # its own tool_calls + the matching results. assemble_and_sign (step 4)
        # later injects the bridge_context into the LAST user message of the
        # working copy — which is the tail's user turn — so signing still works.
        #
        # NOTE: a tail ending in an unanswered assistant(tool_calls) is the LIVE
        # in-flight call — it MUST pass through so the harness/model loop can run
        # the tool and append the result. (We previously DROPPED it as a "400
        # repair" — that ate the call and caused an infinite loop: dropped call →
        # model re-calls → dropped again → ∞. The 400 it was avoiding is a
        # DIFFERENT, narrower case; never delete the live call. See
        # notes/DESIGN-transparent-harness-tool-loops.md.)
        new_messages.extend(tail)
        ctx.plugin_data["basic_session.in_tool_loop"] = True
        ctx.plugin_data["basic_session.loop_tail"] = tail
        logger.info(
            f"basic_session: '{session_key}' — harness tool-loop, "
            f"splicing {len(tail)} tail message(s) through verbatim"
        )
    else:
        latest_user = _extract_latest_user_message(inbound)
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

def observe_response(ctx: "PipelineCtx", config: dict) -> None:
    """Append the closed turn to the stored history. Fires fire-and-forget
    after the response has shipped (D-007 observer); sees only the final,
    closed assistant turn. Stores *rich* assistant fields (reasoning /
    tool_calls) so replay survives picky providers. When a harness tool-loop
    closes, stores the FULL exchange (user + tool turns + final text) so the
    buddy's memory records that it actually ran the tool."""
    session_file = ctx.plugin_data.get("basic_session.file")
    session_key = ctx.plugin_data.get("basic_session.key", "?")
    if not session_file:
        return  # apply_outbound_params didn't run (or session not wired) — nothing to save.

    assistant_turn = _build_rich_assistant_turn(ctx.response)
    if assistant_turn is None:
        return

    # Loop-not-closed guard: an assistant turn still carrying tool_calls means
    # the agent wants a tool, not to talk. Whether the loop is harness-owned
    # (in_tool_loop set at load) or bridge-owned (executor intercept), skip the
    # save — the next lap comes back through and we save when it closes with text.
    if assistant_turn.get("tool_calls"):
        logger.info(
            f"basic_session: '{session_key}' — assistant turn carries tool_calls, "
            f"skipping save (waiting for loop to close)"
        )
        return

    # We store the WORKING-copy user turn — i.e. WITH the bridge_context the
    # bridge injected this turn (caller + that turn's recalled memories). This is
    # what the buddy actually saw, so replaying it is the buddy's faithful memory.
    # Safe and not bloat:
    #   1. bridge_sign.verify_inbound only verifies the LAST user message and
    #      returns — never re-walks history — so a stored signed block is never
    #      re-verified/stripped on replay. No stale-signature downgrade.
    #   2. conversational_memory keeps a per-session seen-list (shown.json) and
    #      never re-injects a memory shown this session. So the recall in stored
    #      history is the ONLY copy — storing clean would lose it (a memory gap).
    history = _load_history(session_file)

    if ctx.plugin_data.get("basic_session.in_tool_loop"):
        # Harness tool-loop just CLOSED (assistant_turn is final text). Persist
        # the full exchange so the buddy remembers running the tool:
        #   user + assistant(tool_calls) + tool(result)… + final assistant(text)
        # The user turn + the harness tool turns come from the working-copy tail
        # (= ctx.request.messages, which carries the bridge_context on its user
        # turn from assemble_and_sign). The final text turn comes from ctx.response.
        loop_messages = _loop_exchange_to_store(ctx.request.messages)
        if not loop_messages:
            return
        history.extend(loop_messages)
        history.append(assistant_turn)
        _save_history(session_file, history)
        logger.info(
            f"basic_session: '{session_key}' — tool-loop closed, saved full "
            f"exchange ({len(loop_messages)} tool message(s) + reply), "
            f"history now {len(history)} message(s)"
        )
        return

    # Plain turn: user + assistant pair.
    user_turn = _extract_latest_user_message(ctx.request.messages)
    if user_turn is None:
        return
    history.append(user_turn)
    history.append(assistant_turn)
    # Storage is always full — no trim on save.
    _save_history(session_file, history)
    logger.info(
        f"basic_session: '{session_key}' — saved, history now {len(history) // 2} turn(s)"
    )


# ---------------------------------------------------------------------------
# Reset policy — never (default) | daily | manual
# ---------------------------------------------------------------------------

def _check_reset(ctx: "PipelineCtx", session_file: str, config: dict) -> tuple[bool, str]:
    """For a non-empty session, decide whether a configured soft reset makes
    it fresh. Returns (is_new, reason). Default (no/`never` config) is a
    continuation."""
    reset_cfg = config.get("session_reset") or {}
    mode = (reset_cfg.get("mode") or "never").lower()

    if mode == "manual":
        flag = ctx.headers.get(_MANUAL_RESET_HEADER, "")
        if str(flag).strip().lower() in ("1", "true", "yes", "reset"):
            return True, "manual_reset"
        return False, "continuation"

    if mode == "daily":
        if _crossed_daily_boundary(session_file, reset_cfg, ctx.timezone):
            return True, "daily_reset"
        return False, "continuation"

    return False, "continuation"


def _crossed_daily_boundary(session_file: str, reset_cfg: dict, timezone: str) -> bool:
    """True if the most recent daily-reset boundary (e.g. 04:00) falls between
    the session file's last write and now. The boundary is a wall-clock time
    in the server timezone; we compare against the file mtime so a session
    that last spoke yesterday-evening wakes fresh after the next 4am rolls by."""
    at = _parse_hhmm(reset_cfg.get("at", "04:00"))
    tz = _resolve_tz(timezone)

    try:
        mtime = os.path.getmtime(session_file)
    except OSError:
        return False  # no file to compare — empty-history path already handled it

    now = datetime.now(tz)
    last = datetime.fromtimestamp(mtime, tz)

    # Most recent boundary at-or-before now.
    boundary_today = now.replace(hour=at.hour, minute=at.minute, second=0, microsecond=0)
    last_boundary = boundary_today if now >= boundary_today else boundary_today - timedelta(days=1)

    return last < last_boundary


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

def _resolve_session_key(ctx: "PipelineCtx") -> str:
    """The session key IS the named session block (``role.session: <name>``).

    A session is a top-level named block in V4, shared by name-and-reference —
    multiple identities can point at one role/session and MUST share its history
    (D-006 / CLAUDE.md). So the key is the session's own name, not the caller's:
    two identities on one role (e.g. ``*_librechat`` + ``*_openclaw``) resolve to
    the SAME file → cross-harness conversation continuity.

    ``basic_session`` only loads from a ``sessions.<name>`` block (its CAPABILITIES
    restrict it to ``session.plugins``), so ``ctx.role.session_key`` is always set
    when this runs — a session that isn't defined doesn't exist, and this code
    wouldn't be running. A missing key means broken state, so we raise rather than
    silently writing to a surprise file."""
    if not ctx.role.session_key:
        raise RuntimeError(
            "basic_session: no session name on ctx.role.session_key — basic_session "
            "must be wired inside a sessions.<name> block. This should be unreachable "
            f"(identity={ctx.identity.key!r}, role={ctx.role.key!r})."
        )
    return _sanitise_key(ctx.role.session_key)


def _sanitise_key(key: str) -> str:
    """Strip characters unsafe for filenames."""
    return re.sub(r"[^\w\-:.]", "_", key)


def _session_file(session_key: str, config: dict) -> str:
    data_dir = config.get("data_dir", _DEFAULT_DATA_DIR)
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, f"{session_key}.json")


def _resolve_max_turns(config: dict) -> int | None:
    """The SEND window in user/agent pairs. None == unlimited. Storage is
    unaffected — this only governs what we forward upstream."""
    raw = config.get("max_turns", _DEFAULT_MAX_TURNS)
    if raw is False or raw is None or raw == 0:
        return None
    return int(raw)


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
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"basic_session: could not save '{path}' — {e}")


# ---------------------------------------------------------------------------
# Message extraction / assembly
# ---------------------------------------------------------------------------

def _extract_latest_user_message(messages: list[dict]) -> dict | None:
    """Return the last user message as a clean ``{"role": "user", "content": ...}``
    dict (multi-part vision content flattened to text). None if absent."""
    for msg in reversed(messages):
        if msg.get("role") == "user":
            content = msg.get("content", "")
            if isinstance(content, list):
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
        out.append(msg)
    return out


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


# ---------------------------------------------------------------------------
# degrade_tool_history — wire-only fold of OLD tool exchanges to assistant prose
# ---------------------------------------------------------------------------
#
# Storage stays rich; the wire gets a windowed, optionally-folded view. Same
# shape as max_turns (full storage, windowed wire), one rung up. The fold turns
# OLD assistant(tool_calls) + tool(result) groups into a SINGLE assistant
# message carrying paired <tool type="call|result" name="…"> tags — the buddy
# still infers its own toolbelt from past calls, while the wire stops dragging
# old (possibly harness-truncated) plumbing turn-over-turn.
#
# Invariants (load-bearing, see TestDegradeToolHistory):
#   * Fold names ONLY the bare tool ``name`` from the OpenAI tool_calls payload.
#     NEVER harness/MCP-server/source labels. Buddies are harness-portable; the
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
    # OpenRouter uses `reasoning` (+ `reasoning_details`). Keep whatever was sent;
    # don't normalise (replaying the same key the provider gave us is the safe bet).
    for rich_key in _RICH_KEYS:
        val = message.get(rich_key)
        if val:
            turn[rich_key] = val

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
