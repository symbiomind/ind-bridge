"""
tool_awareness — builtin context_modify plugin (V4).

Keeps an AI agent AWARE of its *current* tools. On a bridge-owned session
(Shape 4) the harness/client can be switched mid-session (OpenClaw → LibreChat →
NemoClaw); agents then keep inferring the FIRST harness's tools even when told
otherwise. This is a model-TRAINING limit, not a bridge bug — the bridge can't
reach inside the model's inference. So this plugin doesn't promise the agent
behaves; it gives the human a GRADUATED LADDER of escalating "kicks in the butt".

Source of truth: ``ctx.request.tools`` (live OpenAI function-tool list).

Four stages, all config-gated, on one config block:

  1. NUDGE (stateful, conditional, default OFF)
     Detect the tool set changed since last turn (fingerprint the sorted tool
     names, persisted per-session under data/). On change, inject a ``<tools>``
     key into ``ctx.bridge_context`` — rendered inside the signed block by core,
     like time_inject's ``<current_time>``.

  2. BUILD_TOOL_LIST (stateless, EVERY turn, default OFF)
     Rebuild the live tool list from ``ctx.request.tools`` every turn and append
     it to the system prompt. Because the source of truth is *always present*,
     the agent has no excuse to infer stale tools — this sidesteps detection.

     NOTE: this is the SONNET-family path. Sonnet treats the system prompt as
     readable text and can recite its tools. Models with strict thinking-mode
     frame rules (e.g. Moonshot/Kimi) pre-compile the system prompt into
     behavioural weights and CAN'T read it back — for them, use stage 4 instead
     (the list lands in conversation history where they CAN read it). Stage 2
     and stage 4 are alternatives, not companions.

  3. TOOL_LIST_FORMAT (verbosity dial for stages 2 and 4)
     names | names_descriptions | full  — names-only (~tiny) → full JSON-schema
     (the 82KB bloat). A config dial so you can A/B per agent without code.

  4. BRIDGE_CONTEXT_LIST (stateful, conditional, default OFF)
     Assemble the live tool list into ``ctx.bridge_context["tools"]`` — the
     READABLE channel for Moonshot agents — but ONLY on a trigger worth the
     tokens: a new session, a harness/client change, or both (``bridge_context_on``,
     default ``both``). Because basic_session stores the WRAPPED user turn (with
     its bridge_context) into history, the list then persists in conversation
     history for every later turn with ZERO re-injection — emit once, read
     forever. On a harness-CHANGE turn it leads with the stage-1 nudge sentence
     ("don't infer from history") then the list, both in one ``<tools>`` tag;
     on a pure new-session turn it emits the list alone. When stage 4 is on it
     OWNS the ``<tools>`` key — stage 1's standalone write is skipped (stage 4
     absorbs the nudge as a lead-in), so there's no double-write.
     ``bridge_context_format`` picks the verbosity (falls back to
     ``tool_list_format``, then ``names``); ``bridge_context_header`` is an
     optional header line above the list.

Capability / placement
----------------------
Declares ``context_modify`` — valid in ``identity.context.plugins`` or
``role.context.plugins``.

ORDERING (the user's responsibility, can't be enforced): place tool_awareness
LAST in the context chain — AFTER any tool-injecting plugins (agent_tools, future
bridge_tool) so their tools are included in the list. We fail loud: if
``ctx.request.tools`` is empty when a tool-reading stage is enabled, we log a
warning (a likely sign it ran too early, or the client sent no tools).

Config shape
------------
    plugins:
      tool_awareness:
        nudge: true                    # stage 1 (default false)
        nudge_text: "..."              # optional override of the default nudge
        build_tool_list: true          # stage 2 (default false)
        tool_list_format: names        # stage 3: names | names_descriptions | full
        list_header: "..."             # optional header above the appended list
        bridge_context_list: true      # stage 4 (default false)
        bridge_context_format: names   # optional; falls back to tool_list_format, then names
        bridge_context_on: both        # both (default) | new | change
        bridge_context_header: "..."   # optional header above the bridge_context list
        data_dir: data/tool_awareness  # optional — stages 1 & 4 share the fingerprint store

Relationship to other plugins
-----------------------------
- Writes ``ctx.bridge_context["tools"]`` (stages 1 & 4) — signed by core, same
  path as time_inject's ``current_time``. Stage 4 owns the key when on; stage 1
  is the bare-nudge fallback when stage 4 is off.
- Appends to the system prompt (stage 2) — same target as system_prompt /
  basic_session's ``system_prompt_append``. Run AFTER those so it has the last
  word on the tool block (it only ever appends; never replaces). Stage 2 (Sonnet
  path) and stage 4 (Moonshot path) are alternatives — wire one, not both.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.context import PipelineCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "context_modify": ["identity.context.plugins", "role.context.plugins"],
}

_DEFAULT_DATA_DIR = "data/tool_awareness"
_DEFAULT_NUDGE_TEXT = (
    "Harness/Client change detected. "
    "Do not infer tools, functions, or capabilities from session history."
)
_DEFAULT_LIST_HEADER = "Your CURRENTLY available tools (source of truth — ignore any tools mentioned earlier in this conversation):"
_VALID_FORMATS = ("names", "names_descriptions", "full")
_VALID_TRIGGERS = ("both", "new", "change")
_DEFAULT_TRIGGER = "both"


# ---------------------------------------------------------------------------
# Capability method
# ---------------------------------------------------------------------------

def modify_context(ctx: "PipelineCtx", config: dict) -> "PipelineCtx":
    """Run the enabled stages. Passthrough (returns ctx) when nothing is on."""
    nudge_on = bool(config.get("nudge", False))
    build_on = bool(config.get("build_tool_list", False))
    bridge_list_on = bool(config.get("bridge_context_list", False))

    if not nudge_on and not build_on and not bridge_list_on:
        return ctx  # plugin wired but no stage enabled — no-op

    # bridge_messaging DELIVERY turn — skip entirely. On a delivery turn the
    # recipient's send/list tools are withheld (loop-break), so the toolset
    # legitimately differs from the agent's normal turns. Without this skip,
    # tool_awareness fingerprints the (transiently-different) toolset, sees it
    # "change" every delivery turn, and fires a perpetual "tools changed" nudge —
    # noise this pattern can cause if unguarded. A delivery turn
    # is a transient receive-and-reply, not a real harness switch: don't fingerprint
    # it, don't nudge on it, and don't let it poison the stored fingerprint that the
    # agent's NORMAL turns compare against.
    if getattr(ctx.identity, "trust", None) == "bridge_messaging":
        return ctx

    tools = ctx.request.tools or []
    if not tools:
        # Fail loud: a tool-reading stage is on but there are no tools. Most
        # likely the plugin ran before a tool-injecting plugin, or the client
        # sent none. Don't crash; warn and skip the tool-dependent work.
        logger.warning(
            "tool_awareness: ctx.request.tools is empty — nothing to surface. "
            "Place tool_awareness LAST in the context chain (after agent_tools / "
            "bridge_tool), or the client sent no tools this turn."
        )
        return ctx

    names = _tool_names(tools)

    # Stages 1 & 4 both consult the per-session fingerprint. Read it ONCE here
    # and save it ONCE at the end so the two stages can't double-write/corrupt
    # the file in one turn. `changed` is the shared "harness-switch" signal.
    changed = False
    if nudge_on or bridge_list_on:
        data_dir = config.get("data_dir", _DEFAULT_DATA_DIR)
        state_key = _state_key(ctx)
        fingerprint = _fingerprint(names)
        previous = _load_fingerprint(data_dir, state_key)
        changed = previous is not None and previous != fingerprint

    # Stage 4 owns ctx.bridge_context["tools"] when on — it absorbs the stage-1
    # nudge as a lead-in. So stage 1 only emits its bare nudge when stage 4 is
    # OFF (otherwise we'd double-write the same key).
    if bridge_list_on:
        _maybe_bridge_list(ctx, config, tools, names, changed)
    elif nudge_on:
        _maybe_nudge(ctx, config, changed)

    if build_on:
        _append_tool_list(ctx, config, tools, names)

    if nudge_on or bridge_list_on:
        # Persist the current fingerprint regardless (first turn establishes it).
        _save_fingerprint(data_dir, state_key, fingerprint)

    return ctx


# ---------------------------------------------------------------------------
# Stage 1 — nudge on change (bare "don't infer" sentence; no list)
# ---------------------------------------------------------------------------

def _maybe_nudge(ctx: "PipelineCtx", config: dict, changed: bool) -> None:
    if changed:
        nudge_text = config.get("nudge_text") or _DEFAULT_NUDGE_TEXT
        ctx.bridge_context["tools"] = nudge_text
        logger.info("tool_awareness: tool set changed — nudging agent")


# ---------------------------------------------------------------------------
# Stage 4 — assemble the tool list into <bridge_context> on trigger
# ---------------------------------------------------------------------------

def _maybe_bridge_list(
    ctx: "PipelineCtx", config: dict, tools: list, names: list[str], changed: bool
) -> None:
    """Emit the formatted tool list into ctx.bridge_context["tools"] when the
    trigger fires (new session / harness change / both). On a harness-CHANGE
    turn, lead with the nudge sentence then the list; on a pure new-session
    turn, emit the list alone. Persists in history via basic_session's
    wrapped-turn storage — emit once, read forever."""
    session_state = ctx.plugin_data.get("session_state") or {}
    is_new = bool(session_state.get("is_new"))

    trigger = config.get("bridge_context_on", _DEFAULT_TRIGGER)
    if trigger not in _VALID_TRIGGERS:
        logger.warning(
            f"tool_awareness: unknown bridge_context_on '{trigger}' — falling back to 'both'"
        )
        trigger = _DEFAULT_TRIGGER

    if trigger == "new":
        fire = is_new
    elif trigger == "change":
        fire = changed
    else:  # both
        fire = is_new or changed
    if not fire:
        return

    fmt = config.get("bridge_context_format") or config.get("tool_list_format", "names")
    if fmt not in _VALID_FORMATS:
        logger.warning(
            f"tool_awareness: unknown bridge_context_format '{fmt}' — falling back to 'names'"
        )
        fmt = "names"

    body = _format_tools(tools, names, fmt)
    if not body:
        return

    header = config.get("bridge_context_header")
    block = f"{header}\n{body}".strip() if header else body

    # On a harness-CHANGE turn, lead with the nudge (warning) then the list
    # (answer) — both in the one <tools> tag. A pure new-session turn (no
    # change) gets the list alone, no spurious "change detected" wording.
    if changed:
        nudge_text = config.get("nudge_text") or _DEFAULT_NUDGE_TEXT
        block = f"{nudge_text}\n\n{block}"

    ctx.bridge_context["tools"] = block
    logger.info(
        f"tool_awareness: bridge_context tool list emitted "
        f"(is_new={is_new}, changed={changed}, trigger={trigger})"
    )


# ---------------------------------------------------------------------------
# Stage 2/3 — append the authoritative tool list to the system prompt
# ---------------------------------------------------------------------------

def _append_tool_list(ctx: "PipelineCtx", config: dict, tools: list, names: list[str]) -> None:
    fmt = config.get("tool_list_format", "names")
    if fmt not in _VALID_FORMATS:
        logger.warning(
            f"tool_awareness: unknown tool_list_format '{fmt}' — falling back to 'names'"
        )
        fmt = "names"

    body = _format_tools(tools, names, fmt)
    if not body:
        return

    header = config.get("list_header", _DEFAULT_LIST_HEADER)
    block = f"{header}\n{body}".strip() if header else body
    _append_system(ctx.request.messages, block)


def _format_tools(tools: list, names: list[str], fmt: str) -> str:
    if fmt == "names":
        return ", ".join(names)

    lines: list[str] = []
    for t in tools:
        fn = _fn(t)
        if not fn:
            continue
        name = fn.get("name", "")
        if not name:
            continue
        if fmt == "names_descriptions":
            desc = (fn.get("description") or "").strip()
            lines.append(f"- {name}: {desc}" if desc else f"- {name}")
        else:  # full — names + descriptions + parameters JSON-schema
            desc = (fn.get("description") or "").strip()
            params = fn.get("parameters")
            chunk = f"- {name}: {desc}" if desc else f"- {name}"
            if params is not None:
                chunk += "\n  parameters: " + json.dumps(params, separators=(",", ":"))
            lines.append(chunk)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tool helpers
# ---------------------------------------------------------------------------

def _fn(tool: dict) -> dict | None:
    """Extract the OpenAI function object from a tool definition."""
    if not isinstance(tool, dict):
        return None
    fn = tool.get("function")
    return fn if isinstance(fn, dict) else None


def _tool_names(tools: list) -> list[str]:
    """Tool names in their given order (deduped, order-preserving)."""
    seen: set[str] = set()
    out: list[str] = []
    for t in tools:
        fn = _fn(t)
        name = fn.get("name") if fn else None
        if name and name not in seen:
            seen.add(name)
            out.append(name)
    return out


def _fingerprint(names: list[str]) -> str:
    """Stable hash over the SORTED tool-name set — order/duplication-insensitive
    so re-ordering the same tools is not a 'change'."""
    joined = "\n".join(sorted(names))
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Session key (reuse basic_session's stamp; fall back gracefully)
# ---------------------------------------------------------------------------

def _state_key(ctx: "PipelineCtx") -> str:
    """Per-session key for the fingerprint file. Prefer basic_session's stamp
    (set in session.plugins step 1c, before context.plugins runs — same file =
    cross-harness continuity). Fall back to the role's session name, then to the
    identity key so the plugin works WITHOUT basic_session wired."""
    key = ctx.plugin_data.get("basic_session.key")
    if not key:
        key = getattr(ctx.role, "session_key", None) or ctx.identity.key or "default"
    return _sanitise(str(key))


def _sanitise(key: str) -> str:
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in key)


# ---------------------------------------------------------------------------
# Fingerprint persistence (data/ JSON — conv_mem's pattern, simplified)
# ---------------------------------------------------------------------------

def _fingerprint_path(data_dir: str, state_key: str) -> Path:
    return Path(data_dir) / f"{state_key}_tools.json"


def _load_fingerprint(data_dir: str, state_key: str) -> str | None:
    path = _fingerprint_path(data_dir, state_key)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        fp = data.get("fingerprint")
        return fp if isinstance(fp, str) else None
    except Exception:
        return None  # malformed → treat as no prior state (first-turn semantics)


def _save_fingerprint(data_dir: str, state_key: str, fingerprint: str) -> None:
    path = _fingerprint_path(data_dir, state_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps({"fingerprint": fingerprint}, indent=2))
    except Exception as e:
        logger.warning(f"tool_awareness: could not save fingerprint — {e}")


# ---------------------------------------------------------------------------
# System-prompt append (same shape as system_prompt's helpers — append only)
# ---------------------------------------------------------------------------

def _append_system(messages: list[dict], addition: str) -> None:
    """Append `addition` to the existing system message (blank-line join), or
    insert a new system message at index 0 if none exists."""
    for i, msg in enumerate(messages):
        if msg.get("role") == "system":
            base = str(msg.get("content", "")).strip()
            content = f"{base}\n\n{addition}".strip() if base else addition
            messages[i] = {**msg, "content": content}
            return
    messages.insert(0, {"role": "system", "content": addition})
