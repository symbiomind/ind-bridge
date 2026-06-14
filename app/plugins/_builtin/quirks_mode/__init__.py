"""
quirks_mode — builtin outbound_normalize + post_response plugin (V4).

The bridge's **provider-compat shim layer** — the polyfill for the AI-provider
wars. Strict thinking-mode providers (Moonshot/Kimi) raise frame 400s that
tolerant providers (Claude, …) don't. quirks_mode normalizes the outbound frame so
a strict provider accepts a request a harness handed the bridge — for any harness,
on the provider that needs it. It is **scoped + opt-in per agent**, the opposite of
a core vendor-prefix-soup registry: you enable exactly the quirks a model needs,
nothing more, and an agent on a tolerant provider gets none.

Three quirks today (each independently toggleable):
  • reattach_reasoning  — restore reasoning the bridge captured onto a
    round-tripped assistant(tool_calls) turn that came back stripped (harnesses
    drop the reasoning; thinking-mode providers 400 without it). Matched by tool
    name+args (robust to harness id-mangling). Only ever RESTORES reasoning the
    provider itself produced — never invents.
  • mirror_reasoning_key — mirror `reasoning` (OpenRouter's key) →
    `reasoning_content` (the key Moonshot enforces) when a turn has the former but
    not the latter (provider key-drift).
  • close_trailing_orphan — if the frame ENDS in an unanswered assistant(tool_calls)
    (an in-flight harness call), append a plain-language synthetic tool result so a
    strict provider accepts the frame (it 400s on a trailing unanswered call).

Capture side (post_response): when the bridge sees a streamed/assembled turn with
tool_calls AND reasoning, it stashes the reasoning per-session so a later
round-trip can restore it (feeds reattach_reasoning).

Selecting quirks — cheat sheet OR workbench (mutually exclusive), default-OFF
--------------------------------------------------------------------------------
  quirks_mode:
    model: "moonshotai/kimi-k2.5"   # CHEAT SHEET — inherit the recipe in models.yml
  # …or…
  quirks_mode:
    quirks: [reattach_reasoning]    # WORKBENCH — hand-pick by name (bring up a model)

  • `model:` is a key into the shipped `models.yml` table (model string → quirk
    list). Bring up a new model with a hand `quirks:` list, then bottle the working
    set into models.yml as a new `model:` entry.
  • If both are set, `model:` wins and `quirks:` is ignored (with a warning).
  • Default-OFF: `{}` / absent / `quirks: []` / an unknown `model:` → NO quirks
    applied (most quirks would break a tolerant provider — you opt in deliberately).
  data_dir: data/reasoning_reattach   # optional — reattach stash location.

Capability / placement
----------------------
Declares ``outbound_normalize`` (normalize the frame before EVERY resource call —
inbound AND handle_tool_calls intercept re-calls) and ``post_response`` (capture
reasoning on outbound). Valid in identity.context.plugins / role.context.plugins.
Wire it on the agent that talks to the strict provider through a harness-owned tool
loop. (Provider-level placement on resource.plugins is the planned end-state — see
notes/DESIGN-quirks-mode-on-resource.md — not yet wired.)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from app.context import PipelineCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    # Runs on EVERY resource call (inbound + handle_tool_calls intercept re-calls),
    # via _execute_resource_step — so the frame fix also covers bridge-owned tool
    # loops, not just the inbound pass (which context_modify would miss).
    "outbound_normalize": ["identity.context.plugins", "role.context.plugins"],
    # Captures reasoning from the assembled response turn (after delivery).
    "post_response":      ["identity.context.plugins", "role.context.plugins"],
}

_DEFAULT_DATA_DIR = "data/reasoning_reattach"

# Plain-language synthetic tool result used to close a trailing in-flight tool
# call so a strict provider accepts the frame. MUST be plain language, not JSON:
# tested against Moonshot, a JSON placeholder ("{...pending...}") provokes a
# re-call (loop), while a plain instruction makes the model answer (finish=stop).
_SYNTHETIC_RESULT = (
    "The tool result is not available this turn (the call is still in flight on "
    "the client side). Respond to the user normally without it; do not call the "
    "tool again now."
)

# Reasoning arrives under different keys per provider (drift). When capturing, we
# take the first non-empty of these. When re-attaching we always write
# `reasoning_content` — the key Moonshot enforces on round-tripped tool turns.
_REASONING_KEYS = ("reasoning_content", "reasoning")

# The canonical set of quirk names. Each maps to one independently-toggleable
# behaviour below. A name in config (a list, or a models.yml recipe) that isn't
# here is warned-and-ignored (typo guard).
_KNOWN_QUIRKS = ("reattach_reasoning", "mirror_reasoning_key", "close_trailing_orphan")

# The shipped cheat-sheet: model-string → list of quirk names. Lives beside this
# module; bind-mount your own over it to extend/override without touching code.
_MODELS_YML = Path(__file__).with_name("models.yml")


def _load_models_table() -> dict[str, list[str]]:
    """Load the model→quirks recipe table from models.yml (once, at import).
    Fail-loud-but-don't-crash: a missing/garbled file → empty table + warning,
    so a bad bind-mount degrades quirks_mode to default-OFF rather than 500ing
    every request (per 'never fail to start')."""
    if not _MODELS_YML.exists():
        logger.warning(f"quirks_mode: no models.yml at {_MODELS_YML} — `model:` "
                       f"recipes unavailable (use `quirks:` lists instead)")
        return {}
    try:
        data = yaml.safe_load(_MODELS_YML.read_text()) or {}
    except Exception as e:
        logger.warning(f"quirks_mode: could not parse models.yml ({e}) — `model:` "
                       f"recipes unavailable")
        return {}
    if not isinstance(data, dict):
        logger.warning("quirks_mode: models.yml is not a mapping — ignored")
        return {}
    # Coerce each recipe to a list of strings; tolerate scalars/None gracefully.
    table: dict[str, list[str]] = {}
    for model, recipe in data.items():
        if recipe is None:
            table[str(model)] = []
        elif isinstance(recipe, list):
            table[str(model)] = [str(q) for q in recipe]
        else:
            table[str(model)] = [str(recipe)]
    return table


_MODELS_TABLE: dict[str, list[str]] = _load_models_table()


def _resolve_quirks(config: dict) -> list[str]:
    """Resolve the active quirk set from config — cheat sheet (`model:`) OR
    workbench (`quirks:`), mutually exclusive, default-OFF. Returns only KNOWN
    quirk names (unknowns warned-and-dropped). Empty list = nothing enabled."""
    model = config.get("model")
    quirks = config.get("quirks")
    if model and quirks is not None:
        logger.warning("quirks_mode: both `model:` and `quirks:` set — using "
                       "`model:`, ignoring `quirks:`")
    if model:
        recipe = _MODELS_TABLE.get(model)
        if recipe is None:
            logger.warning(f"quirks_mode: no recipe for model {model!r} — add it to "
                           f"models.yml or use a `quirks:` list; NO quirks applied")
            return []
        active = recipe
    else:
        active = quirks or []  # workbench; absent/[] → default-OFF
    unknown = [q for q in active if q not in _KNOWN_QUIRKS]
    if unknown:
        logger.warning(f"quirks_mode: unknown quirk(s) {unknown} — ignored (known: "
                       f"{list(_KNOWN_QUIRKS)})")
    return [q for q in active if q in _KNOWN_QUIRKS]


def _enabled(config: dict, quirk: str) -> bool:
    """Is this quirk active for this config? (default-OFF — only enabled if the
    resolved set names it). Cheap: the recipe table is in-memory."""
    return quirk in _resolve_quirks(config)


# ---------------------------------------------------------------------------
# outbound_normalize — fix the frame before EVERY resource call (inbound + re-call)
# ---------------------------------------------------------------------------

def normalize_outbound(ctx: "PipelineCtx", config: dict) -> "PipelineCtx":
    """Normalize the outbound frame for strict (thinking-mode) providers. Runs
    on EVERY resource call (inbound AND handle_tool_calls intercept re-calls) via
    the executor's `_execute_resource_step` chokepoint. Idempotent — safe to run
    repeatedly (only sets reasoning_content if absent; only appends a synthetic
    result if the tail is a trailing unanswered tool call). Two independent
    repairs, each addressing a provider frame-quirk:

      1. reasoning re-attach — any assistant(tool_calls) turn missing reasoning
         gets the reasoning we stashed for its tool_call_id (Moonshot rejects a
         round-tripped tool turn without reasoning_content).
      2. trailing-orphan close — if the working copy ENDS in an unanswered
         assistant(tool_calls) (an in-flight harness call), append a synthetic
         tool result so the frame is valid (Moonshot rejects a request ending in
         an unanswered tool call). The result is a plain-language "respond
         normally" instruction — tested loop-safe: the model ANSWERS rather than
         re-calling (a JSON placeholder provokes a re-call; plain instruction
         does not). This keeps a harness-owned-loop buddy talking instead of
         400ing, so we can observe the real harness behaviour.

    Each behaviour is independently gated by the resolved quirk set (default-OFF;
    see `_resolve_quirks`). With no quirks active this is a no-op."""
    _log_active_quirks_once(ctx, config)
    _reattach_reasoning(ctx, config)
    _close_trailing_orphan(ctx, config)
    return ctx


def _log_active_quirks_once(ctx: "PipelineCtx", config: dict) -> None:
    """Log the resolved active quirk set once per session — visibility into which
    quirks are live (and whether via a `model:` recipe or a hand `quirks:` list)."""
    seen = ctx.plugin_data.setdefault("quirks_mode.logged", set())
    skey = _session_key(ctx)
    if skey in seen:
        return
    seen.add(skey)
    active = _resolve_quirks(config)
    via = f"model={config['model']!r}" if config.get("model") else "quirks: list"
    if active:
        logger.info(f"quirks_mode: active quirks for '{skey}' ({via}): {active}")
    else:
        logger.info(f"quirks_mode: no quirks active for '{skey}' (default-OFF)")


def _reattach_reasoning(ctx: "PipelineCtx", config: dict) -> None:
    """Two independently-gated quirks operate here:
      • mirror_reasoning_key — mirror `reasoning` → `reasoning_content` (key-drift).
      • reattach_reasoning   — restore stashed reasoning onto a reasoning-less turn.
    Either off → that half is skipped. Both off → this whole pass is a no-op."""
    do_mirror = _enabled(config, "mirror_reasoning_key")
    do_restore = _enabled(config, "reattach_reasoning")
    if not (do_mirror or do_restore):
        return

    stash = _load_stash(config, ctx) if do_restore else {}
    repaired = 0
    mirrored = 0
    for msg in ctx.request.messages:
        if msg.get("role") != "assistant" or not msg.get("tool_calls"):
            continue

        # Moonshot enforces `reasoning_content` SPECIFICALLY. A turn that already
        # carries reasoning under the OpenRouter key (`reasoning`) but NOT
        # `reasoning_content` still 400s — the key drifted. Mirror it across.
        # (This is the case the OpenClaw round-trip exposed: turns came back WITH
        # `reasoning` set, so the old `_has_reasoning` skip left them un-fixed.)
        if msg.get("reasoning_content"):
            continue  # already has the key Moonshot wants — leave it
        existing = msg.get("reasoning")
        if existing:
            if do_mirror:
                msg["reasoning_content"] = existing
                mirrored += 1
            continue

        # No reasoning at all on this turn → restore from the stash (matched by
        # name+args, robust to id mangling).
        if not do_restore:
            continue
        key = _turn_key(msg)
        if key is None:
            continue
        reasoning = stash.get(key)
        if reasoning:
            msg["reasoning_content"] = reasoning
            repaired += 1

    if repaired or mirrored:
        logger.info(
            f"quirks_mode: restored reasoning on {repaired} + mirrored key "
            f"on {mirrored} tool-call turn(s) for '{_session_key(ctx)}'"
        )


def _close_trailing_orphan(ctx: "PipelineCtx", config: dict) -> None:
    """If the working copy ends in an unanswered assistant(tool_calls), append a
    synthetic tool result for each unanswered tool_call_id so the frame is valid.
    Loop-safe: a plain-language 'respond normally' result makes the model answer,
    not re-call. NEVER deletes the call (deleting caused an infinite loop).
    Gated by the close_trailing_orphan quirk (default-OFF)."""
    if not _enabled(config, "close_trailing_orphan"):
        return
    msgs = ctx.request.messages
    if not msgs:
        return
    last = msgs[-1]
    if last.get("role") != "assistant" or not last.get("tool_calls"):
        return

    synthetic = []
    for tc in last.get("tool_calls") or []:
        tcid = tc.get("id")
        if not tcid:
            continue
        synthetic.append({
            "role": "tool",
            "tool_call_id": tcid,
            "content": _SYNTHETIC_RESULT,
        })
    if synthetic:
        msgs.extend(synthetic)
        logger.warning(
            f"quirks_mode: closed {len(synthetic)} trailing unanswered "
            f"tool call(s) with a synthetic result for '{_session_key(ctx)}' — "
            f"in-flight harness call; keeps the buddy responding instead of 400. "
            f"See DESIGN-harness-client-compensation."
        )


# ---------------------------------------------------------------------------
# post_response — capture reasoning from the assembled turn, keyed by tool_call_id
# ---------------------------------------------------------------------------

def observe_response(ctx: "PipelineCtx", config: dict) -> None:
    """If the just-produced turn carries tool_calls AND reasoning, stash the
    reasoning keyed by each tool_call_id so a later round-trip can restore it.
    Only bothers if reattach_reasoning is enabled — no point capturing what we'll
    never restore."""
    if not _enabled(config, "reattach_reasoning"):
        return
    turn = _assembled_turn(ctx)
    if turn is None:
        return

    tool_calls = turn.get("tool_calls")
    if not tool_calls:
        return  # only tool-call turns are at risk of the round-trip 400

    reasoning = _extract_reasoning(turn)
    if not reasoning:
        return  # no reasoning to preserve

    key = _turn_key(turn)
    if key is None:
        return

    stash = _load_stash(config, ctx)
    stash[key] = reasoning
    # Bound the stash so it can't grow forever (keep the most recent N).
    if len(stash) > 200:
        for k in list(stash.keys())[:-200]:
            del stash[k]
    _save_stash(config, ctx, stash)
    logger.debug(
        f"quirks_mode: captured reasoning for tool-turn key={key!r} "
        f"('{_session_key(ctx)}')"
    )


# ---------------------------------------------------------------------------
# Turn / reasoning helpers
# ---------------------------------------------------------------------------

def _turn_key(turn: dict) -> str | None:
    """A match key for an assistant(tool_calls) turn, robust to harness
    id-mangling. The tool_call_id is NOT stable across the round-trip — observed
    with OpenClaw, which REWRITES it non-deterministically:
        emit ``session_status:1`` → round-trip ``sessionstatus1`` (first call),
                                  → ``sessionstatus1587315c9`` (later call,
                                     a per-call hash suffix appended).
    So id-based matching (even normalized) fails. What DOES survive is the call's
    CONTENT: the function ``name`` + ``arguments``. We key on
    ``<name>(<arguments>)`` so capture and round-trip agree.

    Collision note: two calls with identical name+args map to the same key — their
    reasoning is interchangeable enough for this purpose (restoring *some* valid
    reasoning beats a 400). Sorted + joined for multi-call turns."""
    parts = []
    for tc in turn.get("tool_calls") or []:
        if not isinstance(tc, dict):
            continue
        fn = tc.get("function") or {}
        name = fn.get("name") or ""
        args = _normalize_args(fn.get("arguments"))
        if name or args:
            parts.append(f"{name}({args})")
    return "|".join(sorted(parts)) if parts else None


def _normalize_args(args) -> str:
    """Canonicalise tool-call arguments so semantically-identical args match
    regardless of key order / whitespace. Args arrive as a JSON string per the
    OpenAI protocol; fall back to the raw string if it won't parse."""
    if args is None:
        return ""
    try:
        return json.dumps(json.loads(args), sort_keys=True, separators=(",", ":"))
    except (json.JSONDecodeError, TypeError, ValueError):
        return str(args)


def _has_reasoning(msg: dict) -> bool:
    return any(msg.get(k) for k in _REASONING_KEYS)


def _extract_reasoning(turn: dict) -> str | None:
    for k in _REASONING_KEYS:
        val = turn.get(k)
        if isinstance(val, str) and val:
            return val
    return None


def _assembled_turn(ctx: "PipelineCtx") -> dict | None:
    """The reconstructed/assembled assistant turn. On the streaming path the
    executor stashes the full reconstruction (with reasoning + tool_calls) on
    ctx.response['_full_response']; on non-stream it's the response dict itself."""
    resp = ctx.response
    if not isinstance(resp, dict):
        return None
    full = resp.get("_full_response")
    return full if isinstance(full, dict) else resp


# ---------------------------------------------------------------------------
# Session key + stash persistence (data/ JSON, conv_mem's pattern)
# ---------------------------------------------------------------------------

def _session_key(ctx: "PipelineCtx") -> str:
    """Per-session scope. Prefer basic_session's stamp (so a shared session shares
    one stash); fall back to the role's session name, then identity key."""
    key = ctx.plugin_data.get("basic_session.key")
    if not key:
        key = getattr(ctx.role, "session_key", None) or ctx.identity.key or "default"
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in str(key))


def _stash_path(config: dict, ctx: "PipelineCtx") -> Path:
    data_dir = config.get("data_dir", _DEFAULT_DATA_DIR)
    return Path(data_dir) / f"{_session_key(ctx)}_reasoning.json"


def _load_stash(config: dict, ctx: "PipelineCtx") -> dict[str, str]:
    path = _stash_path(config, ctx)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_stash(config: dict, ctx: "PipelineCtx", stash: dict) -> None:
    path = _stash_path(config, ctx)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps(stash, ensure_ascii=False, indent=2))
    except Exception as e:
        logger.warning(f"quirks_mode: could not save stash — {e}")
