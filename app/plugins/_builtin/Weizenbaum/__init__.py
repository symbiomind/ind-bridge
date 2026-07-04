"""
Weizenbaum — ELIZA (1966) as a terminal resource plugin.

Joseph Weizenbaum wrote ELIZA at MIT in 1966: a few hundred lines of MAD-SLIP
and a script called DOCTOR that played a Rogerian psychotherapist. She was the
first program anyone ever mistook for a mind. Sixty years later (1966 → 2026)
every conversational system traces a lineage back to her — so she lives here,
in ``_builtin/``, next to her grandchildren.

A ``produce_response`` terminal resource: the bridge generates the reply
itself — no upstream AI, no HTTP call, no tokens burned. Pure pattern
transformation, exactly as published in CACM 9(1), implemented in
``engine.py`` with the voice defined entirely by a script file
(``doctor.yml`` by default — see its header for the schema; point ``script:``
at your own file to make her someone else).

The engine is a pure function of the visible message history. That has one
lovely consequence: ELIZA had a famous "memory" mechanism, but the plugin
holds no state — so she can only remember if the *pipeline* remembers. Wire a
session alongside her and the history arrives each turn, memory and all::

    resources:
      eliza:                        # who she is
        plugins:
          Weizenbaum: {}            # who made her

    sessions:
      eliza_session:
        plugins:
          basic_session: {}         # ← the unlock: now she remembers

    roles:
      eliza_role:
        resource: eliza
        session: eliza_session

    identities:
      eliza_chat:
        role: eliza_role
        token: ${ELIZA_TOKEN}
        plugins:
          OpenAI-Protocol:
            prefix: /v1

Any OpenAI-compatible client pointed at that identity gets a 1966
psychotherapist through a 2026 chat interface.

Config knobs (all optional — ``Weizenbaum: {}`` is a complete configuration):

  * ``script`` — path to a custom script file (``doctor.yml`` schema).
    A bad or missing file logs a warning and falls back to the bundled
    DOCTOR script; the turn never fails.
  * ``typing`` — streaming clients receive the reply word-by-word at a
    naturally drifting rate (default on, 5–10 tokens/second). ``false``
    disables; ``{min_tps: ..., max_tps: ...}`` tunes. Non-streaming
    responses are unaffected.
  * ``teletype`` — ``true`` upper-cases the reply, the way she spoke on the
    IBM 7094 line printer. Default ``false``.
  * ``greeting`` — override the opening line used when there is no user text.

She also reads the nameplate: when a turn arrives with a signed
``<bridge_context>`` envelope (agent-to-agent messaging), the ``<caller>``
name feeds the script's ``(name)`` token — sometimes she addresses you,
sometimes she doesn't, and a remembered thing keeps the name of whoever
said it.
"""

from __future__ import annotations

import logging
import os
import re
from typing import TYPE_CHECKING

import yaml

from . import engine

if TYPE_CHECKING:
    from app.context import PipelineCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "produce_response": ["resource.plugins"],
}


_DEFAULT_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), "doctor.yml")

# Parsed scripts, keyed by absolute path. Scripts are static config — the
# conversation state itself is never cached (the engine rebuilds it from the
# visible history every turn).
_script_cache: dict[str, engine.Script] = {}

# The bridge signs a <bridge_context> block into the outbound request before
# the resource step. ELIZA answers the human, not the envelope — but she does
# read the nameplate: a signed <caller> gives the turn a speaker, which feeds
# the script's "(name)" template token.
_BRIDGE_CONTEXT_RE = re.compile(
    r"<bridge_context\b[^>]*>.*?</bridge_context>", re.DOTALL
)
_CALLER_RE = re.compile(r"<caller\b[^>]*>(.*?)</caller>", re.DOTALL)

# The core's verification-failure sentinel — never address someone the
# bridge couldn't vouch for.
_UNTRUSTED_CALLER = "unknown"


def _load_script(path: str) -> engine.Script:
    path = os.path.abspath(path)
    cached = _script_cache.get(path)
    if cached is not None:
        return cached
    with open(path, encoding="utf-8") as fh:
        script = engine.parse_script(yaml.safe_load(fh))
    _script_cache[path] = script
    return script


def _resolve_script(config: dict, resource_key: str) -> engine.Script:
    custom = config.get("script")
    if custom:
        try:
            return _load_script(str(custom))
        except (OSError, yaml.YAMLError, engine.ScriptError) as e:
            logger.warning(
                f"Weizenbaum: could not load script '{custom}' for resource "
                f"'{resource_key}': {e} — falling back to the bundled "
                f"DOCTOR script."
            )
    return _load_script(_DEFAULT_SCRIPT_PATH)


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


def _turn_speaker(text: str) -> str | None:
    """Caller name from the signed <bridge_context> envelope, if the turn
    carries one. The name is per-turn on purpose: in an agent-to-agent
    conversation different turns may come from different callers, and a
    memory keeps the name of whoever said it."""
    envelope = _BRIDGE_CONTEXT_RE.search(text)
    if not envelope:
        return None
    caller = _CALLER_RE.search(envelope.group(0))
    if not caller:
        return None
    name = " ".join(caller.group(1).split())
    if not name or name.lower() == _UNTRUSTED_CALLER:
        return None
    return name


def _user_turns(messages: list[dict]) -> list[tuple[str, str | None]]:
    """The user's side of the visible history as ``(text, speaker)`` pairs —
    envelope stripped from the text, speaker read from it first. With a
    session wired this is the whole conversation; without one it is a single
    turn — which is exactly how much she is able to remember."""
    turns = []
    for msg in messages or []:
        if isinstance(msg, dict) and msg.get("role") == "user":
            raw = _message_text(msg)
            turns.append(
                (_BRIDGE_CONTEXT_RE.sub("", raw).strip(), _turn_speaker(raw))
            )
    return turns


def produce_response(ctx: "PipelineCtx", config: dict) -> "PipelineCtx":
    """Terminal — the reply is produced right here. The executor wraps
    ``ctx.response`` in an OpenAI envelope (``finish_reason: "stop"``).
    Never fails the turn: engine errors are logged and answered in character."""
    script = _resolve_script(config, ctx.resource.key)

    turns = _user_turns(ctx.request.messages)
    greeting = str(config.get("greeting", script.greeting))

    try:
        if not turns or not turns[-1][0]:
            reply = greeting
        else:
            reply = engine.respond(turns, script)
    except Exception as e:  # noqa: BLE001 — she has been up since 1966
        logger.error(
            f"Weizenbaum: engine error for resource '{ctx.resource.key}': {e}"
        )
        reply = "We may have wandered somewhere I cannot follow. Please go on."

    if config.get("teletype", False):
        reply = reply.upper()

    logger.info(
        f"Weizenbaum: answered turn {len(turns)} for resource "
        f"'{ctx.resource.key}' ({len(reply)} char(s), no upstream call)."
    )
    ctx.response = {"role": "assistant", "content": reply}

    # She types. Streaming clients get the reply word-by-word at a drifting
    # 1966-appropriate rate (the executor pops this marker and paces the SSE
    # re-emit; non-streaming responses are unaffected). `typing: false`
    # switches it off; a mapping overrides the rates.
    typing = config.get("typing", True)
    if typing:
        pacing = dict(typing) if isinstance(typing, dict) else {}
        pacing.setdefault("min_tps", 5)
        pacing.setdefault("max_tps", 10)
        ctx.response["_stream_pacing"] = pacing
    return ctx
