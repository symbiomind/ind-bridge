"""
session_freshness — bridge-core inference of "is this a new conversation?"

Pure-function module (V4 style — mirrors ``app/stream_reconstruct.py`` and
``app/frame_emit.py``). No I/O, no logging, no ctx dependency. Trivially
unit-testable.

Per D-009: session freshness is data-stamped onto ``ctx.plugin_data
["session_state"]``. The producer is either a session plugin (authoritative)
or bridge-core (this module's inference, fallback when no session plugin
is wired into the role).

The executor decides who stamps by checking whether anything is already
present in ``ctx.plugin_data["session_state"]`` after the session.plugins
walk; if not, it calls :func:`infer_from_messages` and stamps the result.

The rule is deliberately simple: a message list with no ``assistant`` turn
is a fresh conversation from the wire's perspective. False positives are
possible (a tool-result-shaped turn from a stateful harness with no session
plugin will look fresh) — handling those cases is the operator's choice
to wire a session plugin, per D-009's operator-UX punt.
"""

from __future__ import annotations


def infer_from_messages(messages: list[dict]) -> dict:
    """Return a session_state dict inferred from message shape.

    Returns the stamp dict ready to assign to
    ``ctx.plugin_data["session_state"]``:

        {
            "is_new": bool,
            "owner": "bridge_core_inference",
            "reason": "no_assistant_turn" | "assistant_turn_present",
        }

    Only meaningful when no session plugin is wired into the pipeline —
    the executor is responsible for checking that first and not
    overwriting a session-plugin stamp.
    """
    has_assistant = any(m.get("role") == "assistant" for m in messages)
    return {
        "is_new": not has_assistant,
        "owner": "bridge_core_inference",
        "reason": "assistant_turn_present" if has_assistant else "no_assistant_turn",
    }
