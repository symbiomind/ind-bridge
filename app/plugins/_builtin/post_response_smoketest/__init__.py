"""
post_response_smoketest — bundled smoke-test plugin for D-007's
``post_response`` capability.

**This is not a real plugin.** It exists to verify that the executor's
streaming-tee + observer-dispatch plumbing works end-to-end, before
real plugins (conversational_memory, basic_session, audit logging,
metrics) are ported onto the new capability. Operators wiring real
configs should not reference this plugin — its only job is to log a
single diagnostic line proving the new capability fires.

What it does, when wired into a config:

  * Logs a one-line header at INFO level summarising the assistant
    turn that the executor reconstructed, including:
      - identity / role / resource keys
      - finish_reason (verbatim from upstream — provider-dependent)
      - content length
      - tool_call count (if any)
      - reasoning_content presence (Moonshot-style turns)

  * Then logs the *full* ``_full_response`` dict as pretty-printed
    JSON, verbatim — no cherry-picking, no stream-vs-non-stream
    reshape. This makes provider/protocol oddities (OpenRouter
    extras, Moonshot reasoning_content, future non-OpenAI shapes,
    oddity-plugin mutations) visible without code changes. The one
    exception: the stream path's ``raw_chunks`` (original SSE bytes
    accumulated by stream_reconstruct) is replaced with a
    ``"<N chunks, omitted>"`` summary so logs stay readable.

  * That's it. No I/O, no MCP, no storage. The point is to *prove the
    plumbing* and give a faithful debug-window. If you see the log
    lines in the container output after a streamed reply finishes,
    the BackgroundTask + reconstructor + observer dispatch all
    worked.

Wire it in (test config — do not commit to production config.yml):

    roles:
      smoketest_role:
        resource: openrouter
        plugins:
          OpenAI-Protocol: {model: ...}
          post_response_smoketest: {}

The plugin appears alongside OpenAI-Protocol on role.plugins because
post_response and outbound_params share that slot family — the
executor disambiguates by capability, not by slot string.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.context import PipelineCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "post_response": ["identity.plugins", "role.plugins"],
}


def observe_response(ctx: "PipelineCtx", config: dict) -> None:
    """Log a one-line summary of the reconstructed assistant turn.

    The executor calls this *after* the response has been delivered to
    the client (BackgroundTask on the streaming path; create_task on
    the non-streaming path). Latency here doesn't affect the user.

    Errors raised here are logged-and-swallowed by the executor —
    observers must never surface to the client. Still, defensive
    coding is a good habit even when the executor catches.
    """
    response = ctx.response or {}
    full = response.get("_full_response") or {}

    # On the streaming path, _full_response is the reconstructed dict
    # (with finish_reason, tool_calls, reasoning_content). On the
    # non-streaming path, _full_response is the verbatim upstream JSON,
    # which uses a different shape — we read both.
    finish_reason = full.get("finish_reason")
    if not finish_reason:
        # Non-stream path: dig into choices[0].finish_reason
        choices = full.get("choices") or []
        if choices and isinstance(choices[0], dict):
            finish_reason = choices[0].get("finish_reason")

    content = response.get("content") or ""
    tool_calls = full.get("tool_calls")
    if tool_calls is None:
        # Non-stream shape: choices[0].message.tool_calls
        choices = full.get("choices") or []
        if choices and isinstance(choices[0], dict):
            msg = choices[0].get("message") or {}
            tool_calls = msg.get("tool_calls")
    tc_count = len(tool_calls) if isinstance(tool_calls, list) else 0

    reasoning_content = full.get("reasoning_content")
    if not reasoning_content:
        choices = full.get("choices") or []
        if choices and isinstance(choices[0], dict):
            msg = choices[0].get("message") or {}
            reasoning_content = msg.get("reasoning_content")
    has_reasoning = bool(reasoning_content)

    logger.info(
        f"[post_response_smoketest] "
        f"identity='{ctx.identity.key}' "
        f"role='{ctx.role.key or '(none)'}' "
        f"resource='{ctx.resource.key or '(none)'}' "
        f"finish_reason='{finish_reason or '(none)'}' "
        f"content_len={len(content)} "
        f"tool_calls={tc_count} "
        f"has_reasoning={has_reasoning}"
    )

    if not full:
        logger.info("[post_response_smoketest] (no _full_response)")
        return

    body = dict(full)
    if "raw_chunks" in body:
        rc = body["raw_chunks"]
        n = len(rc) if hasattr(rc, "__len__") else "?"
        body["raw_chunks"] = f"<{n} chunks, omitted>"

    try:
        rendered = json.dumps(body, indent=2, ensure_ascii=False, default=str)
    except Exception as e:
        rendered = f"(json.dumps failed: {e!r}; repr fallback)\n{body!r}"

    logger.info("[post_response_smoketest] _full_response:\n%s", rendered)
