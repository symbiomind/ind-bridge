"""
Dev trace — human-readable, post-hoc pipeline visibility for ind-bridge V4.

**Off by default.** Enabled with ``BRIDGE_DEV_TRACE=1`` in the environment.
When enabled, every request through the pipeline executor produces one
top-to-bottom-readable block in a per-identity, per-day log file:

    logs/trace/<identity_key>/<YYYY-MM-DD>.log

The block walks: inbound headers + body, bridge_sign verify result,
the cached pipeline summary, every plugin tuple's start/end (with
config and duration), the resource-step decision, the upstream HTTP
request and response (or stream summary), bridge_sign assemble-and-sign
result, and the final outbound status. Each event is appended to an
in-memory buffer attached to the per-request ``Trace`` object; the
whole block is flushed to disk in a single ``f.write()`` at
``end_request`` time so concurrent requests for the same identity
never interleave their output.

**Why core, not plugin:** a plugin observes ctx only at the slot
where it's declared. Dev visibility into the *executor's own
decisions* (cascade resolution, tuple assembly, short-circuits,
plugin skips, the actual HTTP call shape) cannot be observed from
inside a plugin. So this lives as a core module the executor and
``bridge_sign`` call into directly. Per the same reasoning that
makes ``bridge_sign`` core (D-005) — security can't be opt-in by
config omission, and dev visibility can't be confined to a slot.

**Resilience:** every public call wraps its work in a try/except
that logs a warning to the standard logger but never re-raises.
A debug tool must not break production-shaped traffic. If the
trace file can't be written, the request still completes.

**Plugins can call event() too.** Sonnet-written plugins (and any
future plugin) can call ``dev_trace.event(ctx.dev_trace, "annotate",
note="recalled 2 pairs", ...)`` from inside their capability methods.
Those breadcrumbs land *inline* with the executor's own events in
the trace, giving plugin-internal reasoning visibility alongside
pipeline flow.

Toggles (env vars, cached at module load — restart to change):

    BRIDGE_DEV_TRACE=1                  enable
    BRIDGE_DEV_TRACE_DIR=<path>         override log root (default: <repo>/logs/trace)
    BRIDGE_DEV_TRACE_REDACT_TOKENS=1    redact "Authorization: Bearer ..." (default on)

See ``CLAUDE.md`` for the architecture cheat-sheet and
``~/Documents/ind-v4-brainstorm.md`` for V4 spec context.
"""

from __future__ import annotations

import json
import logging
import os
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone, tzinfo
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

if TYPE_CHECKING:
    from .context import PipelineCtx

logger = logging.getLogger(__name__)


def _resolve_tz() -> tzinfo:
    """Look up ``server.timezone`` from the loaded config and return the
    matching ``ZoneInfo`` tzinfo. Falls back to UTC when:

    - config isn't loaded yet (module-load-time calls before lifespan)
    - ``server.timezone`` isn't set
    - the string isn't a valid IANA zone name (warned once, then UTC)

    Resolved on every call rather than cached at module load, because
    the config may not be loaded when this module is first imported —
    and a process-restart-to-change-timezone would be a worse default
    than "trace files start using Adelaide as soon as config loads."
    The cost (a dict lookup + ZoneInfo construction, both microseconds)
    is fine for a per-request dev tool.
    """
    try:
        # Late import — avoids a circular dependency if config ever
        # decides to import dev_trace itself.
        from . import config as _config
        if not _config.is_config_loaded():
            return timezone.utc
        tz_name = (_config.get_server_cfg() or {}).get("timezone")
        if not tz_name:
            return timezone.utc
        return ZoneInfo(tz_name)
    except ZoneInfoNotFoundError as e:
        _warn_once_bad_tz(str(e))
        return timezone.utc
    except Exception as e:
        _warn_once_bad_tz(f"{type(e).__name__}: {e!r}")
        return timezone.utc


_warned_bad_tz = False


def _warn_once_bad_tz(detail: str) -> None:
    global _warned_bad_tz
    if _warned_bad_tz:
        return
    _warned_bad_tz = True
    logger.warning(
        f"dev_trace: server.timezone resolution failed ({detail}); "
        f"falling back to UTC for this process."
    )


# ---------------------------------------------------------------------------
# Module-level config (cached env-var checks)
# ---------------------------------------------------------------------------

_TRUTHY = {"1", "true", "yes", "on"}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUTHY


_ENABLED: bool = _env_bool("BRIDGE_DEV_TRACE", False)
_REDACT_TOKENS: bool = _env_bool("BRIDGE_DEV_TRACE_REDACT_TOKENS", True)


def _resolve_trace_dir() -> Path:
    override = os.getenv("BRIDGE_DEV_TRACE_DIR")
    if override:
        return Path(override)
    # Default: <app-package>/../logs/trace — ie <repo>/logs/trace inside
    # the container. The docker-compose mount maps host <repo>/logs to
    # /app/logs, so files land in the mapped volume.
    here = Path(__file__).resolve().parent
    return here.parent / "logs" / "trace"


_TRACE_DIR: Path = _resolve_trace_dir()


def is_enabled() -> bool:
    """True iff BRIDGE_DEV_TRACE was set truthy at process startup."""
    return _ENABLED


# ---------------------------------------------------------------------------
# Trace — per-request handle
# ---------------------------------------------------------------------------

@dataclass
class Trace:
    """One per request. Holds the in-memory buffer that gets flushed at
    end_request time; the executor passes this through (via
    ``ctx.dev_trace``) to every helper and to ``bridge_sign``."""

    identity_key: str
    short_id: str
    started_at: float                # time.monotonic() — for duration math
    started_wall_iso: str            # ISO 8601 with timezone — for the header
    buffer: list[str] = field(default_factory=list)
    """Append-only list of formatted lines. ``end_request`` joins with '\\n'
    and writes once."""

    closed: bool = False
    """Defensive — once end_request has flushed, further event() calls
    are silently dropped to avoid double-flushing or appending to a
    closed trace."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def begin_request(ctx: "PipelineCtx") -> Trace | None:
    """Start a new trace for this request. Returns None if tracing is
    disabled. Caller should stash the result on ``ctx.dev_trace``.

    The header block is appended to the buffer (not flushed to disk).
    Inbound body, headers, and identity key are captured here.
    """
    if not _ENABLED:
        return None
    try:
        short_id = os.urandom(3).hex()
        now = time.time()
        wall = datetime.fromtimestamp(now, tz=_resolve_tz())
        wall_iso = wall.isoformat(timespec="milliseconds")

        trace = Trace(
            identity_key=ctx.identity.key,
            short_id=short_id,
            started_at=time.monotonic(),
            started_wall_iso=wall_iso,
        )

        # Header block ----------------------------------------------------
        trace.buffer.append(_RULE)
        trace.buffer.append(
            f"[{wall_iso} {short_id}] REQUEST  identity={ctx.identity.key}"
        )
        trace.buffer.append(_RULE)
        trace.buffer.append("")

        # Inbound block ---------------------------------------------------
        trace.buffer.append("▶ INBOUND")
        trace.buffer.extend(_format_inbound_headers(ctx.headers))
        trace.buffer.extend(_format_inbound_body(ctx.request.raw_body))
        trace.buffer.append("")

        return trace
    except Exception as e:
        # Never let a debug tool kill a request.
        logger.warning(
            f"dev_trace.begin_request failed (request continues without "
            f"trace): {type(e).__name__}: {e!r}"
        )
        return None


_CONTINUATION_KINDS = frozenset({"tuple_end"})
"""Kinds that visually continue the previous event (no leading blank
line above them, no trailing blank line below). Used so a TUPLE
start+end pair reads as one block, not two stanzas with a gap."""


def event(trace: Trace | None, kind: str, **fields: Any) -> None:
    """Append one event to the trace buffer. No-op if trace is None or
    already closed.

    Each ``kind`` has a renderer in ``_RENDERERS``; unknown kinds fall
    back to a generic key=value dump so plugin authors can pass any
    fields they want via ``kind="annotate"`` (or any other label).
    """
    if trace is None or trace.closed:
        return
    try:
        renderer = _RENDERERS.get(kind, _render_generic)
        rendered = renderer(kind, fields)
        if not rendered:
            return
        is_continuation = kind in _CONTINUATION_KINDS
        if is_continuation and trace.buffer and trace.buffer[-1] == "":
            # Drop the blank that the previous (parent) event appended,
            # so the continuation reads as part of the same block.
            trace.buffer.pop()
        trace.buffer.extend(rendered)
        if not is_continuation:
            trace.buffer.append("")
        else:
            trace.buffer.append("")  # one blank after the closing line
    except Exception as e:
        logger.warning(
            f"dev_trace.event(kind={kind!r}) failed (request continues): "
            f"{type(e).__name__}: {e!r}"
        )


def end_request(
    trace: Trace | None,
    *,
    status: str,
    response_summary: str | None = None,
) -> None:
    """Flush the buffered block to disk. No-op if trace is None or
    already closed.

    ``status`` is "ok", "error", "stream-passthrough", or any other
    short label. Caller computes nothing else — duration is taken from
    ``trace.started_at``.
    """
    if trace is None or trace.closed:
        return
    try:
        duration_ms = int((time.monotonic() - trace.started_at) * 1000)

        # Outbound block --------------------------------------------------
        trace.buffer.append("▶ OUTBOUND")
        line = f"  status={status}  duration={duration_ms}ms"
        if response_summary:
            line += f"  {response_summary}"
        trace.buffer.append(line)
        trace.buffer.append("")

        # Footer ----------------------------------------------------------
        end_wall = datetime.now(tz=_resolve_tz())
        end_iso = end_wall.isoformat(timespec="milliseconds")
        trace.buffer.append(_RULE)
        trace.buffer.append(
            f"[{end_iso} {trace.short_id}] END  {status}  {duration_ms}ms"
        )
        trace.buffer.append(_RULE)
        trace.buffer.append("")  # trailing blank line between requests

        block = "\n".join(trace.buffer)
        _flush_block_to_disk(trace.identity_key, block)
    except Exception as e:
        logger.warning(
            f"dev_trace.end_request failed (request already completed): "
            f"{type(e).__name__}: {e!r}"
        )
    finally:
        trace.closed = True


# ---------------------------------------------------------------------------
# Rendering — one function per event kind, returns list of lines
# ---------------------------------------------------------------------------

def _render_generic(kind: str, fields: dict[str, Any]) -> list[str]:
    """Fallback renderer for unknown kinds — used by ``annotate`` and any
    plugin-emitted breadcrumb. Format: ``▶ KIND  key=value  key=value``.
    Long string values get pretty-printed across multiple lines."""
    out = [f"▶ {kind.upper()}"]
    for key, value in fields.items():
        out.extend(_format_field(key, value, indent="  "))
    return out


def _render_verify_inbound(kind: str, fields: dict[str, Any]) -> list[str]:
    """``kind="verify_inbound"`` — fields: result (verified|stripped|skipped|absent),
    optional detail."""
    result = fields.get("result", "?")
    detail = fields.get("detail")
    out = ["▶ VERIFY_INBOUND"]
    out.append(f"  result: {result}")
    if detail:
        out.extend(_format_field("detail", detail, indent="  "))
    return out


def _render_assembly(kind: str, fields: dict[str, Any]) -> list[str]:
    """``kind="assembly"`` — fields: cascade (str), tuples (list of
    (slot, plugin_short_name) pairs)."""
    out = ["▶ ASSEMBLY (cached at startup)"]
    cascade = fields.get("cascade")
    if cascade:
        out.append(f"  cascade:  {cascade}")
    tuples = fields.get("tuples") or []
    out.append(f"  tuples ({len(tuples)}):")
    for i, (slot, plugin_name) in enumerate(tuples, start=1):
        out.append(f"    [{i:02d}] {slot:<32s} {plugin_name}")
    return out


def _render_tuple_start(kind: str, fields: dict[str, Any]) -> list[str]:
    """``kind="tuple_start"`` — fields: index, slot, plugin, method, config.

    ``index=0`` means the tuple isn't part of the request-wide enumeration
    (resource-step tuples — labelled ``(transport)`` / ``(terminal)`` in
    the slot field, so the index would be misleading)."""
    idx = fields.get("index", 0)
    slot = fields.get("slot", "?")
    plugin = fields.get("plugin", "?")
    method = fields.get("method", "?")
    cfg = fields.get("config", {})
    label = f"[{idx:02d}] " if idx else ""
    out = [f"▶ TUPLE {label}{slot}/{plugin}.{method}"]
    if cfg:
        out.append(f"  config: {_format_short_dict(cfg)}")
    else:
        out.append("  config: {}")
    return out


def _render_tuple_end(kind: str, fields: dict[str, Any]) -> list[str]:
    """``kind="tuple_end"`` — fields: result (ok|error|skipped), duration_ms,
    optional message. Returns a list ending in an empty string so the
    visual block (TUPLE start → end) is followed by exactly one blank
    line in the log, not two."""
    result = fields.get("result", "?")
    duration_ms = fields.get("duration_ms", 0)
    message = fields.get("message")
    line = f"  → {result} ({duration_ms}ms)"
    if message:
        line += f"  {message}"
    return [line]


def _render_resource_step(kind: str, fields: dict[str, Any]) -> list[str]:
    """``kind="resource_step"`` — fields: decision, endpoint (optional),
    model (optional), outbound_plugins (optional list of names)."""
    decision = fields.get("decision", "?")
    out = [f"▶ RESOURCE_STEP  decision={decision}"]
    endpoint = fields.get("endpoint")
    if endpoint:
        out.append(f"  endpoint: {endpoint}")
    model = fields.get("model")
    if model:
        out.append(f"  model:    {model}")
    outbound = fields.get("outbound_plugins")
    if outbound:
        out.append(f"  outbound_params plugins: {', '.join(outbound)}")
    return out


def _render_upstream_request(kind: str, fields: dict[str, Any]) -> list[str]:
    """``kind="upstream_request"`` — fields: method, url, headers (dict),
    body (dict)."""
    method = fields.get("method", "POST")
    url = fields.get("url", "?")
    headers = fields.get("headers") or {}
    body = fields.get("body")

    out = [f"▶ UPSTREAM_REQUEST  {method} {url}"]
    out.append(f"  headers ({len(headers)}):")
    for k, v in headers.items():
        v_disp = _redact_header(k, v) if _REDACT_TOKENS else v
        out.append(f"    {k}: {v_disp}")

    if body is not None:
        body_str = _safe_json(body)
        body_bytes = len(body_str.encode("utf-8"))
        out.append(f"  body ({body_bytes} bytes):")
        for line in body_str.splitlines():
            out.append(f"    {line}")
    return out


def _render_upstream_response(kind: str, fields: dict[str, Any]) -> list[str]:
    """``kind="upstream_response"`` — fields: status, duration_ms, body (dict, for
    nonstream) OR stream_summary (str, for stream)."""
    status = fields.get("status", "?")
    duration_ms = fields.get("duration_ms", 0)
    body = fields.get("body")
    stream_summary = fields.get("stream_summary")

    bytes_label = ""
    if body is not None:
        body_str = _safe_json(body)
        bytes_label = f"  {len(body_str.encode('utf-8'))} bytes"

    out = [f"▶ UPSTREAM_RESPONSE  {status}  {duration_ms}ms{bytes_label}"]
    if stream_summary:
        out.append(f"  {stream_summary}")
    elif body is not None:
        out.append("  body (full):")
        for line in _safe_json(body).splitlines():
            out.append(f"    {line}")
    return out


def _render_assemble_and_sign(kind: str, fields: dict[str, Any]) -> list[str]:
    """``kind="assemble_and_sign"`` — fields: bridge_context (dict),
    block (str, the produced XML), injected (bool)."""
    bc = fields.get("bridge_context") or {}
    block = fields.get("block")
    injected = fields.get("injected", True)

    out = ["▶ ASSEMBLE_AND_SIGN"]
    if not bc:
        out.append("  ctx.bridge_context is empty — no block produced")
        return out
    out.append(f"  bridge_context dict ({len(bc)} keys): {_format_short_dict(bc)}")
    if block:
        out.append(f"  signed block ({len(block.encode('utf-8'))} bytes):")
        for line in block.splitlines():
            out.append(f"    {line}")
    out.append(f"  injected into messages: {injected}")
    return out


def _render_error(kind: str, fields: dict[str, Any]) -> list[str]:
    """``kind="error"`` — fields: where (str), exc_type, exc_repr,
    optional traceback."""
    where = fields.get("where", "unknown")
    exc_type = fields.get("exc_type", "?")
    exc_repr = fields.get("exc_repr", "?")
    out = [f"▶ ERROR  at {where}"]
    out.append(f"  {exc_type}: {exc_repr}")
    tb = fields.get("traceback")
    if tb:
        out.append("  traceback:")
        for line in str(tb).splitlines():
            out.append(f"    {line}")
    return out


_RENDERERS: dict[str, Any] = {
    "verify_inbound": _render_verify_inbound,
    "assembly": _render_assembly,
    "tuple_start": _render_tuple_start,
    "tuple_end": _render_tuple_end,
    "resource_step": _render_resource_step,
    "upstream_request": _render_upstream_request,
    "upstream_response": _render_upstream_response,
    "assemble_and_sign": _render_assemble_and_sign,
    "error": _render_error,
    # "annotate" and any plugin-emitted kind falls through to _render_generic.
}


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

_RULE = "═" * 67


def _format_inbound_headers(headers: dict[str, str]) -> list[str]:
    if not headers:
        return ["  headers: (none)"]
    out = [f"  headers ({len(headers)}):"]
    for k, v in headers.items():
        v_disp = _redact_header(k, v) if _REDACT_TOKENS else v
        out.append(f"    {k}: {v_disp}")
    return out


def _format_inbound_body(body: dict | None) -> list[str]:
    if not body:
        return ["  body: (empty)"]
    messages = body.get("messages") or []
    body_json = _safe_json(body)
    body_bytes = len(body_json.encode("utf-8"))

    out = [f"  body ({len(messages)} messages, {body_bytes} bytes):"]
    for msg in messages:
        role = msg.get("role", "?")
        content = msg.get("content", "")
        out.append(f"    [{role}] {_summarise_content(content)}")

    out.append("  full body:")
    for line in body_json.splitlines():
        out.append(f"    {line}")
    return out


def _summarise_content(content: Any) -> str:
    """One-line summary for the [role] preview."""
    if isinstance(content, str):
        flat = content.replace("\n", " ")
        if len(flat) > 80:
            return f"{flat[:77]}... ({len(content)} chars)"
        return flat
    if isinstance(content, list):
        # Multi-part — summarise types and total text length
        type_counts: dict[str, int] = {}
        text_total = 0
        for part in content:
            if isinstance(part, dict):
                t = part.get("type", "?")
                type_counts[t] = type_counts.get(t, 0) + 1
                if t == "text":
                    text_total += len(part.get("text", ""))
        parts = ", ".join(f"{c}×{t}" for t, c in type_counts.items())
        return f"<multi-part: {parts}; text={text_total} chars>"
    if content is None:
        return "<no content>"
    return f"<{type(content).__name__}>"


def _format_field(key: str, value: Any, *, indent: str) -> list[str]:
    """Render a single key=value line, multi-line if the value is big or
    structured."""
    if isinstance(value, (dict, list)):
        rendered = _safe_json(value)
        if "\n" not in rendered and len(rendered) <= 100:
            return [f"{indent}{key}: {rendered}"]
        out = [f"{indent}{key}:"]
        for line in rendered.splitlines():
            out.append(f"{indent}  {line}")
        return out
    text = str(value)
    if "\n" in text:
        out = [f"{indent}{key}:"]
        for line in text.splitlines():
            out.append(f"{indent}  {line}")
        return out
    return [f"{indent}{key}: {text}"]


_SENSITIVE_KEY_HINTS = ("token", "secret", "api_key", "apikey", "password", "auth")
"""Substrings that, when found in a config-dict key, mark that key's value
as sensitive — the trace shows a redacted preview rather than the full
string. Matched case-insensitively. Aimed at outbound-transport plugins
(OpenAI-Protocol's `token: sk-or-...`) but works for any plugin whose
config passes secrets in by name. Bypassable via
``BRIDGE_DEV_TRACE_REDACT_TOKENS=0`` if an operator really wants raw."""


def _is_sensitive_key(key: str) -> bool:
    k = key.lower()
    return any(h in k for h in _SENSITIVE_KEY_HINTS)


def _redact_secret(value: str) -> str:
    """Show first 6 chars of a secret-shaped string, then ``***``. Short
    secrets get fully redacted (8 chars or less = no recoverable prefix)."""
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"
    if len(value) <= 8:
        return "***"
    return f"{value[:6]}***"


def _format_short_dict(d: dict) -> str:
    """One-line dict preview, with values truncated. Used for plugin
    config previews where the full value would be noise. Values for keys
    that look secret-shaped (token / secret / api_key / etc.) are
    redacted unless ``BRIDGE_DEV_TRACE_REDACT_TOKENS=0``."""
    if not d:
        return "{}"
    parts = []
    for k, v in d.items():
        if _REDACT_TOKENS and _is_sensitive_key(str(k)) and isinstance(v, str):
            parts.append(f"{k}={_redact_secret(v)!r}")
            continue
        if isinstance(v, str):
            v_disp = v if len(v) <= 40 else f"{v[:37]}..."
            parts.append(f"{k}={v_disp!r}")
        elif isinstance(v, (dict, list)):
            parts.append(f"{k}=<{type(v).__name__}:{len(v)}>")
        else:
            parts.append(f"{k}={v!r}")
    return "{" + ", ".join(parts) + "}"


def _safe_json(obj: Any) -> str:
    """Pretty-print JSON, falling back to repr() if obj isn't
    JSON-serialisable (the trace must never crash on weird ctx fields)."""
    try:
        return json.dumps(obj, indent=2, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return repr(obj)


def _redact_header(key: str, value: str) -> str:
    """Redact the bearer token in ``Authorization: Bearer ...`` headers.
    Other headers pass through unchanged."""
    if key.lower() != "authorization":
        return value
    if not isinstance(value, str):
        return value
    if value.lower().startswith("bearer "):
        token = value[7:]
        if len(token) <= 8:
            return "Bearer ***"
        return f"Bearer {token[:6]}***"
    return "***"


# ---------------------------------------------------------------------------
# Disk I/O — atomic per-request flush
# ---------------------------------------------------------------------------

def _flush_block_to_disk(identity_key: str, block: str) -> None:
    """Append ``block`` to ``logs/trace/<identity>/<YYYY-MM-DD>.log``,
    creating the directory if needed. Single ``write()`` call so two
    parallel requests for the same identity can never interleave their
    blocks (POSIX append-mode write of <PIPE_BUF bytes is atomic; for
    bigger blocks we open-write-close per request, which serialises at
    the OS level via the file's offset).
    """
    safe_id = _safe_identity_dirname(identity_key)
    today = datetime.now(tz=_resolve_tz()).strftime("%Y-%m-%d")
    target_dir = _TRACE_DIR / safe_id
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{today}.log"
    with target.open("a", encoding="utf-8") as f:
        f.write(block)


def _safe_identity_dirname(identity_key: str) -> str:
    """Strip filesystem-unsafe characters from an identity key. Identity
    keys are operator-controlled config strings; in practice they're
    `[a-zA-Z0-9_-]+`, but defend in case someone names an identity
    something with `/` or `..` in it."""
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in identity_key)
    return safe or "_unnamed"


# ---------------------------------------------------------------------------
# Convenience for the executor's error path
# ---------------------------------------------------------------------------

def emit_exception(trace: Trace | None, where: str, exc: BaseException) -> None:
    """Convenience wrapper used by ``except`` blocks in the executor and
    bridge_sign. Captures the type, repr, and traceback in one call."""
    if trace is None or trace.closed:
        return
    event(
        trace,
        "error",
        where=where,
        exc_type=type(exc).__name__,
        exc_repr=repr(exc),
        traceback=traceback.format_exc(),
    )
