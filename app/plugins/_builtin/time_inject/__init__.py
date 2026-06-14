"""
time_inject — builtin context_modify plugin (V4).

Injects the current friendly local time into ``<bridge_context>``.
Solves UTC confusion for buddies that need to know the correct local time.

Config (in identity.context.plugins / role.context.plugins → time_inject):
  timezone: "Australia/Adelaide"   # any IANA timezone; overrides ctx.timezone if set

Timezone resolution order:
  1. Explicit ``timezone:`` in this plugin's config (highest priority)
  2. ``ctx.timezone`` — resolved from the cascade
     (identity → role → session → resource → server → TZ env → UTC)

Output in bridge_context:
  ctx.bridge_context["current_time"] = "Wednesday, 1st April 2026 - 09:14 AM ACDT"
  → <current_time>Wednesday, 1st April 2026 - 09:14 AM ACDT</current_time>

Capability / placement
----------------------
Declares ``context_modify`` — valid in ``identity.context.plugins`` or
``role.context.plugins`` (same slots V3's hooks ``role.context`` /
``identity.context`` mapped to).

Signing is core's job now
-------------------------
Unlike V3, this plugin does NOT sign anything. It just contributes the
``current_time`` key to ``ctx.bridge_context``; core
(``app.bridge_sign.assemble_and_sign``) assembles + HMAC-signs the whole
``<bridge_context>`` block AFTER the context plugins run (D-005). Keys become
XML tags in insertion order.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.context import PipelineCtx

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "context_modify": ["identity.context.plugins", "role.context.plugins"],
}


# ---------------------------------------------------------------------------
# Capability method
# ---------------------------------------------------------------------------

def modify_context(ctx: "PipelineCtx", config: dict) -> "PipelineCtx":
    """Resolve the timezone, format the friendly local time, and stash it in
    ``ctx.bridge_context["current_time"]`` for core to assemble + sign."""
    tz_name = config.get("timezone") or ctx.timezone

    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        logger.warning(f"time_inject: unknown timezone '{tz_name}', falling back to UTC")
        tz = ZoneInfo("UTC")

    now = datetime.now(tz)

    day = now.day
    suffix = "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    friendly = now.strftime(f"%A, {day}{suffix} %B %Y - %I:%M %p %Z")

    ctx.bridge_context["current_time"] = friendly
    return ctx
