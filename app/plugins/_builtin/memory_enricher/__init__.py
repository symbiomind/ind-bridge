"""
memory_enricher — builtin ``background`` plugin (V4).

Background label-enrichment daemon. ``conversational_memory`` stores every memory
with a nonce label (unenriched). This plugin finds those nonce-only memories, calls
a small LLM to generate reusable semantic labels, and atomically swaps the nonce out
via ``replace_labels``. Those labels are what conv_mem's **L2 "trending" tier**
consumes — no enrichment ⇒ no labels ⇒ L2 stays dark.

Completely out of band — no request pipeline involvement. Runs as a long-lived
asyncio task spawned at startup via the core ``background`` capability (D-011), the
V4-shaped replacement for V3's ``server.startup`` hook.

V4 vs V3 — the hack is GONE
---------------------------
V3 hand-rolled a fake pipeline to borrow an LLM endpoint (resolve identity → role →
build a mini_ctx → dispatch hooks → scrape endpoint_url/token/model → raw POST). V4
deletes that: the enricher resolves a named LLM **resource** directly and POSTs to its
OpenAI endpoint, the same honest way ``conversational_memory`` resolves its memory
resource. "Use this brain for this job," not "enrichment is a pretend chat identity."

Config (read by ``conversational_memory`` from its top-level ``enrichment:`` block
and passed here)::

    conversational_memory:
      resource: memory_mcp          # the memory store (retrieve/replace/stats)
      nonce: 52868312778495         # unenriched marker (enricher finds raw memories by it)
      enrichment:
        resource: openrouter        # SIMPLE path — an LLM resource (OpenAI-Protocol shape)
        model: gemma3:1b            # cascade: this wins; resource default last
        # prompt: "..."             # optional override of the label instructions
        batch_size: 1               # memories per tick (keep low for small models)
        timeout: 120                # LLM call timeout (seconds)
        # identity: my_enricher            # POWER path — NOT yet implemented

DLC-grace: if ``enrichment`` is absent / has no ``resource``, the loop simply never
spawns. conv_mem still works (L3 recency + L1 recall); only L2 trending goes dark.
A pointer, never a hard dependency.

Capability / placement
----------------------
Declares ``background`` — valid only on ``identity.plugins``. Wire it on the SAME
identity that runs conv_mem (it reads conv_mem's config to find the memory resource +
nonce). Per-identity, like a listener.

Adaptive polling (ported verbatim from V3 / enrichment.ts):
  backlog > 100 → 15s ; > 10 → 60s ; > 0 → 300s ; idle → 900s.
``nudge()`` is called by conversational_memory after a successful store — wakes the
loop early so fresh memories enrich within ~15s rather than waiting up to 900s.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from app.context import StartupCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "background": ["identity.plugins"],
}

# Default nonce — must match conversational_memory._DEFAULT_NONCE.
_DEFAULT_NONCE = 52868312778495

# Adaptive interval thresholds (seconds) — verbatim from V3.
_INTERVAL_HIGH   =  15
_INTERVAL_MEDIUM =  60
_INTERVAL_LOW    = 300
_INTERVAL_IDLE   = 900
_WARMUP_DELAY    =  60

# System prompt — ported verbatim from V3 (enrichment.ts SYSTEM_PROMPT, labels only).
# Overridable via enrichment.prompt.
_SYSTEM_PROMPT = (
    "You are a memory categorization system. Your job is to assign reusable topic category labels to conversation excerpts."
    "\n\n"
    "Output ONLY a comma-separated list of 4-6 labels. Rules:"
    "\n- lowercase, hyphenated, no spaces"
    "\n- REUSABLE: labels must be broad enough to apply to many different conversations"
    "\n- CATEGORICAL: use topic categories, not descriptions of specific events"
    "\n- no explanation, no newlines, no punctuation except commas"
    "\n\n"
    "Good examples: plugin-dev,memory-system,configuration,bug-fix,session-management,api-integration"
    "\n"
    "Bad examples: hype-induced-reset,mistress-priority-delay,test-secret-filter-canary (too specific/unique)"
)

# Module-level state.
_nudge_flag: bool = False  # set by conversational_memory after a successful store


# ---------------------------------------------------------------------------
# Public: nudge — called by conversational_memory after store
# ---------------------------------------------------------------------------

def nudge() -> None:
    """Wake the enrichment loop early — call after storing a new memory."""
    global _nudge_flag
    _nudge_flag = True


# ---------------------------------------------------------------------------
# Capability method: start_background — resolve config + return the loop coro
# ---------------------------------------------------------------------------

def start_background(ctx: "StartupCtx", config: dict):
    """Resolve the memory + LLM connections from this identity's conv_mem config
    and return the enrichment-loop coroutine for core to schedule. Returns None
    (no task spawned) when enrichment isn't configured — DLC-grace.

    ``config`` is this plugin's own ``identity.plugins.memory_enricher`` block (may
    be empty). The authoritative settings live in conv_mem's ``enrichment:`` block,
    which we read from the resolved identity config so operators configure it in ONE
    place (next to the memory resource + nonce)."""
    cm_cfg = _find_conv_mem_config(ctx.identity_cfg)
    if cm_cfg is None:
        logger.info(
            f"memory_enricher: identity '{ctx.identity_key}' has no "
            f"conversational_memory config — enrichment disabled."
        )
        return None

    enrichment = cm_cfg.get("enrichment") or {}
    llm_resource_key = enrichment.get("resource")
    if not llm_resource_key:
        logger.info(
            f"memory_enricher: no enrichment.resource for identity "
            f"'{ctx.identity_key}' — enrichment disabled (L2 trending stays dark)."
        )
        return None  # DLC-grace: not configured, don't spawn.

    if enrichment.get("identity"):
        logger.warning(
            "memory_enricher: enrichment.identity (the pipeline path) is not yet "
            "implemented — falling back to enrichment.resource (the simple path)."
        )

    memory_resource_key = cm_cfg.get("resource")
    memory_conn = _resolve_mcp_resource(memory_resource_key)
    if memory_conn is None:
        logger.error(
            f"memory_enricher: memory resource '{memory_resource_key}' "
            f"unresolved — enrichment disabled."
        )
        return None

    llm_conn = _resolve_openai_resource(llm_resource_key)
    if llm_conn is None:
        logger.error(
            f"memory_enricher: LLM resource '{llm_resource_key}' unresolved — "
            f"enrichment disabled."
        )
        return None

    llm_config = {
        "url": llm_conn["url"],
        "token": llm_conn["token"],
        # model cascade: enrichment.model wins; else the resource's own default.
        "model": enrichment.get("model") or llm_conn.get("model") or "",
        "timeout": float(enrichment.get("timeout", 120)),
        "prompt": enrichment.get("prompt") or _SYSTEM_PROMPT,
    }
    if not llm_config["model"]:
        logger.error(
            f"memory_enricher: no model resolved (set enrichment.model or a "
            f"default on resource '{llm_resource_key}') — enrichment disabled."
        )
        return None

    nonce = str(cm_cfg.get("nonce", _DEFAULT_NONCE))
    batch_size = int(enrichment.get("batch_size", 1))

    logger.info(
        f"memory_enricher: starting for identity='{ctx.identity_key}' "
        f"model='{llm_config['model']}' llm='{llm_config['url']}' "
        f"memory='{memory_conn['endpoint_url']}' nonce={nonce} batch_size={batch_size}"
    )
    return _enrichment_loop(memory_conn, llm_config, nonce, batch_size)


# ---------------------------------------------------------------------------
# Config + resource resolution
# ---------------------------------------------------------------------------

def _find_conv_mem_config(identity_cfg: dict) -> dict | None:
    """Locate the conversational_memory plugin config on the resolved identity.
    conv_mem lives in a context slot (identity.context.plugins or
    role.context.plugins). The resolved identity_cfg merges the cascade, so check
    both context blocks."""
    for block_key in ("context",):
        block = identity_cfg.get(block_key) or {}
        plugins = block.get("plugins") or {}
        cm = plugins.get("conversational_memory")
        if isinstance(cm, dict):
            return cm
    return None


def _resolve_mcp_resource(resource_key: str | None) -> dict | None:
    """MCP-shape resource: endpoint_url + token directly on the block."""
    if not resource_key:
        return None
    from app import config as app_config
    cfg = app_config.resolve_resource(resource_key) or {}
    endpoint_url = cfg.get("endpoint_url")
    if not endpoint_url:
        return None
    return {
        "endpoint_url": endpoint_url,
        "token": cfg.get("token") or "",
        "timeout": float(cfg["timeout"]) if cfg.get("timeout") is not None else 120.0,
    }


def _resolve_openai_resource(resource_key: str | None) -> dict | None:
    """OpenAI-Protocol-shape resource: url + token nested under
    plugins.OpenAI-Protocol (the shape chat resources use). Also accepts an
    optional resource-level default model."""
    if not resource_key:
        return None
    from app import config as app_config
    cfg = app_config.resolve_resource(resource_key) or {}
    op = (cfg.get("plugins") or {}).get("OpenAI-Protocol") or {}
    url = op.get("url") or cfg.get("endpoint_url")  # tolerate a bare endpoint_url too
    if not url:
        return None
    return {
        "url": url,
        "token": op.get("token") or cfg.get("token") or "",
        "model": op.get("model"),  # optional resource default (cascade: last)
    }


# ---------------------------------------------------------------------------
# Background enrichment loop (ported verbatim from V3)
# ---------------------------------------------------------------------------

async def _enrichment_loop(memory_conn: dict, llm_config: dict, nonce: str, batch_size: int) -> None:
    global _nudge_flag

    logger.info(f"memory_enricher: loop starting — warmup {_WARMUP_DELAY}s")
    await asyncio.sleep(_WARMUP_DELAY)

    running = False

    while True:
        if running:
            logger.info("memory_enricher: previous tick still running — skipping")
            await asyncio.sleep(_INTERVAL_MEDIUM)
            continue

        if _nudge_flag:
            _nudge_flag = False
            logger.debug("memory_enricher: nudged — processing immediately")

        running = True
        processed = 0
        remaining = 0

        try:
            batch = await _retrieve_unenriched(memory_conn, nonce, batch_size)
            if not batch:
                logger.debug("memory_enricher: no unenriched memories — idling")
            else:
                for mem in batch:
                    mem_id = mem.get("id")
                    content = mem.get("content", "")
                    if not mem_id or not content:
                        continue
                    labels = await _enrich_one(mem_id, content, llm_config)
                    if labels:
                        await _replace_labels(memory_conn, mem_id, nonce, labels)
                        logger.info(f"memory_enricher: #{mem_id} → {labels}")
                        processed += 1
                    else:
                        logger.warning(
                            f"memory_enricher: #{mem_id} — could not generate labels, "
                            f"skipping (stays nonce-labelled, retried next tick)"
                        )
            remaining = await _get_remaining_count(memory_conn, nonce)
        except asyncio.CancelledError:
            logger.info("memory_enricher: loop cancelled — shutting down")
            raise
        except Exception as e:
            logger.error(f"memory_enricher: tick error — {e}", exc_info=True)
        finally:
            running = False

        interval = _adaptive_interval(remaining)
        if processed > 0 or remaining > 0:
            logger.info(
                f"memory_enricher: processed={processed} remaining={remaining} "
                f"next_tick={interval}s"
            )
        await asyncio.sleep(interval)


def _adaptive_interval(remaining: int) -> int:
    if remaining > 100:
        return _INTERVAL_HIGH
    if remaining > 10:
        return _INTERVAL_MEDIUM
    if remaining > 0:
        return _INTERVAL_LOW
    return _INTERVAL_IDLE


# ---------------------------------------------------------------------------
# LLM call + label validation (ported verbatim from V3)
# ---------------------------------------------------------------------------

async def _enrich_one(mem_id, content: str, llm_config: dict) -> str | None:
    """Call the LLM to generate labels for a memory. Retries once on bad output."""
    for attempt in range(1, 3):
        try:
            raw = await _call_llm(llm_config, content)
            labels = _parse_labels(raw)
            if labels:
                return labels
            logger.warning(
                f"memory_enricher: #{mem_id} attempt {attempt} — bad label output: '{raw[:80]}'"
            )
        except Exception as e:
            logger.warning(f"memory_enricher: #{mem_id} attempt {attempt} — error: {e}")
    return None


async def _call_llm(llm_config: dict, content: str) -> str:
    """POST to the OpenAI-compatible endpoint, return raw assistant content."""
    url = llm_config["url"].rstrip("/") + "/chat/completions"
    token = llm_config["token"]
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    body = {
        "model": llm_config["model"],
        "stream": False,
        "messages": [
            {"role": "system", "content": llm_config["prompt"]},
            {"role": "user", "content": f"Label this conversation:\n\n{content}"},
        ],
    }

    async with httpx.AsyncClient(timeout=llm_config["timeout"]) as client:
        r = await client.post(url, headers=headers, json=body)
        r.raise_for_status()
        data = r.json()
        return (
            data.get("choices", [{}])[0].get("message", {}).get("content", "")
            or data.get("message", {}).get("content", "")
        ).strip()


def _parse_labels(raw: str) -> str | None:
    """Parse + validate raw LLM output into a clean comma-separated label string.
    Splits on any delimiter, strips punctuation noise, validates format, requires
    ≥4 tokens, returns first 6 joined by comma. None if below the quality bar
    (triggers retry). Ported verbatim from V3."""
    tokens = [
        re.sub(r"[!.]", "", t).lower().strip()
        for t in re.split(r"[,\s]+", raw)
    ]
    valid = [t for t in tokens if re.fullmatch(r"[a-z][a-z0-9\-]{2,}", t)]
    if len(valid) < 4:
        return None
    return ",".join(valid[:6])


# ---------------------------------------------------------------------------
# memory-mcp-ce MCP calls (ported verbatim from V3)
# ---------------------------------------------------------------------------

async def _retrieve_unenriched(conn: dict, nonce: str, num_results: int) -> list:
    """Fetch a batch of unenriched memories (labelled with nonce only)."""
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    url = conn["endpoint_url"]
    headers = {"Authorization": f"Bearer {conn['token']}"} if conn["token"] else {}
    async with streamablehttp_client(url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(
                "retrieve_memories",
                arguments={"labels": nonce, "num_results": num_results},
            )
            parsed = _parse_mcp_result(result)
            if isinstance(parsed, dict):
                return parsed.get("memories", [])
            return parsed if isinstance(parsed, list) else []


async def _replace_labels(conn: dict, memory_id, nonce: str, new_labels: str) -> None:
    """Atomically swap the nonce label for real semantic labels."""
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    url = conn["endpoint_url"]
    headers = {"Authorization": f"Bearer {conn['token']}"} if conn["token"] else {}
    async with streamablehttp_client(url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            await session.call_tool(
                "replace_labels",
                arguments={"memory_id": memory_id, "target": nonce, "new": new_labels},
            )


async def _get_remaining_count(conn: dict, nonce: str) -> int:
    """Count memories still carrying the nonce label (backlog size)."""
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    url = conn["endpoint_url"]
    headers = {"Authorization": f"Bearer {conn['token']}"} if conn["token"] else {}
    async with streamablehttp_client(url, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("memory_stats", arguments={"labels": nonce})
            parsed = _parse_mcp_result(result)
            if isinstance(parsed, dict):
                return int(parsed.get("matching", 0))
            return 0


def _parse_mcp_result(result) -> list | dict:
    """Extract content from an MCP tool result — same pattern as conversational_memory."""
    try:
        for item in result.content:
            text = getattr(item, "text", None)
            if text:
                return json.loads(text)
    except Exception as e:
        logger.warning(f"memory_enricher: _parse_mcp_result failed — {e}")
    return []
