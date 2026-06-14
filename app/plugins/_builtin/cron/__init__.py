"""
cron — builtin ``background`` plugin (V4).

Fires an agent identity on a schedule with **no external HTTP caller**. The first
mechanism for "the bridge drives a turn from the inside" — an agent reflects at 3am,
says good-morning at 9am, etc. Each scheduled tick synthesises a minimal request
body and drives it through this identity's own pipeline via
``pipeline_executor.execute()``.

The reply goes nowhere on purpose. Wire the cron identity onto a role/session that
runs ``basic_session`` and the synthesised turn lands in the shared history for
free — the agent's next harness connection sees it as memory. cron discards the
response; ``basic_session.observe_response`` already persisted it.

Self-as-caller
--------------
Give the cron identity a ``context:`` block (name / trust / additional) and the
signed ``<bridge_context>`` carries it like any other caller — so a 3am reflection
arrives stamped ``<caller>MyAgent</caller> status="3am Reflection"``. It's calling
itself, and it knows *why* it's awake. No ``bridge-system`` machinery needed.

Config (in identity.plugins.cron — each key is a NAMED job)::

    identities:
      my_agent_cron:
        role: my_agent_role            # shared role ⇒ shared session ⇒ lands in history
        token: ${MY_AGENT_TOKEN}
        plugins:
          cron:
            reflect_3am:
              time: "0 3 * * *"        # standard 5-field cron expression
              prompt_text: "You are calling yourself — reflect on your day."
              # prompt_file: /path/to/prompt.md   # alternative to prompt_text
              # timezone: Australia/Adelaide       # optional; else identity/UTC
              # model: anthropic/claude-opus-4.8   # optional; else resource default
            morning_hello:
              time: "0 9 * * *"
              prompt_text: "Good morning. What's on your mind?"
        context:
          name: MyAgent
          trust: trusted
          additional:
            status: "3am Reflection"

DLC-grace: no ``cron:`` block / no valid job ⇒ the loop never spawns. A bad single
job is warned-and-skipped; the other jobs still run. A bad single *tick* is logged
and swallowed — one failed fire never kills the loop.

Capability / placement
----------------------
Declares ``background`` — valid only on ``identity.plugins``. Per-identity, like a
listener. core (``server._spawn_background_tasks``) calls ``start_background`` once
with this plugin's cascade-merged config (the named-jobs dict) and schedules the
returned coroutine; lifespan shutdown cancels it cleanly.

Parallelarismerers note: a cron identity on a *shared* session is a concurrent
caller. The day a 3am fire overlaps a live LibreChat turn is the first real test of
the session-serialisation idea — until that lands, two concurrent writers to one
session can race (a pre-existing basic_session property, not cron's bug).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import TYPE_CHECKING

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from app.context import StartupCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "background": ["identity.plugins"],
}


# ---------------------------------------------------------------------------
# Capability method: start_background — parse jobs, return the runner coroutine
# ---------------------------------------------------------------------------

def start_background(ctx: "StartupCtx", config: dict):
    """Parse this identity's named cron jobs and return ONE coroutine that runs
    them all (one sleeper-loop per job). Returns None — no task spawned — when no
    valid job is configured (DLC-grace).

    ``config`` is this plugin's cascade-merged ``identity.plugins.cron`` block: a
    dict of ``{job_name: {time, prompt_text|prompt_file, timezone?, model?}}``.
    """
    if not isinstance(config, dict) or not config:
        logger.info(
            f"cron: identity '{ctx.identity_key}' has no cron jobs — nothing to schedule."
        )
        return None

    # An identity-level timezone hint (e.g. from context.additional or a server
    # default surfaced into identity_cfg). Per-job `timezone` overrides it; final
    # fallback is UTC. Mirrors time_inject's resolution order.
    identity_tz = _identity_timezone(ctx.identity_cfg)

    jobs = []
    for job_name, job_cfg in config.items():
        job = _build_job(ctx.identity_key, job_name, job_cfg, identity_tz)
        if job is not None:
            jobs.append(job)

    if not jobs:
        logger.warning(
            f"cron: identity '{ctx.identity_key}' has a cron block but no VALID "
            f"jobs — nothing to schedule."
        )
        return None

    logger.info(
        f"cron: starting identity='{ctx.identity_key}' with {len(jobs)} job(s): "
        f"{', '.join(j['name'] for j in jobs)}"
    )
    return _run_all_jobs(ctx.identity_key, jobs)


# ---------------------------------------------------------------------------
# Job parsing / validation (at startup — fail loud, skip the bad one)
# ---------------------------------------------------------------------------

def _build_job(identity_key: str, job_name: str, job_cfg, identity_tz: str) -> dict | None:
    """Validate one named job's config into a runnable job dict, or None (warn +
    skip) if it's malformed. Reads ``prompt_file`` from disk ONCE here."""
    if not isinstance(job_cfg, dict):
        logger.warning(
            f"cron: identity '{identity_key}' job '{job_name}' is not a mapping — skipped."
        )
        return None

    cron_expr = job_cfg.get("time")
    if not cron_expr or not isinstance(cron_expr, str):
        logger.warning(
            f"cron: identity '{identity_key}' job '{job_name}' has no 'time' "
            f"cron expression — skipped."
        )
        return None

    # Validate the cron expression up front so a typo fails loud at startup.
    try:
        from croniter import croniter
        if not croniter.is_valid(cron_expr):
            raise ValueError("invalid cron expression")
    except ImportError:
        logger.error(
            "cron: the 'croniter' package is not installed — cron disabled. "
            "It's declared in the bridge's main requirements.txt (builtin dep)."
        )
        return None
    except Exception as e:
        logger.warning(
            f"cron: identity '{identity_key}' job '{job_name}' time='{cron_expr}' "
            f"is not a valid cron expression ({e}) — skipped."
        )
        return None

    prompt = _resolve_prompt(identity_key, job_name, job_cfg)
    if prompt is None:
        return None  # already warned

    tz_name = job_cfg.get("timezone") or identity_tz or "UTC"
    try:
        ZoneInfo(tz_name)
    except Exception:
        logger.warning(
            f"cron: identity '{identity_key}' job '{job_name}' unknown timezone "
            f"'{tz_name}' — falling back to UTC."
        )
        tz_name = "UTC"

    # Job-level `additional` — free-form keys this job stamps into the SIGNED
    # <bridge_context> for its fired turn (layered over the identity's own
    # context.additional; job wins on a key collision). Lets one identity's
    # different jobs (3am-reflection vs 9am-hello) carry distinct signed context.
    job_additional = job_cfg.get("additional")
    if job_additional is not None and not isinstance(job_additional, dict):
        logger.warning(
            f"cron: identity '{identity_key}' job '{job_name}' 'additional' must be a "
            f"mapping — ignoring it."
        )
        job_additional = None

    return {
        "name": job_name,
        "cron_expr": cron_expr,
        "tz": tz_name,
        "prompt": prompt,
        "model": job_cfg.get("model") or None,
        "additional": job_additional or {},
    }


def _resolve_prompt(identity_key: str, job_name: str, job_cfg: dict) -> str | None:
    """Resolve the job's prompt, read once at startup. Priority:

      1. ``prompt:`` — an ORDERED list of ``{file|text}`` parts, assembled
         top-to-bottom (same shape as the system_prompt plugin; shares
         ``app.prompt_parts``). Use this to stack a file + inline text.
      2. ``prompt_text`` — a single inline string (shorthand).
      3. ``prompt_file`` — a single file path (shorthand).

    Warn+None if nothing usable is present (DLC-grace: the job is skipped)."""
    prompt_list = job_cfg.get("prompt")
    if prompt_list is not None:
        if not isinstance(prompt_list, list):
            logger.warning(
                f"cron: identity '{identity_key}' job '{job_name}' 'prompt' must be a "
                f"list of {{file|text}} parts — got {type(prompt_list).__name__}; skipped."
            )
            return None
        from app import prompt_parts
        assembled = prompt_parts.load_items(
            prompt_list, source=f"cron[{identity_key}/{job_name}]"
        ).strip()
        if assembled:
            return assembled
        logger.warning(
            f"cron: identity '{identity_key}' job '{job_name}' 'prompt' list produced "
            f"no usable text (all parts empty/unreadable) — skipped."
        )
        return None

    prompt_text = job_cfg.get("prompt_text")
    prompt_file = job_cfg.get("prompt_file")

    if prompt_text and prompt_file:
        logger.warning(
            f"cron: identity '{identity_key}' job '{job_name}' has BOTH prompt_text "
            f"and prompt_file — using prompt_text. (Use a 'prompt:' list to stack both.)"
        )
    if prompt_text:
        return str(prompt_text)

    if prompt_file:
        try:
            with open(prompt_file, "r", encoding="utf-8") as fh:
                content = fh.read().strip()
            if not content:
                logger.warning(
                    f"cron: identity '{identity_key}' job '{job_name}' prompt_file "
                    f"'{prompt_file}' is empty — skipped."
                )
                return None
            return content
        except OSError as e:
            logger.warning(
                f"cron: identity '{identity_key}' job '{job_name}' could not read "
                f"prompt_file '{prompt_file}' ({e}) — skipped."
            )
            return None

    logger.warning(
        f"cron: identity '{identity_key}' job '{job_name}' has neither prompt_text "
        f"nor prompt_file — skipped."
    )
    return None


def _identity_timezone(identity_cfg: dict) -> str | None:
    """Best-effort identity-level timezone hint. We don't have ctx.timezone at
    startup (that's a PipelineCtx field), so peek at a couple of conventional
    spots; per-job `timezone` overrides this anyway, and UTC is the final fallback."""
    if not isinstance(identity_cfg, dict):
        return None
    # Allow a `timezone:` directly on the identity, or under context.additional.
    tz = identity_cfg.get("timezone")
    if tz:
        return str(tz)
    context = identity_cfg.get("context") or {}
    additional = context.get("additional") or {}
    tz = additional.get("timezone")
    return str(tz) if tz else None


# ---------------------------------------------------------------------------
# The runner: one sleeper-loop per job, gathered into a single coroutine
# ---------------------------------------------------------------------------

async def _run_all_jobs(identity_key: str, jobs: list[dict]) -> None:
    """Run every job loop concurrently under one task. CancelledError from
    shutdown propagates out of gather and cancels each child loop cleanly."""
    await asyncio.gather(*(_job_loop(identity_key, job) for job in jobs))


async def _job_loop(identity_key: str, job: dict) -> None:
    """Sleep until the next scheduled time, fire, repeat. One bad fire is logged
    and swallowed; the loop continues. CancelledError re-raises for clean shutdown."""
    from croniter import croniter

    name = job["name"]
    tz = ZoneInfo(job["tz"])
    logger.info(
        f"cron: job '{name}' (identity '{identity_key}') scheduled '{job['cron_expr']}' "
        f"[{job['tz']}]"
    )

    while True:
        now = datetime.now(tz)
        nxt = croniter(job["cron_expr"], now).get_next(datetime)
        delay = max(0.0, (nxt - now).total_seconds())
        logger.debug(
            f"cron: job '{name}' next fire at {nxt.isoformat()} (in {delay:.0f}s)"
        )
        try:
            await asyncio.sleep(delay)
            await _fire(identity_key, job)
        except asyncio.CancelledError:
            logger.info(f"cron: job '{name}' cancelled — shutting down")
            raise
        except Exception as e:
            logger.error(
                f"cron: job '{name}' (identity '{identity_key}') tick error — {e!r} "
                f"— loop continues.",
                exc_info=True,
            )


async def _fire(identity_key: str, job: dict) -> None:
    """Synthesise a minimal request and drive it through the identity's pipeline.
    The response is DISCARDED — basic_session has already persisted the turn."""
    from app import pipeline_executor

    body = {
        "messages": [{"role": "user", "content": job["prompt"]}],
        "stream": False,
        "tools": [],
    }
    if job.get("model"):
        body["model"] = job["model"]
    # Job-level additional → merged into ctx.identity.additional by the executor
    # (job wins over identity context.additional), then signed into <bridge_context>
    # by core's populate_caller. The `_cron_` prefix is popped before the upstream
    # call, so it never leaks to the model provider.
    if job.get("additional"):
        body["_cron_additional"] = job["additional"]

    logger.info(f"cron: firing job '{job['name']}' for identity '{identity_key}'")
    resp = await pipeline_executor.execute(identity_key, body, {})

    # Sanity log only — we sent stream:False so this should be a dict. If a
    # StreamingResponse comes back, the turn still fired; nothing to consume.
    if isinstance(resp, dict):
        finish = (resp.get("choices") or [{}])[0].get("finish_reason")
        logger.info(
            f"cron: job '{job['name']}' fired OK (identity '{identity_key}', "
            f"finish_reason={finish})"
        )
    else:
        logger.info(
            f"cron: job '{job['name']}' fired (identity '{identity_key}', "
            f"non-dict response {type(resp).__name__} — turn still ran)"
        )
