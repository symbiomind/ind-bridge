"""
skills — builtin context_modify plugin (V4).

Surfaces a catalog of Anthropic-standard skills (``<dir>/<skill>/SKILL.md`` with
``name`` + ``description`` YAML frontmatter) to the agent as a formatted list,
APPENDED to the system prompt. The sibling of ``system_prompt``: same capability,
same slot, declared AFTER it so it appends onto the soul block.

Progressive disclosure: only the frontmatter (name + description + path) goes
into the always-on system prompt. The agent reads the full SKILL.md body via its
own filesystem tools ONLY when a skill is relevant — so the catalog is cheap and
the bodies are free until used.

The host↔container path twist
-----------------------------
The bridge reads skill files from HOST paths (it has the host dir bind-mounted).
But an agent opens files through a filesystem MCP server that mounts the same host
dir under a DIFFERENT container path (e.g. host
``/opt/agents/myagent/skills`` is ``/home/myagent/skills`` inside the MCP
container, with ``FS_ALLOWED_DIRS=/home``). If we advertised the host path the
agent couldn't open it. So ``dir:`` carries an optional ``:presented`` relabel —
we WALK the left (host) path but SHOW the right (container) path the agent can
actually open. The relabel is a render-time prefix substitution; the bridge does
NOT parse docker-compose or introspect mounts.

Config shape
------------
    plugins:
      skills:
        items:                                   # ordered list of {text | dir} parts
          - text: "# Skills"
          - text: "Check and read the relevant SKILL.md before acting."
          - dir: /host/shared/skills:/home/shared/skills    # walk left, show right
          - dir: /host/myagent/skills:/home/myagent/skills
          - dir: /host/general/skills                       # no relabel — show host path

``items`` is an ordered list of tagged dicts:
  - ``{text: "..."}`` — inline text, verbatim (reuses app.prompt_parts semantics)
  - ``{dir: "host[:presented]"}`` — a skills directory. Walk ``host`` for
    ``<skill>/SKILL.md``; render each as a catalog line. If ``:presented`` is
    given, the host prefix is rewritten to it in the shown path.

Merge / override
----------------
All ``dir:`` items contribute to ONE skill pool keyed by skill name (the folder
name, or frontmatter ``name`` if present). Dirs are processed in list order;
a skill name seen in a LATER dir REPLACES the earlier one (last-wins) — so
shared defaults can be overridden by agent-specific skills declared later. The
catalog renders in first-seen order (stable), with the last-wins content.

Bad input
---------
Warn-and-skip, never crash (mirrors app.prompt_parts). A missing dir, a folder
with no SKILL.md, unreadable/unparseable frontmatter, or a skill missing a
``description`` → one warning logged and that skill/dir is skipped. The pipeline
continues and the valid skills still render.

Capability / placement
----------------------
Declares ``context_modify`` — valid in ``identity.context.plugins`` or
``role.context.plugins``. Declare AFTER ``system_prompt`` (same slot) so it
appends onto system_prompt's output (within-slot order is config declaration
order). Files are walked/read PER REQUEST (no caching) — add a skill, it shows
next turn, no restart.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.context import PipelineCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "context_modify": ["identity.context.plugins", "role.context.plugins"],
}

SKILL_FILE = "SKILL.md"


# ---------------------------------------------------------------------------
# Capability method
# ---------------------------------------------------------------------------

def modify_context(ctx: "PipelineCtx", config: dict) -> "PipelineCtx":
    """Render the skills block per config and append it to the system message.
    Passthrough (ctx unchanged) when there are no items or nothing renders."""
    items = config.get("items")
    if not items:
        return ctx

    block = _render_block(items)
    if not block:
        return ctx

    from app import system_message
    system_message.append_to_system(ctx.request.messages, block)
    return ctx


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _render_block(items: list) -> str:
    """Walk the ordered ``items`` list, assembling text parts and skill-dir
    catalogs into one block. ``text:`` parts render in list position; ``dir:``
    parts all merge into a single last-wins skill pool that renders at the
    position of the FIRST ``dir:`` item (so the surrounding text reads naturally).

    Returns the joined block (``\\n\\n`` between parts), possibly empty.
    """
    if not isinstance(items, list):
        logger.warning(f"skills: 'items' must be a list, got: {items!r}")
        return ""

    # First pass: collect the merged skill pool (last-wins by name) across all
    # dir items, preserving first-seen order for stable rendering.
    pool: dict[str, str] = {}  # skill_name -> rendered catalog line
    for item in items:
        if isinstance(item, dict) and "dir" in item:
            _walk_dir_into_pool(item["dir"], pool)

    skills_rendered = "\n".join(pool.values()) if pool else ""

    # Second pass: assemble parts in list order. The merged skill list renders
    # once, at the first dir item; later dir items are absorbed (already pooled).
    parts: list[str] = []
    skills_emitted = False
    for item in items:
        if not isinstance(item, dict):
            logger.warning(
                f"skills: item must be a dict with 'text' or 'dir' key, got: {item!r}"
            )
            continue
        if "text" in item:
            text = str(item["text"]).strip()
            if text:
                parts.append(text)
        elif "dir" in item:
            if not skills_emitted and skills_rendered:
                parts.append(skills_rendered)
                skills_emitted = True
        else:
            logger.warning(
                f"skills: unknown item key (expected 'text' or 'dir'): {item!r}"
            )

    return "\n\n".join(parts)


def _walk_dir_into_pool(spec: str, pool: dict[str, str]) -> None:
    """Parse a ``host[:presented]`` dir spec, walk the host path for skill
    folders, and add/overwrite each into ``pool`` (last-wins by skill name).
    Warn-and-skip on any failure."""
    host, presented = _split_dir_spec(spec)
    if not host:
        logger.warning(f"skills: empty dir spec, skipped: {spec!r}")
        return

    try:
        entries = sorted(os.scandir(host), key=lambda e: e.name)
    except FileNotFoundError:
        logger.warning(f"skills: dir not found: '{host}'")
        return
    except NotADirectoryError:
        logger.warning(f"skills: not a directory: '{host}'")
        return
    except OSError as e:
        logger.warning(f"skills: could not list dir '{host}' — {e}")
        return

    for entry in entries:
        if not entry.is_dir():
            continue
        skill_path = os.path.join(entry.path, SKILL_FILE)
        if not os.path.isfile(skill_path):
            logger.warning(
                f"skills: no {SKILL_FILE} in '{entry.path}' — skipped"
            )
            continue
        meta = _read_skill_frontmatter(skill_path)
        if meta is None:
            continue  # already warned
        name = meta.get("name") or entry.name
        description = meta.get("description")
        if not description:
            logger.warning(
                f"skills: '{name}' ({skill_path}) missing 'description' "
                f"frontmatter — skipped"
            )
            continue
        # Relabel the displayed path: host prefix -> presented prefix.
        shown_path = _relabel_path(skill_path, host, presented)
        pool[name] = f"- {name}: {description}  ({shown_path})"


def _split_dir_spec(spec: str) -> tuple[str, str | None]:
    """Split ``host[:presented]`` into (host, presented). A Windows-style drive
    colon isn't a concern here (POSIX paths). Only the FIRST ':' that separates
    two absolute-looking paths is treated as the relabel separator; we split on
    the LAST ':' so a presented path may itself be absolute. Both sides are
    plain absolute POSIX paths in practice."""
    if not isinstance(spec, str):
        return ("", None)
    spec = spec.strip()
    # A presented relabel looks like '/host/path:/container/path'. Split on the
    # ':' that precedes the second leading slash. Simplest robust rule: if there
    # are exactly-two colon-separated absolute paths, split on the single ':'.
    if ":" in spec:
        left, _, right = spec.partition(":")
        left, right = left.strip(), right.strip()
        if left and right:
            return (left, right)
        # Trailing/leading colon with one empty side — treat whole as host.
        return (left or right, None)
    return (spec, None)


def _relabel_path(real_path: str, host_prefix: str, presented: str | None) -> str:
    """Rewrite ``real_path``'s ``host_prefix`` to ``presented`` for display.
    No-op (returns real_path) when no presented prefix is given."""
    if not presented:
        return real_path
    host_prefix = host_prefix.rstrip("/")
    presented = presented.rstrip("/")
    if real_path == host_prefix:
        return presented
    if real_path.startswith(host_prefix + "/"):
        return presented + real_path[len(host_prefix):]
    # Shouldn't happen (real_path is built under host_prefix) — show real.
    return real_path


# ---------------------------------------------------------------------------
# Frontmatter parsing
# ---------------------------------------------------------------------------

def _read_skill_frontmatter(path: str) -> dict | None:
    """Read the leading ``--- ... ---`` YAML frontmatter block from a SKILL.md.
    Returns the parsed dict (at least possibly empty), or None on any failure
    (warn-and-skip). The body below the closing fence is ignored — it's the
    progressive-disclosure part the agent reads on demand."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
    except OSError as e:
        logger.warning(f"skills: could not read '{path}' — {e}")
        return None

    fm = _extract_frontmatter(text)
    if fm is None:
        logger.warning(f"skills: no YAML frontmatter in '{path}' — skipped")
        return None

    import yaml
    try:
        data = yaml.safe_load(fm)
    except yaml.YAMLError as e:
        logger.warning(f"skills: bad frontmatter YAML in '{path}' — {e}")
        return None

    if data is None:
        return {}
    if not isinstance(data, dict):
        logger.warning(
            f"skills: frontmatter in '{path}' is not a mapping — skipped"
        )
        return None
    return data


def _extract_frontmatter(text: str) -> str | None:
    """Return the raw YAML between the leading ``---`` fences, or None if the
    file doesn't open with a frontmatter block. Tolerant of a leading BOM
    (U+FEFF) and blank lines before the first fence."""
    lines = text.lstrip("﻿").splitlines()
    # Skip leading blank lines.
    idx = 0
    while idx < len(lines) and not lines[idx].strip():
        idx += 1
    if idx >= len(lines) or lines[idx].strip() != "---":
        return None
    body: list[str] = []
    for line in lines[idx + 1:]:
        if line.strip() == "---":
            return "\n".join(body)
        body.append(line)
    # No closing fence.
    return None
