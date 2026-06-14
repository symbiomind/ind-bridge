"""
prompt_parts — shared "ordered list of {file|text} parts → assembled string" helper.

A small piece of core shared by plugins that compose a block of text from an
ordered list of parts, each part either an inline ``{text: "..."}`` or a
``{file: "/path"}`` read from disk. Order is significant; parts join with a
blank line. Bad/missing files are warned-and-skipped (fail-loud-not-fail).

First used by ``system_prompt`` (replace/prepend/append lists); ``cron`` reuses
it for stacked ``prompt:`` lists. Single-sourced here so the two don't drift and
so neither plugin reaches into the other.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def load_items(items: list, *, source: str = "prompt_parts") -> str:
    """Assemble an ordered list of ``{file|text}`` part-dicts into one string.

    Each item is a dict with exactly one of:
      - ``{"text": "..."}``  — inline text
      - ``{"file": "/path"}`` — file content (read + stripped)

    Parts join with ``"\\n\\n"`` in list order. Empty parts (blank text, empty
    file) are dropped. A non-dict item, an unknown key, or an unreadable file is
    logged (with ``source`` for context) and skipped — never raises. Returns the
    joined string (possibly empty if nothing usable).
    """
    parts: list[str] = []
    for item in items or []:
        if not isinstance(item, dict):
            logger.warning(
                f"{source}: item must be a dict with 'file' or 'text' key, got: {item!r}"
            )
            continue
        if "file" in item:
            path = item["file"]
            try:
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                if content:
                    parts.append(content)
            except FileNotFoundError:
                logger.warning(f"{source}: file not found: '{path}'")
            except Exception as e:
                logger.warning(f"{source}: could not read '{path}' — {e}")
        elif "text" in item:
            content = str(item["text"]).strip()
            if content:
                parts.append(content)
        else:
            logger.warning(
                f"{source}: unknown item key (expected 'file' or 'text'): {item!r}"
            )
    return "\n\n".join(parts)
