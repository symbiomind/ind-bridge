"""
system_message — shared "read/write the single system message" helper.

A small piece of core shared by plugins that need to read or rewrite the one
``role: "system"`` message in an OpenAI-protocol message list. First used by
``system_prompt`` (replace/prepend/append a soul block); ``skills`` reuses it to
APPEND a skills catalog after system_prompt's output.

Single-sourced here so the two plugins can't drift on the "exactly one system
message" invariant (an existing system message is overwritten in place;
otherwise one is inserted at index 0). Same drift-prevention rationale as
``app.prompt_parts``.
"""

from __future__ import annotations


def extract_system(messages: list[dict]) -> str | None:
    """Return the stripped content of the first ``role: "system"`` message, or
    None if there is none (or it's blank)."""
    for msg in messages:
        if msg.get("role") == "system":
            content = msg.get("content", "")
            return str(content).strip() or None
    return None


def set_system(messages: list[dict], content: str) -> None:
    """Set the system message content to ``content``. Overwrites an existing
    system message in place; inserts a new one at index 0 if none exists.
    Guarantees exactly one system message."""
    for i, msg in enumerate(messages):
        if msg.get("role") == "system":
            messages[i] = {**msg, "content": content}
            return
    messages.insert(0, {"role": "system", "content": content})


def append_to_system(messages: list[dict], block: str) -> None:
    """Append ``block`` after the current system message (separated by a blank
    line). If there is no system message, ``block`` becomes the system message.
    No-op when ``block`` is empty/blank."""
    block = (block or "").strip()
    if not block:
        return
    current = extract_system(messages)
    combined = f"{current}\n\n{block}".strip() if current else block
    set_system(messages, combined)
