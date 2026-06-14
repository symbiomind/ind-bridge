# system_prompt

Puts the agent's **soul in the bridge**, not the harness. A harness (OpenClaw,
LibreChat, …) may or may not inject a system prompt; the bridge shouldn't depend
on it. With this plugin the identity is itself regardless of which harness is
upstream — the harness becomes just a toolbag.

Ported from V3 (`mind-span-ce`). Manipulates the system prompt from files or
inline text at role/identity scope. Supports **replace**, **prepend**, **append**.

## Capabilities

| Capability | Slot |
|---|---|
| `context_modify` | `identity.context.plugins`, `role.context.plugins` |

Pure inbound-body modifier — no tools, no listener, no upstream re-call.
(V4 has no `session.context` slot per D-006; the session layer is owned by
`basic_session`.)

## Config

```yaml
system_prompt:
  replace:           # yeet upstream system, rebuild from this list in order
    - text: "If SOUL.md is present, embody its persona and tone."
    - file: /workspace/my_agent/SOUL.md
  prepend:           # insert before the base (after replace, or upstream)
    - file: /workspace/shared/header.md
    - text: "Header text."
  append:            # insert after the base
    - text: "Always respond in Australian English."
    - file: /workspace/shared/footer.md
```

Rules:

- Each of `replace` / `prepend` / `append` is **optional**. Absent all three →
  the plugin is a no-op (passthrough).
- Items are tagged dicts:
  - `{file: /path}` — read from disk; **missing files warn and are skipped**.
  - `{text: "..."}` — used verbatim.
  - Unknown item keys warn and are skipped.
- **Application order:** `replace` (or upstream passthrough) → `prepend` →
  `append`. Parts join with a blank line (`\n\n`).
- **Files are read per request** (no caching) — edit `SOUL.md` and it takes
  effect on the next turn, no restart.
- **Exactly one system message** results: an existing system message is
  overwritten in place; otherwise one is inserted at index 0.

Deep-merges across the cascade (role provides defaults, identity overrides
per-key; lists replace, they don't concat).

## vs basic_session

`basic_session` has its own simple system-prompt builder — `system_prompt:`
(file list) + `system_prompt_append:` (string) — for **standalone/self-contained
sessions** that just want a prompt with no other plugins. It is a *subset*:
file-list only (no inline `text:`), no `prepend`, session-scope only. **It is not
deprecated** — it stays for the simple case.

This plugin is the powerful one. They **compose** via executor order:

```
1c. basic_session.apply_outbound_params  → builds a system prompt (or none)
2.  system_prompt.modify_context         → runs AFTER → FINAL WORD
4.  assemble_and_sign                     → injects <bridge_context>
```

- With `replace:` set, `system_prompt` yeets whatever's there (including
  basic_session's output) and rebuilds → the agent's soul is authoritative.
- With no `replace`, it `prepend`/`append`s onto whatever basic_session (or the
  upstream harness) produced.

So you can let basic_session set a base and have `system_prompt` decorate it, or
let `system_prompt` own the whole thing with `replace`. Both work; the plugin
always wins ties because it runs later.

## Example (copy-paste ready)

Wire an agent's soul into the bridge so it stays itself on *any* harness:

```yaml
roles:
  my_agent_role:
    context:
      plugins:
        system_prompt:
          replace:
            - text: "# Project Context"
            - text: "The following project context files have been loaded:"
            - text: "If SOUL.md is present, embody its persona and tone. Avoid stiff, generic replies; follow its guidance unless higher-priority instructions override it."
            - file: /workspace/my_agent/AGENTS.md
            - file: /workspace/my_agent/SOUL.md
            - file: /workspace/my_agent/IDENTITY.md
            - file: /workspace/my_agent/USER.md
```
