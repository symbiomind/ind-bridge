# basic_session

Bridge-owned conversation state. The harness thinks every request is a fresh
context; **the bridge owns the session entirely** — loading history, rebuilding
the message list, and saving the turn. This is what moves an agent into Shape 4
(session-in-the-bridge): yeet the harness, the agent is still the agent.

It is also the **authoritative producer of the `session_state` contract** (D-009).
Because it owns the history file, it *knows* whether a session is fresh — so the
`conversational_memory` wakeup cascade (L3 recency / L2 trending) fires on ground
truth instead of bridge-core's message-shape inference.

## Capabilities

| Capability | Slot | Method | When |
|---|---|---|---|
| `outbound_params` | `session.plugins` | `apply_outbound_params` | step 1c — load, rebuild, stamp `session_state` |
| `post_response` | `session.plugins` | `observe_response` | after delivery — save the closed turn |

Both live in **one `sessions.<name>.plugins` block** (D-010 permits `post_response`
in `session.plugins` — saving the turn *is* conversation-state work).

## Two knobs, one honest meaning each

- **Storage is always full.** A session file is text; disk is cheap. We never trim
  what we keep — no config knob for it. The wakeup/recency cascade can therefore
  reach *past* the send window into deep history.
- **`max_turns` is the SEND window** — how many user/agent *pairs* go upstream each
  turn. Defaults to `20` (bounded, so a fresh wire-up can't blow context + wallet on
  "hello, I'm back"). Set `false`/`0` to send everything, knowingly.

> V3 fused these into one number that trimmed both disk and wire. V4 splits them:
> store everything, send a window.

## Rich storage (replay-safe)

Assistant turns are stored *rich* — `reasoning_content` and `tool_calls` are
preserved when the provider returns them — so replaying history to picky backends
(Moonshot et al., which 400 on round-tripped tool_calls missing `reasoning_content`)
survives the trip. The save side skips turns that still carry `tool_calls` (loop not
yet closed); the closing turn is saved when it comes back through.

## Reset (OFF by default)

A fresh session happens when the file is gone/empty — **deleting the session file is
the always-available hard reset.** That's the zero-config, zero-surprise baseline.

Opt into a soft boundary with `session_reset`.

| `mode` | Behaviour |
|---|---|
| `never` (default) | Fresh only when file empty/missing. |
| `daily` | A background scheduler archives the session file at the configured `at:` time (renames it to a dated copy beside the original — nothing is destroyed). The next read takes the empty-file path → fresh. |
| `manual` | Fresh when the request carries header `x-session-reset: 1` (or `true`/`yes`/`reset`). |

**Why `daily` is event-driven (not read-driven).** It schedules an *archive* at
the boundary rather than comparing the file's mtime on each read. A read-time
heuristic is racy: anything that writes the session file mid-day (a `cron`
heartbeat, a background reflection) bumps the mtime past the boundary and
silently consumes the reset before the next human turn ever sees it. An archive
either happened or it didn't — it can't be raced. The reset fires for the
*human's* first turn of the day, which is the moment the wake cascade should run.
(Archiving also means the gap is non-destructive: yesterday's session is a dated
file beside the live one, not gone.)

**Timezone.** The `at:` boundary resolves its clock in this order, most specific
first: a `timezone:` on the `session_reset` block → an identity-level hint →
the global `server.timezone` → `UTC`. So a plugin honours `server.timezone` by
default, but **any session that wants its own clock can set `session_reset.timezone`** —
the bridge doesn't care why you'd run two sessions on different timezones; it just
does what the config says.

## `degrade_tool_history` (opt-in, default off)

Same shape as `max_turns`, one rung up: **storage stays rich, the wire gets a
folded view.** When the send window contains more than one *closed* tool
exchange (`assistant(tool_calls) → tool(result)…`), the older ones fold into a
single `role: assistant` message carrying paired XML tags:

```
<tool type="call" name="get_memory">{"id": 1476}</tool>
<tool type="result" name="get_memory" truncated="harness">…visible bytes…</tool>
```

The most-recent closed exchange stays raw, so the agent can still reason from
fresh tool results. The in-flight harness tool-loop tail (current request)
always passes through verbatim — the fold operates on the stored window only.

**Why bother:** harnesses (LibreChat especially) can truncate tool results at
their door before the bridge sees them, leaving `[truncated: N chars exceeded M
limit]` markers in the content. `basic_session` stores faithfully (it's the
agent's real memory of what arrived), and the windowed history is re-sent to
the model each turn. Without the fold, old truncated plumbing accumulates on
the wire turn-over-turn. With it, the agent still remembers having used the
tool (the `<tool type="call">` breadcrumb survives), the result content is
preserved verbatim (truncation marker included, with `truncated="harness"`
flagging it honestly), but the protocol-shape bloat is gone.

**Format invariants** (load-bearing — harness-portability depends on these):

- The `name=` attribute carries the **bare tool name** from the OpenAI
  `tool_calls` payload, nothing else. NEVER `personal-mcp:get_memory`, NEVER
  `<tool source="…">`, NEVER any harness/server label. An agent that survives a
  LibreChat↔OpenClaw switch must see identical folded prose from both sides.
- `truncated="harness"` is the **only** hint about lossiness. It applies when
  the result content contains `[truncated:`. Honest without naming names — any
  harness with a marker triggers it.
- The fold runs in RAM during `apply_outbound_params`. **Storage on disk is
  never touched.** Delete the session file → hard reset still works; inspect
  it → raw protocol shape still there.

**What `basic_session` does NOT do here:** smart summarisation, per-tool
policies, context-budget-aware folding, configurable `keep_recent`. Those are
`advanced_session`'s playground.

## Configuration

```yaml
sessions:
  my_session:
    plugins:
      basic_session:
        max_turns: 20                    # SEND window in pairs; false/0 = send all. Storage is always full.
        data_dir: data/basic_session     # session file location (default as shown)
        system_prompt:                   # optional file list — replaces upstream system prompt
          - /workspace/my_agent/SOUL.md
        system_prompt_append: "..."      # optional string appended to whatever system is used
        session_reset:                   # optional; OFF by default
          mode: never                    # never | daily | manual
          at: "04:00"                    # daily only — boundary time
          # timezone: America/New_York   # daily only — overrides server.timezone for this session
        degrade_tool_history: false      # opt-in: fold OLD tool exchanges to
                                         # assistant prose on SEND (storage stays full).
                                         # Keeps the most-recent closed exchange raw.

roles:
  my_agent:
    session: my_session
    resource: openrouter
    ...
```

Session key is the harness-supplied `x-session-key` header if present (lets the
harness segment sessions — e.g. per LibreChat conversation id), else `identity:role`.

## Orthogonality: the session does NOT touch tool-calling

`basic_session` declares only `outbound_params` (load) + `post_response` (save).
Tool-calling lives entirely *between* those two windows and is owned by the executor:

- **Passthrough** — no intercept/modify plugins wired → raw bytes flow; the session
  forces nothing to assemble.
- **Intercept** — `agent_tools` / `bridge_message` (`handle_tool_calls`) → the
  executor's own buffer + claim + execute + re-call loop runs between load and save;
  the session never sees intermediate laps.

`observe_response` is an *observer* — it can't intercept, re-call, or block; it only
watches the final, closed assistant turn. This is why V3's `in_tool_loop` tail-splice
is gone, not ported: V3 made the session babysit the loop because V3 had no
executor-owned loop. V4's executor owns it; the session stays just the session.

## What's deliberately NOT here (→ `advanced_session`, Soon™)

Summarisation, pluggable history backends, branching, dream-state hooks, pluggable
reset policies, token-usage accounting. `basic_session` is deliberately the floor —
advanced session will be insane compared to basic, but basic earns its name honestly.

`degrade_tool_history` ships the **mechanism** (the wire-only fold + the format)
with a dumb age-based trigger (keep most-recent, fold older). The richer policy
surface — LLM-distilled prose instead of structured tags, per-tool policies,
context-budget-aware folding — lives in `advanced_session`.
