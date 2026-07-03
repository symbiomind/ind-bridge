# tool_awareness

Keeps an AI agent **aware of its current tools**. On a bridge-owned session
(Shape 4) the harness/client can be switched mid-session (OpenClaw → LibreChat →
NemoClaw); agents then keep inferring the **first** harness's tools even when
told otherwise.

That's a model-**training** limitation, not a bridge bug — the bridge can present
perfect information but can't reach inside the model's inference. So this plugin
doesn't promise the agent behaves; it gives you a **graduated ladder of escalating
nudges** (configurable "kicks in the butt").

Source of truth: `ctx.request.tools` (the live OpenAI function-tool list).

## Capabilities

| Capability | Slot |
|---|---|
| `context_modify` | `identity.context.plugins`, `role.context.plugins` |

## The three stages (pick your lane — they compose)

| Stage | Knob | State | Fires | Does |
|---|---|---|---|---|
| 1. Nudge | `nudge: true` | stateful (fingerprint) | on **change** only | injects `<tools>…</tools>` into the signed `<bridge_context>` |
| 2. Tool list | `build_tool_list: true` | stateless | **every turn** | appends the live tool list to the system prompt |
| 3. Verbosity | `tool_list_format:` | — | — | how verbose stage 2 is |

**Pick your lane.** Stage 1 is the cheap poke — "oi, the tools moved" — only when
they actually moved. Stage 2 is the sledgehammer — the *authoritative list, every
turn*, so the agent has nothing to infer. They overlap in purpose; most setups want
**one or the other**, not both stacked by default. Stage 2 generally does the job
*better* (it removes the inference rather than arguing with it) at the cost of tokens
every turn; stage 1 is near-free but only reminds, it doesn't ground.

## Config

```yaml
roles:
  my_agent:
    context:
      plugins:
        # conversational_memory, system_prompt, agent_tools, … FIRST
        tool_awareness:                 # LAST in the context chain
          nudge: true                   # stage 1 (default false)
          nudge_text: "..."             # optional override
          build_tool_list: true         # stage 2 (default false)
          tool_list_format: names       # stage 3: names | names_descriptions | full
          list_header: "..."            # optional header above the list
          data_dir: data/tool_awareness # optional (stage 1 fingerprint store)
```

### `tool_list_format` (stage 3)

Real numbers from a live OpenClaw trace (**25 tools**):

| Format | Output | Size (≈) |
|---|---|---|
| `names` | `agents_list, browser, canvas, …` | ~200 chars |
| `names_descriptions` | `- browser: Control the browser via …` per tool | moderate (descriptions are long) |
| `full` | names + descriptions + full `parameters` JSON-schema | ~82 KB (the bloat) |

Which one an agent needs is a **dogfooding** question — the dial exists so you can
A/B per agent without touching code.

## Ordering matters (your responsibility)

Place `tool_awareness` **last** in the context chain, **after** any tool-injecting
plugins (`agent_tools`, future `bridge_tool`) so their tools are in the list. This
can't be enforced — but if `ctx.request.tools` is empty when a stage that reads
tools is on, the plugin **logs a warning** and skips the tool work (fail loud,
never fail to start).

## How detection works (stage 1)

Each turn it hashes the **sorted tool-name set** (so re-ordering the same tools is
*not* a change), compares to a fingerprint persisted per session under `data_dir`,
and nudges only when it differs — then updates the stored fingerprint. The session
key prefers `basic_session`'s stamp (so two harness-identities sharing one session
share one fingerprint = cross-harness continuity), falling back to the role's
session name, then the identity key — so it works **without** `basic_session` wired.

## What it does *not* do (yet)

The A/B-testing future use — surfacing **both** harnesses' tool sets deliberately
(OpenClaw vs NemoClaw, one agent, comparative context) — is the natural next home
for this plugin but is out of scope for v1. See `notes/IDEA-tool-change-detector.md`.
