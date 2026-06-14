# memory_enricher

Background label-enrichment daemon. `conversational_memory` stores every memory with
a **nonce** label (unenriched). This plugin finds those nonce-only memories, calls a
small LLM to generate 4-6 reusable semantic labels, and atomically swaps the nonce out
via `replace_labels`. Those labels are what conv_mem's **L2 "trending" tier** consumes
— no enrichment ⇒ no labels ⇒ L2 stays dark.

Runs completely out of band — a long-lived asyncio task, no request-pipeline
involvement.

## Capabilities

| Capability | Slot |
|---|---|
| `background` | `identity.plugins` |

`background` (D-011) is the core "spawn-a-loop-at-startup" capability — the generic
cousin of `listener`. Core calls this plugin's `start_background(StartupCtx, config)`
once during lifespan startup, schedules the returned coroutine, and cancels it on
shutdown. It's the V4-shaped replacement for V3's `server.startup` hook.

## What changed from V3 (the hack is gone)

V3 hand-rolled a fake pipeline to *borrow* an LLM endpoint: resolve identity → role →
build a `mini_ctx` → dispatch hooks → scrape `endpoint_url`/`token`/`model` → raw POST.
Config *said* it was an identity; code *bypassed* the pipeline.

V4 deletes that. The enricher resolves a named LLM **resource** directly and POSTs to
its OpenAI endpoint — the same honest way `conversational_memory` resolves its memory
resource. "Use this brain for this job," not "enrichment is a pretend chat identity."

## Config

Settings live in **conv_mem's** `enrichment:` block (so operators configure memory +
nonce + enrichment in one place). Wire `memory_enricher` on the **same identity** that
runs conv_mem:

```yaml
identities:
  my_agent_harness:
    role: my_agent
    plugins:
      memory_enricher: {}          # background capability — spawns the loop

roles:
  my_agent:
    context:
      plugins:
        conversational_memory:
          resource: memory_mcp           # the memory store (retrieve/replace/stats)
          nonce: 52868312778495          # unenriched marker
          enrichment:
            resource: openrouter         # SIMPLE path — an LLM resource (OpenAI shape)
            model: gemma3:1b             # cascade: this wins; resource default last
            # prompt: "..."              # optional override of label instructions
            batch_size: 1                # memories per tick (keep low for small models)
            timeout: 120                 # LLM call timeout (seconds)
            # identity: my_enricher      # POWER path — NOT yet implemented
```

### Two resource shapes

The enricher resolves **two** resources, each in its native shape:

| Use | Resource shape | Where url/token live |
|---|---|---|
| memory store | MCP | `endpoint_url` / `token` on the resource block |
| LLM brain | OpenAI-Protocol | `url` / `token` under `plugins.OpenAI-Protocol` |

`model` resolves by cascade: `enrichment.model` wins; else an optional default `model`
on the LLM resource's `OpenAI-Protocol` block.

## DLC-grace (no hard dependency)

If `enrichment:` is absent or has no `resource:`, the loop simply **never spawns**.
conv_mem keeps working — L3 recency + L1 recall are unaffected; only L2 trending goes
dark. The enricher is a pointer conv_mem *can* use, never one it *needs*.

## Adaptive polling

Backlog-aware tick interval (ported verbatim from V3):

| Backlog | Interval |
|---|---|
| > 100 | 15s |
| > 10 | 60s |
| > 0 | 300s |
| idle | 900s |

`nudge()` is called by `conversational_memory` after a successful store — it wakes the
loop early so a fresh memory enriches within ~15s instead of waiting up to 900s. New
memories land at the top of the queue; the backlog drains underneath.

## The power path (not yet built)

`enrichment.identity: <name>` will invoke a real identity *pipeline* internally — with
no HTTP endpoint — which is the V4 bridge's actual thesis ("the agent lives at the
bridge; an identity shouldn't need a URL to be callable"). It rides the same
`background` seam plus a future "invoke a pipeline programmatically" executor entry
point, and unblocks cron-triggered identities + the bridge originating its own messages.
Today it logs a warning and falls back to `enrichment.resource`. See
`notes/BRAINSTORM-memory-enricher-V4.md`.
