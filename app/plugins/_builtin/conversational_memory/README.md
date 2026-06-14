# conversational_memory

Automatic cross-session memory for AI buddies via memory-mcp-ce.

This is the V4 port of the V1→V3 plugin that started the whole project. AIs need to be reminded to remember to remember, and the bridge does it without the model having to consciously call a tool. **The agent doesn't decide to recall or store — the bridge does it.**

## What it does

**Recall** (capability: `context_modify`) — on every turn, semantically searches stored memories by the user's last message, threshold-filters, dedup-filters against recently-shown, and injects the top N as `<recalled_memories>` into the signed `<bridge_context>` block before the model sees the request.

**Store** (capability: `post_response`) — after the response has reached the client, fires the (user, agent) pair into memory-mcp-ce as a fire-and-forget background task. Skips housekeeping turns (HEARTBEAT_OK / NO_REPLY end-anchored, /new session openers).

## V4 vs V3 differences

- **Single config block.** V3 declared once at `role.context.plugins`. V4 also declares once at `*.context.plugins` — both `context_modify` (recall) and `post_response` (store) read the same config dict. Per the spec note in `ind-v4-brainstorm.md`, `post_response` is permitted in context slots when paired with `context_modify` as one logical knob set.
- **Async-native.** V4's `modify_context` and `observe_response` are `async def`; the executor schedules `observe_response` fire-and-forget after delivery. V3's `_run_async` thread-spawn / `_bg_pool` ThreadPoolExecutor / `_run_async_background` callback chain are all gone.
- **Lazy resource resolution.** V3 cached the memory-MCP endpoint at `server.startup`. V4 has no `server.startup` capability — first call resolves and caches per-resource-key.
- **`<user>` attribution from `ctx.identity`.** V3 preferred `bridge_sign.verified_caller` plugin_data over `ctx.identity`. V4 reads `ctx.identity` directly. Multi-hop attribution will land when bridge_message ports.
- **Source default**: `{agent_alias}:{resource_key}` (V3 used `{alias}:{provider/model}`). Theseus-resilient — same agent on provider-a model-1 → model-2 stays one bucket; provider swap is a different bucket.
- **L2/L3 wakeup.** V3 fired recency + trending on session-start. V4 ports the L3 (recency) / L2 (trending) cascade, gated on a fresh session. Freshness is the `ctx.plugin_data["session_state"]` contract (D-009): authoritatively stamped by `basic_session` when wired, or inferred from message shape by bridge-core (`app/session_freshness.py`) otherwise.

## Configuration

Declare under `roles.<name>.context.plugins` (most common) or `identities.<name>.context.plugins` (per-identity overrides):

```yaml
conversational_memory:
  resource: memory_mcp        # required — resource key (declares endpoint_url + token)
  agent_alias: Alice          # optional — agent name; source = "alice:<resource_key>"
  store: true                 # default; set false for recall-only callers (cron, etc.)
  retrieve:
    fetch: 10                 # how many to pull from memory-mcp-ce (wide net)
    inject: 2                 # how many to put in front of the model (narrow)
    threshold: 0.50           # minimum similarity (0.0-1.0)
    recall_source: "*"        # filter — omit for "this agent only"; "*" for all; or any fuzzy match
  nonce: 52868312778495       # MUST NOT change once deployed — enricher uses it
  decay_minutes: 60           # optional — shown-state expiry; omit for session-scoped
  data_dir: data/conversational_memory  # optional
```

Resource block (under `server.resources`):

```yaml
resources:
  memory_mcp:
    endpoint_url: http://memory-mcp-ce:8080
    token: ${MEMORY_MCP_TOKEN}
```

## Worked example

```yaml
roles:
  my_besterest_buddy:
    resource: openrouter
    plugins:
      OpenAI-Protocol: {model: minimax/minimax-m2.7}
    context:
      plugins:
        conversational_memory:
          resource: memory_mcp
          agent_alias: Alice
          retrieve:
            fetch: 10
            inject: 2
            threshold: 0.50

identities:
  user_via_librechat:
    role: my_besterest_buddy
    token: ${USER_TOKEN}
    context:
      name: User
      trust: trusted

  cron_recall_only:
    role: my_besterest_buddy
    context:
      name: cron
      trust: agent
      plugins:
        conversational_memory:
          store: false              # recall-only
          retrieve:
            recall_source: "Alice:*"
```

Stored source for `user_via_librechat`: `alice:openrouter` (Theseus-resilient across model bumps under the same provider).

## Caller-identity requirement

V4 enforces that any identity wiring `*.context.plugins` (or `*.response.plugins`) must declare `context.name` somewhere in its cascade. Without it, the identity is disabled at startup with a loud warning. *Identity Not Decided* means identity must be decidable — context-aware plugins need someone to be aware of.

## Knobs explained

| Knob | Default | What it does |
|------|---------|--------------|
| `resource` | required | Resource key referencing `server.resources.<key>` with `endpoint_url` + `token` |
| `agent_alias` | none | Agent display name. Becomes part of stored source and `<agent alias=...>` |
| `store` | `true` | Set `false` to opt out of storing for this identity (recall-only) |
| `retrieve.fetch` | `max(inject*5, 10)` | How many memories to pull from MCP (wide net) |
| `retrieve.inject` | `5` | How many to inject after threshold + dedup-shown filtering |
| `retrieve.threshold` | `0.75` | Minimum similarity (0.0-1.0). Compared against MCP's value |
| `retrieve.recall_source` | derived | Source filter. Omit for `{alias}:{resource}`; `"*"` for all; any string for fuzzy match |
| `nonce` | `52868312778495` | Hidden label for the enrichment agent; **never change once deployed** |
| `decay_minutes` | none | Shown-state expiry in minutes. Omit for session-only (no decay file format) |
| `data_dir` | `data/conversational_memory` | Where shown-state files live |
| `recall_timeout` | `5.0` | Hard timeout for the MCP retrieve call (fail-open) |

## Failure modes (all fail-open)

- **Resource missing / no endpoint_url** → log error, skip recall and store this turn (request continues without memory).
- **MCP timeout** → log error, no recall this turn (request continues).
- **MCP exception during retrieve** → log warning, no recall this turn.
- **MCP exception during store** → log error with `exc_info`, response was already delivered, no impact on user.
- **Housekeeping turn detected** → log debug, skip store (intentional — heartbeats and /new openers are not conversation worth remembering).

## Design references

- `~/Documents/ind-v4-brainstorm.md` — the spec, especially the "Plugin Capability Contract" section's `conversational_memory` worked example.
- `~/Documents/ind-v4-decisions.md` — D-007 (`post_response`) and D-009 (slot-loosening for paired recall+store plugins).
- `project_bridge_origin_story.md` (auto-memory) — why this plugin existed in V1, drove the harness→bridge pivot, and is *the* canonical "AI in the loop" plugin for the bridge.

## Deferred for later

- **L2 wakeup** (recency cascade on `/new`) — needs `is_new_session` from basic_session.
- **L3 wakeup** (trending labels on `/new`) — same dependency.
- **memory_enricher nudge** — the V3 plugin pings the enricher after store; will re-add when enricher ports.
- **Verified-caller multi-hop attribution** — bridge_message will drive this primitive when it ports.
