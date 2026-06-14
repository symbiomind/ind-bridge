# context_stripper

Strips client-injected baggage from `ctx.request.messages` before the request
leaves the bridge. Ported from V3 (`mind-span-ce`) and reshaped for V4: where
V3 had a single mutually-exclusive `client_mode`, V4 takes a **set of
independently-toggleable filters**.

## Capabilities

| Capability | Slot |
|---|---|
| `context_modify` | `identity.context.plugins`, `role.context.plugins` |

Pure inbound-body modifier — no tools, no listener, no upstream re-call.

## Why the shape changed (V3 → V4)

V3's `client_mode` was a single selector that doubled as
`IdentityInfo.client_mode`. It conflated *who the client is* with *which
transforms to apply*, and it could only ever pick one. V4 dropped
`client_mode` from the identity entirely — so the plugin config is now the
only source, and it's expressed as what it always was: a set of filters.

## Config

```yaml
context_stripper:
  librechat: {}          # present  → enabled. runs FIRST.
  openclaw:              # runs SECOND, chained after librechat's output
    enabled: true        # default-on when present; false = keep-but-dormant
```

Rules:

- **Presence of a filter key = ON.** `openclaw: {}` enables it.
- **`enabled: false`** is the explicit dormant override — keeps the block (and
  any future sub-options) in config without running it.
- **Order = YAML order.** Filters chain top-to-bottom; each filter's output
  feeds the next. `librechat` (collapse 91→1) then `openclaw` (strip prefix off
  that 1) is the canonical pairing.
- **Unknown filter name** → warned and skipped; recognised filters still run.

Deep-merges across the cascade: a role sets a filter default, an identity
overrides it per-key (`openclaw: {enabled: false}` dormants a role's
`openclaw: {}`).

## Filters

| Filter | What it does |
|---|---|
| `librechat` | LibreChat sends full conversation history every turn. Keep only the last user message. |
| `openclaw` | Strip OpenClaw's `Sender (untrusted metadata):` JSON-block + `[timestamp]` prefix off the last user turn. Handles string and multi-part (list) content. History left intact. |

> **Note on `openclaw`:** as agents migrate fully onto signed
> `<bridge_context>` (verified by core `bridge_sign`), this prefix path becomes
> redundant — the untrusted-metadata concern moves into the signed block.
> Kept for the non-`bridge_context` OpenClaw path.

## Placement

Wire it where the strip should happen. If you also inject signed context, place
`context_stripper` **before** the inject so the injection lands in the surviving
message.

```yaml
identities:
  my_agent:
    context:
      plugins:
        context_stripper:
          openclaw: {}
```

## Adding a filter

Add a `_strip_*(messages, filter_cfg) -> list` function and register it in
`_FILTERS`. The key becomes the config name. `filter_cfg` is the per-filter dict
(`{}` when the value isn't a dict), so a filter can grow sub-options without any
change to the dispatch loop.
