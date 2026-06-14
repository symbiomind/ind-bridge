# time_inject

Injects the current friendly local time into `<bridge_context>`. Solves UTC
confusion for buddies that need to know the correct local time.

Output (a key in `ctx.bridge_context`, which core renders as an XML tag):

```xml
<current_time>Wednesday, 1st April 2026 - 09:14 AM ACDT</current_time>
```

## Capabilities

| Capability | Slot |
|---|---|
| `context_modify` | `identity.context.plugins`, `role.context.plugins` |

## Config

```yaml
roles:
  my_agent:
    context:
      plugins:
        time_inject:
          timezone: Australia/Adelaide   # optional
```

`timezone:` is optional. Resolution order (highest wins):

1. Explicit `timezone:` in this plugin's config
2. `ctx.timezone` — resolved from the cascade
   (identity → role → session → resource → server → `TZ` env → UTC)

An unknown/invalid timezone logs a warning and falls back to UTC.

## Signing is core's job

Unlike V3, this plugin does **not** sign anything. It only contributes the
`current_time` key to `ctx.bridge_context`. Core
(`app.bridge_sign.assemble_and_sign`) assembles + HMAC-signs the whole
`<bridge_context>` block **after** the context plugins run (D-005). Because
the plugin runs in a context slot before assembly, `<current_time>` lands
*inside* the signed block.
