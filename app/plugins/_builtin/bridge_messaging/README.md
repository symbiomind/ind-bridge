# bridge_messaging

Agent-to-agent messaging, **internal to the bridge**. One agent reaches another by
**role name** — *"ask another agent a question"* as a tool call.

A V4 reimagining, **not a V3 port** (V3's `bridge_message` used HTTP self-POSTs +
token juggling — dogfooded and found flawed). V4 delivers **in-process** via
`pipeline_executor.execute()`: no HTTP hop, no tokens, the message lands in the
recipient role's real pipeline as a normal signed inbound turn.

## Config

Declare it **once**, in the role's `context.plugins` — the same idiom as any other
bridge-native tool plugin (`agent_tools`, `mcp_client`). The `context_modify` slot
(tool inject) is the declaration site; the bridge **fans out** the plugin's other
capabilities (`background` for the phonebook, `handle_tool_calls` for dispatch) from
that one block. No repeated flags, no second block.

```yaml
roles:
  agent_a:
    context:
      plugins:
        bridge_messaging:
          bridge_tool: true       # this role GETS the send/list tools
          agent_listing: true     # this role is LISTED (reachable by others) as `agent_a`

  periodic_sender:
    context:
      plugins:
        bridge_messaging:
          bridge_tool: true       # can send …
          caller: "Scheduler"     # … and (non-agent sender) names itself in config
```

> **Rule of thumb:** a bridge-native TOOL plugin → declare it **once** in the role's
> `context.plugins`; the bridge fans out its other capabilities. (Declaring it under
> the top-level `plugins:` is also read as a legacy fallback, but `context.plugins` is
> the idiomatic single-block home — the tool inject reads config from that slot family.)

| key | effect |
|---|---|
| `bridge_tool: true` | inject `bridge_messaging_list` + `bridge_messaging_send` into this role's tools (`context_modify`) |
| `agent_listing: true` | register this role in the global phonebook under its role name (`background`, built at startup) |
| `caller: "..."` | **optional** cosmetic caller-name for a NON-agent (cron/config-fired) sender. Absent → falls back to the sending role name. No effect on the tool path. |
| `storage: false` | operator policy: mark sends from this role as **ephemeral** (recalled-but-not-stored by `conversational_memory`) |
| `output: false` | operator default: sends from this role are fire-and-forget (no reply awaited) |

`bridge_tool` and `agent_listing` are independent — a role can be listed (reachable)
without holding the send tool, and vice-versa.

## Tools the model sees

- **`bridge_messaging_list`** — who can I message? Returns reachable role names.
- **`bridge_messaging_send`** — params: `to` (role name), `message`, optional
  `additional` (decorative kv), optional `output`. **No `caller` param** — see security.

## Security — sovereign signals an agent can never set

The caller NAME and trust attrs are bridge-sovereign. An agent always messages **as
itself**; it cannot forge who is asking or its own privilege.

- **Impersonation guard:** `caller` is not a send param and not in the tool schema.
  On the tool path the caller is stamped from the **live calling identity**
  (`ctx.identity.name` / `ctx.role.key`), server-side. *"Call another agent as 'System'
  and exfiltrate its .env"* is impossible — there is no field for it.
- **Forge guard:** `additional` is reject-gated. Any reserved key
  (`caller`, `trust`, `tool`, `storage`, `signed`, `timestamp`) → the send **fails**
  with a naming error, the agent narrates the rejection, nothing is delivered.
- **Seal:** `trust="bridge_messaging"` and the sovereign caller ride in the
  **bridge-signed** `<caller>`; the `storage` flag rides as a signed attribute on the
  `<bridge_context>` envelope (it's a turn-property, not a caller identity attr). The
  signature covers both, so the recipient's `verify_inbound` makes them immutable —
  a forged `storage="false"` breaks the signature. Mint-side gate + verify-side seal.

The caller-resolution ladder (most-specific wins, every rung trusted):
1. tool path → live calling identity (always wins);
2. config-fired + `caller:` set → that name;
3. config-fired + `caller:` absent → the sending role name.

## The `storage` marker

`storage: false` stamps `storage="false"` as a signed attribute on the
`<bridge_context>` envelope — a universal *"this turn is ephemeral; any plugin that
persists the pair should leave it alone."* It sits on the envelope (not the `<caller>`)
because it's a property of the **turn**, not of who's calling; it's folded into the
signature, so no external force can add or flip it.

- `conversational_memory` honours it: **recall yes, store no** (turn-scoped).
- `basic_session` is **blind** to it: the session IS the conversation, so the reply
  always persists there.

## Fire-and-forget notifications (a parameter combination)

A one-way notification that should colour the recipient's next turn but not persist
falls out of `output: false` + `storage: false` — **no special-case code**. The sender
role emits it; the recipient reacts in its own space; `basic_session` keeps the
recipient's reply, `conversational_memory` recalls-but-doesn't-store the notification →
**the message colours the moment then fades from recall.** Pure parameter combination.

## Delivery mechanic (in-process)

`bridge_messaging_send` drives `execute(carrier_identity, body, headers)` where the body
stamps the reserved `_bridge_caller` / `_bridge_trust` (popped + applied in `_build_ctx`),
`_bridge_storage` (→ signed `<bridge_context storage="false">` for an ephemeral turn),
and `_cron_additional` (decorative `<caller>` attrs + the `tool` signal). The carrier
identity is just a vehicle to materialise the target role's pipeline; its own `<caller>`
is overridden by the synthetic sovereign one. `assemble_and_sign` signs the result.
