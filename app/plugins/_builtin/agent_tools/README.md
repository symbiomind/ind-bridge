# agent_tools

D-008 smoke-test plugin. Demonstrates the `handle_tool_calls` capability
end-to-end with `rng_message`, a tool that returns a random string from
a pool. No I/O, no auth, no signing — the smallest possible consumer of
the intercept mechanism.

## Capabilities

| Capability | Slot |
|---|---|
| `context_modify` | `identity.context.plugins`, `role.context.plugins` |
| `handle_tool_calls` | `identity.plugins`, `role.plugins` |

`OWNED_TOOLS = ["rng_message"]` — the validator rejects collisions with
other plugins claiming the same tool name on the same identity.

## How it fits the V4 four-category executor model (D-008)

`handle_tool_calls` is the **intercept** category — pre-delivery, may
re-call upstream. When wired, the executor forces the upstream call
non-streaming so the assembled frame is available to the plugin; if the
client requested streaming, the post-pre-delivery frame is re-emitted
as SSE (Q-K resolved-by-design).

## Demo config

```yaml
roles:
  smoketest_role:
    resource: openrouter
    plugins:
      OpenAI-Protocol: {model: anthropic/claude-sonnet-4-6}
      agent_tools: {}                  # handle_tool_calls slot
    context:
      plugins:
        agent_tools:
          tools: [rng_message]         # context_modify slot

identities:
  my_agent:
    role: smoketest_role
    token: ${MY_AGENT_TOKEN}
    plugins:
      OpenAI-Protocol: {}              # listener
```

Then ask the identity to roll a random message:

```
> Roll the dice for me, bridge.
```

The LLM sees `rng_message` as an available tool, calls it, the bridge
intercepts the call, executes locally, splices the result, re-calls
upstream so the model narrates the message in its own voice.

## Authoring contract — `handle_tool_calls(ctx, config) -> str | dict`

The executor sets `ctx.plugin_data["handle_tool_calls.claimed"]` to
the claimed tool_call dict before invoking the plugin. The plugin:

- Reads the claim off `ctx.plugin_data`.
- Executes the tool.
- Returns the result as a string (or a dict — the executor's
  `_normalise_tool_result_content` handles both).

The plugin **does not** build tool messages, modify
`ctx.request.messages`, or re-call upstream. The executor owns all of
that.

If the plugin raises, the executor wraps the exception in a synthetic
error tool_result so the agent narrates the failure in their own voice.
The client never sees a 500.
