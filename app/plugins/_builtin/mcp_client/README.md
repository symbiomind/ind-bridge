# mcp_client

Connect MCP (Model Context Protocol) servers and offer their tools to the AI
agent as bridge-native tools. Add a server to config → the agent gains its tools
with **zero hand-written tool code**.

## What it does

- **Discovers** each configured server's tools at startup (`background` capability,
  fail-open — a server down at boot is skipped, the bridge still starts).
- **Injects** the discovered tools into the agent's OpenAI function list
  (`ctx.request.tools`, like `agent_tools` — *not* into `<bridge_context>`).
- **Executes** them when the agent calls one (`handle_tool_calls`), routing to the
  right server with a fresh connection per call.

## Tool naming (three layers)

```
bridge_native__  +  <server_key>__  +  <mcp_tool_name>
e.g.  bridge_native__diary__store_memory
```

- `bridge_native__` — the bridge-wide invariant (core; applied at injection,
  stripped at dispatch).
- `<server_key>__` — this plugin's sub-namespace (the per-server config key), so
  two servers exposing the same tool name don't collide.
- `<mcp_tool_name>` — the server's real tool name (stored verbatim; a name
  containing `__` survives).

## Config

The server map is a dict of `server_key -> {resource, tools?}`, written **once**
at `role.plugins.mcp_client`. The `identity.plugins` and `context.plugins`
entries are bare `mcp_client: {}` markers (discovery trigger + tool injection).

```yaml
resources:
  my_agent_diary_mcp: { endpoint_url: http://...:6005/mcp, token: ${SECRET_A}, timeout: 120 }
  personal_mcp:       { endpoint_url: http://...:6006/mcp, token: ${SECRET_B}, timeout: 120 }

identities:
  my_agent:
    plugins:
      mcp_client: {}                              # background discovery trigger

roles:
  my_agent_role:
    plugins:
      mcp_client:                                 # the server map (+ handle_tool_calls)
        diary:        { resource: my_agent_diary_mcp }
        personal-mcp: { resource: personal_mcp, tools: [read_file, write_file] }  # optional allowlist
    context:
      plugins:
        mcp_client: {}                            # inject tools
```

## Per-server ACL + param injection (`allow` / `deny` / `params`)

A declarative, per-role, per-server policy layer between a resident and an MCP
server — **enforcement, not prompting**. A denied tool is neither *offered* (the
model never sees it) nor *callable from crafted history* (dispatch re-checks).
Param injection stamps sovereign fields the model shouldn't fill itself.

```yaml
roles:
  my_agent_role:
    plugins:
      mcp_client:
        diary:
          resource: my_agent_diary_mcp
          allow: [retrieve_memories]        # whitelist — only these visible/callable
          deny:  [store_memory]             # blacklist — beats allow (deny-wins)
          params:                           # gateway-level argument injection
            append:                         # SHALLOW overwrite of named keys
              retrieve_memories:
                source: "my_agent"          # stamped server-side; model can't override
            # replace:                      # discard the model's args entirely
            #   some_tool: { query: "fixed" }
```

- **`allow`** — whitelist. If present, only listed tools are exposed/callable.
  Per-server `tools:` is a back-compat **alias** for `allow` (unioned if both).
- **`deny`** — blacklist. Removes a tool even from an `allow` list. On overlap,
  **deny wins** (more-restrictive-wins, fail-safe).
- **`params.append`** — per-tool dict; **shallow-overwrites** the named top-level
  argument keys verbatim (no deep-merge, no list-concat), clobbering whatever the
  model supplied. The fix for a small model inventing a bogus `source`.
- **`params.replace`** — per-tool dict; discards the model's args and substitutes
  the operator's. `append` runs after `replace`, so it wins overlapping keys.
- Injected values are passed through **verbatim** — the bridge is schema-agnostic;
  a wrong type is rejected by the MCP server and narrated as a tool-error.

## Constraints

- A `server_key` must **not** contain `__` (collides with the sub-namespace
  separator) — such a server is warned-and-skipped.
- An allowlist on the `context.plugins` block accepts either the full
  `server__tool` name or the bare tool name (this is a separate, outer layer from
  the per-server `allow`/`deny` above; a tool must pass both).
- Author the server map once at role level — same-named keys across cascade
  levels deep-merge.

## Parked for future-us

- Reconnect/retry of a server that was down at startup discovery (needs more
  dogfooding/research).
- Cross-plugin collisions among dynamically-discovered tools (can't be validated
  at startup since `OWNED_TOOLS` is a callable).
