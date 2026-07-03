# file_resource

A terminal resource plugin: writes the inbound message to a file and returns a
fixed acknowledgement. The first consumer of the `produce_response` capability —
the plugin *is* the response source, so no outbound network call is made.

## Capabilities

| Capability | Slot |
|---|---|
| `produce_response` | `resource.plugins` |

A resource declaring `produce_response` is **terminal**: the executor
short-circuits all outbound transport. A resource may not also declare an
`outbound_params` transport plugin (e.g. `OpenAI-Protocol`) — the validator
rejects that as a terminal-vs-transport conflict.

## Config

| Key | Required | Default | Meaning |
|---|---|---|---|
| `path` | yes | — | File to write to. Parent directories are created if missing. If absent, the plugin logs a warning, writes nothing, and still returns the reply. |
| `append` | no | `true` | `true` appends a record per turn; `false` truncates the file each turn (last record wins). |
| `reply` | no | `"message received"` | The acknowledgement text returned to the caller. |
| `capture` | no | `message` | WHAT to write. `message` = the latest `role: "user"` message text. `full` = the entire assembled outbound `messages` list, **including the signed `<bridge_context>` block**. |
| `format` | no | `text` | HOW to serialise. `text` = plain text; `json` = `json.dumps(..., indent=2)`; `markdown` = role/content sections. |

### `capture: full` — inspecting the signed envelope

The bridge assembles and signs the `<bridge_context>` block into the request
**before** the resource step, so a `full` capture records the exact envelope an
AI backend would have received — the signed `<caller>`, its `trust`, and any
`storage` marker included. Pair with `format: json` for a byte-faithful audit
record, or `format: markdown` to read it.

## Demo config

```yaml
resources:
  log_sink:
    plugins:
      file_resource:
        path: /data/messages.log
        append: true
        reply: "message received"
        capture: full        # record the whole signed envelope
        format: json         # byte-faithful

roles:
  log_role:
    resource: log_sink
    # no session, no transport plugin — a terminal file sink
```

A role using this resource produces output without any AI backend. It can be
reached by any sender shape the bridge supports — an HTTP listener on an
identity, a scheduled trigger, or in-process agent-to-agent messaging — and the
file is written identically in each case.

## Authoring contract — `produce_response(ctx, config) -> PipelineCtx`

The executor calls `produce_response` instead of making an outbound request. The
plugin serialises the inbound request per `capture`/`format` (string content or
concatenated `text` content-parts) to `path`, then sets:

```python
ctx.response = {"role": "assistant", "content": reply}
```

and returns `ctx`. The executor wraps `ctx.response` in a minimal OpenAI-shaped
envelope (`finish_reason: "stop"`). File I/O errors are logged but never fail the
turn — the acknowledgement is returned regardless.
