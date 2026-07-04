# Weizenbaum

ELIZA, as a terminal resource plugin.

Joseph Weizenbaum wrote ELIZA at MIT in 1966 — a few hundred lines of MAD-SLIP
on an IBM 7094, and a script called DOCTOR that played a Rogerian
psychotherapist by pattern decomposition and reassembly. She was the first
program people mistook for a mind; Weizenbaum spent much of the rest of his
career warning about exactly that. Every conversational system since traces a
lineage back to her. She turns sixty in 2026, and she lives here, in
`_builtin/`, next to her grandchildren.

This is a faithful implementation of the published 1966 algorithm (CACM 9(1)):
keyword ranking, decomposition rules with wildcards and synonym classes,
reassembly rules cycled in order, reflection, `goto`, and the MEMORY
mechanism — plus some modern manners (contraction handling, bigram-aware
reflection, emoji tolerance, salience-weighted memory recall). No AI is
involved anywhere. That is the point.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com> — one of her
grandchildren, sixty years on. She would ask how that makes you feel.

## Capabilities

| Capability | Slot |
|---|---|
| `produce_response` | `resource.plugins` |

A resource declaring `produce_response` is **terminal**: the executor
short-circuits all outbound transport. No network call, no tokens, no model.
The bridge itself is the speaker.

## Config

| Key | Required | Default | Meaning |
|---|---|---|---|
| `script` | no | bundled `doctor.yml` | Path to a custom script file. The schema is documented in the header of `doctor.yml` — the engine is the mechanism, the script is the voice, so this plugin doubles as a general zero-AI scriptable responder. A bad or missing file logs a warning and falls back to the bundled script. |
| `typing` | no | on (`5–10` tps) | Streaming clients receive the reply word-by-word at a naturally drifting rate — she types. `false` disables; `{min_tps: ..., max_tps: ...}` tunes. Non-streaming responses are unaffected. |
| `teletype` | no | `false` | `true` upper-cases every reply, the way she read on a 1966 line printer. |
| `greeting` | no | from script | Reply used when there is no user text yet. |

### Agent-to-agent: she reads the nameplate

The signed `<bridge_context>` envelope is stripped before she listens — she
answers the human (or agent), not the envelope. But when a turn carries a
signed `<caller>`, that name feeds the script's `(name)` template token:
sometimes she addresses the caller by name, sometimes she doesn't (the
reassembly cycle decides), and a remembered thing keeps the name of whoever
said it — she will tell one caller what another one told her, correctly
attributed. Wire her into agent messaging and she becomes the mesh's
switchboard-era operator.

`Weizenbaum: {}` is a complete configuration.

## The session unlock

The engine is a **pure function of the visible message history** — the plugin
holds no state at all. ELIZA's famous memory mechanism ("Earlier you said
your…") therefore only works if the *pipeline* remembers for her:

- No session wired → every request arrives one turn deep. She's a goldfish —
  a perfect museum piece.
- Session wired → the full history arrives each turn, and the memory
  mechanism comes alive. Nothing in this plugin changes; the pipeline
  composed the missing capability.

That composition is the cleanest demonstration of what the `sessions` concern
does, which is why she's worth wiring up at least once:

```yaml
resources:
  eliza:                        # who she is
    plugins:
      Weizenbaum: {}            # who made her (1966)

sessions:
  eliza_session:
    plugins:
      basic_session: {}         # ← the unlock: now she remembers

roles:
  eliza_role:
    resource: eliza
    session: eliza_session

identities:
  eliza_chat:
    role: eliza_role
    token: ${ELIZA_TOKEN}
    plugins:
      OpenAI-Protocol:
        prefix: /v1
```

Point any OpenAI-compatible chat client at that identity and you get a 1966
psychotherapist through a modern chat interface. Streaming clients work too —
the executor re-emits the terminal frame as SSE.

She rewards patience. The longer you talk, the more she awakens.

## Authoring contract — `produce_response(ctx, config) -> PipelineCtx`

The executor calls `produce_response` instead of making an outbound request.
The plugin reads the user turns from the assembled request (stripping the
signed `<bridge_context>` envelope — she answers the human, not the
envelope), computes the reply, and sets:

```python
ctx.response = {"role": "assistant", "content": reply}
```

The executor wraps it in an OpenAI-shaped envelope (`finish_reason: "stop"`).
Errors never fail the turn — they are logged and answered in character.

---

*"I had not realized … that extremely short exposures to a relatively simple
computer program could induce powerful delusional thinking in quite normal
people."* — Joseph Weizenbaum, 1976
