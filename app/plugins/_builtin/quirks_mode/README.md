# quirks_mode

The bridge's **provider-compat shim layer** — the polyfill for the AI-provider wars.

Strict thinking-mode providers (Moonshot/Kimi) reject frames that tolerant providers
(Claude, …) accept happily. `quirks_mode` normalizes the outbound message frame so a
strict provider takes a request a harness handed the bridge — for **any** harness, on the
provider that needs it. It's the [bridge's switch-harnesses thesis](../../../../notes/DESIGN-harness-client-compensation.md)
made concrete: the bridge is the stable layer; any harness + any provider swap underneath,
and the bridge speaks each one's dialect in the middle.

It is **scoped + opt-in per agent** — the *opposite* of a core vendor-prefix-soup registry.
You enable exactly the quirks a model needs, nothing more; an agent on a tolerant provider
gets none. (Browser quirks mode: the legitimate, designed "handle non-compliant input
gracefully" switch — not a hack.)

## The quirks (three today)

| Quirk | What it does |
|---|---|
| `reattach_reasoning` | Restores reasoning the bridge captured onto a round-tripped `assistant(tool_calls)` turn that came back stripped. Harnesses drop the reasoning when they own the tool loop; thinking-mode providers 400 without it. Matched by tool **name + arguments** (robust to harness id-mangling — OpenClaw rewrites ids, LibreChat reuses `:0`). Only ever **restores reasoning the provider itself produced** — never invents. |
| `mirror_reasoning_key` | Mirrors `reasoning` (OpenRouter's key) → `reasoning_content` (the key Moonshot *enforces*) when a turn has the former but not the latter — provider key-drift. |
| `close_trailing_orphan` | If the frame **ends** in an unanswered `assistant(tool_calls)` (an in-flight harness call), appends a plain-language synthetic tool result so a strict provider accepts the frame (Moonshot 400s on a trailing unanswered call). Loop-safe: a plain "respond normally" result makes the model answer, not re-call (a JSON placeholder loops). |

`reattach_reasoning` is fed by a **capture** step (`post_response`): when the bridge sees a
streamed/assembled turn with `tool_calls` **and** reasoning, it stashes the reasoning
per-session (keyed by tool name+args) so a later round-trip can restore it.

## Selecting quirks — cheat sheet OR workbench (default-OFF)

Two mutually-exclusive ways to choose which quirks are active:

```yaml
# CHEAT SHEET — inherit a known-good recipe from models.yml
quirks_mode:
  model: "moonshotai/kimi-k2.5"

# …or WORKBENCH — hand-pick quirks by name (for bringing up a new model)
quirks_mode:
  quirks: [reattach_reasoning, mirror_reasoning_key]
```

- **`model:`** is a key into the shipped [`models.yml`](models.yml) table (model string →
  quirk list). Use it for known-working setups — any agent on that model inherits the
  proven recipe.
- **`quirks:`** is a hand list. Use it to *bring up* a new model: read what each quirk does,
  toggle them until it works, then **graduate** the working set into `models.yml` as a new
  `model:` entry — the cheat sheet grows one characterised model at a time.
- If both are set, **`model:` wins** and `quirks:` is ignored (with a warning).

**Default-OFF.** Nothing is on unless explicitly selected — most quirks are provider-specific
compensations that would *break* a tolerant provider, so you opt in deliberately:

| Config | Active quirks |
|---|---|
| `quirks_mode: {}` (or absent) | none |
| `quirks_mode: {quirks: []}` | none |
| `quirks_mode: {model: "<unknown>"}` | none **+ loud warning** ("no recipe — add it to models.yml or use a quirks: list") |
| `quirks_mode: {model: "moonshotai/kimi-k2.5"}` | the k2.5 recipe |
| `quirks_mode: {quirks: [reattach_reasoning]}` | just `reattach_reasoning` |

An unknown quirk **name** (a typo in a list, or a recipe naming an unknown quirk) is
warned-and-dropped.

## models.yml — the cheat-sheet table

[`models.yml`](models.yml) ships with the plugin and maps known model strings to their proven
quirk recipes — the plugin *is* the compat database. Editing it is YAML, not code. Because
the bridge runs in Docker, you can **bind-mount your own `models.yml`** over the shipped one
to extend/override it without touching plugin code.

## Optional config

```yaml
quirks_mode:
  model: "moonshotai/kimi-k2.5"
  data_dir: data/reasoning_reattach   # optional — reattach stash location
```

`data_dir` is where `reattach_reasoning` stashes captured reasoning (default
`data/reasoning_reattach`). It reuses `basic_session`'s session key for per-session scoping,
so a shared session shares one stash.

## Capability / placement

Declares `outbound_normalize` (normalize the frame before **every** resource call — inbound
**and** `handle_tool_calls` intercept re-calls, via the executor's `_execute_resource_step`
chokepoint) and `post_response` (capture reasoning on outbound). Valid in
`identity.context.plugins` / `role.context.plugins`. Wire it on the agent that talks to the
strict provider through a harness-owned tool loop.

Putting it on a **role** (e.g. `my_agent_role`) covers all of that agent's identities — one
wiring works no matter which harness it's reached through.

## Relationship to the proper fix

The end-state is a **fully bridge-owned tool loop** — then the bridge never loses the
reasoning in the first place, and quirks like `reattach_reasoning` become moot. Provider-level
placement (`quirks_mode` on `resource.plugins`, so any agent on a strict resource inherits it)
is the planned next step — see [`notes/DESIGN-quirks-mode-on-resource.md`](../../../../notes/DESIGN-quirks-mode-on-resource.md).
Until then, this keeps harness-owned agents working on strict providers across any harness.
It depends on the bridge actually *capturing* reasoning while streaming — see the key-drift
fix in `app/stream_reconstruct.py` (it accumulates `reasoning` + `reasoning_details`, not just
`reasoning_content`).
