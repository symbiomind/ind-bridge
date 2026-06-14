# quirks_mode — when and where to wire it (FAQ)

A note for anyone puzzling over a `quirks_mode:` block in a config file.

## What does a `quirks_mode` block mean?

```yaml
quirks_mode:
  model: "moonshotai/kimi-k2.5"
```

It means **"apply the known-good quirk recipe for that model."** `model:` is a key into the
shipped [`models.yml`](models.yml) cheat-sheet, which maps a model string to the list of
provider-compat quirks it needs. For `moonshotai/kimi-k2.5` that's all three
(`reattach_reasoning`, `mirror_reasoning_key`, `close_trailing_orphan`).

## Two ways to select quirks — cheat sheet OR workbench

- **`model:` (cheat sheet)** — inherit a proven recipe from `models.yml`. For known-working
  setups.
- **`quirks: [...]` (workbench)** — hand-pick quirks by name. For *bringing up* a new model:
  read what each quirk does, toggle them until it works, then **graduate** the working set
  into `models.yml` as a new `model:` entry.

They're mutually exclusive — set both and `model:` wins (with a warning).

**Default-OFF.** No quirks are applied unless explicitly selected (`{}` / absent / `[]` /
an unknown `model:` → nothing on). Most quirks would *break* a tolerant provider, so you opt
in deliberately. This is the whole point of `quirks_mode` vs an always-on shim: an agent on
Claude carries none.

### New-model bring-up workflow (worked example)

When testing `moonshotai/kimi-k2.6`: switch to a `quirks:` list, drop quirks one at a time to
see which are still needed (maybe k2.6 doesn't need `close_trailing_orphan`), find the working
set, then add a `moonshotai/kimi-k2.6:` entry to `models.yml` with that set — and switch back
to the `model:` form. The cheat sheet grows one characterised model at a time.

## Why wire it at the ROLE (not an identity)?

Wiring at `roles.<name>.context.plugins.quirks_mode` means ALL identities on that role inherit
it automatically:

    identities.librechat_agent ─┐
                                ├─→ role: my_agent_role ─→ quirks_mode
    identities.harness_b_agent ─┘

**This is why the harness switch works seamlessly.** One wiring covers the agent no matter
which harness (LibreChat / OpenClaw / any future harness) it's reached through. If it were on
a single identity, only that harness's path would get the compensation.

(Provider-level placement on `resource.plugins` — so any agent on a strict resource inherits
it automatically — is the planned next step; see `notes/DESIGN-quirks-mode-on-resource.md`.)

## Why do only some agents have it?

`quirks_mode` compensates for **a provider's strict thinking-mode frame rules**. Only agents on
providers that enforce those rules need it:

| Agent / role | Provider | Quirks? |
|---|---|---|
| an agent on Moonshot/Kimi | **Moonshot** — strict | **yes** (k2.5 recipe) |
| an agent on OpenRouter | openrouter — tolerant | no |
| an agent on Claude | Claude — tolerant | no |

Put `quirks_mode` on a Claude agent with no `model:`/`quirks:` and it just no-ops (default-OFF).
The "secret": **it's not agent-specific by nature — it's Moonshot-specific.**

## What does it actually do? (three quirks — see __init__.py / README.md)

It normalizes the outbound message frame so a strict provider accepts it, across ANY harness:

1. **`close_trailing_orphan`** — if the request ends in an unanswered `assistant(tool_calls)`
   (an in-flight harness call), append a plain-language synthetic tool result so the frame is
   valid. (Moonshot 400s on a request ending in an unanswered call.)
2. **`reattach_reasoning`** — restore reasoning the bridge captured, matched by tool
   **name + arguments** (NOT tool_call_id — harnesses rewrite ids; OpenClaw appends a per-call
   hash, LibreChat reuses `:0`).
3. **`mirror_reasoning_key`** — if a turn carries `reasoning` (OpenRouter's key) but not
   `reasoning_content` (the key Moonshot *enforces*), mirror it across.
