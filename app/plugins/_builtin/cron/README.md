# cron

Fire an agent on a schedule — **no external caller required**. The first mechanism
for "the bridge drives a turn from the inside." An agent reflects at 3am, says
good-morning at 9am; each tick synthesises a request and runs it through the
identity's own pipeline via `pipeline_executor.execute()`.

Capability: **`background`** (valid only on `identity.plugins`). core spawns it once
at startup and cancels it cleanly at shutdown — same machinery as `memory_enricher`.

## Config

Each key under `cron:` is a **named job** (one identity can hold several schedules):

```yaml
identities:
  my_agent_cron:
    role: my_agent_role          # shared role ⇒ shared session ⇒ turn lands in history
    token: ${MY_AGENT_TOKEN}
    plugins:
      cron:
        reflect_3am:
          time: "0 3 * * *"      # standard 5-field cron expression
          prompt_text: "You are calling yourself — reflect on your day."
          # prompt_file: /path/to/prompt.md   # alternative to prompt_text
          # timezone: Australia/Adelaide       # optional; else identity tz / UTC
          # model: anthropic/claude-opus-4.8   # optional; else resource default
        morning_hello:
          time: "0 9 * * *"
          prompt_text: "Good morning. What's on your mind?"
    context:
      name: MyAgent
      trust: trusted
      additional:
        status: "3am Reflection"
```

| Key | Required | Meaning |
|---|---|---|
| `time` | yes | 5-field cron expression (validated at startup via `croniter`) |
| `prompt_text` | one of | The synthetic user turn's content |
| `prompt_file` | one of | Path to read the prompt from (read once at startup) |
| `timezone` | no | IANA tz for this job; else identity tz; else UTC |
| `model` | no | Override the model; else the resource default applies |

## How it works

- **Self-as-caller.** Give the cron identity a `context:` block — the signed
  `<bridge_context>` carries `<caller>MyAgent</caller>` with whatever `additional`
  status you set, so the agent knows *who's* calling (itself) and *why* it's awake.
- **The reply is discarded.** Wire the identity onto a role/session running
  `basic_session` and the turn is persisted to the shared history for free —
  cron never has to do anything with the response.
- **Named jobs run concurrently.** One sleeper-loop per job, gathered under a
  single background task.

## Robustness

- No `cron:` block / no valid job → never spawns (DLC-grace; bridge boots fine).
- A malformed single job is warned-and-skipped at startup; siblings still run.
- A failed single tick is logged and swallowed; the loop keeps ticking.
- Shutdown cancels every job loop cleanly.

## Note — parallelarismerers

A cron identity on a **shared** session is a concurrent caller. The day a 3am fire
overlaps a live LibreChat turn is the first real test of session serialisation;
until that lands, two concurrent writers to one session can race (a pre-existing
`basic_session` property, not cron's bug). See the design doc.
