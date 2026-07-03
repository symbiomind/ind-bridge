# skills

Surface a catalog of **Anthropic-standard skills** to an agent in their system
prompt. A skill is a folder containing a `SKILL.md` with YAML frontmatter:

```
docx/
└── SKILL.md
```

```markdown
---
name: docx
description: "Use this skill whenever the user wants to create, read, edit, or manipulate Word documents (.docx files)…"
---

# (body — full instructions, read on demand by the agent)
```

Only the **frontmatter** (`name` + `description`) plus a path is injected into the
always-on system prompt. The agent reads the full `SKILL.md` body via its own
filesystem tools **only when the skill is relevant** — progressive disclosure: the
catalog is cheap, the bodies are free until used.

The sibling of [`system_prompt`](../system_prompt/README.md): same capability,
same slot. Declare `skills` **after** `system_prompt` so it appends onto the soul
block.

## Capability / placement

Declares `context_modify` — valid in `identity.context.plugins` or
`role.context.plugins`. Within a slot, plugins fire in **config declaration
order**, so declaring `skills` after `system_prompt` makes it append after it.

## Config

```yaml
context:
  plugins:
    system_prompt:                # runs first
      replace:
        - file: /opt/agents/myagent/SOUL.md
    skills:                       # runs after → appends to the system message
      items:
        - text: "# Skills"
        - text: "Check and read the relevant SKILL.md before acting."
        - dir: /opt/agents/shared/skills:/home/shared/skills
        - dir: /opt/agents/myagent/skills:/home/myagent/skills
        - dir: /opt/agents/general/skills        # no relabel
```

`items` is an **ordered** list of tagged dicts:

- `{text: "..."}` — inline text, verbatim. Renders in list position.
- `{dir: "host[:presented]"}` — a skills directory (see path mapping below).

### The host↔container path mapping (important)

The bridge reads skill files from **host** paths (it has the host dir
bind-mounted). But an agent opens files through a **filesystem MCP server** that
mounts the same host dir under a **different container path**. For example,
myagent's `myagent-filesystem` compose declares:

```yaml
volumes:
  - /opt/agents/myagent:/home/myagent
  - /opt/agents/shared:/home/shared
# FS_ALLOWED_DIRS=/home
```

So a skill the bridge reads at `/opt/agents/myagent/skills/docx/SKILL.md`
is, to the agent, at `/home/myagent/skills/docx/SKILL.md`. If we advertised the host path
it couldn't open it (it doesn't exist in its container).

`dir: host:presented` solves this: the plugin **walks the host (left) path** but
**shows the presented (right) path** in the catalog. The `presented` side must
match what the agent's filesystem MCP actually exposes (`FS_ALLOWED_DIRS` + the
compose mount). Omit `:presented` when the agent and bridge share a namespace —
the host path is shown as-is.

The bridge does **not** parse docker-compose or introspect mounts — the relabel
is a render-time prefix substitution, declared inline where it's used.

## Merge / override

All `dir:` items contribute to **one skill pool keyed by skill name** (the
frontmatter `name`, or the folder name if absent). Dirs are processed in list
order; a skill name seen in a **later** dir **replaces** the earlier one
(last-wins). So shared defaults can be overridden by agent-specific skills
declared later:

```yaml
- dir: …/shared/skills:/home/shared/skills   # docx (default)
- dir: …/myagent/skills:/home/myagent/skills # docx here REPLACES the shared one
```

The merged catalog renders **once**, at the position of the **first** `dir:`
item, in first-seen order (later content wins).

## Render format

Each skill renders as:

```
- <name>: <description>  (<presented-path>/<skill>/SKILL.md)
```

## Bad input

**Warn-and-skip, never crash** (mirrors `app.prompt_parts`). A missing dir, a
folder with no `SKILL.md`, an unreadable/unparseable frontmatter block, or a
skill missing a `description` → one warning is logged and that skill/dir is
skipped. The pipeline continues; valid skills still render.

## Notes

- Files are **walked and read per request** (no caching) — add or edit a skill
  and it takes effect on the next turn, no restart.
- Reuses `app.system_message` (the shared "exactly one system message" helper,
  also used by `system_prompt`).
- The agent still needs filesystem **tools** (e.g. `mcp_client` → a filesystem
  MCP server) to actually *read* a skill body. `skills` only advertises the
  catalog; it doesn't grant access.
