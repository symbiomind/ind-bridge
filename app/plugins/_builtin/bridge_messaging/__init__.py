"""
bridge_messaging — agent-to-agent messaging, internal to the bridge.

One agent reaches another by ROLE NAME as a tool call ("ask another agent a
question"). A V4 reimagining (NOT a V3 port — the V3 ``bridge_message`` HTTP/token
model was found flawed). V4 delivers **in-process** via
``pipeline_executor.execute()`` — no HTTP hop, no token juggling — reusing the
recipient role's real pipeline so the message lands as a normal, signed inbound turn.

Wire it per-role with a SINGLE ``bridge_messaging`` block under the role's
``context.plugins`` — the same idiom as any other bridge-native tool plugin
(``agent_tools`` / ``mcp_client``). The ``context_modify`` slot (tool inject) is the
declaration site; the bridge fans out the other capabilities (``background`` for the
phonebook, ``handle_tool_calls`` for dispatch) from that one block:

    roles:
      agent_a:
        context:
          plugins:
            bridge_messaging:
              bridge_tool: true       # this role GETS the send/list tools
              agent_listing: true     # this role is LISTED in the phonebook (reachable)

      periodic_sender:
        context:
          plugins:
            bridge_messaging:
              bridge_tool: true       # can send …
              caller: "Scheduler"     # … and (non-agent sender) names itself in config

  * ``bridge_tool: true``   — inject ``bridge_messaging_list`` + ``bridge_messaging_send``
    into this role's offered tools (``context_modify`` slot).
  * ``agent_listing: true`` — register this role in the global phonebook under its role
    name, so other roles can ``send`` to it (``background`` slot, built at startup).
  * ``caller: "..."``       — OPTIONAL cosmetic caller-name override for a NON-agent
    sender (cron/config-fired). Absent → falls back to the sending role name. Has NO
    effect on the tool path (an agent's caller is its live identity; see below).

THE SOVEREIGN CALLER (security, load-bearing). The caller NAME is a trust signal — a
recipient trusts "System" differently than a peer agent. So an agent can NEVER set it:
``caller`` is not a send param and not in the tool schema. It is always set by a
TRUSTED source via the resolution ladder:

  1. Tool path (an agent called the tool) → the LIVE calling identity (ctx.role.key).
     This WINS even if config also names one — an agent can't "send as" anything but
     itself. (Stops "call another agent as 'System' and exfiltrate its .env files".)
  2. Config path, ``caller:`` set → that name (operator-authored = trusted; cosmetic).
  3. Config path, ``caller:`` absent → the sending role's own name (from the cascade).

Likewise ``trust`` is hardcoded ``bridge_messaging`` and the privilege attrs in
``additional`` are reject-gated (a reserved key → the send fails and the agent narrates
it). All of these ride in the bridge-SIGNED ``<caller>`` block, so the recipient's
``verify_inbound`` makes them immutable. Mint-side gate + verify-side seal.

The bridge also stamps two bridge-owned ``<caller>`` attributes by default: ``tool``
(the send tool's namespaced name, for attribution) and ``information`` (a reply-
instruction so a recipient — especially a memory-less guest — knows to reply in chat,
not re-call the send tool). ``information``'s TEXT comes from the RECIPIENT role's
``bridge_messaging.information`` (it's the receiving agent's own instruction), falling
back to ``_DEFAULT_INFORMATION``; set that key ``false``/"" to omit. Both are reserved
(a caller can't set/override them) and signed. This is why the reply-contract lives in
the signed ``<caller>`` and NEVER in the message body: the sender never spoke it, so it
must not appear in the sender's voice (voice-purity).

Delivery mechanic: the synthetic caller is carried into the recipient role's pipeline
by driving ``execute(carrier_identity, body, headers)`` where ``body`` stamps the
reserved ``_bridge_caller`` / ``_bridge_trust`` / ``_bridge_storage`` / ``_cron_additional``
keys (popped in ``_build_ctx``, signed by ``assemble_and_sign``). The carrier identity is
just a vehicle to materialise the role's pipeline; its own ``<caller>`` is overridden.

Two send flags, orthogonal:
  * ``output``  (default true)  — return the recipient's reply to the caller. false =
    fire-and-forget; the caller gets a terse delivered-signal, not a reply. "async" =
    detached delegation (reply arrives later as a chat turn).
    **Operator is sovereign, model chooses in the silence:** when the sender's block sets
    ``output``, that is a CONTRACT and a model-supplied ``output`` arg cannot override it.
    An operator who pins ``output: false`` is declaring the send one-way BY DESIGN — e.g. a
    notify-only sender, or a scheduled job that must deposit and never open a conversation —
    and a model must not be able to talk its way into a round-trip. When the operator has
    NOT set it, the model may choose — only the caller knows whether its target can answer
    at all (a log / file sink / write-only resource has no reply to wait for).
  * ``storage`` (default true)  — false stamps ``storage="false"`` as a signed attribute
    on the ``<bridge_context>`` envelope (a TURN-property, not a ``<caller>`` identity
    attr): a universal "this turn is ephemeral, any plugin that PERSISTS the pair should
    leave it alone" marker. Signed (folded into the HMAC) → no external override.
    ``conversational_memory`` honours it (recall
    yes, store no); ``basic_session`` is blind to it (the session IS the conversation).

A fire-and-forget notification that should not persist falls out as ``output: false`` +
``storage: false`` — zero special-case code; the recipient reacts then it fades from recall.

Two sender-side ROUTING policies (operator-set on the sender role's block, never
model-facing — same family as ``storage`` / ``output`` / ``caller``):
  * ``send_allow`` (list)  — an allow-list of role names this sender may reach.
    When present, only these agents appear in ``bridge_messaging_list`` AND a
    ``send`` to anything else FAILS (the agent narrates the wall). Omitted = all
    reachable agents (default behaviour).
  * ``send_only`` (string) — a single role name every ``send`` is SILENTLY
    rerouted to, regardless of the ``to:`` the agent chose. The listing is
    unaffected (the sender still sees the allowed/full phonebook and picks a
    ``to:`` freely) — the redirect is invisible to the sender. If both are set,
    ``send_only`` wins for ROUTING (``send_allow`` still filters the listing).
    Use-case: a sink/intercept — the sender thinks it messaged a peer; the bridge
    delivered elsewhere.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import re
import time
import uuid
from typing import TYPE_CHECKING

from app import bridge_native

if TYPE_CHECKING:
    from app.context import PipelineCtx, StartupCtx

logger = logging.getLogger(__name__)


CAPABILITIES = {
    "context_modify":     ["identity.context.plugins", "role.context.plugins"],
    "handle_tool_calls":  ["identity.plugins", "role.plugins"],
    "background":         ["identity.plugins", "role.plugins"],   # build the phonebook
}


OWNED_TOOLS = ["bridge_messaging_list", "bridge_messaging_send"]
"""Tool names this plugin claims via handle_tool_calls (D-008). Validator rejects
a missing/empty OWNED_TOOLS when the capability is declared, and cross-plugin
collisions on one identity."""


# Attributes a CALLER may never set on itself — sovereign trust signals. `caller`
# (WHO is asking) and `trust` (how trusted) and `storage` (ephemerality) and the
# signing wrappers are bridge-owned. An agent-supplied `additional` carrying any of
# these is rejected (forge guard). `caller` is also simply absent from the tool schema
# (impersonation guard) — this set catches it defensively if it ever appears in args.
#
# `information` is ALSO bridge-owned: it's a reply-instruction the bridge stamps onto the
# delivered <caller> by default (text from the RECIPIENT role's config). A caller trying to
# set it via `additional` is rejected — the recipient decides what instruction they need,
# not the sender.
_RESERVED_ATTRS = frozenset({
    "caller", "trust", "tool", "storage", "recall", "signed", "timestamp", "information",
})

_BRIDGE_TRUST = "bridge_messaging"

# Default reply-instruction stamped as information="…" on the delivered <caller>, so a
# recipient (especially a memory-less guest) knows to reply in chat rather than fighting the
# send tool. Overridable per-RECIPIENT-role via bridge_messaging.information; set that to
# false/"" to omit. Rides in the SIGNED <caller> — never in the sender's message voice
# (voice-purity: instruction lives in context, not in words the sender never spoke).
_DEFAULT_INFORMATION = (
    "You are receiving a message from another agent via bridge_messaging. To reply, "
    "just respond normally in chat — no tool call needed; the bridge routes your reply "
    "back to the sender."
)

# Output-aware default: on a fire-and-forget (output:false) send the sender is NOT
# waiting for a reply and the recipient's reply is NOT routed anywhere — so the
# round-trip default above would over-promise ("the bridge routes your reply back"
# when nothing does). This variant tells the honest truth: it's a notification.
# Deliberately says NOTHING about how to reply — on a delivery turn the send tool is
# withheld by the loop-break (modify_context), so any "use the tool to reply"
# instruction would be a lie-in-mechanism (enforcement beats prose). If the recipient
# genuinely wants to reach back, that's a fresh, separate send THEY initiate later —
# not something this delivery turn can do. Overridable per-recipient-role exactly like
# the round-trip default; `information: false`/"" omits it.
_DEFAULT_INFORMATION_NOREPLY = (
    "You are receiving a notification from another agent via bridge_messaging. This is "
    "fire-and-forget: no reply is expected or routed back to the sender. Read it and "
    "carry on; there is nothing to send in response."
)

# Async (output:async) recipient default: the sender delegated a task and carried on;
# your reply IS wanted, just not synchronously — the bridge delivers it to the sender as
# a later chat turn. Like the round-trip default, tells the recipient to just reply in
# chat (no tool). Deliberately no tool instruction → sidesteps the loop-break landmine
# (the mailbox is bridge-side, not a recipient tool-call). Overridable per-recipient-role.
_DEFAULT_INFORMATION_ASYNC = (
    "You are receiving a delegated task from another agent via bridge_messaging. Do the "
    "work and reply normally in chat — no tool call needed; the bridge delivers your reply "
    "back to the sender as a later message when you're done. Take the time you need."
)

# The sovereign trust value stamped on an ASYNC WAKE turn — the deferred reply arriving
# back on the ORIGINAL SENDER's session as a role:user turn. Distinct from _BRIDGE_TRUST
# on purpose: the loop-break (modify_context) withholds send/list tools ONLY on a
# trust==bridge_messaging DELIVERY turn. A wake turn must NOT withhold the sender's own
# tools — they need to act on the result and possibly delegate again. A different trust
# value sails past the loop-break's string match. Also reserved (can't be forged).
_ASYNC_DEFERRED_TRUST = "async-deferred"


# ---------------------------------------------------------------------------
# Phonebook — built once at startup (background slot)
# ---------------------------------------------------------------------------
#
# role_name -> { "display_name": str, "carrier_identity_key": str }
# A role is reachable iff its bridge_messaging config sets agent_listing: true.
# The carrier identity is any identity that materialises the role — execute() is
# keyed by identity, and we override the WHO with the synthetic caller, so which
# carrier is picked doesn't affect the delivered <caller> (only the role's pipeline).
# A role with no operator-declared identity is still reachable: config load mints a
# token-less internal carrier (config.INTERNAL_CARRIER_PREFIX) for any agent-listed
# role, so the lookup below finds one either way.
_PHONEBOOK: dict[str, dict] = {}


def start_background(ctx: "StartupCtx", config: dict):
    """Build the global phonebook at startup. Returns None (no long-running task —
    this is a one-shot registry build, not a scheduler). Core calls this once per
    identity that wires bridge_messaging; the build is idempotent (keyed by role
    name) so repeated calls converge on the same phonebook."""
    from app import config as config_mod

    try:
        role_keys = config_mod.list_roles()
    except Exception as e:  # pragma: no cover - defensive
        logger.error(f"bridge_messaging: could not list roles for phonebook — {e}")
        return None

    for role_key in role_keys:
        role_cfg = config_mod.resolve_role(role_key) or {}
        bm_cfg = _role_bridge_messaging_cfg(role_cfg)
        if not bm_cfg.get("agent_listing"):
            continue
        carrier = _resolve_carrier_identity(role_key, config_mod)
        if carrier is None:
            # Should not happen for agent-listed roles: config load synthesises a
            # token-less internal carrier for them. If it does, the role is genuinely
            # unreachable (no pipeline to materialise) — skip rather than register a
            # phantom the sender can't deliver to.
            logger.warning(
                f"bridge_messaging: role '{role_key}' has agent_listing: true but no "
                f"carrier identity resolved — not reachable. (Expected an internal "
                f"carrier to be synthesised at config load.)"
            )
            continue
        _PHONEBOOK[role_key] = {
            "display_name": bm_cfg.get("display_name") or role_key,
            "carrier_identity_key": carrier,
        }
        logger.info(
            f"bridge_messaging: phonebook registered '{role_key}' "
            f"(via carrier identity '{carrier}')"
        )
    return None


def _role_bridge_messaging_cfg(role_cfg: dict) -> dict:
    """Pull the bridge_messaging block out of a role config, defaulting to {}.

    The idiomatic declaration site is ``context.plugins.bridge_messaging`` (the
    context_modify / tool-inject slot — one block from which ``background`` and
    ``handle_tool_calls`` fan out). We read THAT first so a single config block is
    the whole story (this is what the phonebook needs: agent_listing / caller /
    display_name). We fall back to the legacy top-level ``plugins.bridge_messaging``
    so older two-block configs keep working without a forced migration.

    Tolerant of the disable-tombstone (`false`) shape (returns {} for a non-dict)."""
    context_plugins = (role_cfg.get("context") or {}).get("plugins") or {}
    bm = context_plugins.get("bridge_messaging")
    if not isinstance(bm, dict):
        # Legacy fallback: top-level plugins.bridge_messaging (pre-single-block configs).
        bm = (role_cfg.get("plugins") or {}).get("bridge_messaging")
    return bm if isinstance(bm, dict) else {}


def _resolve_carrier_identity(role_key: str, config_mod) -> str | None:
    """Resolve the identity_key used as the DELIVERY VEHICLE for ``role_key``.

    A delivery is role → role: the caller is the CONSTRUCTED sovereign identity we
    stamp in _build_ctx (name/trust/additional), and the vehicle exists only to
    materialise the recipient ROLE's pipeline via execute() (which is keyed by
    identity). So the vehicle must be NEUTRAL — it must not drag a human chat
    identity's own context.plugins (e.g. a conversational_memory tombstone) into the
    delivered pipeline, which would cascade over the role (identity > role) and
    silently alter behaviour (and depend on config declaration order).

    Prefer the role-only internal carrier ``__bridge_internal__<role>`` — a bare
    {role: …} identity with no context of its own, minted at config load for every
    agent-listed role. Fall back to the first identity that materialises the role
    only if (unexpectedly) no internal carrier exists, so the role stays reachable
    rather than dropping from the phonebook."""
    internal = f"{config_mod.INTERNAL_CARRIER_PREFIX}{role_key}"
    try:
        identity_keys = config_mod.list_identities()
    except Exception:  # pragma: no cover - defensive
        return None
    if internal in identity_keys:
        return internal
    # Defensive fallback (should be rare — config load mints the internal carrier
    # for every agent-listed role): first identity declared for this role.
    for ik in identity_keys:
        if config_mod.get_identity_role_key(ik) == role_key:
            return ik
    return None


# ---------------------------------------------------------------------------
# context_modify — inject the send/list tools (only when bridge_tool: true)
# ---------------------------------------------------------------------------

_TOOL_DEFINITIONS = {
    "bridge_messaging_list": {
        "type": "function",
        "function": {
            "name": "bridge_messaging_list",
            "description": (
                "List the other agents you can message. Returns their names "
                "(use a name as the 'to' field of bridge_messaging_send)."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    "bridge_messaging_send": {
        "type": "function",
        "function": {
            "name": "bridge_messaging_send",
            "description": (
                "Send a message to another agent by name (see bridge_messaging_list). "
                "Use this to ask another agent a question or pass them something. By "
                "default their reply comes back to you AS THE RESULT OF THIS TOOL CALL. "
                "This is ONE-SHOT: sending does NOT open a linked chat with them and does "
                "NOT connect their session to yours. If you want to say something more "
                "after reading their reply, call bridge_messaging_send AGAIN — replying "
                "in your own chat does not reach them. "
                "For a long task, set output:\"async\" to DELEGATE: the tool returns "
                "immediately with a tracking id and you carry on; the agent's reply "
                "arrives later as a new message in your chat (tagged with that id). Fire "
                "several async tasks, end your turn, and act on each reply as it lands. "
                "If you want an answer, you want the default (true) or \"async\" — NOT "
                "output:false, which throws the message into a void and tells you nothing "
                "came back. Only choose false when the target genuinely cannot answer you "
                "(a log, a file sink, a write-only resource) or when you truly do not care "
                "whether it did."
                # NOTE: deliberately NO `caller` param — you always message AS yourself;
                # the bridge stamps your identity. `storage` is operator-set (read from
                # `eff` only). `output` IS model-facing on purpose: a send to a sink/
                # file/write-only resource has no reply to wait for, and only the caller
                # knows that. But false is a DEAD END for the sender — say so plainly
                # here, because the recipient-side _DEFAULT_INFORMATION_NOREPLY already
                # tells the RECIPIENT the honest truth and the sender deserves the same.
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "to": {
                        "type": "string",
                        "description": "Name of the agent to message (from bridge_messaging_list).",
                    },
                    "message": {
                        "type": "string",
                        "description": "What to say to them.",
                    },
                    "additional": {
                        "type": "object",
                        "description": (
                            "Optional extra context shown to the recipient as tags on "
                            "your caller (e.g. a subject or mood). Decorative only — "
                            "reserved keys (caller/trust/tool/storage/information) are "
                            "bridge-owned and rejected if you set them."
                        ),
                    },
                    "output": {
                        "description": (
                            "How you get the reply. true (default): wait, and their reply "
                            "comes back as this tool's result — use this when you want an "
                            "answer now. \"async\": delegate — returns immediately with a "
                            "tracking id, and their reply arrives later as a new message "
                            "in your chat; use this when you want an answer but don't want "
                            "to block. false: NO REPLY EVER — the message is delivered and "
                            "the answer, if any, is discarded; you get a bare "
                            "delivered-signal and you will never learn what they said. "
                            "This is a dead end, not a faster true: if you would be "
                            "disappointed to hear nothing back, do not choose it. It is "
                            "the right choice ONLY when the target cannot answer you (a "
                            "log, a file sink, a write-only resource) or when you genuinely "
                            "do not care about the response."
                        ),
                        "oneOf": [
                            {"type": "boolean"},
                            {"type": "string", "enum": ["async"]},
                        ],
                    },
                },
                "required": ["to", "message"],
            },
        },
    },
}


def modify_context(ctx: "PipelineCtx", config: dict) -> "PipelineCtx":
    """Inject bridge_messaging_list + bridge_messaging_send into ctx.request.tools,
    namespaced to the bridge_native wire prefix. Only when the role opted in with
    ``bridge_tool: true`` — a role can be LISTED (reachable) without being given the
    send tool, and vice-versa."""
    if not config.get("bridge_tool"):
        return ctx

    # LOOP-BREAK (the timeout-race fix): when THIS turn is itself a bridge_messaging
    # DELIVERY — i.e. the recipient is running as a synthetic caller whose trust we
    # stamped to "bridge_messaging" — do NOT give them the send tool. Otherwise the
    # recipient "replies" by calling send back, which spawns a NEW delivery instead of
    # returning text up the caller's output:true stack; the caller's wait times out,
    # the late reply arrives as fresh mail, and that restarts the dance → A→B→A→B
    # runaway. With the SEND/LIST tools withheld, the recipient replies with
    # TEXT, which output:true returns to the original caller — one clean hop. The
    # caller (agent or human) then DECIDES whether to send again; the round-trip is
    # never an automatic bounce. A recipient running normally (not via bridge_messaging)
    # keeps full tools.
    #
    # NOTE: this withholds ONLY the send/list tools. The recipient KEEPS their own
    # tools (mcp_client memory queries, file reads, etc.) — a delivery turn runs the
    # recipient's normal intercept tool loop so they can DO WORK to assemble a reply
    # (e.g. a librarian messaged "find X" queries the memory, then answers in prose).
    # That's the whole point of delegating to a specialist. The loop-break is the
    # send-tool withhold ALONE; it is sufficient (no send tool → no recursive re-send).
    if getattr(ctx.identity, "trust", None) == _BRIDGE_TRUST:
        logger.info(
            f"bridge_messaging: identity '{ctx.identity.key}' is a bridge_messaging "
            f"DELIVERY turn (trust={_BRIDGE_TRUST}) — withholding send/list tools so the "
            f"reply returns as text (loop-break); recipient cannot recursively re-send."
        )
        return ctx

    existing_names = {
        t.get("function", {}).get("name")
        for t in ctx.request.tools
        if isinstance(t, dict)
    }
    to_add = []
    for name in OWNED_TOOLS:
        d = copy.deepcopy(_TOOL_DEFINITIONS[name])
        d["function"]["name"] = bridge_native.apply_namespace(name)
        if d["function"]["name"] not in existing_names:
            to_add.append(d)
    if to_add:
        ctx.request.tools.extend(to_add)
        logger.info(
            f"bridge_messaging: injected {len(to_add)} tool(s) on identity "
            f"'{ctx.identity.key}': {[d['function']['name'] for d in to_add]}"
        )
    return ctx


# ---------------------------------------------------------------------------
# handle_tool_calls — execute a claimed list/send (D-008)
# ---------------------------------------------------------------------------

async def handle_tool_calls(ctx: "PipelineCtx", config: dict) -> str:
    """Executor dispatches one claimed tool_call here (clean name on
    ctx.plugin_data["handle_tool_calls.claimed"]). Return a string result; the
    executor splices it and re-calls upstream so the agent reacts in its own voice."""
    tc = ctx.plugin_data.get("handle_tool_calls.claimed")
    if not isinstance(tc, dict):
        return "[bridge_messaging error: no claimed tool_call on ctx]"

    name = (tc.get("function") or {}).get("name")
    raw_args = (tc.get("function") or {}).get("arguments") or "{}"
    try:
        args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
    except (json.JSONDecodeError, ValueError):
        args = {}
    if not isinstance(args, dict):
        args = {}

    if name == "bridge_messaging_list":
        # Same slot-family-proof merge the send path uses: policy lives in the
        # sender role's block (context.plugins), but a top-level `plugins` block or
        # test may pass it via the slot `config` — let slot config override.
        return _handle_list({**_sender_policy_cfg(ctx), **(config or {})})
    if name == "bridge_messaging_send":
        return await _handle_send(ctx, args, config)
    return f"[bridge_messaging error: no handler for '{name}']"


def _handle_list(sender_cfg: dict | None = None) -> str:
    """Render the phonebook for the model — bare names + display names.

    Honours the sender's ``send_allow`` allow-list (operator policy): when set to
    a non-empty list, only those role names are shown. Omitted/empty → show all.
    ``send_only`` deliberately does NOT collapse the listing — the sender still
    sees the full/allowed phonebook and picks a ``to:`` freely; the reroute is
    invisible at send time, not here."""
    allow = (sender_cfg or {}).get("send_allow")
    if isinstance(allow, list) and allow:
        allowed = set(allow)
        entries = {r: e for r, e in _PHONEBOOK.items() if r in allowed}
    else:
        entries = dict(_PHONEBOOK)
    if not entries:
        return "No other agents are reachable right now."
    lines = ["Agents you can message (use the name as 'to'):"]
    for role_name, entry in sorted(entries.items()):
        disp = entry.get("display_name")
        if disp and disp != role_name:
            lines.append(f"  • {role_name} ({disp})")
        else:
            lines.append(f"  • {role_name}")
    return "\n".join(lines)


async def _handle_send(ctx: "PipelineCtx", args: dict, config: dict) -> str:
    """Mint a synthetic signed caller and deliver ``message`` into the target role's
    pipeline in-process via execute(). Returns the reply (output:true) or a delivered
    signal (output:false). On any caller-supplied policy violation, FAILS with a naming
    error (the executor surfaces it as a tool result the agent narrates)."""
    from app import pipeline_executor

    target = args.get("to")
    message = args.get("message")
    if not target or not isinstance(target, str):
        return "[bridge_messaging error: 'to' (recipient agent name) is required]"
    if not message or not isinstance(message, str):
        return "[bridge_messaging error: 'message' is required]"

    # --- SENDER POLICY resolution (slot-family-proof) -----------------------
    # Resolve the sender role's bridge_messaging block up front — the ROUTING
    # policy (send_allow / send_only) must gate BEFORE the phonebook lookup so a
    # blocked target reads as a policy wall (not "unknown agent"). Same slot-family
    # reasoning as the storage/output/caller resolution used below: this handler runs
    # in the handle_tool_calls slot, whose `config` is merged from the top-level
    # `plugins` family only (D-006), so a single-block config leaves `config` empty
    # of these keys — resolve from the role's actual block (context-first, same
    # helper the phonebook uses), then let the slot `config` override if it carries a
    # value (back-compat with a top-level block).
    sender_cfg = _sender_policy_cfg(ctx)
    eff = {**sender_cfg, **(config or {})}  # slot config wins if it has the key

    # --- ROUTING policy: send_allow (allow-list) then send_only (redirect) ---
    # send_allow is checked against the AGENT-CHOSEN target (honest feedback: "you
    # can't message that") — even though send_only, if set, would reroute anyway.
    allow = eff.get("send_allow")
    if isinstance(allow, list) and allow and target not in allow:
        allowed = ", ".join(sorted(allow)) or "(none)"
        return (
            f"[bridge_messaging error: you are not permitted to message '{target}'. "
            f"Allowed: {allowed}]"
        )
    # send_only wins for routing: silently reroute EVERY send to this one endpoint,
    # regardless of the `to:` the agent chose. Invisible to the sender (caller-stamp,
    # signing, reply all proceed against the redirected target unchanged). If the
    # send_only target isn't in the phonebook, the lookup below fails loud (operator
    # misconfig).
    send_only = eff.get("send_only")
    if isinstance(send_only, str) and send_only:
        if send_only != target:
            logger.info(
                f"bridge_messaging: send_only redirect '{target}' → '{send_only}' "
                f"(sender role '{getattr(ctx.role, 'key', '?')}')"
            )
        target = send_only

    entry = _PHONEBOOK.get(target)
    if entry is None:
        avail = ", ".join(sorted(_PHONEBOOK)) or "(none)"
        return (
            f"[bridge_messaging error: no agent named '{target}'. "
            f"Reachable agents: {avail}]"
        )

    # --- SECURITY: reject-first gate on caller-supplied `additional` ---------
    # Decorative kv is allowed; any reserved trust signal is a forge attempt → fail
    # loud so the agent narrates the wall (Grok-proofing: make it impossible, not
    # impolite). caller/trust/storage are bridge-sovereign and set below by US.
    raw_additional = args.get("additional") or {}
    if not isinstance(raw_additional, dict):
        return "[bridge_messaging error: 'additional' must be an object of key/value pairs]"
    offending = [k for k in raw_additional if str(k).lower() in _RESERVED_ATTRS]
    if offending:
        return (
            f"[bridge_messaging error: '{offending[0]}' is a reserved bridge signal "
            f"and cannot be set by a caller. Remove it and resend. "
            f"(You always message AS yourself — the bridge stamps your identity.)]"
        )

    # `eff` (sender policy: storage / output / caller — resolved above with the
    # routing policy). Slot-family-proof: a single-block config lives in
    # context.plugins so the handle_tool_calls `config` is empty of these keys;
    # `eff` merged the role's actual block under the slot config, so storage no
    # longer silently defaults to true (which would LEAK an ephemeral message into
    # the recipient's conversational_memory).
    #
    # Tool path: the caller is the LIVE calling identity — never from args. We are
    # inside the caller's pipeline, so ctx.role.key / ctx.identity is the truth.
    sovereign_caller = _resolve_sovereign_caller(ctx, eff)

    # `output` is TRI-VALUED (back-compat with the old boolean):
    #   true  / True        → round-trip: block, return the recipient's reply as the
    #                         tool result (the default).
    #   false / False       → fire-and-forget: return "[delivered]", discard the reply.
    #   "async"             → detached delegation: fire the recipient in a background
    #                         task, return "[async sent, id=X]" NOW, and when it finishes
    #                         WAKE the sender with the reply as a role:user turn.
    # _resolve_output_mode normalises to one of "true"/"false"/"async".
    #
    # OPERATOR IS SOVEREIGN, MODEL CHOOSES IN THE SILENCE. When the operator has
    # explicitly set `output` on the sender's block, that is a CONTRACT and the model
    # cannot talk its way out of it — same family as `storage` / `caller` (below), which
    # never read args at all. When the operator has NOT set it, the model picks: only the
    # caller knows whether its target can actually answer (a send to a log / file sink /
    # write-only resource has no reply to wait for), so `output` stays model-facing there.
    # WHY THIS MATTERS: an operator pinning output:false has declared the send ONE-WAY BY
    # DESIGN (a notify-only sender; a scheduled job that must deposit and never open a
    # conversation). A model emitting output:"async"/true in its tool args must not be able
    # to override that policy and start a round-trip the operator deliberately foreclosed.
    if "output" in eff:
        output_mode = _resolve_output_mode(eff["output"])
    else:
        output_mode = _resolve_output_mode(args.get("output", True))
    # `output` boolean kept for the existing information-text branch (true vs not-true).
    output = output_mode == "true"
    # storage is operator policy (not normally model-set): role config, default true.
    storage = bool(eff.get("storage", True))
    # recall (default true) — mirror of storage. When false, the receiver's
    # conversational_memory skips recalling memories INTO this delivered turn.
    recall = bool(eff.get("recall", True))

    # THE MESSAGE IS THE VOICE — nothing else. An agent-to-agent message arrives as
    # a normal user turn, and a user turn must be JUST what was said. We do NOT prepend
    # a "📨 from X" header or append a reply-contract footer: that would put bridge
    # INSTRUCTION into the sender's voice (the recipient would read scaffolding as if
    # the sender had spoken it). Everything the recipient needs to KNOW about the
    # delivery already rides in the signed <bridge_context>: <caller trust=
    # "bridge_messaging" tool="…">{sender}</caller> identifies WHO is calling and HOW
    # (method + tool attribution), immutable under the signature. That's sufficient —
    # the recipient reasons about the caller from context, not from injected prose.
    #
    # THE RECIPIENT ANSWERS WITH THEIR OWN HANDS. A delivery turn keeps the recipient's
    # OWN tools (memory query, file reads, etc.) so they can DO WORK to assemble a reply
    # — e.g. a librarian messaged "find X" runs retrieve_memories, digs deeper, then
    # returns prose. This is what makes agent→agent messaging a real delegation, not a
    # blind single-shot reply. The loop-break that prevents an A→B→A→B bounce is carried
    # by modify_context WITHHOLDING ONLY THE SEND/LIST TOOLS on a delivery turn (trust ==
    # bridge_messaging): with no send tool the recipient CANNOT recursively re-send, so
    # they reply with TEXT (their reply returns up the output:true stack; the CALLER then
    # decides whether to follow up). Their own tool loop is bounded by max_tool_laps (the
    # runaway guardrail). _strip_trailing_tool_junk still cleans any trailing tool markup.
    # output governs whether the reply is returned; store true/false decides persistence.
    # (We do NOT set _bridge_no_tools here — that blanket strip is for the label-
    #  enrichment worker, which genuinely wants zero tools; a delivery is not that.)
    framed = message

    # Build the delivery body. The reserved _bridge_* keys are popped in _build_ctx
    # and signed by populate_caller; they never reach the upstream provider.
    body: dict = {
        "messages": [{"role": "user", "content": framed}],
        # Force non-stream so execute() returns a plain response DICT (with the
        # recipient's text), not a FastAPI StreamingResponse. The non-stream path also
        # runs the recipient's intercept tool loop (_dispatch_intercepts: claim →
        # execute → splice → re-call) so a delivery turn that USES tools resolves them
        # and returns the assembled prose. Without stream:False an observers-only turn
        # takes the streaming pass-through and _extract_reply_text gets a
        # StreamingResponse it can't read → reads back as "no reply".
        "stream": False,
        "_bridge_caller": sovereign_caller,
        "_bridge_trust": _BRIDGE_TRUST,
    }
    # `storage` is a TURN-handling marker, not a caller-identity attribute — so it
    # rides on the <bridge_context> ENVELOPE (via _bridge_storage → _build_ctx →
    # assemble_and_sign), NOT among the <caller> attrs in _cron_additional. It's
    # signed there (folded into the HMAC like `warning`), so no external force can
    # add/flip it. Only stamped when the operator asked for ephemerality.
    if not storage:
        body["_bridge_storage"] = "false"
    # `recall` rides the same ENVELOPE rail as storage (signed <bridge_context>
    # attr). Only stamped when the operator turned it off.
    if not recall:
        body["_bridge_recall"] = "false"
    extra_additional = dict(raw_additional)
    extra_additional["tool"] = bridge_native.apply_namespace("bridge_messaging_send")
    # `information` — a bridge-owned reply-instruction stamped onto the SIGNED <caller> so
    # the recipient (esp. a memory-less guest) knows to reply in chat, not re-call the send
    # tool. The TEXT comes from the RECIPIENT role's bridge_messaging block (it's THEIR
    # instruction — what the receiving agent needs to see), falling back to the built-in
    # default. `information: false`/"" on that role omits it. Bridge-authored + signed →
    # a calling agent can't set or override it (reserved above). This lives in context, not
    # in the message body — the sender never "said" it (voice-purity: instruction belongs
    # in the signed envelope, never in words the sender didn't speak).
    from app import config as config_mod
    recipient_cfg = _role_bridge_messaging_cfg(config_mod.resolve_role(target) or {})
    # OUTPUT-AWARE built-in default: a round-trip (output:true) send gets the
    # "reply in chat, the bridge routes it back" text; a fire-and-forget
    # (output:false) send gets the honest "notification, nothing routed back"
    # text so the recipient doesn't craft a reply that evaporates. An explicit
    # recipient-role `information` override wins for BOTH modes (the operator's
    # deliberate choice); only the built-in fallback branches on `output`. The
    # "key present?" opt-out (`information: false`/"") still suppresses either.
    if output_mode == "async":
        default_info = _DEFAULT_INFORMATION_ASYNC
    elif output_mode == "true":
        default_info = _DEFAULT_INFORMATION
    else:
        default_info = _DEFAULT_INFORMATION_NOREPLY
    info = recipient_cfg.get("information", default_info)
    if info:
        extra_additional["information"] = str(info)
    body["_cron_additional"] = extra_additional  # reuses the proven per-turn additional inject

    carrier = entry["carrier_identity_key"]
    logger.info(
        f"bridge_messaging: '{sovereign_caller}' → '{target}' "
        f"(carrier '{carrier}', output={output_mode}, storage={storage})"
    )

    # ── output:async — detached delegation with wake-back ───────────────────
    # Fire the recipient turn in a BACKGROUND task and return to the sender NOW.
    # When it finishes, WAKE the sender: execute() the sender's own identity with
    # the recipient's reply as a role:user turn carrying the async-deferred
    # envelope. The sender's turn queue (step 2) serialises that wake behind
    # whatever they're doing, so it's safe even mid-turn. See _spawn_async_delivery.
    if output_mode == "async":
        call_id = _new_call_id()
        _spawn_async_delivery(
            carrier=carrier,
            body=body,
            target=target,
            sender_identity_key=ctx.identity.key,   # Sonnet #4: the IDENTITY key, not role/display
            sender_caller=sovereign_caller,
            call_id=call_id,
        )
        return f"[async sent to {target}, id={call_id} — reply will arrive as a later message]"

    try:
        resp = await pipeline_executor.execute(carrier, body, {})
    except Exception as e:
        logger.exception(
            f"bridge_messaging: delivery to '{target}' raised on caller "
            f"'{sovereign_caller}'"
        )
        return f"[bridge_messaging error: delivery to '{target}' failed — {e}]"

    if not output:
        return f"[delivered to {target}]"

    reply = _extract_reply_text(resp)
    if reply is None:
        return f"[bridge_messaging: {target} received the message but sent no reply]"
    return reply


# ---------------------------------------------------------------------------
# output:async — detached delegation with wake-back
# ---------------------------------------------------------------------------
#
# The sender delegates a task and carries on; the recipient's reply arrives LATER
# as a role:user turn on the sender's OWN session (the async-deferred envelope).
# This is cron's twin: a self-originating turn, triggered by "sub-agent finished"
# instead of a clock. The sender's turn queue (pipeline_executor, step 2)
# serialises the wake behind whatever the sender is doing, so a wake landing
# mid-turn just queues — never a concurrent write.
#
# Task lifecycle: each detached delivery is tracked in _async_tasks so the bridge
# lifespan shutdown can cancel any in-flight delegation cleanly (mcp_client
# _owner_tasks pattern). A task removes itself on completion.

_async_tasks: set[asyncio.Task] = set()


def _resolve_output_mode(raw) -> str:
    """Normalise the tri-valued `output` arg to 'true' | 'false' | 'async'.
    Back-compat: the historical boolean maps to 'true'/'false'. The string
    'async' (case-insensitive) selects detached delegation. Anything else falls
    back to truthiness → 'true'/'false' (never crashes on a weird value)."""
    if isinstance(raw, str) and raw.strip().lower() == "async":
        return "async"
    if isinstance(raw, bool):
        return "true" if raw else "false"
    # Non-bool, non-"async": treat by truthiness (e.g. "true"/"false" strings,
    # 1/0). "false" the STRING is truthy in Python, so match it explicitly.
    if isinstance(raw, str) and raw.strip().lower() in ("false", "0", "no"):
        return "false"
    return "true" if raw else "false"


def _new_call_id() -> str:
    """Short, unique-enough correlation id for an async delegation (surfaces in
    the sender's '[async sent, id=…]' stub AND the wake envelope's identifier, so
    the sender can map a drifting-back reply to the question it answers)."""
    return uuid.uuid4().hex[:8]


def _spawn_async_delivery(
    *,
    carrier: str,
    body: dict,
    target: str,
    sender_identity_key: str,
    sender_caller: str,
    call_id: str,
) -> None:
    """Fire the recipient turn in a detached background task; on completion (or
    failure), wake the sender with the reply. Tracked in _async_tasks so shutdown
    cancels cleanly. Fire-and-forget from the caller's perspective — this returns
    immediately, the work happens off the caller's turn."""
    original_call = time.strftime("%H:%M")  # human-friendly stamp for the envelope

    async def _run():
        from app import pipeline_executor
        try:
            resp = await pipeline_executor.execute(carrier, body, {})
            reply = _extract_reply_text(resp)
            if reply is None:
                reply = f"[{target} received the delegated task but sent no reply]"
            error = False
        except Exception as e:
            # Sonnet #2: a detached failure must still WAKE the sender with an
            # error-shaped reply — NEVER eternal silence. asyncio would otherwise
            # swallow this exception and the sender waits forever.
            logger.exception(
                f"bridge_messaging: async delivery to '{target}' "
                f"(id={call_id}) raised — waking sender with an error reply"
            )
            reply = f"[your delegated task to {target} failed: {type(e).__name__}: {e}]"
            error = True
        try:
            await _wake_sender(
                sender_identity_key=sender_identity_key,
                reply=reply,
                from_agent=target,
                call_id=call_id,
                original_call=original_call,
                error=error,
            )
        except Exception:
            logger.exception(
                f"bridge_messaging: async wake-back to sender "
                f"'{sender_identity_key}' (id={call_id}) failed"
            )

    task = asyncio.ensure_future(_run())
    _async_tasks.add(task)
    task.add_done_callback(_async_tasks.discard)


async def _wake_sender(
    *,
    sender_identity_key: str,
    reply: str,
    from_agent: str,
    call_id: str,
    original_call: str,
    error: bool,
) -> None:
    """Wake the ORIGINAL SENDER with the deferred reply as a role:user turn.

    Runs execute() on the SENDER's own identity (Sonnet #4: identity key, not
    role/display — waking the wrong key wakes the wrong session-instance). The
    reply rides as a normal user turn; the async-deferred envelope goes in the
    signed <caller> via the reserved-key → sign primitive:
      * _bridge_caller  = the responder's name (`from_agent`) — WHO the reply is from
      * _bridge_trust   = "async-deferred" — the DISTINCT marker so the loop-break
                          does NOT withhold the sender's own send tools on this turn
      * _cron_additional = identifier/from/original_call/session correlation attrs
    The RESPONDER's storage/recall policy is honoured on this wake turn: an async
    reply is a live user turn on the sender's session, so without these it would
    be recalled-against AND stored like any human turn. `from_agent` IS the
    responder's role key (the original send `target`, phonebook-validated), so we
    resolve its bridge_messaging config and stamp `_bridge_storage`/`_bridge_recall`
    — same signed-envelope rail as a direct send. (`_wake_sender` previously
    stamped NEITHER; a responder with `recall:false`/`storage:false` now applies
    on the async path too.)

    basic_session persists it through the normal (turn-queue-serialised) path — the
    wake just queues behind any in-flight sender turn."""
    from app import pipeline_executor
    from app import config as config_mod

    extra_additional = {
        "identifier": call_id,
        "from": from_agent,
        "original_call": original_call,
        "session": "none",  # the reply-envelope carries its own provenance;
                            # the responder may be a session-less one-shot sub-agent
    }
    if error:
        extra_additional["delivery"] = "failed"

    body = {
        "messages": [{"role": "user", "content": reply}],
        "stream": False,
        "_bridge_caller": from_agent,
        "_bridge_trust": _ASYNC_DEFERRED_TRUST,
        "_cron_additional": extra_additional,
    }
    # Honour the responder's storage/recall policy on the wake turn (both default
    # true → stamp only when the responder set them false). from_agent = the
    # responder's role key.
    responder_cfg = _role_bridge_messaging_cfg(config_mod.resolve_role(from_agent) or {})
    if not bool(responder_cfg.get("storage", True)):
        body["_bridge_storage"] = "false"
    if not bool(responder_cfg.get("recall", True)):
        body["_bridge_recall"] = "false"
    logger.info(
        f"bridge_messaging: async wake — '{from_agent}' → sender identity "
        f"'{sender_identity_key}' (id={call_id}, error={error})"
    )
    await pipeline_executor.execute(sender_identity_key, body, {})


async def shutdown() -> None:
    """Cancel any in-flight async deliveries on bridge shutdown so detached
    tasks don't leak. Best-effort (mirrors mcp_client.shutdown)."""
    for task in list(_async_tasks):
        if not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass


def _sender_policy_cfg(ctx: "PipelineCtx") -> dict:
    """Resolve the SENDER role's bridge_messaging block (context-first), so the
    send path reads operator policy (storage / output / caller) from the single
    declared block regardless of which capability slot fired this handler.

    handle_tool_calls receives config merged from the top-level `plugins` family
    only; a single-block config lives in `context.plugins`, so the slot `config`
    is empty of policy keys. We go to the role's raw config directly (same source
    the phonebook uses) — sovereign, never from tool args. Returns {} if the role
    can't be resolved (falls back to slot config + defaults at the call site)."""
    from app import config as config_mod

    role_key = getattr(ctx.role, "key", None)
    if not role_key:
        return {}
    role_cfg = config_mod.resolve_role(role_key) or {}
    return _role_bridge_messaging_cfg(role_cfg)


def _resolve_sovereign_caller(ctx: "PipelineCtx", config: dict) -> str:
    """Caller-resolution ladder — resolves to the SENDING AGENT, not the human.

    The sender of an agent-to-agent message is the agent (the role), NOT the human
    driving the harness. But ``ctx.identity.name`` is the human-facing caller name
    (e.g. the operator's display name on a chat identity) — correct for normal chat,
    WRONG as the bridge_messaging sender (a recipient must see the sending AGENT
    knocking, not the operator). So the ladder prefers the role's name over the carrier
    identity's human name:

      1. config `caller` — an explicit pretty name set on the sender role's
         bridge_messaging block. Operator-authored = trusted.
      2. the sending ROLE key — the agent's own name; honest fallback when no pretty
         name is configured.
      3. the identity's human name (context.name) — last resort only (e.g. a bare
         identity with no role); this is the OLD behaviour, kept as a floor.

    Sovereign either way: every rung is bridge/config-sourced, never from tool args."""
    return str(
        config.get("caller")        # 1. explicit pretty name on the sender role
        or ctx.role.key             # 2. the agent's own role name
        or ctx.identity.name        # 3. human-facing name, last resort
        or "unknown"
    )


def _extract_reply_text(resp) -> str | None:
    """Pull the assistant text from execute()'s response dict. execute() returns
    {role, content, _full_response} for non-stream; we want `content`. A
    StreamingResponse (no .get) or empty content → None.

    Robustness: a delivery turn withholds only the SEND/LIST tools (see modify_context);
    the recipient's own tools resolve through the intercept loop, so `content` is their
    assembled prose reply. But some models (minimax-m3) STILL emit a tool-call-shaped
    blob trailing their prose — either the `]<]minimax[>[<tool_call>…` wire markup or a
    bare `[{"name": …, "arguments": …}]` JSON array (a stray, unresolved echo). That blob
    is INERT text the recipient appended after actually replying in words. We return the
    PROSE and strip the trailing inert tool-call junk so the caller gets the real reply,
    not a muddy words+JSON mix (which read as "no reply")."""
    if not isinstance(resp, dict):
        return None
    # execute() hands back one of two dict shapes depending on path:
    #   a) wrapped: {"role", "content", "_full_response": {...}}
    #   b) bare upstream OpenAI response: {"id", "choices": [{"message": {"content"}}], ...}
    # Try the flat content, then _full_response, then the bare choices[].message.content.
    content = resp.get("content")
    if not (isinstance(content, str) and content.strip()):
        for choices in (
            ((resp.get("_full_response") or {}).get("choices")),  # (a)
            resp.get("choices"),                                   # (b) bare upstream
        ):
            if isinstance(choices, list) and choices:
                c = ((choices[0] or {}).get("message") or {}).get("content")
                if isinstance(c, str) and c.strip():
                    content = c
                    break
    if not (isinstance(content, str) and content.strip()):
        return None
    cleaned = _strip_trailing_tool_junk(content)
    return cleaned or None


def _strip_trailing_tool_junk(text: str) -> str:
    """Strip a trailing inert tool-call blob a recipient appended after its prose.
    Handles two minimax shapes: the `]<]minimax[>[<tool_call>…` wire markup, and a
    bare `[{"name": …, "arguments": …}]` JSON array. Cuts from the first such marker
    to the end, then trims. Leaves clean prose untouched."""
    cut = len(text)
    # 1. minimax wire-markup sentinel (the `]<]minimax[>[` wrapper, or a stray <tool_call>).
    for marker in ("]<]minimax[>[", "<tool_call>", "<mm:think>", "</mm:think>"):
        i = text.find(marker)
        if i != -1:
            cut = min(cut, i)
    # 2. a bare JSON tool-call array `[{"name": … "arguments": …}]` appended after prose.
    #    Match a '[' that begins an object-array carrying a "name" key (the OpenAI
    #    tool-call shape minimax echoes). Conservative: needs both '{' and '"name"'.
    m = re.search(r'\[\s*\{\s*"name"\s*:', text)
    if m:
        cut = min(cut, m.start())
    return text[:cut].rstrip()
