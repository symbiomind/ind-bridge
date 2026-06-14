"""
OpenAI-Protocol — listener + outbound transport plugin for ind-bridge V4.

Speaks the OpenAI chat-completions protocol on both sides:

  * On ``identity.plugins`` (listener capability): registers a FastAPI POST
    route at ``<prefix>/chat/completions``, extracts the bearer token,
    resolves the identity, runs the pipeline executor, returns the
    OpenAI-shaped response.

  * On ``*.plugins`` (outbound_params capability): configures the outbound
    HTTP call by setting ``ctx.resource.endpoint_url``,
    ``ctx.resource.endpoint_token``, and stashing custom headers in
    ``ctx.plugin_data["openai-protocol.headers"]``. The executor's
    ``_do_http_call`` reads these and fires the actual httpx POST.

You don't have to use both capabilities. A pipeline that listens via MCP
and forwards via OpenAI uses only this plugin's outbound_params side; a
pipeline that listens via OpenAI and terminates at Eliza uses only the
listener side. Same plugin, two capabilities, declared once.

Config knobs:

    On identity.plugins:
        prefix: /v1            # URL prefix for the chat/completions route

    On *.plugins (any cascade level for outbound_params):
        url:    https://...    # base endpoint or full /chat/completions
        token:  sk-...         # bearer token
        model:  some/model     # overrides the inbound model (per-identity)
        headers: {x-org: ...}  # custom headers merged on the outbound call
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse

from app import auth, pipeline_executor

if TYPE_CHECKING:
    from app.context import PipelineCtx, StartupCtx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Capability declaration (the contract — read by capabilities.py validator)
# ---------------------------------------------------------------------------

CAPABILITIES = {
    "listener":         ["identity.plugins"],
    "outbound_params":  ["identity.plugins", "role.plugins",
                         "session.plugins", "resource.plugins"],
}


# ---------------------------------------------------------------------------
# Route-ownership registry — closes the shared-route leak
# ---------------------------------------------------------------------------
#
# Multiple identities legitimately share ONE FastAPI route (same `/v1` prefix);
# FastAPI registers the path once and the handler disambiguates by bearer token.
# But "valid token" must not imply "reachable on this route" — an identity is
# reachable IFF it DECLARED the listener plugin that owns the route (D-003: a
# trigger is just a listener plugin, no special-casing). This registry records,
# per route_path, every identity that declared the listener. The handler rejects
# a request whose resolved identity isn't an owner — so an identity can't ride a
# route registered by someone else without declaring the listener itself.
_ROUTE_OWNERS: dict[str, set[str]] = {}   # route_path → {identity_key, ...}


# ---------------------------------------------------------------------------
# listener capability — register a FastAPI route per identity
# ---------------------------------------------------------------------------

def setup_listener(ctx: "StartupCtx", config: dict) -> None:
    """Register a POST ``<prefix>/chat/completions`` route for this
    identity. Each identity that declares ``OpenAI-Protocol`` on its
    ``identity.plugins`` slot gets its own route (in practice, all
    identities share the same path string but route to the *same handler*
    that disambiguates by bearer token — multiple registrations of the
    same path with FastAPI is a no-op after the first, so we register
    once per process and rely on the handler to do per-token resolution).

    To make this work cleanly, we register the route on the FIRST call
    and skip on subsequent calls — but we do log the per-identity
    activation so operators see which identities are reachable via this
    listener.
    """
    prefix = config.get("prefix", "/v1") if isinstance(config, dict) else "/v1"
    route_path = f"{prefix.rstrip('/')}/chat/completions"

    # Record ownership FIRST — every declaring identity is an owner of this
    # route, whether or not it's the one that registers the FastAPI handler.
    # This must run on the skip path below too (the multi-owner shared-route
    # case), so it sits ahead of the early-return.
    _ROUTE_OWNERS.setdefault(route_path, set()).add(ctx.identity_key)

    app = ctx.app
    # Detect if we've already registered this route — avoid duplicate
    # FastAPI registration warnings on multi-identity setups.
    existing = {r.path for r in app.routes if hasattr(r, "path")}
    if route_path in existing:
        logger.debug(
            f"OpenAI-Protocol listener: route '{route_path}' already "
            f"registered; identity '{ctx.identity_key}' shares it "
            f"(token-based routing handles the multiplexing)."
        )
        return

    # response_model=None tells FastAPI not to derive a Pydantic response
    # model from the return-type annotation — the handler legitimately
    # returns either dict OR StreamingResponse, which isn't a Pydantic-
    # representable union.
    @app.post(route_path, response_model=None)
    async def chat_completions(request: Request) -> "dict | StreamingResponse":
        # Extract bearer token + resolve identity
        auth_header = request.headers.get("authorization", "")
        token = auth.extract_bearer_token(auth_header)
        if not token:
            raise HTTPException(status_code=401, detail="Missing or malformed bearer token")
        identity_key = auth.resolve_identity_from_token(token)
        if identity_key is None:
            raise HTTPException(status_code=401, detail="Unknown bearer token")

        # Route-ownership enforcement: the resolved identity must have DECLARED
        # the listener plugin that owns THIS route. A valid token alone is not
        # enough — riding another identity's shared route (token valid, but no
        # listener of its own) is the leak we close here. 403, not 401: the
        # token is valid and the identity is known, but it's not permitted on
        # this route (authorization, not authentication).
        owners = _ROUTE_OWNERS.get(route_path, set())
        if identity_key not in owners:
            logger.warning(
                f"Rejected request: identity '{identity_key}' resolved from a "
                f"valid token but is not a listener on route '{route_path}' "
                f"(declares no OpenAI-Protocol listener). Declare the listener "
                f"on this identity or remove its token."
            )
            raise HTTPException(
                status_code=403,
                detail="Identity is not a listener on this route",
            )

        # Parse body
        try:
            body = await request.json()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON body: {e}")
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="Body must be a JSON object")

        # Headers (lowercased; framework-internal items filtered)
        headers = {
            k.lower(): v for k, v in request.headers.items()
            if k.lower() not in {"authorization", "content-length"}
        }

        # Run the pipeline
        try:
            response = await pipeline_executor.execute(identity_key, body, headers)
        except pipeline_executor.PipelineNotReady as e:
            logger.warning(
                f"Pipeline not ready for identity '{identity_key}': {e}"
            )
            raise HTTPException(status_code=503, detail=str(e))
        except pipeline_executor.PipelineFailure as e:
            logger.error(
                f"Pipeline failure for identity '{identity_key}': {e}",
                exc_info=True,
            )
            raise HTTPException(status_code=502, detail=str(e))
        except Exception as e:
            logger.error(
                f"Unexpected error in pipeline for identity '{identity_key}': "
                f"{type(e).__name__}: {e!r}",
                exc_info=True,
            )
            raise HTTPException(status_code=500, detail="Internal pipeline error")

        return response

    logger.info(
        f"OpenAI-Protocol listener registered route POST {route_path} "
        f"(first identity binding it: '{ctx.identity_key}'; subsequent "
        f"identities share the route via token-based routing)."
    )


# ---------------------------------------------------------------------------
# outbound_params capability — configure the outbound HTTP call
# ---------------------------------------------------------------------------

def apply_outbound_params(ctx: "PipelineCtx", config: dict) -> "PipelineCtx":
    """Configure the outbound call from the cascade-merged plugin config.

    Sets:
      - ``ctx.resource.endpoint_url``  ← config["url"]
      - ``ctx.resource.endpoint_token`` ← config["token"]
      - Custom headers stashed in ``ctx.plugin_data["openai-protocol.headers"]``
      - Optionally overrides ``ctx.request.model`` if config["model"] is set

    The actual HTTP call is fired by the executor's ``_do_http_call``,
    which reads these fields. This separation means transport plugins
    only configure; the executor calls.
    """
    if not isinstance(config, dict):
        return ctx

    url = config.get("url")
    if url:
        ctx.resource.endpoint_url = url

    token = config.get("token")
    if token:
        ctx.resource.endpoint_token = token

    headers = config.get("headers")
    if isinstance(headers, dict) and headers:
        existing = ctx.plugin_data.get("openai-protocol.headers", {})
        if not isinstance(existing, dict):
            existing = {}
        existing.update(headers)
        ctx.plugin_data["openai-protocol.headers"] = existing

    model = config.get("model")
    if model:
        ctx.request.model = model

    return ctx
