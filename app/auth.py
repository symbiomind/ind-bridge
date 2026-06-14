"""
Auth utilities.

Core auth is a thin utility — token → identity_key lookup only.
The actual client-facing authentication (extracting the bearer token from an
HTTP request, returning 401, etc.) is the responsibility of the server plugin
that registered the route (e.g. OpenAI-Provider at identities.*.plugins).

This module is referenced by pipeline.py for internal token resolution.
"""

import logging

from .config import get_identity_key_for_token

logger = logging.getLogger(__name__)


def resolve_identity_from_token(token: str) -> str | None:
    """
    Look up a bearer token in the config token map.

    Returns the identity_key string if found, or None if the token is unknown.
    Logs a warning on miss (useful for catching misconfigured clients).
    """
    identity_key = get_identity_key_for_token(token)
    if identity_key is None:
        logger.warning("Rejected request: unrecognised bearer token.")
    return identity_key


def extract_bearer_token(auth_header: str) -> str | None:
    """
    Extract the token from a 'Bearer <token>' Authorization header value.

    Returns the token string, or None if the header is missing or malformed.
    Server plugins call this when handling their own routes.
    """
    if not auth_header:
        return None
    lower = auth_header.lower()
    if not lower.startswith("bearer "):
        return None
    token = auth_header[len("bearer "):].strip()
    return token if token else None
