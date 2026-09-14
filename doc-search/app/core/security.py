"""Credential handling.

PROTOTYPE: tenant identity is derived from a hashed API key looked up in
PostgreSQL. The raw key is never stored or logged.
PRODUCTION: identity comes from a validated OIDC/JWT access token; see
`decode_bearer_token_claims` and docs/SUBMISSION.md section "Security".
"""
from __future__ import annotations

import hashlib
import hmac
from typing import Any


def hash_api_key(api_key: str) -> str:
    """SHA-256 of the API key. Keys are high-entropy random strings, so a fast
    hash is appropriate here (unlike user passwords, which need Argon2/bcrypt)."""
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def constant_time_equals(a: str, b: str) -> bool:
    return hmac.compare_digest(a, b)


def decode_bearer_token_claims(token: str) -> dict[str, Any]:  # pragma: no cover - production path
    """Placeholder for the production auth path.

    In production this verifies the JWT signature against the IdP's JWKS
    (cached, rotated), validates `iss`, `aud`, `exp`, `nbf`, and returns claims.
    Tenant identity is then read from a trusted claim (e.g. `tenant_id` / `org_id`)
    and the client-supplied `tenant` query parameter is only ever used as an
    assertion to cross-check, never as the source of identity.
    """
    raise NotImplementedError("JWT validation is not implemented in the prototype.")
