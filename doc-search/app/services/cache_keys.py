"""Cache key construction.

EVERY key is prefixed with `t:{tenant_id}:`. Tenant identity is part of the key
namespace, so a cache lookup for tenant A can never return tenant B's payload
even if the query text is byte-identical.

Search results are additionally namespaced by a per-tenant *generation* counter.
Invalidating a tenant's entire search cache is then a single INCR - no SCAN, no
key enumeration, no cross-tenant blast radius. Superseded keys are never read
again and fall out on their own TTL.
"""
from __future__ import annotations

import hashlib
import re

_WHITESPACE = re.compile(r"\s+")


def normalize_query(text: str) -> str:
    """`  Quarterly   REPORT ` and `quarterly report` share one cache entry."""
    return _WHITESPACE.sub(" ", text.strip().lower())


def search_generation_key(tenant_id: str) -> str:
    return f"t:{tenant_id}:search:gen"


def search_key(tenant_id: str, generation: int, *, query: str, page: int, size: int,
               fuzzy: bool, highlight: bool, facets: bool) -> str:
    canonical = f"{normalize_query(query)}|p={page}|s={size}|f={int(fuzzy)}|h={int(highlight)}|a={int(facets)}"
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
    return f"t:{tenant_id}:search:g{generation}:{digest}"


def document_key(tenant_id: str, document_id: str) -> str:
    return f"t:{tenant_id}:doc:{document_id}"


def tenant_key(api_key_hash: str) -> str:
    # Keyed by credential hash, never by the raw API key.
    return f"auth:key:{api_key_hash}"


def rate_limit_key(tenant_id: str, bucket: str) -> str:
    return f"rl:{tenant_id}:{bucket}"
