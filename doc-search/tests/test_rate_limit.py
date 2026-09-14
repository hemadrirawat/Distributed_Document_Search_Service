"""Per-tenant rate limiting: enforcement, headers, bucket separation, refill."""
from __future__ import annotations

import asyncio

import pytest

from app.services.rate_limiter import RateLimiter

pytestmark = pytest.mark.asyncio


async def _set_limit(container, tenant_id: str, rpm: int) -> None:
    from app.models import Tenant

    async with container.database.session() as session:
        tenant = await session.get(Tenant, tenant_id)
        tenant.rate_limit_per_minute = rpm
        await session.commit()
    await container.cache.backend.close()  # drop the cached tenant record


async def test_exceeding_the_limit_returns_429_with_retry_after(client, auth_a, container):
    await _set_limit(container, "acme", 12)  # capacity = max(5, 3) = 5 tokens
    statuses = [(await client.get("/search?q=x", headers=auth_a)).status_code for _ in range(10)]
    assert 429 in statuses

    throttled = next(s for s in statuses if s == 429)
    assert throttled == 429
    response = await client.get("/search?q=x", headers=auth_a)
    if response.status_code == 429:
        body = response.json()["error"]
        assert body["code"] == "rate_limited"
        assert int(response.headers["Retry-After"]) >= 1
        assert body["details"]["limit_per_minute"] == 12


async def test_successful_responses_carry_rate_limit_headers(client, auth_a):
    response = await client.get("/search?q=x", headers=auth_a)
    assert response.status_code == 200
    assert int(response.headers["X-RateLimit-Limit"]) == 120
    assert int(response.headers["X-RateLimit-Remaining"]) >= 0


async def test_read_and_write_buckets_are_separate(client, auth_a, container):
    """Burst-writing must not consume the tenant's search budget."""
    await _set_limit(container, "acme", 12)
    for _ in range(10):
        await client.post("/documents", json={"title": "t", "content": "body"}, headers=auth_a)
    # The write bucket is drained; the search bucket is untouched.
    assert (await client.get("/search?q=body", headers=auth_a)).status_code == 200


async def test_token_bucket_allows_a_burst_then_refills():
    """Token-bucket maths, exercised directly against the local fallback path.

    rpm=12 -> capacity = max(5, 12*0.25) = 5 tokens, refill = 0.2 tokens/second.
    """
    limiter = RateLimiter(cache_client=_NoRedisBackend(), burst_ratio=0.25)
    decisions = [await limiter.check("acme", "search", 12) for _ in range(8)]
    assert sum(d.allowed for d in decisions) == 5
    assert decisions[-1].retry_after_seconds > 0
    assert decisions[-1].degraded is True  # no Redis -> local fallback, still bounded

    await asyncio.sleep(0.3)
    # Tokens accrue continuously rather than resetting on a window boundary.
    assert (await limiter.check("acme", "search", 1200)).allowed is True


async def test_limiter_is_isolated_per_tenant_and_per_bucket():
    limiter = RateLimiter(cache_client=_NoRedisBackend(), burst_ratio=0.25)
    for _ in range(6):
        await limiter.check("acme", "search", 12)
    assert (await limiter.check("acme", "search", 12)).allowed is False
    assert (await limiter.check("globex", "search", 12)).allowed is True
    assert (await limiter.check("acme", "write", 12)).allowed is True


class _NoRedisBackend:
    """Stands in for a cache client whose Redis connection is unavailable."""

    backend = None
