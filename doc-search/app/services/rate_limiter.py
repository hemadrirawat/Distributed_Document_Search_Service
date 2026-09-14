"""Per-tenant rate limiting.

Algorithm: token bucket, evaluated atomically inside a Redis Lua script.
  * Token bucket (not fixed window) so a tenant gets a usable burst allowance
    without being able to double its quota across a window boundary.
  * Lua = read-modify-write in one round trip, so the limit holds across every
    API replica rather than per-process. Redis is the shared state that makes
    the API servers stateless.
  * The clock comes from redis TIME, not the caller, so replica clock skew
    cannot be used to mint extra tokens.

Keys are `rl:{tenant_id}:{bucket}` - the tenant is part of the key, so limits are
per-tenant and one noisy tenant cannot consume another's budget.

Degradation: if Redis is unavailable the limiter falls back to a process-local
bucket. Protection becomes approximate (N replicas => up to N x limit) but the
service stays available and abuse is still bounded. Failing closed on a cache
outage would convert a Redis blip into a full outage.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from app.core.metrics import rate_limit_events
from app.services.cache_keys import rate_limit_key

logger = logging.getLogger(__name__)

TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local t = redis.call('TIME')
local now = tonumber(t[1]) + (tonumber(t[2]) / 1000000)

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end

tokens = math.min(capacity, tokens + math.max(0, now - ts) * refill_rate)

local allowed = 0
local retry_after = 0
if tokens >= cost then
  allowed = 1
  tokens = tokens - cost
else
  retry_after = (cost - tokens) / refill_rate
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', key, ttl)
return {allowed, tostring(tokens), tostring(retry_after)}
"""


@dataclass(slots=True)
class RateLimitDecision:
    allowed: bool
    limit_per_minute: int
    remaining: int
    retry_after_seconds: float
    degraded: bool = False


class _LocalBucket:
    __slots__ = ("tokens", "ts")

    def __init__(self, tokens: float, ts: float) -> None:
        self.tokens = tokens
        self.ts = ts


class RateLimiter:
    def __init__(self, cache_client, burst_ratio: float = 0.25) -> None:
        self._cache = cache_client
        self._burst_ratio = burst_ratio
        self._script = None
        self._local: dict[str, _LocalBucket] = {}

    def _capacity(self, limit_per_minute: int) -> float:
        # Burst allowance, floored so very small quotas remain usable.
        return max(5.0, limit_per_minute * self._burst_ratio)

    async def check(self, tenant_id: str, bucket: str, limit_per_minute: int) -> RateLimitDecision:
        capacity = self._capacity(limit_per_minute)
        refill_rate = limit_per_minute / 60.0
        key = rate_limit_key(tenant_id, bucket)
        backend = getattr(self._cache, "backend", None)
        redis_client = getattr(backend, "client", None)

        if redis_client is not None:
            try:
                if self._script is None:
                    self._script = redis_client.register_script(TOKEN_BUCKET_LUA)
                allowed, tokens, retry_after = await self._script(
                    keys=[key], args=[capacity, refill_rate, 1, 120]
                )
                decision = RateLimitDecision(
                    allowed=bool(int(allowed)),
                    limit_per_minute=limit_per_minute,
                    remaining=int(float(tokens)),
                    retry_after_seconds=float(retry_after),
                )
                rate_limit_events.labels(outcome="allowed" if decision.allowed else "throttled").inc()
                return decision
            except Exception as exc:
                logger.warning("rate limiter degraded to local bucket", extra={"error": str(exc)})

        decision = self._check_local(key, capacity, refill_rate)
        decision.limit_per_minute = limit_per_minute
        rate_limit_events.labels(outcome="allowed_degraded" if decision.allowed else "throttled_degraded").inc()
        return decision

    def _check_local(self, key: str, capacity: float, refill_rate: float) -> RateLimitDecision:
        now = time.monotonic()
        bucket = self._local.get(key) or _LocalBucket(capacity, now)
        bucket.tokens = min(capacity, bucket.tokens + max(0.0, now - bucket.ts) * refill_rate)
        bucket.ts = now
        if bucket.tokens >= 1:
            bucket.tokens -= 1
            allowed, retry_after = True, 0.0
        else:
            allowed, retry_after = False, (1 - bucket.tokens) / refill_rate
        self._local[key] = bucket
        return RateLimitDecision(allowed, 0, int(bucket.tokens), retry_after, degraded=True)
