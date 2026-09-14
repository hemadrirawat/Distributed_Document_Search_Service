"""Cache port + Redis / in-memory adapters.

Availability contract: the cache is an optimisation, never a dependency. Every
operation is fail-open - if Redis is unreachable the call is logged, counted,
and treated as a miss so requests keep succeeding at higher latency.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Protocol, runtime_checkable

from app.core.metrics import cache_events, dependency_duration

logger = logging.getLogger(__name__)


@runtime_checkable
class CacheBackend(Protocol):
    async def get(self, key: str) -> str | None: ...
    async def set(self, key: str, value: str, ttl_seconds: int) -> None: ...
    async def delete(self, *keys: str) -> None: ...
    async def incr(self, key: str) -> int: ...
    async def ping(self) -> bool: ...
    async def close(self) -> None: ...


class RedisCache:
    def __init__(self, url: str, timeout_seconds: float = 0.25) -> None:
        from redis.asyncio import Redis

        self._timeout = timeout_seconds
        self._client = Redis.from_url(
            url,
            decode_responses=True,
            socket_timeout=timeout_seconds,
            socket_connect_timeout=timeout_seconds,
            health_check_interval=30,
        )

    @property
    def client(self):
        return self._client

    async def get(self, key: str) -> str | None:
        start = time.perf_counter()
        value = await self._client.get(key)
        dependency_duration.labels(dependency="redis", operation="get").observe(time.perf_counter() - start)
        return value

    async def set(self, key: str, value: str, ttl_seconds: int) -> None:
        await self._client.set(key, value, ex=ttl_seconds)

    async def delete(self, *keys: str) -> None:
        if keys:
            await self._client.delete(*keys)

    async def incr(self, key: str) -> int:
        return int(await self._client.incr(key))

    async def ping(self) -> bool:
        try:
            return bool(await self._client.ping())
        except Exception:
            return False

    async def close(self) -> None:
        await self._client.aclose()


class InMemoryCache:
    """Process-local cache used by tests. Not for multi-replica production use -
    each replica would hold an independent, divergent view."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[str, float]] = {}
        self.available = True
        self.set_calls = 0

    def _guard(self) -> None:
        if not self.available:
            raise ConnectionError("cache unavailable")

    async def get(self, key: str) -> str | None:
        self._guard()
        entry = self._store.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at < time.time():
            self._store.pop(key, None)
            return None
        return value

    async def set(self, key: str, value: str, ttl_seconds: int) -> None:
        self._guard()
        self.set_calls += 1
        self._store[key] = (value, time.time() + ttl_seconds)

    async def delete(self, *keys: str) -> None:
        self._guard()
        for key in keys:
            self._store.pop(key, None)

    async def incr(self, key: str) -> int:
        self._guard()
        current = int((await self.get(key)) or 0) + 1
        await self.set(key, str(current), 86400)
        return current

    async def ping(self) -> bool:
        return self.available

    async def close(self) -> None:
        self._store.clear()


class CacheClient:
    """Fail-open JSON wrapper around a CacheBackend."""

    def __init__(self, backend: CacheBackend, name: str = "redis") -> None:
        self._backend = backend
        self._name = name

    @property
    def backend(self) -> CacheBackend:
        return self._backend

    async def get_json(self, key: str) -> Any | None:
        try:
            raw = await self._backend.get(key)
        except Exception as exc:
            cache_events.labels(cache=self._name, outcome="error").inc()
            logger.warning("cache get failed; serving from origin", extra={"error": str(exc)})
            return None
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            await self.delete(key)
            return None

    async def set_json(self, key: str, value: Any, ttl_seconds: int) -> None:
        try:
            await self._backend.set(key, json.dumps(value, default=str), ttl_seconds)
        except Exception as exc:
            cache_events.labels(cache=self._name, outcome="error").inc()
            logger.warning("cache set failed", extra={"error": str(exc)})

    async def delete(self, *keys: str) -> None:
        try:
            await self._backend.delete(*keys)
        except Exception as exc:
            cache_events.labels(cache=self._name, outcome="error").inc()
            logger.warning("cache delete failed", extra={"error": str(exc)})

    async def incr(self, key: str) -> int | None:
        try:
            return await self._backend.incr(key)
        except Exception as exc:
            cache_events.labels(cache=self._name, outcome="error").inc()
            logger.warning("cache incr failed", extra={"error": str(exc)})
            return None

    async def ping(self) -> bool:
        try:
            return await asyncio.wait_for(self._backend.ping(), timeout=1.0)
        except Exception:
            return False

    async def close(self) -> None:
        await self._backend.close()
