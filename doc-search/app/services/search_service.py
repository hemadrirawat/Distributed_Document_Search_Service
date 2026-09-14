"""Search orchestration: cache lookup -> engine query -> cache fill.

Tenant isolation is enforced here *and* in the engine adapter: the tenant id is
taken from the authenticated principal and passed as a non-negotiable filter.
Nothing in the request body or query string can widen the scope.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any

from app.clients.cache import CacheClient
from app.clients.search_engine import SearchEngine, SearchQuery
from app.core.config import Settings
from app.core.errors import DependencyUnavailableError
from app.core.metrics import cache_events, search_duration
from app.schemas.search import FacetValue, SearchHit, SearchResponse
from app.services.cache_keys import search_generation_key, search_key

logger = logging.getLogger(__name__)


class SearchService:
    def __init__(self, engine: SearchEngine, cache: CacheClient, settings: Settings) -> None:
        self._engine = engine
        self._cache = cache
        self._settings = settings

    # --- cache generation -------------------------------------------------
    async def current_generation(self, tenant_id: str) -> int:
        value = await self._cache.get_json(search_generation_key(tenant_id))
        try:
            return int(value) if value is not None else 0
        except (TypeError, ValueError):
            return 0

    async def invalidate_tenant(self, tenant_id: str) -> None:
        """O(1) invalidation of every cached search result for one tenant."""
        result = await self._cache.incr(search_generation_key(tenant_id))
        if result is None:
            # Redis unavailable: nothing to invalidate because nothing is cached.
            logger.warning("search cache invalidation skipped (cache unavailable)")

    def _ttl(self) -> int:
        """Jittered TTL so entries created by one traffic spike do not all expire
        in the same second and stampede the cluster together."""
        base = self._settings.search_cache_ttl_seconds
        jitter = base * self._settings.cache_ttl_jitter_ratio
        return max(1, int(base + random.uniform(-jitter, jitter)))

    # --- query ------------------------------------------------------------
    async def search(self, tenant_id: str, query: SearchQuery) -> SearchResponse:
        started = time.perf_counter()
        generation = await self.current_generation(tenant_id)
        key = search_key(
            tenant_id, generation,
            query=query.text, page=query.page, size=query.size,
            fuzzy=query.fuzzy, highlight=query.highlight, facets=query.facets,
        )

        cached = await self._cache.get_json(key)
        if cached is not None:
            cache_events.labels(cache="search", outcome="hit").inc()
            search_duration.labels(source="cache").observe(time.perf_counter() - started)
            payload = dict(cached)
            payload["cached"] = True
            payload["took_ms"] = int((time.perf_counter() - started) * 1000)
            return SearchResponse.model_validate(payload)

        cache_events.labels(cache="search", outcome="miss").inc()
        response = await self._single_flight(key, tenant_id, query)
        search_duration.labels(source="engine").observe(time.perf_counter() - started)
        return response

    # Single-flight: concurrent identical misses in this process collapse into one
    # engine query. Bounds stampede amplification to the number of API replicas
    # rather than the number of concurrent requests.
    _inflight: dict[str, asyncio.Future] = {}

    async def _single_flight(self, key: str, tenant_id: str, query: SearchQuery) -> SearchResponse:
        existing = SearchService._inflight.get(key)
        if existing is not None:
            cache_events.labels(cache="search", outcome="coalesced").inc()
            return await asyncio.shield(existing)

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        SearchService._inflight[key] = future
        try:
            response = await self._execute(tenant_id, query)
            await self._cache.set_json(key, self._cacheable(response), self._ttl())
            if not future.done():
                future.set_result(response)
            return response
        except Exception as exc:
            if not future.done():
                future.set_exception(exc)
            raise
        finally:
            SearchService._inflight.pop(key, None)

    @staticmethod
    def _cacheable(response: SearchResponse) -> dict[str, Any]:
        payload = response.model_dump(mode="json")
        payload["cached"] = False
        return payload

    async def _execute(self, tenant_id: str, query: SearchQuery) -> SearchResponse:
        try:
            result = await self._engine.search(query)
        except DependencyUnavailableError:
            raise  # already an explicit, retryable signal (circuit open)
        except Exception as exc:
            # A search-cluster failure is a dependency problem, not a server bug:
            # surface 503 + Retry-After semantics instead of a generic 500.
            logger.error("search engine query failed", extra={"error": str(exc)})
            raise DependencyUnavailableError("search") from exc
        return SearchResponse(
            query=query.text,
            tenant_id=tenant_id,
            page=query.page,
            size=query.size,
            total=result.total,
            total_is_lower_bound=result.total_is_lower_bound,
            took_ms=result.took_ms,
            cached=False,
            hits=[
                SearchHit(
                    id=hit.document_id,
                    title=hit.title,
                    score=hit.score,
                    snippet=hit.snippet,
                    content_type=hit.content_type,
                    tags=hit.tags,
                    metadata=hit.metadata,
                    created_at=hit.created_at,
                    updated_at=hit.updated_at,
                )
                for hit in result.hits
            ],
            facets={
                name: [FacetValue(value=value, count=count) for value, count in buckets]
                for name, buckets in result.facets.items()
            },
        )
