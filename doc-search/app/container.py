"""Composition root.

All adapters are constructed here and injected via FastAPI dependencies. Swapping
Redis for an in-memory cache, or OpenSearch for the in-memory engine, is a
constructor change - no call-site in the API or service layer knows which
implementation it is talking to. This is what makes the test-suite fast and the
production wiring explicit.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from app.clients.cache import CacheClient, InMemoryCache, RedisCache
from app.clients.inmemory_engine import InMemorySearchEngine
from app.clients.queue import EventPublisher, InMemoryPublisher, RabbitMQPublisher
from app.clients.search_engine import SearchEngine
from app.core.config import Settings
from app.db.database import Database
from app.services.health_service import HealthService, Probe
from app.services.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


@dataclass
class Container:
    settings: Settings
    database: Database
    cache: CacheClient
    engine: SearchEngine
    publisher: EventPublisher
    rate_limiter: RateLimiter
    health: HealthService

    async def startup(self) -> None:
        # Postgres is fail-fast: without the schema the service cannot do anything
        # correct, and a crash here is a clear signal of a misconfigured deploy.
        await self.database.create_schema()
        await self.database.seed_tenants()

        # OpenSearch and RabbitMQ are start-tolerant. A dependency that is briefly
        # unavailable during a rollout should not crash-loop the pod; /health
        # reports the real state and the adapters reconnect lazily.
        try:
            await self.engine.ensure_index()
        except Exception as exc:
            logger.error("search index bootstrap failed; starting degraded", extra={"error": str(exc)})

        connect = getattr(self.publisher, "connect", None)
        if connect is not None:
            try:
                await connect()
            except Exception as exc:
                logger.error("queue connection failed; starting degraded", extra={"error": str(exc)})

    async def shutdown(self) -> None:
        await self.engine.close()
        await self.cache.close()
        await self.publisher.close()
        await self.database.close()


def _build_health(settings: Settings, database: Database, cache: CacheClient,
                  engine: SearchEngine, publisher: EventPublisher) -> HealthService:
    return HealthService(
        probes=[
            Probe("postgres", True, database.ping),
            Probe("opensearch", True, engine.ping),
            Probe("redis", False, cache.ping),      # degraded, not down: cache is fail-open
            Probe("rabbitmq", False, publisher.ping),  # degraded: writes still durable in PG
        ],
        service=settings.app_name,
        environment=settings.environment,
        timeout_seconds=settings.health_check_timeout_seconds,
    )


def build_container(settings: Settings) -> Container:
    """Production / docker-compose wiring."""
    from app.clients.opensearch_engine import OpenSearchEngine

    database = Database(settings)
    cache = CacheClient(RedisCache(settings.redis_url, settings.redis_timeout_seconds))
    engine = OpenSearchEngine(settings)
    publisher = RabbitMQPublisher(settings)
    return Container(
        settings=settings,
        database=database,
        cache=cache,
        engine=engine,
        publisher=publisher,
        rate_limiter=RateLimiter(cache, settings.rate_limit_burst_ratio),
        health=_build_health(settings, database, cache, engine, publisher),
    )


def build_test_container(settings: Settings) -> Container:
    """Test wiring: SQLite + in-memory cache/engine/queue, same code paths."""
    database = Database(settings)
    cache = CacheClient(InMemoryCache(), name="memory")
    engine = InMemorySearchEngine()
    publisher = InMemoryPublisher()
    return Container(
        settings=settings,
        database=database,
        cache=cache,
        engine=engine,
        publisher=publisher,
        rate_limiter=RateLimiter(cache, settings.rate_limit_burst_ratio),
        health=_build_health(settings, database, cache, engine, publisher),
    )
