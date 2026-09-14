"""Async PostgreSQL access: engine, session factory, bootstrap and health probe."""
from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.core.config import Settings
from app.core.security import hash_api_key
from app.models import Base, Tenant

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        is_sqlite = settings.database_url.startswith("sqlite")
        kwargs: dict = {"echo": False, "future": True}
        if is_sqlite:
            # Tests: a single shared in-memory connection.
            kwargs.update(poolclass=StaticPool, connect_args={"check_same_thread": False})
        else:
            # Bounded pool: Postgres connections are expensive and a hard scaling limit.
            # api_replicas * (pool_size + max_overflow) must stay under max_connections
            # (see docs/SUBMISSION.md -> Storage strategy; PgBouncer in production).
            kwargs.update(
                pool_size=settings.db_pool_size,
                max_overflow=settings.db_max_overflow,
                pool_timeout=settings.db_pool_timeout_seconds,
                pool_pre_ping=True,
                pool_recycle=1800,
            )
        self.engine = create_async_engine(settings.database_url, **kwargs)
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self.session_factory() as session:
            yield session

    async def create_schema(self) -> None:
        """Prototype convenience. Production uses Alembic migrations (see README)."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def seed_tenants(self) -> None:
        """Seed demo tenants from configuration. Prototype-only substitute for an
        identity provider / tenant-provisioning service."""
        async with self.session_factory() as session:
            for tenant_id, api_key in self._settings.seeded_tenants:
                existing = await session.get(Tenant, tenant_id)
                key_hash = hash_api_key(api_key)
                if existing is None:
                    session.add(
                        Tenant(
                            id=tenant_id,
                            name=tenant_id.title(),
                            api_key_hash=key_hash,
                            rate_limit_per_minute=self._settings.default_rate_limit_per_minute,
                        )
                    )
                elif existing.api_key_hash != key_hash:
                    existing.api_key_hash = key_hash
            await session.commit()

    async def ping(self) -> bool:
        async with self.session_factory() as session:
            await session.execute(select(text("1")))
        return True

    async def close(self) -> None:
        await self.engine.dispose()
