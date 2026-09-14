"""FastAPI dependencies: container access, DB session, authentication, rate limiting.

Dependency order on a protected route is exactly the order in the architecture
diagram: authenticate -> resolve tenant -> rate limit -> handler.
"""
from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.container import Container
from app.core.context import tenant_id_var
from app.core.errors import ForbiddenError, RateLimitedError, UnauthorizedError
from app.core.metrics import cache_events, tenant_requests
from app.core.security import hash_api_key
from app.models import Tenant
from app.repositories.document_repository import DocumentRepository, TenantRepository
from app.services.cache_keys import tenant_key
from app.services.document_service import DocumentService
from app.services.search_service import SearchService


def get_container(request: Request) -> Container:
    return request.app.state.container


ContainerDep = Annotated[Container, Depends(get_container)]


async def get_session(container: ContainerDep) -> AsyncIterator[AsyncSession]:
    async with container.database.session() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]


async def get_current_tenant(
    request: Request,
    container: ContainerDep,
    session: SessionDep,
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> Tenant:
    """Resolve the *authenticated* tenant.

    The tenant is derived from the credential server-side. A client-supplied
    `tenant` parameter is never trusted as identity - it is only cross-checked
    (see `enforce_tenant_scope`).

    Hot path: the tenant record is cached in Redis for `tenant_cache_ttl_seconds`
    so authentication does not add a Postgres round trip to every request.
    """
    if not x_api_key:
        raise UnauthorizedError("Missing X-API-Key header.")

    key_hash = hash_api_key(x_api_key)
    cache_key = tenant_key(key_hash)

    cached = await container.cache.get_json(cache_key)
    if cached is not None:
        cache_events.labels(cache="tenant", outcome="hit").inc()
        tenant = Tenant(**json.loads(cached) if isinstance(cached, str) else cached)
    else:
        cache_events.labels(cache="tenant", outcome="miss").inc()
        tenant = await TenantRepository(session).get_by_api_key_hash(key_hash)
        if tenant is None:
            # Same response for unknown key and disabled tenant: no enumeration oracle.
            raise UnauthorizedError()
        await container.cache.set_json(
            cache_key,
            {"id": tenant.id, "name": tenant.name, "api_key_hash": tenant.api_key_hash,
             "status": tenant.status, "rate_limit_per_minute": tenant.rate_limit_per_minute},
            container.settings.tenant_cache_ttl_seconds,
        )

    tenant_id_var.set(tenant.id)
    request.state.tenant_id = tenant.id
    return tenant


TenantDep = Annotated[Tenant, Depends(get_current_tenant)]


def enforce_tenant_scope(requested_tenant: str | None, tenant: Tenant) -> str:
    """The API contract exposes `?tenant={tenantId}`. It is accepted, but only as
    an assertion: if it disagrees with the authenticated principal the request is
    rejected rather than silently rescoped."""
    if requested_tenant and requested_tenant != tenant.id:
        raise ForbiddenError("The requested tenant does not match the authenticated tenant.")
    return tenant.id


def rate_limited(bucket: str):
    """Per-tenant, per-bucket limiter. Separate buckets keep a burst of writes
    from consuming a tenant's search budget."""

    async def dependency(request: Request, container: ContainerDep, tenant: TenantDep) -> None:
        decision = await container.rate_limiter.check(tenant.id, bucket, tenant.rate_limit_per_minute)
        request.state.rate_limit = decision
        tenant_requests.labels(tenant=tenant.id, operation=bucket).inc()
        if not decision.allowed:
            raise RateLimitedError(decision.retry_after_seconds, tenant.rate_limit_per_minute)

    return dependency


def get_search_service(container: ContainerDep) -> SearchService:
    return SearchService(container.engine, container.cache, container.settings)


SearchServiceDep = Annotated[SearchService, Depends(get_search_service)]


def get_document_service(container: ContainerDep, session: SessionDep,
                         search_service: SearchServiceDep) -> DocumentService:
    return DocumentService(
        repository=DocumentRepository(session),
        session=session,
        publisher=container.publisher,
        cache=container.cache,
        search_service=search_service,
        settings=container.settings,
    )


DocumentServiceDep = Annotated[DocumentService, Depends(get_document_service)]
