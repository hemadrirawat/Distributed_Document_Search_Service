"""ASGI application factory.

`main.py` wires and starts; it contains no business logic. API servers are
stateless - every piece of mutable state lives in Postgres, Redis, OpenSearch or
RabbitMQ - which is what makes horizontal scaling a replica-count change.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.exception_handlers import register_exception_handlers
from app.api.routes import documents, health, search
from app.container import Container, build_container
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.middleware.request_context import RequestContextMiddleware

DESCRIPTION = """
Multi-tenant distributed document search service.

**Authentication** - send `X-API-Key: <key>`. Tenant identity is derived from the
key server-side; the `tenant` query parameter is validated against it, never trusted as identity.
"""


def create_app(container: Container | None = None, settings: Settings | None = None) -> FastAPI:
    settings = settings or (container.settings if container else get_settings())
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        active: Container = app.state.container
        await active.startup()
        try:
            yield
        finally:
            await active.shutdown()

    app = FastAPI(
        title="Distributed Document Search Service",
        description=DESCRIPTION,
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        openapi_url="/openapi.json",
    )
    app.state.container = container or build_container(settings)
    app.add_middleware(RequestContextMiddleware)
    register_exception_handlers(app)
    app.include_router(health.router)
    app.include_router(documents.router)
    app.include_router(search.router)
    return app


# Served with `uvicorn app.main:create_app --factory`. Using a factory rather than a
# module-level instance keeps import side-effect free, which is what lets the test
# suite build the app with in-process adapters.
