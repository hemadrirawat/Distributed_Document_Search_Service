"""Test harness.

The suite runs against the *real* application - real routing, real middleware,
real services, real repositories, real SQL (SQLite) - with only the three
out-of-process dependencies swapped for in-process adapters that implement the
same ports. The in-memory search engine performs genuine scoring and filtering,
and the in-memory publisher holds events until explicitly drained, so the
asynchronous indexing boundary is exercised rather than papered over.
"""
from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.clients.queue import DocumentEvent
from app.container import build_test_container
from app.core.config import Settings
from app.main import create_app
from app.workers.processor import IndexingProcessor
from app.workers.reconciler import Reconciler

TENANT_A, KEY_A = "acme", "acme-test-key"
TENANT_B, KEY_B = "globex", "globex-test-key"


@pytest.fixture
def settings() -> Settings:
    return Settings(
        environment="test",
        log_level="WARNING",
        database_url="sqlite+aiosqlite:///:memory:",
        seed_tenants=f"{TENANT_A}:{KEY_A},{TENANT_B}:{KEY_B}",
        search_cache_ttl_seconds=30,
        document_cache_ttl_seconds=60,
        tenant_cache_ttl_seconds=60,
        default_rate_limit_per_minute=120,
        reconcile_stale_after_seconds=0,
    )


@pytest_asyncio.fixture
async def container(settings: Settings):
    container = build_test_container(settings)
    await container.startup()
    yield container
    await container.shutdown()


@pytest_asyncio.fixture
async def client(container) -> AsyncClient:
    app = create_app(container=container)
    # Lifespan is driven by the container fixture, so the transport skips it.
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest.fixture
def auth_a() -> dict[str, str]:
    return {"X-API-Key": KEY_A}


@pytest.fixture
def auth_b() -> dict[str, str]:
    return {"X-API-Key": KEY_B}


@pytest.fixture
def processor(container) -> IndexingProcessor:
    return IndexingProcessor(container.database, container.engine, container.cache)


@pytest.fixture
def reconciler(container) -> Reconciler:
    return Reconciler(container.database, container.publisher, container.settings)


@pytest.fixture
def run_indexer(container, processor):
    """Drain the queue and run the indexing worker - the async boundary, made explicit."""

    async def _run() -> list[DocumentEvent]:
        events = container.publisher.drain()
        await processor.process(events)
        return events

    return _run


@pytest_asyncio.fixture
async def indexed_documents(client, auth_a, run_indexer):
    """A small, deterministic corpus for tenant A."""
    corpus = [
        {"title": "Quarterly financial report", "content": "Revenue grew across all regions this quarter.",
         "content_type": "application/pdf", "tags": ["finance", "q3"], "metadata": {"department": "finance"}},
        {"title": "Engineering onboarding guide", "content": "How to set up your laptop and deploy your first service.",
         "content_type": "text/markdown", "tags": ["engineering"], "metadata": {"department": "engineering"}},
        {"title": "Security policy", "content": "Password rotation, encryption requirements and incident reporting.",
         "content_type": "application/pdf", "tags": ["security", "policy"], "metadata": {"department": "security"}},
    ]
    ids = []
    for document in corpus:
        response = await client.post("/documents", json=document, headers=auth_a)
        assert response.status_code == 202
        ids.append(response.json()["id"])
    await run_indexer()
    return ids
