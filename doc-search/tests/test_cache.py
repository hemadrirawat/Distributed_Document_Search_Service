"""Caching: hits, invalidation on write/delete, stampede coalescing, fail-open."""
from __future__ import annotations

import asyncio

import pytest

from app.services.cache_keys import normalize_query, search_generation_key, search_key

pytestmark = pytest.mark.asyncio


async def test_repeat_query_is_served_from_cache(client, auth_a, indexed_documents):
    first = (await client.get("/search?q=report", headers=auth_a)).json()
    second = (await client.get("/search?q=report", headers=auth_a)).json()
    assert first["cached"] is False
    assert second["cached"] is True
    assert [h["id"] for h in first["hits"]] == [h["id"] for h in second["hits"]]


async def test_query_normalization_shares_one_cache_entry(client, auth_a, indexed_documents):
    await client.get("/search?q=report", headers=auth_a)
    normalized = (await client.get("/search?q=%20%20REPORT%20%20", headers=auth_a)).json()
    assert normalized["cached"] is True
    assert normalize_query("  REPORT  ") == "report"


async def test_creating_a_document_invalidates_the_tenant_search_cache(client, auth_a, container, run_indexer):
    await client.get("/search?q=report", headers=auth_a)
    assert (await client.get("/search?q=report", headers=auth_a)).json()["cached"] is True

    before = await container.cache.get_json(search_generation_key("acme"))
    await client.post("/documents", json={"title": "New report", "content": "another report"}, headers=auth_a)
    after = await container.cache.get_json(search_generation_key("acme"))
    assert int(after) > int(before or 0)

    # Generation bump => the previous entry is unreachable, no SCAN required.
    assert (await client.get("/search?q=report", headers=auth_a)).json()["cached"] is False


async def test_delete_invalidates_document_cache_immediately(client, auth_a, container):
    created = (await client.post("/documents", json={"title": "Temp", "content": "temp body"}, headers=auth_a)).json()
    await client.get(f"/documents/{created['id']}", headers=auth_a)  # populate cache
    await client.delete(f"/documents/{created['id']}", headers=auth_a)
    # Must not be served from the stale cache entry.
    assert (await client.get(f"/documents/{created['id']}", headers=auth_a)).status_code == 404


async def test_concurrent_identical_misses_collapse_into_one_engine_query(client, auth_a, indexed_documents, container):
    """Cache stampede protection: 20 simultaneous identical misses hit the engine once."""
    calls = {"n": 0}
    original = container.engine.search

    async def counting_search(query):
        calls["n"] += 1
        await asyncio.sleep(0.02)
        return await original(query)

    container.engine.search = counting_search
    responses = await asyncio.gather(*[client.get("/search?q=onboarding", headers=auth_a) for _ in range(20)])
    assert all(r.status_code == 200 for r in responses)
    assert calls["n"] == 1


async def test_service_keeps_serving_when_cache_is_down(client, auth_a, indexed_documents, container):
    """Redis is an optimisation, not a dependency: fail-open, degraded latency only."""
    container.cache.backend.available = False
    response = await client.get("/search?q=report", headers=auth_a)
    assert response.status_code == 200
    assert response.json()["cached"] is False
    assert response.json()["total"] == 1

    document_response = await client.get(f"/documents/{indexed_documents[0]}", headers=auth_a)
    assert document_response.status_code == 200


async def test_cache_key_structure_is_tenant_scoped_and_stable():
    key = search_key("acme", 3, query="  Quarterly  Report ", page=2, size=10,
                     fuzzy=False, highlight=True, facets=False)
    assert key.startswith("t:acme:search:g3:")
    same = search_key("acme", 3, query="quarterly report", page=2, size=10,
                      fuzzy=False, highlight=True, facets=False)
    assert key == same
    other_tenant = search_key("globex", 3, query="quarterly report", page=2, size=10,
                              fuzzy=False, highlight=True, facets=False)
    assert key != other_tenant
    other_page = search_key("acme", 3, query="quarterly report", page=3, size=10,
                            fuzzy=False, highlight=True, facets=False)
    assert key != other_page


async def test_indexing_worker_invalidates_a_cached_empty_result(client, auth_a, container, run_indexer):
    """Without worker-side invalidation a query issued during the indexing lag would
    pin an empty result set for a full TTL and hide the document after it went live."""
    await client.post("/documents", json={"title": "Delayed", "content": "visible after indexing"}, headers=auth_a)
    empty = (await client.get("/search?q=visible", headers=auth_a)).json()
    assert empty["total"] == 0
    assert (await client.get("/search?q=visible", headers=auth_a)).json()["cached"] is True

    await run_indexer()

    fresh = (await client.get("/search?q=visible", headers=auth_a)).json()
    assert fresh["cached"] is False
    assert fresh["total"] == 1
