"""Tenant isolation - the security-critical invariant of the whole service."""
from __future__ import annotations

import re

import pytest

pytestmark = pytest.mark.asyncio


async def test_search_never_returns_another_tenants_documents(client, auth_a, auth_b, run_indexer):
    await client.post("/documents", json={"title": "Acme salary bands", "content": "Confidential acme compensation data"},
                      headers=auth_a)
    await client.post("/documents", json={"title": "Globex salary bands", "content": "Confidential globex compensation data"},
                      headers=auth_b)
    await run_indexer()

    a_results = (await client.get("/search?q=confidential&tenant=acme", headers=auth_a)).json()
    b_results = (await client.get("/search?q=confidential&tenant=globex", headers=auth_b)).json()

    assert a_results["total"] == 1
    assert b_results["total"] == 1
    assert "Acme" in a_results["hits"][0]["title"]
    assert "Globex" in b_results["hits"][0]["title"]


async def test_get_document_of_other_tenant_returns_404_not_403(client, auth_a, auth_b):
    created = (await client.post("/documents", json={"title": "Private", "content": "acme only"}, headers=auth_a)).json()
    response = await client.get(f"/documents/{created['id']}", headers=auth_b)
    # 404 rather than 403: a 403 would confirm that the id exists.
    assert response.status_code == 404


async def test_delete_document_of_other_tenant_is_rejected_and_has_no_effect(client, auth_a, auth_b):
    created = (await client.post("/documents", json={"title": "Private", "content": "acme only"}, headers=auth_a)).json()
    assert (await client.delete(f"/documents/{created['id']}", headers=auth_b)).status_code == 404
    # The owner still has it.
    assert (await client.get(f"/documents/{created['id']}", headers=auth_a)).status_code == 200


async def test_tenant_parameter_cannot_override_authenticated_identity(client, auth_a, auth_b, run_indexer):
    await client.post("/documents", json={"title": "Globex secret", "content": "globex classified"}, headers=auth_b)
    await run_indexer()
    # Tenant A authenticates but asserts tenant=globex -> rejected, not silently rescoped.
    response = await client.get("/search?q=classified&tenant=globex", headers=auth_a)
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "forbidden"


async def test_omitted_tenant_parameter_defaults_to_authenticated_tenant(client, auth_a, run_indexer):
    await client.post("/documents", json={"title": "Acme handbook", "content": "internal acme handbook"}, headers=auth_a)
    await run_indexer()
    response = await client.get("/search?q=handbook", headers=auth_a)
    assert response.status_code == 200
    assert response.json()["tenant_id"] == "acme"


async def test_cache_keys_are_namespaced_per_tenant(client, auth_a, auth_b, run_indexer, container):
    """Identical query text from two tenants must not share a cache entry."""
    await client.post("/documents", json={"title": "Acme report", "content": "shared word report"}, headers=auth_a)
    await client.post("/documents", json={"title": "Globex report", "content": "shared word report"}, headers=auth_b)
    await run_indexer()

    await client.get("/search?q=report", headers=auth_a)
    await client.get("/search?q=report", headers=auth_b)

    keys = list(container.cache.backend._store.keys())
    search_keys = [k for k in keys if re.search(r":search:g\d+:", k)]
    assert len(search_keys) == 2
    assert any(k.startswith("t:acme:") for k in search_keys)
    assert any(k.startswith("t:globex:") for k in search_keys)

    b_response = (await client.get("/search?q=report", headers=auth_b)).json()
    assert b_response["cached"] is True
    assert b_response["hits"][0]["title"] == "Globex report"


async def test_rate_limits_are_isolated_per_tenant(client, auth_a, auth_b, container):
    """Tenant A exhausting its bucket must not throttle tenant B."""
    from app.models import Tenant

    async with container.database.session() as session:
        for tenant_id in ("acme", "globex"):
            tenant = await session.get(Tenant, tenant_id)
            tenant.rate_limit_per_minute = 20  # capacity = max(5, 20*0.25) = 5
        await session.commit()
    await container.cache.backend.delete(*list(container.cache.backend._store.keys()))

    throttled = 0
    for _ in range(12):
        if (await client.get("/search?q=anything", headers=auth_a)).status_code == 429:
            throttled += 1
    assert throttled > 0, "tenant A should have been throttled"
    assert (await client.get("/search?q=anything", headers=auth_b)).status_code == 200
