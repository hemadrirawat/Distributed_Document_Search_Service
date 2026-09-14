"""End-to-end tests against the real docker-compose stack.

Run with:  docker compose up -d && pytest -m integration
They are skipped by default so `pytest` stays fast and hermetic in CI's unit stage.
These are the tests that exercise the OpenSearch, Redis and RabbitMQ *adapters*,
which the in-process suite substitutes.
"""
from __future__ import annotations

import asyncio
import os
import uuid

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
API_KEY_A = os.getenv("TEST_API_KEY_A", "acme-dev-key-001")
API_KEY_B = os.getenv("TEST_API_KEY_B", "globex-dev-key-002")


@pytest.fixture
async def http():
    import httpx

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        yield client


async def _wait_for_indexing(http, query: str, headers: dict, expected: int, timeout: float = 20.0) -> dict:
    """Polls until the asynchronous pipeline catches up - the honest way to test an
    eventually consistent read model."""
    deadline = asyncio.get_running_loop().time() + timeout
    body = {}
    while asyncio.get_running_loop().time() < deadline:
        body = (await http.get("/search", params={"q": query}, headers=headers)).json()
        if body.get("total") == expected:
            return body
        await asyncio.sleep(0.5)
    pytest.fail(f"indexing did not converge: expected {expected}, last={body.get('total')}")


async def test_health_reports_all_real_dependencies_up(http):
    body = (await http.get("/health")).json()
    assert body["status"] == "healthy", body
    assert {d["name"] for d in body["dependencies"]} == {"postgres", "opensearch", "redis", "rabbitmq"}


async def test_full_document_lifecycle_against_real_services(http):
    marker = f"integration{uuid.uuid4().hex[:10]}"
    headers = {"X-API-Key": API_KEY_A}

    created = (await http.post("/documents", headers=headers, json={
        "title": f"Integration {marker}",
        "content": f"This document exists to verify the {marker} pipeline end to end.",
        "content_type": "text/plain",
        "tags": ["integration"],
        "metadata": {"suite": "integration"},
    })).json()

    assert (await http.get(f"/documents/{created['id']}", headers=headers)).status_code == 200

    body = await _wait_for_indexing(http, marker, headers, 1)
    assert body["hits"][0]["id"] == created["id"]
    assert body["hits"][0]["score"] > 0

    assert (await http.delete(f"/documents/{created['id']}", headers=headers)).status_code == 204
    await _wait_for_indexing(http, marker, headers, 0)


async def test_tenant_isolation_against_real_opensearch(http):
    marker = f"isolation{uuid.uuid4().hex[:10]}"
    await http.post("/documents", headers={"X-API-Key": API_KEY_A},
                    json={"title": "A", "content": f"tenant a {marker}"})
    await http.post("/documents", headers={"X-API-Key": API_KEY_B},
                    json={"title": "B", "content": f"tenant b {marker}"})

    a = await _wait_for_indexing(http, marker, {"X-API-Key": API_KEY_A}, 1)
    b = await _wait_for_indexing(http, marker, {"X-API-Key": API_KEY_B}, 1)
    assert a["hits"][0]["title"] == "A"
    assert b["hits"][0]["title"] == "B"

    forbidden = await http.get("/search", params={"q": marker, "tenant": "globex"},
                               headers={"X-API-Key": API_KEY_A})
    assert forbidden.status_code == 403


async def test_redis_cache_is_shared_across_requests(http):
    headers = {"X-API-Key": API_KEY_A}
    await http.get("/search", params={"q": "integration"}, headers=headers)
    second = (await http.get("/search", params={"q": "integration"}, headers=headers)).json()
    assert second["cached"] is True
