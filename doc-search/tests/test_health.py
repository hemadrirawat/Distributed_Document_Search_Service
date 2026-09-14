"""Health endpoint: dependency reporting, degradation semantics, no info leakage."""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.asyncio


async def test_health_reports_every_dependency(client):
    response = await client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    names = {d["name"] for d in body["dependencies"]}
    assert names == {"postgres", "opensearch", "redis", "rabbitmq"}
    assert all(d["status"] == "up" for d in body["dependencies"])
    assert all(d["latency_ms"] is not None for d in body["dependencies"])


async def test_health_is_public_and_leaks_no_connection_details(client):
    response = await client.get("/health")  # no API key required
    assert response.status_code == 200
    text = response.text.lower()
    for secret in ("password", "postgresql://", "redis://", "amqp://", "localhost:", "api_key"):
        assert secret not in text


async def test_non_critical_dependency_failure_degrades_but_stays_available(client, container):
    container.cache.backend.available = False
    response = await client.get("/health")
    assert response.status_code == 200  # still serving
    body = response.json()
    assert body["status"] == "degraded"
    redis = next(d for d in body["dependencies"] if d["name"] == "redis")
    assert redis["status"] == "down"
    assert redis["critical"] is False


async def test_critical_dependency_failure_reports_unhealthy_with_503(client, container):
    container.engine.available = False
    response = await client.get("/health")
    assert response.status_code == 503
    assert response.json()["status"] == "unhealthy"
    assert (await client.get("/health/ready")).status_code == 503
    # Liveness stays up: the process is healthy, so Kubernetes must not restart it.
    assert (await client.get("/health/live")).status_code == 200


async def test_metrics_endpoint_exposes_prometheus_series(client, auth_a, indexed_documents):
    await client.get("/search?q=report", headers=auth_a)
    response = await client.get("/metrics")
    assert response.status_code == 200
    body = response.text
    assert "http_request_duration_seconds" in body
    assert "cache_events_total" in body
    assert "search_duration_seconds" in body
