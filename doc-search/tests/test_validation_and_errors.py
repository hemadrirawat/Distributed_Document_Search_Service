"""Input validation, auth failures and error-envelope consistency."""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.asyncio


async def test_missing_api_key_is_rejected(client):
    response = await client.post("/documents", json={"title": "t", "content": "c"})
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


async def test_unknown_api_key_is_rejected_without_revealing_why(client):
    response = await client.get("/search?q=x", headers={"X-API-Key": "definitely-not-a-real-key"})
    assert response.status_code == 401
    message = response.json()["error"]["message"].lower()
    assert "tenant" not in message and "not found" not in message


@pytest.mark.parametrize(
    "payload,reason",
    [
        ({"content": "no title"}, "title is required"),
        ({"title": "no content"}, "content is required"),
        ({"title": "", "content": "c"}, "title cannot be empty"),
        ({"title": "t", "content": ""}, "content cannot be empty"),
        ({"title": "t", "content": "c", "tags": ["x" * 65]}, "tag too long"),
        ({"title": "t", "content": "c", "metadata": {"k": {"nested": 1}}}, "metadata must be scalar"),
        ({"title": "t", "content": "c", "unexpected": 1}, "unknown fields are rejected"),
        ({"title": "x" * 513, "content": "c"}, "title too long"),
    ],
)
async def test_invalid_payloads_return_422(client, auth_a, payload, reason):
    response = await client.post("/documents", json=payload, headers=auth_a)
    assert response.status_code == 422, reason
    body = response.json()["error"]
    assert body["code"] == "validation_error"
    assert body["details"]["fields"]


async def test_oversized_document_is_rejected(client, auth_a):
    response = await client.post("/documents", json={"title": "big", "content": "x" * 1_000_001}, headers=auth_a)
    assert response.status_code == 422


async def test_query_string_injection_is_treated_as_literal_text(client, auth_a, run_indexer):
    """Query text is passed as a parameter to the engine DSL, never concatenated,
    so control characters cannot alter the query structure or escape the tenant filter."""
    await client.post("/documents", json={"title": "Safe doc", "content": "ordinary content"}, headers=auth_a)
    await run_indexer()
    for hostile in ['*', '" OR 1=1 --', "{'match_all':{}}", "content:* AND tenant_id:globex"]:
        response = await client.get("/search", params={"q": hostile}, headers=auth_a)
        assert response.status_code == 200
        assert response.json()["tenant_id"] == "acme"


async def test_every_error_uses_the_same_envelope(client, auth_a):
    responses = [
        await client.post("/documents", json={"title": "t", "content": "c"}),          # 401
        await client.get("/documents/00000000-0000-0000-0000-000000000000", headers=auth_a),  # 404
        await client.post("/documents", json={"bad": 1}, headers=auth_a),              # 422
    ]
    for response in responses:
        error = response.json()["error"]
        assert set(error) == {"code", "message", "request_id", "details"}
        assert response.headers["X-Request-ID"] == error["request_id"]


async def test_request_id_is_propagated_from_upstream(client, auth_a):
    response = await client.get("/health", headers={"X-Request-ID": "trace-abc-123"})
    assert response.headers["X-Request-ID"] == "trace-abc-123"
