"""POST / GET / DELETE /documents - lifecycle, contracts and async semantics."""
from __future__ import annotations

import uuid

import pytest

pytestmark = pytest.mark.asyncio

DOC = {"title": "Incident postmortem", "content": "Root cause was a saturated connection pool.",
       "tags": ["ops"], "metadata": {"severity": "sev2"}}


async def test_create_returns_202_with_pending_status(client, auth_a):
    response = await client.post("/documents", json=DOC, headers=auth_a)
    assert response.status_code == 202
    body = response.json()
    # 202, not 201: durable in Postgres, not yet searchable.
    assert body["status"] == "pending"
    assert body["tenant_id"] == "acme"
    assert body["version"] == 1
    assert response.headers["Location"] == f"/documents/{body['id']}"
    assert uuid.UUID(body["id"])


async def test_create_publishes_exactly_one_indexing_event(client, auth_a, container):
    response = await client.post("/documents", json=DOC, headers=auth_a)
    events = container.publisher.drain()
    assert len(events) == 1
    assert events[0].type == "document.index"
    assert events[0].document_id == response.json()["id"]
    assert events[0].version == 1


async def test_read_after_write_is_immediate_on_source_of_truth(client, auth_a):
    created = (await client.post("/documents", json=DOC, headers=auth_a)).json()
    # GET reads Postgres, so it is consistent immediately even though search is not.
    response = await client.get(f"/documents/{created['id']}", headers=auth_a)
    assert response.status_code == 200
    body = response.json()
    assert body["title"] == DOC["title"]
    assert body["content"] == DOC["content"]
    assert body["metadata"] == {"severity": "sev2"}
    assert body["status"] == "pending"


async def test_status_becomes_indexed_after_worker_runs(client, auth_a, run_indexer):
    created = (await client.post("/documents", json=DOC, headers=auth_a)).json()
    await run_indexer()
    body = (await client.get(f"/documents/{created['id']}", headers=auth_a)).json()
    assert body["status"] == "indexed"
    assert body["indexed_at"] is not None


async def test_get_unknown_document_returns_404_envelope(client, auth_a):
    response = await client.get(f"/documents/{uuid.uuid4()}", headers=auth_a)
    assert response.status_code == 404
    error = response.json()["error"]
    assert error["code"] == "not_found"
    assert error["request_id"]


async def test_get_with_malformed_uuid_returns_422(client, auth_a):
    response = await client.get("/documents/not-a-uuid", headers=auth_a)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"


async def test_delete_removes_document_and_publishes_delete_event(client, auth_a, container, run_indexer):
    created = (await client.post("/documents", json=DOC, headers=auth_a)).json()
    await run_indexer()

    response = await client.delete(f"/documents/{created['id']}", headers=auth_a)
    assert response.status_code == 204

    events = container.publisher.drain()
    assert [e.type for e in events] == ["document.delete"]
    # Version bumped on delete so the tombstone always wins in the search index.
    assert events[0].version == 2

    assert (await client.get(f"/documents/{created['id']}", headers=auth_a)).status_code == 404


async def test_delete_is_reflected_in_search_after_worker_runs(client, auth_a, run_indexer):
    created = (await client.post("/documents", json=DOC, headers=auth_a)).json()
    await run_indexer()
    assert (await client.get("/search?q=postmortem&tenant=acme", headers=auth_a)).json()["total"] == 1

    await client.delete(f"/documents/{created['id']}", headers=auth_a)
    await run_indexer()
    assert (await client.get("/search?q=postmortem&tenant=acme", headers=auth_a)).json()["total"] == 0


async def test_delete_twice_returns_404(client, auth_a):
    created = (await client.post("/documents", json=DOC, headers=auth_a)).json()
    assert (await client.delete(f"/documents/{created['id']}", headers=auth_a)).status_code == 204
    assert (await client.delete(f"/documents/{created['id']}", headers=auth_a)).status_code == 404
