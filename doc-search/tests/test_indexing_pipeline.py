"""Asynchronous indexing pipeline: idempotency, ordering, failure and recovery."""
from __future__ import annotations

import pytest

from app.clients.queue import EVENT_INDEX, DocumentEvent
from app.models import DocumentStatus

pytestmark = pytest.mark.asyncio


async def test_replayed_event_is_a_harmless_no_op(client, auth_a, container, processor):
    """At-least-once delivery means duplicates are normal, not exceptional."""
    await client.post("/documents", json={"title": "Idempotent", "content": "duplicate delivery test"}, headers=auth_a)
    events = container.publisher.drain()

    await processor.process(events)
    first = (await client.get("/search?q=duplicate", headers=auth_a)).json()
    await processor.process(events)   # same events delivered again
    await processor.process(events)
    second = (await client.get("/search?q=duplicate", headers=auth_a)).json()

    assert first["total"] == 1
    assert second["total"] == 1


async def test_stale_event_cannot_resurrect_a_deleted_document(client, auth_a, container, processor):
    """Out-of-order delivery: an old index event arriving after a delete must lose
    on external version comparison."""
    created = (await client.post("/documents", json={"title": "Ordering", "content": "ordering guarantee"},
                                 headers=auth_a)).json()
    index_events = container.publisher.drain()
    await processor.process(index_events)

    await client.delete(f"/documents/{created['id']}", headers=auth_a)
    await processor.process(container.publisher.drain())   # delete, version 2
    await processor.process(index_events)                  # stale index, version 1

    assert (await client.get("/search?q=ordering", headers=auth_a)).json()["total"] == 0


async def test_batch_of_events_is_processed_in_one_bulk_call(client, auth_a, container, processor):
    calls = {"n": 0}
    original = container.engine.bulk

    async def counting_bulk(operations):
        calls["n"] += 1
        return await original(operations)

    container.engine.bulk = counting_bulk
    for i in range(20):
        await client.post("/documents", json={"title": f"Batch {i}", "content": "bulk indexing"}, headers=auth_a)
    outcome = await processor.process(container.publisher.drain())

    assert len(outcome.indexed) == 20
    assert calls["n"] == 1  # one _bulk request, not 20 individual writes


async def test_search_cluster_failure_leaves_events_retryable(client, auth_a, container, processor):
    await client.post("/documents", json={"title": "Retry me", "content": "retryable failure"}, headers=auth_a)
    events = container.publisher.drain()

    container.engine.available = False
    outcome = await processor.process(events)
    assert outcome.indexed == []
    assert len(outcome.failed) == 1  # -> nacked -> retry queue -> DLQ after max attempts

    # Postgres already has the document: no data was lost.
    async with container.database.session() as session:
        from sqlalchemy import select

        from app.models import Document
        document = (await session.execute(select(Document))).scalars().first()
        assert document.status == DocumentStatus.PENDING.value

    container.engine.available = True
    retry = await processor.process(events)
    assert len(retry.indexed) == 1
    assert (await client.get("/search?q=retryable", headers=auth_a)).json()["total"] == 1


async def test_lost_event_is_recovered_by_the_reconciler(client, auth_a, container, reconciler, processor):
    """The queue publish fails (broker outage). Postgres still commits, and the
    reconciler sweep republishes the document once the broker recovers."""
    container.publisher.available = False
    response = await client.post("/documents", json={"title": "Lost event", "content": "recovered by reconciler"},
                                 headers=auth_a)
    assert response.status_code == 202  # write still succeeds: PG is the source of truth
    assert container.publisher.events == []

    container.publisher.available = True
    republished = await reconciler.sweep_once()
    assert republished == 1

    await processor.process(container.publisher.drain())
    assert (await client.get("/search?q=reconciler", headers=auth_a)).json()["total"] == 1


async def test_event_for_a_vanished_row_is_skipped_not_retried_forever(container, processor):
    """A poison event must terminate, not loop through the retry queue indefinitely."""
    import uuid

    outcome = await processor.process(
        [DocumentEvent(document_id=str(uuid.uuid4()), tenant_id="acme", version=1, type=EVENT_INDEX)]
    )
    assert outcome.failed == []
    assert len(outcome.skipped) == 1


async def test_malformed_document_id_is_discarded(container, processor):
    outcome = await processor.process([DocumentEvent(document_id="not-a-uuid", tenant_id="acme", version=1)])
    assert outcome.skipped == ["not-a-uuid"]
    assert outcome.failed == []
