"""Transport-agnostic indexing logic.

Kept separate from the RabbitMQ consumer so it can be unit-tested directly and
reused by the reconciler and by any future transport (Kafka, SQS).

Idempotency & ordering: events are deduplicated by document id keeping the
highest version, current state is re-read from Postgres, and the write to
OpenSearch uses external versioning. A replayed, duplicated or out-of-order
event therefore cannot resurrect stale content or undo a delete.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field

from app.clients.queue import DocumentEvent
from app.clients.search_engine import DeleteOperation, IndexOperation, SearchEngine
from app.core.metrics import index_events, worker_batch_size
from app.models import Document, DocumentStatus, as_utc

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ProcessOutcome:
    indexed: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)   # retryable
    skipped: list[str] = field(default_factory=list)  # terminal no-op (row vanished)


def to_search_source(document: Document) -> dict:
    return {
        "document_id": str(document.id),
        "tenant_id": document.tenant_id,
        "title": document.title,
        "content": document.content,
        "content_type": document.content_type,
        "tags": list(document.tags or []),
        "metadata": dict(document.doc_metadata or {}),
        "version": document.version,
        "created_at": as_utc(document.created_at).isoformat(),
        "updated_at": as_utc(document.updated_at).isoformat(),
    }


class IndexingProcessor:
    """Consumes indexing events and writes the search read model.

    It also owns the *second* cache invalidation. The API invalidates on write so
    stale results are not served; the worker invalidates again once documents
    actually become searchable, otherwise a query issued during the indexing lag
    would cache an empty result set for a full TTL and hide the new document long
    after it was indexed. The staleness window is therefore bounded by indexing
    lag, not by the cache TTL.
    """

    def __init__(self, database, engine: SearchEngine, cache=None) -> None:
        self._database = database
        self._engine = engine
        self._cache = cache

    async def process(self, events: list[DocumentEvent]) -> ProcessOutcome:
        outcome = ProcessOutcome()
        if not events:
            return outcome
        worker_batch_size.observe(len(events))

        # Collapse duplicates: only the newest version of each document matters.
        latest: dict[str, DocumentEvent] = {}
        for event in events:
            current = latest.get(event.document_id)
            if current is None or event.version >= current.version:
                latest[event.document_id] = event

        ids: list[uuid.UUID] = []
        malformed: set[str] = set()
        for document_id in latest:
            try:
                ids.append(uuid.UUID(document_id))
            except ValueError:
                # Terminal, not retryable: never send a poison message round the retry loop.
                logger.error("discarding malformed document id", extra={"document_id": document_id})
                outcome.skipped.append(document_id)
                malformed.add(document_id)

        from app.repositories.document_repository import DocumentRepository

        async with self._database.session() as session:
            repository = DocumentRepository(session)
            rows = {str(d.id): d for d in await repository.get_many(ids)}

            operations: list[IndexOperation | DeleteOperation] = []
            index_ids: list[str] = []
            delete_ids: list[str] = []

            for document_id in latest:
                if document_id in malformed:
                    continue
                document = rows.get(document_id)
                if document is None:
                    # Hard-deleted (e.g. retention job) between publish and consume.
                    outcome.skipped.append(document_id)
                    continue
                if document.status == DocumentStatus.DELETED.value:
                    operations.append(DeleteOperation(document_id, document.tenant_id, document.version))
                    delete_ids.append(document_id)
                else:
                    operations.append(
                        IndexOperation(document_id, document.tenant_id, document.version, to_search_source(document))
                    )
                    index_ids.append(document_id)

            if not operations:
                return outcome

            try:
                result = await self._engine.bulk(operations)
            except Exception as exc:
                logger.error("bulk indexing failed", extra={"error": str(exc), "batch": len(operations)})
                index_events.labels(operation="bulk", outcome="error").inc()
                outcome.failed.extend(latest.keys())
                return outcome

            succeeded = set(result.succeeded)
            outcome.indexed = [d for d in index_ids if d in succeeded]
            outcome.deleted = [d for d in delete_ids if d in succeeded]
            outcome.failed = [d for d in (index_ids + delete_ids) if d not in succeeded]

            # Flip PENDING -> INDEXED only for documents the cluster confirmed.
            await repository.mark_indexed([uuid.UUID(d) for d in outcome.indexed])
            await session.commit()

        await self._invalidate(latest, outcome)
        index_events.labels(operation="index", outcome="success").inc(len(outcome.indexed))
        index_events.labels(operation="delete", outcome="success").inc(len(outcome.deleted))
        if outcome.failed:
            index_events.labels(operation="bulk", outcome="failure").inc(len(outcome.failed))
        return outcome

    async def _invalidate(self, latest: dict[str, DocumentEvent], outcome: ProcessOutcome) -> None:
        """Bump the search-cache generation for each tenant whose visible corpus changed."""
        if self._cache is None:
            return
        changed = set(outcome.indexed) | set(outcome.deleted)
        tenants = {latest[d].tenant_id for d in changed if d in latest}
        from app.services.cache_keys import search_generation_key

        for tenant_id in tenants:
            await self._cache.incr(search_generation_key(tenant_id))
