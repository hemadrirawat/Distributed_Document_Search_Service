"""Document write/read orchestration.

Write path ordering is deliberate:
  1. Validate  (no I/O wasted on bad input)
  2. Persist in Postgres and COMMIT      <- durability boundary; the API's promise
  3. Publish the indexing event          <- best-effort; failure is recoverable
  4. Invalidate the tenant's search cache

Postgres is committed before publishing so we never advertise a document the
database does not have. The inverse risk - a committed row whose event was lost -
is handled by the reconciler sweep in the worker, which re-publishes any document
left outside INDEXED/DELETED. See docs/SUBMISSION.md -> Consistency model.
"""
from __future__ import annotations

import logging
import uuid

from app.clients.cache import CacheClient
from app.clients.queue import EVENT_DELETE, EVENT_INDEX, DocumentEvent, EventPublisher
from app.core.config import Settings
from app.core.errors import NotFoundError
from app.core.metrics import cache_events, index_events, queue_publish_events
from app.models import Document, DocumentStatus
from app.repositories.document_repository import DocumentRepository
from app.schemas.document import DocumentCreate, DocumentResponse
from app.services.cache_keys import document_key
from app.services.search_service import SearchService

logger = logging.getLogger(__name__)


class DocumentService:
    def __init__(
        self,
        repository: DocumentRepository,
        session,
        publisher: EventPublisher,
        cache: CacheClient,
        search_service: SearchService,
        settings: Settings,
    ) -> None:
        self._repository = repository
        self._session = session
        self._publisher = publisher
        self._cache = cache
        self._search = search_service
        self._settings = settings

    async def create(self, tenant_id: str, payload: DocumentCreate) -> Document:
        document = Document(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            title=payload.title,
            content=payload.content,
            content_type=payload.content_type,
            tags=payload.tags,
            doc_metadata=payload.metadata,
            version=1,
            status=DocumentStatus.PENDING.value,
        )
        await self._repository.create(document)
        await self._session.commit()  # durability boundary

        await self._publish(
            DocumentEvent(
                document_id=str(document.id),
                tenant_id=tenant_id,
                version=document.version,
                type=EVENT_INDEX,
            )
        )
        await self._search.invalidate_tenant(tenant_id)
        index_events.labels(operation="create", outcome="accepted").inc()
        return document

    async def get(self, tenant_id: str, document_id: uuid.UUID) -> DocumentResponse:
        key = document_key(tenant_id, str(document_id))
        cached = await self._cache.get_json(key)
        if cached is not None:
            cache_events.labels(cache="document", outcome="hit").inc()
            return DocumentResponse.model_validate(cached)

        cache_events.labels(cache="document", outcome="miss").inc()
        document = await self._repository.get(tenant_id, document_id)
        if document is None:
            # 404 (not 403) for another tenant's id: a 403 would confirm the id exists.
            raise NotFoundError("Document not found.")
        response = DocumentResponse.from_model(document)
        await self._cache.set_json(key, response.model_dump(mode="json"), self._settings.document_cache_ttl_seconds)
        return response

    async def delete(self, tenant_id: str, document_id: uuid.UUID) -> None:
        document = await self._repository.get(tenant_id, document_id)
        if document is None:
            raise NotFoundError("Document not found.")

        await self._repository.soft_delete(document)
        await self._session.commit()

        # Invalidate reads immediately so the API never serves a deleted document
        # from cache, even though index removal is asynchronous.
        await self._cache.delete(document_key(tenant_id, str(document_id)))
        await self._search.invalidate_tenant(tenant_id)
        await self._publish(
            DocumentEvent(
                document_id=str(document.id),
                tenant_id=tenant_id,
                version=document.version,  # bumped by soft_delete
                type=EVENT_DELETE,
            )
        )
        index_events.labels(operation="delete", outcome="accepted").inc()

    async def _publish(self, event: DocumentEvent) -> None:
        try:
            await self._publisher.publish(event)
        except Exception as exc:
            # Non-fatal: the row is already durable. The reconciler will re-publish.
            queue_publish_events.labels(outcome="failure").inc()
            logger.error(
                "failed to publish indexing event; deferred to reconciler",
                extra={"document_id": event.document_id, "event_type": event.type, "error": str(exc)},
            )
