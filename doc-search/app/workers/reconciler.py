"""Reconciliation sweep - the safety net for the async indexing pipeline.

Postgres is the source of truth. Any document sitting in PENDING/FAILED past a
grace period means its event was lost (publish failure, broker restart before
persistence, worker crash, DLQ). The sweep republishes those documents, which
makes the pipeline eventually consistent *by construction* rather than by luck.

This is a deliberately simple stand-in for a transactional outbox, which is the
production answer (insert row + outbox record in one transaction, relay tails the
outbox). The trade-off is documented in docs/SUBMISSION.md.
"""
from __future__ import annotations

import asyncio
import logging

from app.clients.queue import EVENT_DELETE, EVENT_INDEX, DocumentEvent, EventPublisher
from app.core.config import Settings
from app.models import DocumentStatus
from app.repositories.document_repository import DocumentRepository

logger = logging.getLogger(__name__)


class Reconciler:
    def __init__(self, database, publisher: EventPublisher, settings: Settings) -> None:
        self._database = database
        self._publisher = publisher
        self._settings = settings

    async def sweep_once(self) -> int:
        async with self._database.session() as session:
            repository = DocumentRepository(session)
            stale = await repository.find_stale(
                older_than_seconds=self._settings.reconcile_stale_after_seconds,
                limit=self._settings.reconcile_batch_size,
            )
            for document in stale:
                event_type = EVENT_DELETE if document.status == DocumentStatus.DELETED.value else EVENT_INDEX
                try:
                    await self._publisher.publish(
                        DocumentEvent(
                            document_id=str(document.id),
                            tenant_id=document.tenant_id,
                            version=document.version,
                            type=event_type,
                        )
                    )
                except Exception as exc:
                    logger.error("reconciler republish failed", extra={"error": str(exc)})
                    break
        if stale:
            logger.warning("reconciler republished stale documents", extra={"count": len(stale)})
        return len(stale)

    async def run_forever(self) -> None:
        while True:
            try:
                await self.sweep_once()
            except Exception as exc:
                logger.error("reconciler sweep failed", extra={"error": str(exc)})
            await asyncio.sleep(self._settings.reconcile_interval_seconds)
