"""Data access for documents. All reads are tenant-scoped by construction —
there is no method that can fetch a document without a tenant predicate."""
from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import timedelta

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Document, DocumentStatus, utcnow


class DocumentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, document: Document) -> Document:
        self._session.add(document)
        await self._session.flush()
        return document

    async def get(self, tenant_id: str, document_id: uuid.UUID, *, include_deleted: bool = False) -> Document | None:
        stmt = select(Document).where(Document.id == document_id, Document.tenant_id == tenant_id)
        if not include_deleted:
            stmt = stmt.where(Document.status != DocumentStatus.DELETED.value)
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def get_many(self, document_ids: Sequence[uuid.UUID]) -> list[Document]:
        """Cross-tenant by design: used only by the indexing worker, which operates
        on the whole corpus and carries the tenant on each event."""
        if not document_ids:
            return []
        stmt = select(Document).where(Document.id.in_(list(document_ids)))
        return list((await self._session.execute(stmt)).scalars().all())

    async def soft_delete(self, document: Document) -> Document:
        """Soft delete + version bump. The bumped version is replayed as the
        external version of the OpenSearch delete, so a delete can never be
        overwritten by an in-flight index event for an older version."""
        now = utcnow()
        document.status = DocumentStatus.DELETED.value
        document.deleted_at = now
        document.updated_at = now
        document.version += 1
        await self._session.flush()
        return document

    async def mark_indexed(self, document_ids: Sequence[uuid.UUID]) -> None:
        if not document_ids:
            return
        await self._session.execute(
            update(Document)
            .where(Document.id.in_(list(document_ids)), Document.status == DocumentStatus.PENDING.value)
            .values(status=DocumentStatus.INDEXED.value, indexed_at=utcnow())
        )

    async def mark_failed(self, document_ids: Sequence[uuid.UUID]) -> None:
        if not document_ids:
            return
        await self._session.execute(
            update(Document)
            .where(Document.id.in_(list(document_ids)), Document.status == DocumentStatus.PENDING.value)
            .values(status=DocumentStatus.FAILED.value)
        )

    async def find_stale(self, *, older_than_seconds: int, limit: int) -> list[Document]:
        """Reconciliation sweep: documents that Postgres accepted but that the
        search index never confirmed (lost publish, worker crash, DLQ'd event)."""
        cutoff = utcnow() - timedelta(seconds=older_than_seconds)
        stmt = (
            select(Document)
            .where(
                Document.status.in_([DocumentStatus.PENDING.value, DocumentStatus.FAILED.value]),
                Document.updated_at < cutoff,
            )
            .order_by(Document.updated_at)
            .limit(limit)
        )
        return list((await self._session.execute(stmt)).scalars().all())


class TenantRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_api_key_hash(self, api_key_hash: str):
        from app.models import Tenant

        stmt = select(Tenant).where(Tenant.api_key_hash == api_key_hash, Tenant.status == "active")
        return (await self._session.execute(stmt)).scalar_one_or_none()
