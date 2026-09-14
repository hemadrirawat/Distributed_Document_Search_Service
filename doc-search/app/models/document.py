from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum

from sqlalchemy import JSON, BigInteger, DateTime, ForeignKey, Index, String, Text, Uuid
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, utcnow

JsonType = JSON().with_variant(JSONB, "postgresql")


class DocumentStatus(str, Enum):
    PENDING = "pending"    # persisted in Postgres, not yet visible in the search index
    INDEXED = "indexed"    # confirmed present in OpenSearch
    FAILED = "failed"      # indexing exhausted retries; picked up by the reconciler / DLQ
    DELETED = "deleted"    # soft-deleted; removal from the index is asynchronous


class Document(Base):
    """Source of truth for document content and metadata.

    `version` is a monotonically increasing integer bumped on every mutation. It is
    replayed into OpenSearch as an *external* version, which makes indexing
    idempotent and immune to out-of-order event delivery.
    """

    __tablename__ = "documents"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[str] = mapped_column(String(64), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    content_type: Mapped[str] = mapped_column(String(64), nullable=False, default="text/plain")
    tags: Mapped[list] = mapped_column(JsonType, nullable=False, default=list)
    doc_metadata: Mapped[dict] = mapped_column("metadata", JsonType, nullable=False, default=dict)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default=DocumentStatus.PENDING.value)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    indexed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        # Every tenant-scoped read is (tenant_id, id) or (tenant_id, created_at).
        Index("ix_documents_tenant_created", "tenant_id", "created_at"),
        Index("ix_documents_tenant_status", "tenant_id", "status"),
        # Drives the reconciler sweep: "documents stuck outside INDEXED/DELETED".
        Index("ix_documents_status_updated", "status", "updated_at"),
    )
