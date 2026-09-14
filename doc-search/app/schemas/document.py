from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

MAX_CONTENT_CHARS = 1_000_000  # ~1 MB of text; larger payloads belong in object storage
MAX_TAGS = 32
MAX_METADATA_KEYS = 50


class DocumentCreate(BaseModel):
    """Request body for POST /documents. Validation happens before any I/O."""

    model_config = ConfigDict(extra="forbid")

    title: str = Field(..., min_length=1, max_length=512)
    content: str = Field(..., min_length=1, max_length=MAX_CONTENT_CHARS)
    content_type: str = Field(default="text/plain", max_length=64)
    tags: list[str] = Field(default_factory=list, max_length=MAX_TAGS)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("tags")
    @classmethod
    def _validate_tags(cls, tags: list[str]) -> list[str]:
        cleaned = [t.strip() for t in tags if t and t.strip()]
        if any(len(t) > 64 for t in cleaned):
            raise ValueError("each tag must be at most 64 characters")
        return cleaned

    @field_validator("metadata")
    @classmethod
    def _validate_metadata(cls, metadata: dict[str, Any]) -> dict[str, Any]:
        if len(metadata) > MAX_METADATA_KEYS:
            raise ValueError(f"metadata supports at most {MAX_METADATA_KEYS} keys")
        for key, value in metadata.items():
            if not isinstance(key, str) or len(key) > 64:
                raise ValueError("metadata keys must be strings of at most 64 characters")
            if not isinstance(value, (str, int, float, bool)) and value is not None:
                raise ValueError("metadata values must be scalar (string, number, boolean or null)")
            if isinstance(value, str) and len(value) > 1024:
                raise ValueError("metadata string values must be at most 1024 characters")
        return metadata


class DocumentAccepted(BaseModel):
    """202 response: the document is durably stored, indexing is asynchronous."""

    id: uuid.UUID
    tenant_id: str
    status: str = Field(..., description="'pending' until the indexing worker confirms the document is searchable")
    version: int
    created_at: datetime


class DocumentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tenant_id: str
    title: str
    content: str
    content_type: str
    tags: list[str]
    metadata: dict[str, Any]
    version: int
    status: str
    created_at: datetime
    updated_at: datetime
    indexed_at: datetime | None = None

    @classmethod
    def from_model(cls, document) -> DocumentResponse:
        return cls(
            id=document.id,
            tenant_id=document.tenant_id,
            title=document.title,
            content=document.content,
            content_type=document.content_type,
            tags=list(document.tags or []),
            metadata=dict(document.doc_metadata or {}),
            version=document.version,
            status=document.status,
            created_at=document.created_at,
            updated_at=document.updated_at,
            indexed_at=document.indexed_at,
        )
