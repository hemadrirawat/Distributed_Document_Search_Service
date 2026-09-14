from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class SearchHit(BaseModel):
    id: str
    title: str
    score: float = Field(..., description="Relevance score from the search engine (BM25)")
    snippet: str | None = Field(None, description="Highlighted fragment; falls back to a content prefix")
    content_type: str | None = None
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class FacetValue(BaseModel):
    value: str
    count: int


class SearchResponse(BaseModel):
    query: str
    tenant_id: str
    page: int
    size: int
    total: int = Field(..., description="Total matching documents, capped at search_track_total_hits")
    total_is_lower_bound: bool = Field(False, description="True when `total` was capped for performance")
    took_ms: int
    cached: bool = False
    hits: list[SearchHit]
    facets: dict[str, list[FacetValue]] = Field(default_factory=dict)
