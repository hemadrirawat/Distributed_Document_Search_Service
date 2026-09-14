"""Search engine port (interface) + the data carried across it.

Two adapters implement this protocol:
  * OpenSearchEngine  - production / docker-compose path
  * InMemorySearchEngine - test path with a real (small) BM25-style scorer, so the
    test-suite exercises ranking, tenant filtering, pagination and highlighting
    rather than asserting on mocks.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, runtime_checkable


@dataclass(slots=True)
class IndexOperation:
    document_id: str
    tenant_id: str
    version: int
    source: dict[str, Any]


@dataclass(slots=True)
class DeleteOperation:
    document_id: str
    tenant_id: str
    version: int


@dataclass(slots=True)
class BulkResult:
    succeeded: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SearchQuery:
    tenant_id: str
    text: str
    page: int = 1
    size: int = 10
    fuzzy: bool = False
    highlight: bool = True
    facets: bool = False

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.size


@dataclass(slots=True)
class EngineHit:
    document_id: str
    score: float
    title: str
    snippet: str | None
    content_type: str | None
    tags: list[str]
    metadata: dict[str, Any]
    created_at: datetime | None
    updated_at: datetime | None


@dataclass(slots=True)
class EngineResult:
    hits: list[EngineHit]
    total: int
    total_is_lower_bound: bool
    took_ms: int
    facets: dict[str, list[tuple[str, int]]] = field(default_factory=dict)


@runtime_checkable
class SearchEngine(Protocol):
    async def ensure_index(self) -> None: ...
    async def bulk(self, operations: list[IndexOperation | DeleteOperation]) -> BulkResult: ...
    async def search(self, query: SearchQuery) -> EngineResult: ...
    async def ping(self) -> bool: ...
    async def close(self) -> None: ...
