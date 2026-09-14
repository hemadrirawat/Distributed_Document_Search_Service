"""In-memory search adapter used by the automated test-suite.

It is a genuine (small) retrieval engine - tokenisation, stemming-free BM25-style
scoring, tenant filtering, pagination, highlighting, facets and external
versioning - not a mock. That lets the test-suite assert on real behaviour
(ranking order, isolation, idempotency) without requiring a live cluster, while
integration tests cover the OpenSearch adapter itself.
"""
from __future__ import annotations

import math
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.clients.search_engine import (
    BulkResult,
    DeleteOperation,
    EngineHit,
    EngineResult,
    IndexOperation,
    SearchQuery,
)

TOKEN_RE = re.compile(r"[a-z0-9]+")
STOPWORDS = {"the", "a", "an", "of", "and", "or", "to", "in", "is", "it", "for", "on"}


def tokenize(text: str) -> list[str]:
    return [t for t in TOKEN_RE.findall(text.lower()) if t not in STOPWORDS]


@dataclass
class _StoredDoc:
    document_id: str
    tenant_id: str
    version: int
    source: dict[str, Any] = field(default_factory=dict)


class InMemorySearchEngine:
    def __init__(self) -> None:
        self._docs: dict[str, _StoredDoc] = {}
        self.available = True  # tests flip this to simulate a cluster outage

    async def ensure_index(self) -> None:
        return None

    def _guard(self) -> None:
        if not self.available:
            raise RuntimeError("search engine unavailable")

    async def bulk(self, operations: list[IndexOperation | DeleteOperation]) -> BulkResult:
        self._guard()
        result = BulkResult()
        for op in operations:
            existing = self._docs.get(op.document_id)
            # External versioning: an older or duplicate event is a successful no-op.
            if existing is not None and op.version <= existing.version:
                result.succeeded.append(op.document_id)
                continue
            if isinstance(op, IndexOperation):
                self._docs[op.document_id] = _StoredDoc(op.document_id, op.tenant_id, op.version, dict(op.source))
            else:
                self._docs.pop(op.document_id, None)
            result.succeeded.append(op.document_id)
        return result

    async def search(self, query: SearchQuery) -> EngineResult:
        self._guard()
        start = time.perf_counter()
        # Tenant isolation is applied before scoring - other tenants' documents are
        # not part of the candidate set at all.
        corpus = [d for d in self._docs.values() if d.tenant_id == query.tenant_id]
        terms = tokenize(query.text)
        scored: list[tuple[float, _StoredDoc]] = []

        if terms:
            vocabulary: set[str] = set()
            doc_tokens: dict[str, tuple[list[str], list[str]]] = {}
            for doc in corpus:
                title_tokens = tokenize(str(doc.source.get("title", "")))
                content_tokens = tokenize(str(doc.source.get("content", "")))
                doc_tokens[doc.document_id] = (title_tokens, content_tokens)
                vocabulary.update(title_tokens)
                vocabulary.update(content_tokens)

            effective = []
            for term in terms:
                if term in vocabulary or not query.fuzzy:
                    effective.append(term)
                else:
                    effective.append(_closest(term, vocabulary) or term)

            n_docs = max(len(corpus), 1)
            df = Counter()
            for title_tokens, content_tokens in doc_tokens.values():
                present = set(title_tokens) | set(content_tokens)
                for term in set(effective):
                    if term in present:
                        df[term] += 1

            for doc in corpus:
                title_tokens, content_tokens = doc_tokens[doc.document_id]
                title_counts, content_counts = Counter(title_tokens), Counter(content_tokens)
                matched = 0
                score = 0.0
                length_norm = 1.0 + math.log1p(len(content_tokens)) / 10
                for term in effective:
                    tf = title_counts[term] * 3 + content_counts[term]
                    if tf == 0:
                        continue
                    matched += 1
                    idf = math.log(1 + (n_docs - df[term] + 0.5) / (df[term] + 0.5))
                    score += idf * (tf / (tf + 1.2)) / length_norm
                # `operator: and` semantics unless fuzzy widens it to `or`.
                required = 1 if query.fuzzy else len(effective)
                if matched >= required and score > 0:
                    scored.append((score, doc))

        scored.sort(key=lambda pair: (-pair[0], pair[1].document_id))
        total = len(scored)
        window = scored[query.offset: query.offset + query.size]
        hits = [self._to_hit(score, doc, terms, query.highlight) for score, doc in window]

        facets: dict[str, list[tuple[str, int]]] = {}
        if query.facets:
            ct, tg = Counter(), Counter()
            for _, doc in scored:
                if doc.source.get("content_type"):
                    ct[doc.source["content_type"]] += 1
                for tag in doc.source.get("tags") or []:
                    tg[tag] += 1
            facets = {"content_type": ct.most_common(10), "tags": tg.most_common(10)}

        return EngineResult(
            hits=hits,
            total=total,
            total_is_lower_bound=False,
            took_ms=int((time.perf_counter() - start) * 1000),
            facets=facets,
        )

    @staticmethod
    def _to_hit(score: float, doc: _StoredDoc, terms: list[str], highlight: bool) -> EngineHit:
        content = str(doc.source.get("content", ""))
        snippet: str | None = content[:160]
        if highlight and terms:
            lowered = content.lower()
            for term in terms:
                position = lowered.find(term)
                if position >= 0:
                    begin = max(0, position - 60)
                    fragment = content[begin: begin + 160]
                    snippet = re.sub(f"({re.escape(term)})", r"<em>\1</em>", fragment, flags=re.IGNORECASE)
                    break

        def _dt(value: Any) -> datetime | None:
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    return None
            return None

        return EngineHit(
            document_id=doc.document_id,
            score=round(score, 6),
            title=str(doc.source.get("title", "")),
            snippet=snippet,
            content_type=doc.source.get("content_type"),
            tags=list(doc.source.get("tags") or []),
            metadata=dict(doc.source.get("metadata") or {}),
            created_at=_dt(doc.source.get("created_at")),
            updated_at=_dt(doc.source.get("updated_at")),
        )

    async def ping(self) -> bool:
        return self.available

    async def close(self) -> None:
        return None


def _closest(term: str, vocabulary: set[str]) -> str | None:
    """Single-edit-distance match, mirroring OpenSearch `fuzziness: AUTO` for short terms."""
    best: str | None = None
    for candidate in vocabulary:
        if abs(len(candidate) - len(term)) > 1:
            continue
        if _edit_distance_at_most_one(term, candidate) and (best is None or len(candidate) < len(best)):
            best = candidate
    return best


def _edit_distance_at_most_one(a: str, b: str) -> bool:
    if a == b:
        return True
    if len(a) == len(b):
        return sum(1 for x, y in zip(a, b, strict=True) if x != y) == 1
    shorter, longer = (a, b) if len(a) < len(b) else (b, a)
    i = j = 0
    skipped = False
    while i < len(shorter) and j < len(longer):
        if shorter[i] != longer[j]:
            if skipped:
                return False
            skipped = True
            j += 1
            continue
        i += 1
        j += 1
    return True
