"""OpenSearch adapter.

Design notes that matter at 10M+ documents:
  * `_routing = tenant_id` co-locates a tenant's documents on one shard, so a
    tenant search touches one shard instead of fanning out to all of them. This
    is the single biggest lever for p95 latency at high QPS.
  * External versioning (`version_type=external`) makes indexing idempotent:
    replayed or out-of-order events produce a 409 which we treat as success.
  * `track_total_hits` is capped - exact counts over 10M docs are expensive and
    nobody paginates past page 1000.
  * Only the fields needed for the result card are fetched via `_source`
    includes; full content is never shipped back from the cluster.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from opensearchpy import AsyncOpenSearch

from app.clients.search_engine import (
    BulkResult,
    DeleteOperation,
    EngineHit,
    EngineResult,
    IndexOperation,
    SearchQuery,
)
from app.core.config import Settings
from app.core.metrics import dependency_duration
from app.core.resilience import CircuitBreaker, with_retry

logger = logging.getLogger(__name__)

SOURCE_FIELDS = ["document_id", "title", "content_type", "tags", "metadata", "created_at", "updated_at"]


def index_settings(shards: int, replicas: int) -> dict[str, Any]:
    return {
        "settings": {
            "index": {
                "number_of_shards": shards,
                "number_of_replicas": replicas,
                "refresh_interval": "1s",
                "max_result_window": 10000,
            },
            "analysis": {
                "filter": {
                    "english_stop": {"type": "stop", "stopwords": "_english_"},
                    "english_stemmer": {"type": "stemmer", "language": "english"},
                },
                "analyzer": {
                    "doc_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "asciifolding", "english_stop", "english_stemmer"],
                    }
                },
            },
        },
        "mappings": {
            # `strict` at the root: an unexpected top-level field is a bug, not a
            # new mapping. Mapping explosion is a classic cluster-killer.
            "dynamic": "strict",
            "properties": {
                "document_id": {"type": "keyword"},
                "tenant_id": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "analyzer": "doc_analyzer",
                    "fields": {"raw": {"type": "keyword", "ignore_above": 256}},
                },
                "content": {"type": "text", "analyzer": "doc_analyzer"},
                "content_type": {"type": "keyword"},
                "tags": {"type": "keyword"},
                # Customer-defined metadata: dynamic, but strings map to keyword
                # (filterable/aggregatable) rather than analysed text.
                "metadata": {"type": "object", "dynamic": True},
                "version": {"type": "long"},
                "created_at": {"type": "date"},
                "updated_at": {"type": "date"},
            },
            "dynamic_templates": [
                {"metadata_strings": {"path_match": "metadata.*", "match_mapping_type": "string",
                                      "mapping": {"type": "keyword", "ignore_above": 1024}}}
            ],
        },
    }


class OpenSearchEngine:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._index = settings.opensearch_index
        self._client = AsyncOpenSearch(
            hosts=[settings.opensearch_url],
            timeout=settings.opensearch_timeout_seconds,
            max_retries=0,          # retries are handled by `with_retry` with jitter
            retry_on_timeout=False,
        )
        self._breaker = CircuitBreaker(
            "opensearch",
            failure_threshold=settings.circuit_failure_threshold,
            recovery_seconds=settings.circuit_recovery_seconds,
        )

    @property
    def circuit(self) -> CircuitBreaker:
        return self._breaker

    async def ensure_index(self) -> None:
        """Creates `documents-v1` behind the `documents` alias. The alias indirection
        is what makes zero-downtime reindexing possible (see README -> Reindexing)."""
        physical = f"{self._index}-v1"
        if not await self._client.indices.exists(index=physical):
            await self._client.indices.create(
                index=physical,
                body=index_settings(self._settings.opensearch_shards, self._settings.opensearch_replicas),
            )
            logger.info("created search index", extra={"index": physical})
        if not await self._client.indices.exists_alias(name=self._index):
            await self._client.indices.put_alias(index=physical, name=self._index)

    async def bulk(self, operations: list[IndexOperation | DeleteOperation]) -> BulkResult:
        if not operations:
            return BulkResult()
        body: list[dict[str, Any]] = []
        for op in operations:
            meta = {
                "_index": self._index,
                "_id": op.document_id,
                "routing": op.tenant_id,
                "version": op.version,
                "version_type": "external",
            }
            if isinstance(op, IndexOperation):
                body.append({"index": meta})
                body.append(op.source)
            else:
                body.append({"delete": meta})

        start = time.perf_counter()
        response = await with_retry(
            lambda: self._client.bulk(body=body, refresh=False),
            attempts=3,
            operation="opensearch.bulk",
        )
        dependency_duration.labels(dependency="opensearch", operation="bulk").observe(time.perf_counter() - start)

        result = BulkResult()
        for item in response.get("items", []):
            action, payload = next(iter(item.items()))
            doc_id = payload.get("_id", "")
            status = payload.get("status", 500)
            # 409 = version conflict -> a newer version already won: idempotent no-op.
            # 404 on delete -> already absent: idempotent no-op.
            if status < 300 or status == 409 or (action == "delete" and status == 404):
                result.succeeded.append(doc_id)
            else:
                result.failed.append(doc_id)
                logger.error("bulk item failed", extra={"document_id": doc_id, "status": status,
                                                        "reason": str(payload.get("error"))[:300]})
        return result

    def _build_body(self, query: SearchQuery) -> dict[str, Any]:
        match: dict[str, Any] = {
            "multi_match": {
                "query": query.text,
                "fields": ["title^3", "content"],   # title matches outrank body matches
                "type": "best_fields",
                "operator": "and",
            }
        }
        if query.fuzzy:
            match["multi_match"]["fuzziness"] = "AUTO"
            match["multi_match"]["operator"] = "or"
            match["multi_match"]["minimum_should_match"] = "2<70%"

        body: dict[str, Any] = {
            "from": query.offset,
            "size": query.size,
            "track_total_hits": self._settings.search_track_total_hits,
            "_source": {"includes": SOURCE_FIELDS},
            "query": {
                "bool": {
                    "must": [match],
                    # Tenant isolation: a non-scoring `filter` clause, always present,
                    # always server-derived. Cached as a bitset by OpenSearch.
                    "filter": [{"term": {"tenant_id": query.tenant_id}}],
                }
            },
        }
        if query.highlight:
            body["highlight"] = {
                "pre_tags": ["<em>"],
                "post_tags": ["</em>"],
                "fields": {"content": {"fragment_size": 160, "number_of_fragments": 1},
                           "title": {"number_of_fragments": 0}},
            }
        if query.facets:
            body["aggs"] = {
                "content_type": {"terms": {"field": "content_type", "size": 10}},
                "tags": {"terms": {"field": "tags", "size": 10}},
            }
        return body

    async def search(self, query: SearchQuery) -> EngineResult:
        body = self._build_body(query)
        start = time.perf_counter()

        async def _run():
            return await self._client.search(
                index=self._index,
                body=body,
                routing=query.tenant_id,    # single-shard query
                preference=query.tenant_id,  # stable shard-replica choice -> better cache hit rate
            )

        response = await self._breaker.call(
            lambda: with_retry(_run, attempts=2, operation="opensearch.search")
        )
        dependency_duration.labels(dependency="opensearch", operation="search").observe(time.perf_counter() - start)

        total_block = response["hits"]["total"]
        hits = [self._to_hit(raw) for raw in response["hits"]["hits"]]
        facets: dict[str, list[tuple[str, int]]] = {}
        for name, agg in (response.get("aggregations") or {}).items():
            facets[name] = [(b["key"], b["doc_count"]) for b in agg.get("buckets", [])]
        return EngineResult(
            hits=hits,
            total=total_block["value"],
            total_is_lower_bound=total_block.get("relation") == "gte",
            took_ms=response.get("took", 0),
            facets=facets,
        )

    @staticmethod
    def _to_hit(raw: dict[str, Any]) -> EngineHit:
        source = raw.get("_source", {})
        highlight = raw.get("highlight", {})
        snippet = None
        if highlight.get("content"):
            snippet = highlight["content"][0]
        elif highlight.get("title"):
            snippet = highlight["title"][0]

        def _dt(value: str | None) -> datetime | None:
            if not value:
                return None
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None

        return EngineHit(
            document_id=source.get("document_id", raw.get("_id", "")),
            score=float(raw.get("_score") or 0.0),
            title=source.get("title", ""),
            snippet=snippet,
            content_type=source.get("content_type"),
            tags=list(source.get("tags") or []),
            metadata=dict(source.get("metadata") or {}),
            created_at=_dt(source.get("created_at")),
            updated_at=_dt(source.get("updated_at")),
        )

    async def ping(self) -> bool:
        try:
            return bool(await self._client.ping())
        except Exception:
            return False

    async def close(self) -> None:
        await self._client.close()
