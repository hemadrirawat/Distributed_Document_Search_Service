"""Search behaviour: relevance, pagination, isolation of the async boundary,
bonus features and graceful degradation."""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.asyncio


async def test_document_is_not_searchable_until_indexed(client, auth_a, container):
    """Explicitly asserts the eventual-consistency contract rather than hiding it."""
    await client.post("/documents", json={"title": "Fresh doc", "content": "brand new content"}, headers=auth_a)
    before = (await client.get("/search?q=brand&tenant=acme", headers=auth_a)).json()
    assert before["total"] == 0

    from app.workers.processor import IndexingProcessor
    await IndexingProcessor(container.database, container.engine, container.cache).process(container.publisher.drain())

    after = (await client.get("/search?q=brand&tenant=acme", headers=auth_a)).json()
    assert after["total"] == 1


async def test_search_returns_ranked_results_with_metadata(client, auth_a, indexed_documents):
    response = await client.get("/search?q=report&tenant=acme&size=10", headers=auth_a)
    assert response.status_code == 200
    body = response.json()
    assert body["total"] >= 1
    hit = body["hits"][0]
    assert hit["title"] == "Quarterly financial report"
    assert hit["score"] > 0
    assert hit["content_type"] == "application/pdf"
    assert hit["tags"] == ["finance", "q3"]
    assert hit["metadata"] == {"department": "finance"}
    assert hit["created_at"] is not None
    assert body["took_ms"] >= 0


async def test_title_matches_outrank_body_only_matches(client, auth_a, run_indexer):
    await client.post("/documents", json={"title": "Kubernetes", "content": "unrelated filler text here"}, headers=auth_a)
    await client.post("/documents", json={"title": "Unrelated filler", "content": "a passing mention of kubernetes"},
                      headers=auth_a)
    await run_indexer()

    hits = (await client.get("/search?q=kubernetes", headers=auth_a)).json()["hits"]
    assert len(hits) == 2
    assert hits[0]["title"] == "Kubernetes"
    assert hits[0]["score"] > hits[1]["score"]


async def test_pagination_splits_results_without_overlap(client, auth_a, run_indexer):
    for i in range(7):
        await client.post("/documents", json={"title": f"Runbook {i}", "content": "shared runbook procedure"},
                          headers=auth_a)
    await run_indexer()

    page1 = (await client.get("/search?q=runbook&page=1&size=3", headers=auth_a)).json()
    page2 = (await client.get("/search?q=runbook&page=2&size=3", headers=auth_a)).json()
    page3 = (await client.get("/search?q=runbook&page=3&size=3", headers=auth_a)).json()

    assert page1["total"] == 7
    assert [len(p["hits"]) for p in (page1, page2, page3)] == [3, 3, 1]
    ids = [h["id"] for p in (page1, page2, page3) for h in p["hits"]]
    assert len(set(ids)) == 7


async def test_highlighting_marks_matched_terms(client, auth_a, indexed_documents):
    body = (await client.get("/search?q=encryption&highlight=true", headers=auth_a)).json()
    assert body["total"] == 1
    assert "<em>" in body["hits"][0]["snippet"]


async def test_fuzzy_search_tolerates_a_typo(client, auth_a, indexed_documents):
    exact = (await client.get("/search?q=engineering", headers=auth_a)).json()
    assert exact["total"] == 1
    typo = (await client.get("/search?q=enginering", headers=auth_a)).json()
    assert typo["total"] == 0
    fuzzy = (await client.get("/search?q=enginering&fuzzy=true", headers=auth_a)).json()
    assert fuzzy["total"] == 1


async def test_faceted_search_returns_bucket_counts(client, auth_a, indexed_documents):
    body = (await client.get("/search?q=report&facets=true", headers=auth_a)).json()
    assert body["total"] == 1
    assert body["facets"]["content_type"] == [{"value": "application/pdf", "count": 1}]
    assert {"value": "finance", "count": 1} in body["facets"]["tags"]


async def test_no_matches_returns_empty_result_not_error(client, auth_a, indexed_documents):
    body = (await client.get("/search?q=zzzznonexistentterm", headers=auth_a)).json()
    assert body["total"] == 0
    assert body["hits"] == []


async def test_search_requires_non_empty_query(client, auth_a):
    assert (await client.get("/search?q=", headers=auth_a)).status_code == 422
    assert (await client.get("/search", headers=auth_a)).status_code == 422


async def test_page_size_is_capped(client, auth_a):
    assert (await client.get("/search?q=x&size=500", headers=auth_a)).status_code == 422


async def test_search_engine_outage_surfaces_503_not_500(client, auth_a, container):
    """Degradation must be an explicit, retryable signal - never a leaked traceback."""
    container.engine.available = False
    response = await client.get("/search?q=anything", headers=auth_a)
    assert response.status_code == 503
    error = response.json()["error"]
    assert error["code"] == "dependency_unavailable"
    # No internal detail leaks to the client; the traceback goes to the logs only.
    assert "Traceback" not in response.text
    assert "unavailable" in error["message"].lower()
