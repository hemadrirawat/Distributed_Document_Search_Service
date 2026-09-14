from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.api.deps import ContainerDep, SearchServiceDep, TenantDep, enforce_tenant_scope, rate_limited
from app.clients.search_engine import SearchQuery
from app.schemas.common import ErrorResponse
from app.schemas.search import SearchResponse

router = APIRouter(tags=["search"])


@router.get(
    "/search",
    response_model=SearchResponse,
    summary="Full-text search within the authenticated tenant",
    description=(
        "Executes a relevance-ranked (BM25) full-text query. The tenant filter is derived from the "
        "authenticated principal and applied inside the search engine query - the `tenant` parameter is "
        "only cross-checked against it. Results are cached per tenant for a short TTL."
    ),
    responses={
        401: {"model": ErrorResponse, "description": "Missing or invalid API key"},
        403: {"model": ErrorResponse, "description": "`tenant` does not match the authenticated tenant"},
        429: {"model": ErrorResponse, "description": "Per-tenant rate limit exceeded"},
        503: {"model": ErrorResponse, "description": "Search cluster unavailable (circuit open)"},
    },
    dependencies=[Depends(rate_limited("search"))],
)
async def search_documents(
    tenant: TenantDep,
    container: ContainerDep,
    service: SearchServiceDep,
    q: Annotated[str, Query(min_length=1, max_length=512, description="Query text")],
    tenant_param: Annotated[str | None, Query(alias="tenant", description="Asserted tenant id; must match the API key")] = None,
    page: Annotated[int, Query(ge=1, le=1000, description="1-based page number")] = 1,
    size: Annotated[int, Query(ge=1, le=50, description="Page size")] = 10,
    fuzzy: Annotated[bool, Query(description="Enable typo tolerance (fuzziness AUTO)")] = False,
    highlight: Annotated[bool, Query(description="Return highlighted snippets")] = True,
    facets: Annotated[bool, Query(description="Return content_type and tag facet counts")] = False,
) -> SearchResponse:
    tenant_id = enforce_tenant_scope(tenant_param, tenant)
    size = min(size, container.settings.max_page_size)
    # Deep pagination guard: `from + size` beyond max_result_window is refused by
    # the cluster and is a memory hazard. search_after is the production answer.
    max_page = max(1, container.settings.max_result_window // size)
    page = min(page, max_page)
    return await service.search(
        tenant_id,
        SearchQuery(tenant_id=tenant_id, text=q, page=page, size=size,
                    fuzzy=fuzzy, highlight=highlight, facets=facets),
    )
