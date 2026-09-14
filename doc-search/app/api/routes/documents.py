from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Response, status

from app.api.deps import DocumentServiceDep, TenantDep, rate_limited
from app.schemas.common import ErrorResponse
from app.schemas.document import DocumentAccepted, DocumentCreate, DocumentResponse

router = APIRouter(prefix="/documents", tags=["documents"])

COMMON_ERRORS = {
    401: {"model": ErrorResponse, "description": "Missing or invalid API key"},
    429: {"model": ErrorResponse, "description": "Per-tenant rate limit exceeded"},
}


@router.post(
    "",
    response_model=DocumentAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Index a new document",
    description=(
        "Persists the document in PostgreSQL and publishes an indexing event. "
        "Returns 202 (not 201) because the document becomes *searchable* asynchronously; "
        "`status` is `pending` until the indexing worker confirms it. "
        "The document is immediately retrievable via GET /documents/{id} (read-after-write on the source of truth)."
    ),
    responses={**COMMON_ERRORS, 422: {"model": ErrorResponse, "description": "Validation error"}},
    dependencies=[Depends(rate_limited("write"))],
)
async def create_document(
    payload: DocumentCreate,
    tenant: TenantDep,
    service: DocumentServiceDep,
    response: Response,
) -> DocumentAccepted:
    document = await service.create(tenant.id, payload)
    response.headers["Location"] = f"/documents/{document.id}"
    return DocumentAccepted(
        id=document.id,
        tenant_id=document.tenant_id,
        status=document.status,
        version=document.version,
        created_at=document.created_at,
    )


@router.get(
    "/{document_id}",
    response_model=DocumentResponse,
    summary="Retrieve document details",
    description="Reads from PostgreSQL (source of truth) through a per-tenant Redis cache. "
                "Returns 404 for documents owned by another tenant - a 403 would confirm the id exists.",
    responses={**COMMON_ERRORS, 404: {"model": ErrorResponse, "description": "Not found or not owned by this tenant"}},
    dependencies=[Depends(rate_limited("read"))],
)
async def get_document(
    document_id: uuid.UUID,
    tenant: TenantDep,
    service: DocumentServiceDep,
) -> DocumentResponse:
    return await service.get(tenant.id, document_id)


@router.delete(
    "/{document_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove a document",
    description="Soft-deletes in PostgreSQL, invalidates cache synchronously, and publishes a delete "
                "event so the search index is cleaned up asynchronously.",
    responses={**COMMON_ERRORS, 404: {"model": ErrorResponse, "description": "Not found or not owned by this tenant"}},
    dependencies=[Depends(rate_limited("write"))],
)
async def delete_document(
    document_id: uuid.UUID,
    tenant: TenantDep,
    service: DocumentServiceDep,
) -> Response:
    await service.delete(tenant.id, document_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
