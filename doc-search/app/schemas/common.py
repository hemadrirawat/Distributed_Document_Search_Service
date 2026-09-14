from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ErrorBody(BaseModel):
    code: str = Field(..., examples=["not_found"])
    message: str = Field(..., examples=["Resource not found."])
    request_id: str = Field(..., examples=["01J8Z9K2QF"])
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    """Single, consistent error envelope for every non-2xx response."""

    error: ErrorBody
