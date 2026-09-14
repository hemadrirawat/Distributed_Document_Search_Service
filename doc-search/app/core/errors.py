"""Domain errors mapped to stable API error codes.

Internal details (stack traces, driver errors, SQL) are never surfaced to clients;
they are logged server-side with the request id for correlation.
"""
from __future__ import annotations

from typing import Any


class AppError(Exception):
    status_code: int = 500
    code: str = "internal_error"
    message: str = "An internal error occurred."

    def __init__(self, message: str | None = None, *, details: dict[str, Any] | None = None) -> None:
        self.message = message or self.message
        self.details = details or {}
        super().__init__(self.message)


class BadRequestError(AppError):
    status_code = 400
    code = "bad_request"
    message = "The request is invalid."


class UnauthorizedError(AppError):
    status_code = 401
    code = "unauthorized"
    message = "Missing or invalid API credentials."


class ForbiddenError(AppError):
    status_code = 403
    code = "forbidden"
    message = "The authenticated tenant may not access this resource."


class NotFoundError(AppError):
    status_code = 404
    code = "not_found"
    message = "Resource not found."


class PayloadTooLargeError(AppError):
    status_code = 413
    code = "payload_too_large"
    message = "Document payload exceeds the configured limit."


class RateLimitedError(AppError):
    status_code = 429
    code = "rate_limited"
    message = "Per-tenant rate limit exceeded."

    def __init__(self, retry_after_seconds: float, limit: int) -> None:
        super().__init__(details={"retry_after_seconds": round(retry_after_seconds, 3), "limit_per_minute": limit})
        self.retry_after_seconds = retry_after_seconds
        self.limit = limit


class DependencyUnavailableError(AppError):
    status_code = 503
    code = "dependency_unavailable"
    message = "A downstream dependency is unavailable. Please retry."

    def __init__(self, dependency: str) -> None:
        super().__init__(f"Dependency '{dependency}' is unavailable. Please retry.", details={"dependency": dependency})
        self.dependency = dependency
