"""Uniform error envelope. Internal detail never crosses the API boundary."""
from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.context import request_id_var
from app.core.errors import AppError, RateLimitedError

logger = logging.getLogger(__name__)


def _envelope(code: str, message: str, details: dict | None = None) -> dict:
    return {
        "error": {
            "code": code,
            "message": message,
            "request_id": request_id_var.get(),
            "details": details or {},
        }
    }


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def handle_app_error(request: Request, exc: AppError) -> JSONResponse:
        headers = {}
        if isinstance(exc, RateLimitedError):
            headers["Retry-After"] = str(max(1, int(exc.retry_after_seconds) + 1))
        if exc.status_code >= 500:
            logger.error("application error", extra={"code": exc.code, "detail": exc.message})
        return JSONResponse(
            status_code=exc.status_code,
            content=_envelope(exc.code, exc.message, exc.details),
            headers=headers,
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        # Field-level feedback is safe and useful; the raw input is not echoed back.
        fields = [
            {"field": ".".join(str(p) for p in err.get("loc", [])[1:]), "reason": err.get("msg", "invalid")}
            for err in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content=_envelope("validation_error", "Request validation failed.", {"fields": fields[:20]}),
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_exception(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = {401: "unauthorized", 403: "forbidden", 404: "not_found", 405: "method_not_allowed"}.get(
            exc.status_code, "http_error"
        )
        return JSONResponse(status_code=exc.status_code, content=_envelope(code, str(exc.detail)))

    @app.exception_handler(Exception)
    async def handle_unexpected(request: Request, exc: Exception) -> JSONResponse:
        # Full traceback to logs (correlated by request_id); generic message to the client.
        logger.exception("unhandled exception", extra={"path": request.url.path})
        return JSONResponse(
            status_code=500,
            content=_envelope("internal_error", "An internal error occurred. Please retry."),
        )
