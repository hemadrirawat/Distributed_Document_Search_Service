"""Request correlation, structured access logging and latency metrics.

Also the place where rate-limit headers are attached, so every response carries
the tenant's current budget without each handler repeating the logic.
"""
from __future__ import annotations

import logging
import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from app.core.context import request_id_var, tenant_id_var
from app.core.metrics import http_request_duration

logger = logging.getLogger("api.access")


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Honour an upstream correlation id (API gateway / mesh) when present.
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:16]
        request_id_var.set(request_id)
        tenant_id_var.set("-")
        request.state.request_id = request_id
        started = time.perf_counter()

        response = await call_next(request)

        duration = time.perf_counter() - started
        route = request.scope.get("route")
        # Use the route *template* (/documents/{document_id}), never the raw path,
        # so metric cardinality stays bounded.
        route_label = getattr(route, "path", request.url.path)
        http_request_duration.labels(
            method=request.method, route=route_label, status=str(response.status_code)
        ).observe(duration)

        response.headers["X-Request-ID"] = request_id
        decision = getattr(request.state, "rate_limit", None)
        if decision is not None and decision.limit_per_minute:
            response.headers["X-RateLimit-Limit"] = str(decision.limit_per_minute)
            response.headers["X-RateLimit-Remaining"] = str(max(0, decision.remaining))

        logger.info(
            "request completed",
            extra={
                "method": request.method,
                "route": route_label,
                "status": response.status_code,
                "duration_ms": round(duration * 1000, 2),
            },
        )
        return response
