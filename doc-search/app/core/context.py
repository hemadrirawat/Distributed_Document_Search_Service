"""Request-scoped context propagated through logs and metrics."""
from __future__ import annotations

import contextvars

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")
tenant_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("tenant_id", default="-")
