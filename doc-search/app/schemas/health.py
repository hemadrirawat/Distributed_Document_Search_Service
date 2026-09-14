from __future__ import annotations

from pydantic import BaseModel, Field


class DependencyHealth(BaseModel):
    """Dependency status. Deliberately free of hostnames, versions and credentials —
    /health is often exposed to load balancers and probes outside the trust boundary."""

    name: str
    status: str = Field(..., description="up | down")
    critical: bool = Field(..., description="If false, a failure degrades rather than fails the service")
    latency_ms: float | None = None


class HealthResponse(BaseModel):
    status: str = Field(..., description="healthy | degraded | unhealthy")
    service: str
    environment: str
    dependencies: list[DependencyHealth]
