"""Dependency health aggregation for GET /health.

Criticality drives the verdict:
  * Postgres and OpenSearch are critical - without them reads/writes fail.
  * Redis and RabbitMQ are non-critical - the service degrades (cold cache,
    deferred indexing) but keeps serving, so they yield `degraded`, not `unhealthy`.
Every probe is bounded by a timeout and run concurrently so /health cannot itself
become a slow endpoint that trips the load balancer.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from app.schemas.health import DependencyHealth, HealthResponse


@dataclass(slots=True)
class Probe:
    name: str
    critical: bool
    check: Callable[[], Awaitable[bool]]


class HealthService:
    def __init__(self, probes: list[Probe], service: str, environment: str, timeout_seconds: float = 1.0) -> None:
        self._probes = probes
        self._service = service
        self._environment = environment
        self._timeout = timeout_seconds

    async def _run(self, probe: Probe) -> DependencyHealth:
        start = time.perf_counter()
        try:
            ok = bool(await asyncio.wait_for(probe.check(), timeout=self._timeout))
        except Exception:
            ok = False
        return DependencyHealth(
            name=probe.name,
            status="up" if ok else "down",
            critical=probe.critical,
            latency_ms=round((time.perf_counter() - start) * 1000, 2),
        )

    async def check(self) -> HealthResponse:
        results = await asyncio.gather(*(self._run(p) for p in self._probes))
        status = "healthy"
        if any(r.status == "down" and r.critical for r in results):
            status = "unhealthy"
        elif any(r.status == "down" for r in results):
            status = "degraded"
        return HealthResponse(
            status=status,
            service=self._service,
            environment=self._environment,
            dependencies=list(results),
        )
