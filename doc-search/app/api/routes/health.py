from __future__ import annotations

from fastapi import APIRouter, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api.deps import ContainerDep
from app.schemas.health import HealthResponse

router = APIRouter(tags=["operations"])


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Aggregate health with dependency status",
    description="Reports per-dependency status. Critical dependency down -> 503 `unhealthy`; "
                "non-critical down -> 200 `degraded`. No hostnames, versions or credentials are exposed.",
)
async def health(container: ContainerDep, response: Response) -> HealthResponse:
    result = await container.health.check()
    if result.status == "unhealthy":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return result


@router.get("/health/live", summary="Liveness probe", description="Process is running. No dependency I/O.")
async def liveness() -> dict[str, str]:
    return {"status": "alive"}


@router.get(
    "/health/ready",
    summary="Readiness probe",
    description="Used by Kubernetes/the load balancer to decide whether to route traffic. "
                "Fails only on critical dependencies so a Redis blip does not drain the fleet.",
)
async def readiness(container: ContainerDep, response: Response) -> dict[str, str]:
    result = await container.health.check()
    ready = result.status != "unhealthy"
    if not ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ready" if ready else "not_ready"}


@router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
