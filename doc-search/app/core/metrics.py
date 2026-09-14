"""Prometheus metrics. Exposed at GET /metrics for scraping.

Cardinality note: tenant_id is intentionally NOT a label on latency histograms.
With thousands of tenants that would explode series count; per-tenant traffic is
tracked on a single counter and detailed per-tenant analysis is done from logs.
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1.0, 2.0, 5.0)

http_request_duration = Histogram(
    "http_request_duration_seconds",
    "End-to-end HTTP request latency",
    labelnames=("method", "route", "status"),
    buckets=LATENCY_BUCKETS,
)
search_duration = Histogram(
    "search_duration_seconds",
    "Search execution latency by source (cache or engine)",
    labelnames=("source",),
    buckets=LATENCY_BUCKETS,
)
dependency_duration = Histogram(
    "dependency_duration_seconds",
    "Downstream dependency call latency",
    labelnames=("dependency", "operation"),
    buckets=LATENCY_BUCKETS,
)
cache_events = Counter("cache_events_total", "Cache outcomes", labelnames=("cache", "outcome"))
rate_limit_events = Counter("rate_limit_events_total", "Rate limiter outcomes", labelnames=("outcome",))
tenant_requests = Counter("tenant_requests_total", "Requests per tenant and operation", labelnames=("tenant", "operation"))
index_events = Counter("index_events_total", "Indexing pipeline outcomes", labelnames=("operation", "outcome"))
queue_publish_events = Counter("queue_publish_events_total", "Queue publish outcomes", labelnames=("outcome",))
circuit_state = Gauge("circuit_breaker_state", "0=closed 1=half_open 2=open", labelnames=("dependency",))
worker_batch_size = Histogram(
    "worker_batch_size", "Events processed per worker batch", buckets=(1, 5, 10, 25, 50, 100, 250)
)
