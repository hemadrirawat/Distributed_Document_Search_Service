"""Application configuration. All values are overridable via environment variables."""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    # --- application ---
    app_name: str = "distributed-document-search"
    environment: str = "local"
    log_level: str = "INFO"
    debug: bool = False

    # --- postgres (source of truth) ---
    database_url: str = "postgresql+asyncpg://docsearch:docsearch@postgres:5432/docsearch"
    db_pool_size: int = 10
    db_max_overflow: int = 20
    db_pool_timeout_seconds: float = 5.0
    db_statement_timeout_ms: int = 3000

    # --- redis (cache + rate limiting) ---
    redis_url: str = "redis://redis:6379/0"
    redis_timeout_seconds: float = 0.25
    search_cache_ttl_seconds: int = 30
    document_cache_ttl_seconds: int = 60
    tenant_cache_ttl_seconds: int = 60
    cache_ttl_jitter_ratio: float = 0.2

    # --- opensearch (search read model) ---
    opensearch_url: str = "http://opensearch:9200"
    opensearch_index: str = "documents"
    opensearch_shards: int = 3
    opensearch_replicas: int = 1
    opensearch_timeout_seconds: float = 2.0
    search_track_total_hits: int = 10000
    max_result_window: int = 10000
    max_page_size: int = 50

    # --- rabbitmq (async indexing) ---
    rabbitmq_url: str = "amqp://docsearch:docsearch@rabbitmq:5672/"
    queue_exchange: str = "documents"
    queue_name: str = "documents.index"
    queue_retry_name: str = "documents.index.retry"
    queue_dlq_name: str = "documents.index.dlq"
    queue_retry_delay_ms: int = 5000
    max_index_attempts: int = 3
    publish_timeout_seconds: float = 2.0

    # --- worker ---
    worker_prefetch: int = 200
    worker_batch_size: int = 100
    worker_batch_linger_ms: int = 300
    reconcile_interval_seconds: int = 30
    reconcile_stale_after_seconds: int = 60
    reconcile_batch_size: int = 500

    # --- rate limiting ---
    default_rate_limit_per_minute: int = 120
    rate_limit_burst_ratio: float = 0.25  # burst capacity = rpm * ratio

    # --- resilience ---
    circuit_failure_threshold: int = 5
    circuit_recovery_seconds: float = 10.0
    health_check_timeout_seconds: float = 1.0

    # --- prototype-only auth seed: "tenant_id:api_key,tenant_id:api_key" ---
    seed_tenants: str = "acme:acme-dev-key-001,globex:globex-dev-key-002"

    @property
    def seeded_tenants(self) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = []
        for raw in self.seed_tenants.split(","):
            raw = raw.strip()
            if not raw or ":" not in raw:
                continue
            tenant_id, api_key = raw.split(":", 1)
            pairs.append((tenant_id.strip(), api_key.strip()))
        return pairs


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
