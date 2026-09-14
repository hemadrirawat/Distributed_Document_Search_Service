# Requirement Matrix

Every requirement extracted from the assessment brief, mapped to where it is implemented, how it is demonstrated, and how it is verified. This matrix was built **before** implementation and used as the acceptance checklist afterwards — the verification pass is recorded in [`SELF_AUDIT.md`](SELF_AUDIT.md).

Legend — **Type:** M = mandatory, B = bonus, D = documentation deliverable.

## A. Scenario capabilities

| # | Requirement | Type | Where implemented | File / component | Demonstrated by | Verified by |
|---|---|---|---|---|---|---|
| A1 | 10M+ documents across tenants | M | Shard + routing strategy; tenant-indexed schema; partitioning path | `clients/opensearch_engine.py` `index_settings`, `models/document.py` indexes | SUBMISSION §4, README §11 | Design review; shard sizing maths stated |
| A2 | Full-text search with relevance ranking | M | BM25 `multi_match` over custom analyzer, `title^3` | `opensearch_engine._build_query`, `inmemory_engine.search` | `GET /search` returns `score` | `test_search.py::test_title_matches_outrank_body_only_matches` |
| A3 | <500 ms p95 | M | Routing, cache, single-flight, capped totals, page bounds | `search_service.py`, `routes/search.py`, engine adapter | README §11 lever table; `/metrics` histograms | `scripts/bench.py` harness; latency SLI query documented |
| A4 | 1000+ concurrent searches/sec | M | Stateless async API, replicas, cache, OpenSearch read replicas | `main.py` factory, `docker-compose.yml`, `container.py` | `--scale api=N`; SUBMISSION §7 | Bench harness; no measured claim made |
| A5 | Tenant isolation and security | M | Server-derived tenant; engine filter; scoped repos/cache/limits | `api/deps.py`, `services/cache_keys.py`, `repositories/` | 404 on cross-tenant, 403 on param mismatch | `test_tenant_isolation.py` (6 tests) |
| A6 | Horizontal scalability | M | No in-process state; independent API and worker tiers | `container.py`, `workers/consumer.py`, compose | Scale flags in README §6 | Design review; state audit in SELF_AUDIT |
| A7 | Fault tolerance | M | Retry+jitter, circuit breaker, retry queue, DLQ, reconciler, fail-open | `core/resilience.py`, `clients/queue.py`, `workers/reconciler.py` | Degradation table SUBMISSION §7 | `test_indexing_pipeline.py`, `test_cache.py`, `test_health.py` |

## B. Mandatory API endpoints

| # | Requirement | Type | Where implemented | File | Demonstrated by | Verified by |
|---|---|---|---|---|---|---|
| B1 | `POST /documents` | M | Validate → persist → publish → invalidate → 202 | `api/routes/documents.py`, `services/document_service.py` | README §8 curl #2 | `test_documents.py` (4 tests) |
| B2 | `GET /search?q=&tenant=` | M | Auth → scope check → limit → cache → engine | `api/routes/search.py`, `services/search_service.py` | curl #4 | `test_search.py` (11 tests) |
| B3 | `GET /documents/{id}` | M | Cache-aside read from Postgres, tenant-scoped | `api/routes/documents.py` | curl #3 | `test_documents.py`, `test_tenant_isolation.py` |
| B4 | `DELETE /documents/{id}` | M | Soft delete, sync invalidation, async index delete | `api/routes/documents.py` | curl #11 | `test_documents.py` (3 tests) |
| B5 | Consistent error responses | M | Single envelope with `request_id` | `api/exception_handlers.py` | README §7 | `test_validation_and_errors.py::test_every_error_uses_the_same_envelope` |
| B6 | Request validation | M | Pydantic strict schemas, query constraints | `schemas/document.py`, `routes/search.py` | 422 examples | `test_validation_and_errors.py` (10 cases) |
| B7 | No internal errors leaked | M | Catch-all handler logs traceback, returns generic | `api/exception_handlers.py` | — | `test_search.py::test_search_engine_outage_surfaces_503_not_500` |

## C. Prototype components

| # | Requirement | Type | Where implemented | File | Demonstrated by | Verified by |
|---|---|---|---|---|---|---|
| C1 | Basic multi-tenancy | M | Tenant registry + header-derived identity | `models/tenant.py`, `api/deps.py` | Two seeded tenants | `test_tenant_isolation.py` |
| C2 | Search functionality | M | OpenSearch adapter behind a port | `clients/opensearch_engine.py` | curl #4 | `test_search.py`, integration suite |
| C3 | Search engine choice justified | M | — | README §4, SUBMISSION §4 | Written rationale + rejected alternatives | Review |
| C4 | Caching layer | M | Redis cache-aside, 3 caches, generation invalidation | `clients/cache.py`, `services/cache_keys.py` | `cached` flag in response | `test_cache.py` (8 tests) |
| C5 | Per-tenant rate limiting | M | Redis Lua token bucket, per-tenant keys | `services/rate_limiter.py` | 429 + headers, curl #10 | `test_rate_limit.py` (5 tests) |
| C6 | Health check with dependency status | M | Parallel bounded probes, criticality model | `services/health_service.py`, `routes/health.py` | curl #1 | `test_health.py` (5 tests) |
| C7 | Async processing / message queue | M | RabbitMQ + worker + retry/DLQ topology | `clients/queue.py`, `workers/` | 202 + status transition | `test_indexing_pipeline.py` (7 tests) |
| C8 | Docker / docker-compose | M | 5 services, health-gated startup | `docker-compose.yml`, `Dockerfile` | `make up` | Compose config validated |
| C9 | Clear README | D | — | `README.md` | 15 sections | Reviewer walkthrough |
| C10 | curl and/or Postman samples | D | — | README §8, `postman_collection.json` | 12 curl flows + collection | JSON validated |
| C11 | Architecture diagrams | D | 5 Mermaid diagrams, labelled arrows | README §3, SUBMISSION §1 | — | `SELF_AUDIT.md` diagram cross-check |
| C12 | Production-readiness analysis | D | — | SUBMISSION §7 | All 7 categories | SELF_AUDIT |
| C13 | Experience showcase | D | — | SUBMISSION §8 | 4 structured sections | **Placeholders — candidate must complete** |
| C14 | AI-tool usage note | D | — | SUBMISSION §9 | — | Review |

## D. Architecture qualities to demonstrate

| # | Requirement | Type | Where demonstrated | Evidence |
|---|---|---|---|---|
| D1 | Horizontal scaling | M | `container.py`, compose scale flags | No in-process state except the single-flight map (per-replica optimisation only) |
| D2 | Stateless API servers | M | `main.py` factory, all state externalised | Rate limits in Redis, sessions per-request, no local disk |
| D3 | Search cluster scalability | M | `index_settings`, routing | Shard/replica config; sizing maths in SUBMISSION §4 |
| D4 | Database strategy | M | `models/`, `db/database.py` | Indexes, pooling, transactions, partitioning path |
| D5 | Cache strategy | M | `services/cache_keys.py`, `search_service.py` | Keys, TTLs, invalidation, stampede, fail-open |
| D6 | Asynchronous indexing | M | `workers/` | 202 + `pending` → `indexed` transition |
| D7 | Fault tolerance | M | `core/resilience.py`, queue topology, reconciler | Failure-mode table |
| D8 | Tenant isolation | M | deps, repos, engine filter, cache keys, limiter | 6 dedicated tests |
| D9 | Security boundaries | M | `core/security.py`, deps, handlers | Hashed keys, 404-not-403, no traces |
| D10 | Rate limiting | M | `services/rate_limiter.py` | Per-tenant, per-bucket |
| D11 | Observability | M | `core/logging.py`, `core/metrics.py`, middleware | JSON logs + 11 metric families |
| D12 | Health checks | M | `services/health_service.py` | 3 endpoints, criticality model |

## E. Documentation deliverables

| # | Requirement | Type | Location | Verified |
|---|---|---|---|---|
| E1 | Architecture doc, 2–3 pages | D | `SUBMISSION.md` §1–6 | Length check in SELF_AUDIT |
| E2 | High-level architecture diagram | D | README §3.1, SUBMISSION §1 | Diagram cross-check |
| E3 | Indexing data-flow diagram | D | README §3.2 | Diagram cross-check |
| E4 | Search data-flow diagram | D | README §3.3 | Diagram cross-check |
| E5 | Deletion flow diagram | D | README §3.4 | Diagram cross-check |
| E6 | Deployment/scaling diagram | B | README §3.5 | Marked production-only |
| E7 | Storage strategy | D | SUBMISSION §4 | — |
| E8 | API design + contract examples | D | README §7 | Matches OpenAPI at `/docs` |
| E9 | Consistency model + trade-offs | D | SUBMISSION §5 | Explicitly eventual, not strong |
| E10 | Caching strategy across layers | D | SUBMISSION §6 | 3-cache table |
| E11 | Message queue usage | D | SUBMISSION §2 | Retry/DLQ/idempotency/ordering |
| E12 | Multi-tenancy + isolation strategy | D | SUBMISSION §6 | 6 isolation dimensions |
| E13 | Assumptions | D | README §13 | 8 stated |
| E14 | Trade-offs | D | README §14 | 11 stated |

## F. Production-readiness categories

| # | Category | Required sub-topics | Location |
|---|---|---|---|
| F1 | Scalability | 100× documents, 100× traffic | SUBMISSION §7 |
| F2 | Resilience | Circuit breakers, retries, failover, dependency failures | SUBMISSION §7 |
| F3 | Security | AuthN, authZ, encryption in transit/at rest, API security | SUBMISSION §6, §7 |
| F4 | Observability | Metrics, logs, tracing | SUBMISSION §7 |
| F5 | Performance | DB optimisation, index management, query optimisation | SUBMISSION §7 |
| F6 | Operations | Deployment, zero-downtime, backup, recovery, reindexing | SUBMISSION §7 |
| F7 | SLA | 99.95%, failure domains, multi-AZ, monitoring, incident response | SUBMISSION §7 |

## G. Bonus opportunities

| # | Bonus | Status | Where | Verified |
|---|---|---|---|---|
| G1 | Fuzzy search | **Implemented** | `?fuzzy=true`; `fuzziness: AUTO` | `test_search.py::test_fuzzy_search_tolerates_a_typo` |
| G2 | Highlighting | **Implemented** | `?highlight=true` (default) | `test_search.py::test_highlighting_marks_matched_terms` |
| G3 | Faceted search | **Implemented** | `?facets=true`; terms aggregations | `test_search.py::test_faceted_search_returns_bucket_counts` |
| G4 | Performance benchmarks | **Harness implemented, not run** | `scripts/bench.py` | Reproducible; no unmeasured claim made |
| G5 | Blue-green deployment | **Documented, not implemented** | SUBMISSION §7 Operations | Alias-flip strategy for index migration |
| G6 | Cloud cost optimisation | **Documented** | SUBMISSION §7 Operations | Tiering, spot workers, right-sized shards |
| G7 | Open-source contributions | **Not applicable** | — | Candidate to add links if any |

## H. Assessment-process requirements

| # | Requirement | Status | Location |
|---|---|---|---|
| H1 | Requirement matrix built first | Done | This document |
| H2 | Final self-audit (PASS/PARTIAL/FAIL) | Done | `SELF_AUDIT.md` §1 |
| H3 | Diagram cross-check (17 questions) | Done | `SELF_AUDIT.md` §2 |
| H4 | Code ↔ documentation consistency check | Done | `SELF_AUDIT.md` §3 |
| H5 | Reviewer-perspective review | Done | `REVIEWER_NOTES.md` |
| H6 | No fabricated experience | Enforced | SUBMISSION §8 placeholders |
| H7 | Honest performance claims | Enforced | README §11, G4 above |
