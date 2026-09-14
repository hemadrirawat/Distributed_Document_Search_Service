# Reviewer Notes — Reviewing This Submission as the Interviewer

Written in the voice of the person evaluating this submission, against the brief's stated criteria: architectural thinking, code quality, scalability awareness, security mindset, production maturity, communication. Issues found here that were worth fixing have been fixed; the ones left open are left open deliberately, with reasons.

---

## Strong points

**The consistency story is stated, not dodged.** Most submissions claim "eventual consistency" in a sentence and then write tests that quietly index synchronously. Here the in-memory publisher holds events until a test drains them, and there is a test whose entire purpose is asserting that a document is *not* searchable yet. The per-operation guarantee table distinguishes `GET /documents/{id}` (immediate) from `GET /search` (eventual). Returning `202` rather than `201` is the same instinct applied to the API contract.

**Tenant isolation is enforced structurally.** The filter lives inside the engine query, repository methods cannot be called without a tenant, cache keys are namespaced by tenant, rate-limit buckets are keyed by tenant. The `?tenant=` parameter required by the brief is accepted but treated as an assertion that must match the authenticated principal — a 403 on mismatch rather than a silent re-scope. Returning 404 rather than 403 on cross-tenant access shows awareness that a 403 is an existence oracle. Six dedicated tests.

**Idempotency is solved properly.** External versioning in OpenSearch, driven by a `version` column that the delete path increments, means duplicate delivery, out-of-order delivery and delete-vs-index races all resolve correctly without distributed locks or deduplication tables. The test that replays a stale index event after a delete and asserts the document stays gone is the right test to have written.

**Cache invalidation avoids the obvious trap.** A generation counter gives O(1) per-tenant invalidation with no `SCAN`, no key enumeration and no cross-tenant blast radius. The stampede handling is two-layered (jittered TTL + single-flight) and the 20-way concurrent-miss test proves the coalescing works rather than asserting it.

**Failure behaviour is defined per dependency and tested by injection.** Redis fail-open with a local rate-limit fallback, RabbitMQ failure leaving writes durable and recoverable via the reconciler, OpenSearch failure surfacing as 503 behind a circuit breaker, poison events terminating instead of looping. The health model's critical/non-critical split is what makes "degraded" a real state rather than a label.

**The honesty holds up under pressure.** No throughput numbers are claimed, and a harness is shipped instead. The experience section is placeholders rather than plausible fiction. The self-audit lists six bugs the author found and fixed, including one — the cached-empty-result bug — that a test run exposed and that most submissions would never notice.

**The test suite tests behaviour.** The in-memory search engine does real tokenisation and scoring, so "title matches outrank body matches" is an assertion about ranking, not about a mock. `assert calls["n"] == 1` for bulk batching and for stampede coalescing are the kind of assertions that catch real regressions.

## Weak points and where I would push

**Single-flight is per-process.** With 20 API replicas, 20 concurrent misses on the same key still produce 20 cluster queries. The submission says this explicitly and bounds the amplification, which is the right disclosure, but a Redis `SET NX` lock with stale-while-revalidate would be the real answer at high QPS. *Left open deliberately: it adds a round trip to every miss and the honest bound is more useful than an unmeasured optimisation.*

**The reconciler is not a transactional outbox.** There is a window — commit succeeds, publish fails, and the document is unsearchable for up to `reconcile_stale_after_seconds + reconcile_interval_seconds` (~90 s worst case). The submission names this, quantifies it, and names the outbox as the fix. *I would ask the candidate to walk through the outbox implementation at the whiteboard.*

**Multiple worker replicas will duplicate reconciler sweeps.** Each worker runs its own `Reconciler`, so N workers means N republished events for the same stale document. This is harmless — the events are idempotent — but it is wasted work, and a leader election (or simply running the reconciler as a separate singleton deployment / CronJob) would be cleaner. *Known limitation; called out here rather than discovered by the reviewer.*

**`_routing = tenant_id` creates hot-shard risk.** A tenant with 40% of the corpus lands entirely on one shard. The submission raises this and names the mitigation (dedicated index or composite routing key), but does not implement tenant-size detection. *Correct scope decision for a 3–4 hour exercise; a good interview question.*

**Tenant cache invalidation is TTL-only.** Disabling a tenant or rotating its key takes up to 60 seconds to take effect, because the credential cache has no explicit invalidation path. For a security-relevant cache that window is arguably too long. *Would fix in production with a pub/sub invalidation channel; noted rather than silently shipped.*

**Postgres `mark_indexed` writes on every batch.** At high ingest this is a steady stream of small updates on a hot table. Workable at the stated scale, but at 100× ingest I would batch further or move the status signal to a cheaper store.

**No `PUT /documents/{id}`.** The brief did not require it and the schema and pipeline are version-ready, but a reviewer will notice the gap. *Documented as assumption #2.*

**Benchmarks are absent.** The harness exists and the reasoning for not fabricating numbers is sound, but a candidate who ran it and pasted real output would score higher on "performance benchmarks" as a bonus item. *The right fix is to run `make bench` before submitting, not to invent a table.*

## Missing requirements

None mandatory. One deliberate gap: **the four experience examples are placeholders** and must be completed by the candidate. Every other item in the brief — including all four endpoints, multi-tenancy, caching, rate limiting, health with dependency status, async processing, docker-compose, README, curl and Postman samples, architecture diagrams, production-readiness analysis and the AI note — is present and, where it is a runtime behaviour, tested.

## Questionable decisions worth challenging

| Decision | The challenge | The defence I would expect |
|---|---|---|
| `202` instead of `201` on create | Non-standard; some clients expect `201` | The resource is durable but not yet searchable, and the status field says so. `201` would misrepresent the guarantee |
| Soft deletes | Row count grows forever; GDPR erasure is not a status flag | Auditability and safe recovery now; a retention job for hard erasure is named as future work. I would want that job scoped |
| Rate limiter fails open | A Redis outage removes abuse protection | Availability over precision, with a local bucket keeping abuse bounded per replica. Reasonable, but a tenant deliberately timing a Redis outage gets N× their quota |
| `track_total_hits` capped at 10,000 | Users see "10,000+" instead of a real count | Standard practice at scale; the response carries `total_is_lower_bound` so clients are not misled |
| SHA-256 for API keys | Not a password hash | Correct — keys are high-entropy random strings, so a slow KDF buys nothing. Good sign that the candidate knows the difference |
| In-memory adapters in tests | Are these real tests? | The engine does genuine scoring and version resolution; integration tests cover the real adapters. I would still want the integration suite run before merge |

## Security review

Good: credentials stored only as hashes; identity derived server-side; identical responses for unknown and disabled keys; 404 not 403; strict validation with unknown fields rejected and sizes bounded; parameterised queries with an injection test driving hostile strings through the real endpoint; no stack traces in responses; health endpoint asserted free of connection details; non-root container; `.env` git-ignored.

Gaps, all disclosed: authentication is mocked (flagged, with the JWT seam marked in `core/security.py`); OpenSearch security plugin disabled in compose (flagged inline); no TLS locally; demo API keys in `.env.example` — acceptable as clearly-labelled local credentials but a reviewer should confirm they are never used anywhere real; tenant credential cache has no explicit invalidation.

## Scalability review

The important insight is present: **Postgres is not on the search path at all.** That single property is what makes the 100× traffic story credible, and many candidates miss it. Shard sizing uses real numbers (20–40 GB/shard), the primaries-for-corpus versus replicas-for-QPS distinction is correct, and backpressure is understood as "spikes become queue depth, not failed writes."

Not addressed: multi-region, cross-shard relevance scoring at very high shard counts (distributed IDF drift), and per-tenant resource fairness beyond request-rate limiting — a tenant issuing expensive queries within their rate limit can still degrade a shared cluster.

## Interview questions I would ask

1. Walk me through what happens if the worker crashes mid-batch, after `_bulk` returns but before `mark_indexed` commits. *(Answer: documents are correctly indexed but stay `pending`; the reconciler republishes and the external version makes the reindex a no-op. Convergent, with extra work — good.)*
2. Two events for the same document are processed by two workers simultaneously. What prevents the older one from winning? *(External versioning; but push on whether `version` bumps are safe under concurrent writers — the current model is single-writer-per-document via a single-row update.)*
3. Your cache generation counter is in Redis. Redis restarts and loses the counter. What breaks? *(Generation resets to 0, so old keys could be reachable again — but they were evicted with the same restart. Worth confirming the candidate reasons it through rather than guessing.)*
4. One tenant has 8M of your 10M documents. What happens, and what do you do?
5. Why RabbitMQ and not Kafka? What would change your mind?
6. Your p95 alert is firing but engine p95 is flat. What is your first hypothesis? *(Cache hit ratio dropped — the metrics are deliberately split to make this diagnosable.)*
7. Why 404 instead of 403 for another tenant's document? Where else does that reasoning apply?
8. How would you do read-after-write search if a customer demanded it?
9. Your connection pool is 10 with 20 overflow and you scale to 50 replicas. What breaks?
10. Walk me through a zero-downtime mapping change on a 1B-document index.
11. The DLQ has 50,000 messages. What do you do first?
12. You need to delete all of one tenant's data within 24 hours for GDPR. What is the runbook?

## Areas where the candidate should be prepared to explain trade-offs

Eventual consistency and why it is acceptable here; RabbitMQ versus Kafka and the conditions that flip the decision; generation-counter invalidation versus per-key deletion; fail-open versus fail-closed for the rate limiter, and the security argument against their own choice; tenant routing versus index-per-tenant versus a shared index with no routing; soft versus hard deletes; thin versus fat events; and why the benchmark table is empty.

## Verdict

**Strong hire signal on architectural thinking, security mindset and communication.** The design decisions are deliberate, the trade-offs are named with what was given up, and the failure modes are engineered rather than hoped about. Code quality is good: clean layering, ports and adapters, dependency injection, no logic in `main.py`, consistent error handling.

The two things that would move this from strong to exceptional: **run the benchmark harness and publish real numbers**, and **complete the experience section**. The first is ten minutes of work and directly addresses a bonus criterion. The second is mandatory — the submission is incomplete without it, and the placeholders, while the right call in the absence of real information, cannot ship as-is.
