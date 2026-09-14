#!/usr/bin/env python3
"""Reproducible search benchmark for the local prototype.

    python scripts/bench.py --seed 5000 --concurrency 50 --requests 5000

Reports p50/p95/p99 and achieved throughput. Results are only meaningful as a
*relative* signal on a laptop running every dependency in a single Docker VM -
they are NOT evidence that the production SLA is met. See README -> Performance.
"""
from __future__ import annotations

import argparse
import asyncio
import random
import statistics
import time
import uuid

import httpx

TERMS = ["report", "policy", "revenue", "kubernetes", "incident", "onboarding", "security", "quarterly"]


async def seed(client: httpx.AsyncClient, headers: dict, count: int) -> None:
    print(f"seeding {count} documents...")
    semaphore = asyncio.Semaphore(20)

    async def one(i: int) -> None:
        async with semaphore:
            await client.post("/documents", headers=headers, json={
                "title": f"{random.choice(TERMS).title()} document {i}",
                "content": " ".join(random.choices(TERMS, k=80)) + f" {uuid.uuid4().hex}",
                "content_type": random.choice(["text/plain", "application/pdf", "text/markdown"]),
                "tags": random.sample(TERMS, k=2),
                "metadata": {"batch": str(i // 500)},
            })

    await asyncio.gather(*(one(i) for i in range(count)))
    print("seeded; waiting 15s for the indexing pipeline to drain")
    await asyncio.sleep(15)


async def run(client: httpx.AsyncClient, headers: dict, requests: int, concurrency: int,
              cache_bust: bool) -> list[float]:
    latencies: list[float] = []
    semaphore = asyncio.Semaphore(concurrency)

    async def one() -> None:
        async with semaphore:
            query = random.choice(TERMS)
            if cache_bust:
                query = f"{query} {uuid.uuid4().hex[:6]}"  # forces an engine round trip
            started = time.perf_counter()
            response = await client.get("/search", params={"q": query}, headers=headers)
            if response.status_code == 200:
                latencies.append((time.perf_counter() - started) * 1000)

    started = time.perf_counter()
    await asyncio.gather(*(one() for _ in range(requests)))
    elapsed = time.perf_counter() - started
    print(f"  completed {len(latencies)}/{requests} in {elapsed:.2f}s -> {len(latencies)/elapsed:.0f} req/s")
    return latencies


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round(p / 100 * (len(ordered) - 1))))
    return ordered[index]


def report(label: str, latencies: list[float]) -> None:
    if not latencies:
        print(f"{label}: no successful responses")
        return
    print(
        f"{label:<22} n={len(latencies):<6} "
        f"p50={percentile(latencies, 50):7.2f}ms  "
        f"p95={percentile(latencies, 95):7.2f}ms  "
        f"p99={percentile(latencies, 99):7.2f}ms  "
        f"mean={statistics.mean(latencies):7.2f}ms  max={max(latencies):7.2f}ms"
    )


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--api-key", default="acme-dev-key-001")
    parser.add_argument("--seed", type=int, default=0, help="documents to index before benchmarking")
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=50)
    args = parser.parse_args()

    headers = {"X-API-Key": args.api_key}
    limits = httpx.Limits(max_connections=args.concurrency * 2, max_keepalive_connections=args.concurrency * 2)
    async with httpx.AsyncClient(base_url=args.url, timeout=30.0, limits=limits) as client:
        health = (await client.get("/health")).json()
        print(f"health: {health['status']}")
        if args.seed:
            await seed(client, headers, args.seed)

        print("\nwarmup...")
        await run(client, headers, 200, args.concurrency, cache_bust=True)

        print("\ncache-bypass (every query reaches OpenSearch):")
        report("cold / engine", await run(client, headers, args.requests, args.concurrency, cache_bust=True))

        print("\nrepeat queries (Redis cache in play - realistic mixed traffic):")
        report("warm / cached", await run(client, headers, args.requests, args.concurrency, cache_bust=False))

        print("\nNOTE: single-host numbers. Not a production capacity claim.")


if __name__ == "__main__":
    asyncio.run(main())
