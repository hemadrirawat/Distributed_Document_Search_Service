#!/usr/bin/env python3
"""Index a small demo corpus across two tenants so search returns something interesting."""
from __future__ import annotations

import asyncio

import httpx

BASE_URL = "http://localhost:8000"
TENANTS = {"acme-dev-key-001": "acme", "globex-dev-key-002": "globex"}

CORPUS = {
    "acme": [
        ("Q3 financial report", "Revenue grew 24% year over year, driven by enterprise renewals in EMEA.",
         "application/pdf", ["finance", "q3"]),
        ("Engineering onboarding guide", "Set up your laptop, clone the monorepo and deploy your first service.",
         "text/markdown", ["engineering", "onboarding"]),
        ("Incident postmortem: search outage", "A saturated connection pool caused cascading timeouts in the search tier.",
         "text/markdown", ["engineering", "incident"]),
        ("Information security policy", "Password rotation, encryption at rest requirements and incident reporting.",
         "application/pdf", ["security", "policy"]),
    ],
    "globex": [
        ("Supplier contract template", "Standard terms for component suppliers including SLA and penalty clauses.",
         "application/pdf", ["legal", "procurement"]),
        ("Warehouse operations manual", "Inbound receiving, stock rotation and quarterly inventory reconciliation.",
         "text/plain", ["operations"]),
    ],
}


async def main() -> None:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=15.0) as client:
        for api_key, tenant in TENANTS.items():
            for title, content, content_type, tags in CORPUS[tenant]:
                response = await client.post("/documents", headers={"X-API-Key": api_key}, json={
                    "title": title, "content": content, "content_type": content_type,
                    "tags": tags, "metadata": {"source": "seed"},
                })
                response.raise_for_status()
                print(f"[{tenant}] {response.json()['id']}  {title}")
    print("\nSeeded. Indexing is asynchronous - allow a few seconds, then:")
    print('  curl -s "http://localhost:8000/search?q=report&tenant=acme" -H "X-API-Key: acme-dev-key-001" | python3 -m json.tool')


if __name__ == "__main__":
    asyncio.run(main())
