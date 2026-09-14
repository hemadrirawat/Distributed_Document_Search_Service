"""Event publisher port + RabbitMQ / in-memory adapters.

Events are *thin*: they carry identifiers and a version, not the document body.
The worker re-reads current state from Postgres (the source of truth) before
indexing. That keeps messages small, avoids stale payloads when a document is
mutated while an event is in flight, and makes the pipeline self-healing.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from app.core.metrics import queue_publish_events

logger = logging.getLogger(__name__)

EVENT_INDEX = "document.index"
EVENT_DELETE = "document.delete"


@dataclass(slots=True)
class DocumentEvent:
    document_id: str
    tenant_id: str
    version: int
    type: str = EVENT_INDEX
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    occurred_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_dict(payload: dict[str, Any]) -> DocumentEvent:
        return DocumentEvent(
            document_id=payload["document_id"],
            tenant_id=payload["tenant_id"],
            version=int(payload["version"]),
            type=payload.get("type", EVENT_INDEX),
            event_id=payload.get("event_id", str(uuid.uuid4())),
            occurred_at=payload.get("occurred_at", ""),
        )


@runtime_checkable
class EventPublisher(Protocol):
    async def publish(self, event: DocumentEvent) -> None: ...
    async def ping(self) -> bool: ...
    async def close(self) -> None: ...


class RabbitMQPublisher:
    def __init__(self, settings) -> None:
        self._settings = settings
        self._connection = None
        self._channel = None
        self._exchange = None

    async def connect(self) -> None:
        import aio_pika

        self._connection = await aio_pika.connect_robust(self._settings.rabbitmq_url)
        self._channel = await self._connection.channel(publisher_confirms=True)
        self._exchange, _ = await declare_topology(self._channel, self._settings)

    async def publish(self, event: DocumentEvent) -> None:
        import aio_pika

        if self._exchange is None:
            await self.connect()
        message = aio_pika.Message(
            body=event.to_json().encode(),
            content_type="application/json",
            message_id=event.event_id,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # survives a broker restart
            headers={"tenant_id": event.tenant_id},
        )
        # Publisher confirms: `publish` only returns once the broker has accepted
        # and persisted the message.
        await self._exchange.publish(message, routing_key=event.type)
        queue_publish_events.labels(outcome="success").inc()

    async def ping(self) -> bool:
        try:
            return self._connection is not None and not self._connection.is_closed
        except Exception:
            return False

    async def close(self) -> None:
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()


async def declare_topology(channel, settings):
    """Declare exchange + main/retry/DLQ queues.

    documents (topic)
      -> documents.index          main work queue, DLX = documents.retry
      -> documents.index.retry    TTL 5s, DLX = documents (message re-enters main queue)
      -> documents.index.dlq      terminal; requires human/automated intervention
    """
    import aio_pika

    exchange = await channel.declare_exchange(settings.queue_exchange, aio_pika.ExchangeType.TOPIC, durable=True)
    retry_exchange = await channel.declare_exchange(f"{settings.queue_exchange}.retry",
                                                    aio_pika.ExchangeType.TOPIC, durable=True)
    dlq_exchange = await channel.declare_exchange(f"{settings.queue_exchange}.dlq",
                                                  aio_pika.ExchangeType.TOPIC, durable=True)

    main_queue = await channel.declare_queue(
        settings.queue_name, durable=True,
        arguments={"x-dead-letter-exchange": f"{settings.queue_exchange}.retry"},
    )
    retry_queue = await channel.declare_queue(
        settings.queue_retry_name, durable=True,
        arguments={"x-message-ttl": settings.queue_retry_delay_ms,
                   "x-dead-letter-exchange": settings.queue_exchange},
    )
    dlq = await channel.declare_queue(settings.queue_dlq_name, durable=True)

    for routing_key in (EVENT_INDEX, EVENT_DELETE):
        await main_queue.bind(exchange, routing_key)
        await retry_queue.bind(retry_exchange, routing_key)
        await dlq.bind(dlq_exchange, routing_key)
    return exchange, main_queue


class InMemoryPublisher:
    """Test double that behaves like a durable queue: events accumulate until
    drained, so tests can assert the async boundary explicitly (a document is
    NOT searchable until the worker has consumed its event)."""

    def __init__(self) -> None:
        self.events: list[DocumentEvent] = []
        self.available = True
        self.published_count = 0

    async def publish(self, event: DocumentEvent) -> None:
        if not self.available:
            queue_publish_events.labels(outcome="failure").inc()
            raise ConnectionError("queue unavailable")
        self.events.append(event)
        self.published_count += 1
        queue_publish_events.labels(outcome="success").inc()

    def drain(self) -> list[DocumentEvent]:
        events, self.events = self.events, []
        return events

    async def ping(self) -> bool:
        return self.available

    async def close(self) -> None:
        self.events.clear()
