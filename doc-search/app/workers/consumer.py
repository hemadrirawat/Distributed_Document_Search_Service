"""RabbitMQ consumer process: `python -m app.workers.consumer`.

Batching: messages are accumulated up to `worker_batch_size` or
`worker_batch_linger_ms`, whichever comes first, then written with a single
OpenSearch `_bulk` call. Bulk writes are the difference between a cluster that
ingests millions of documents and one that falls over on per-document refreshes.

Failure handling: a failed batch is nacked without requeue, which dead-letters it
to `documents.index.retry` (TTL 5s), which dead-letters back to the main queue.
After `max_index_attempts` (counted from the `x-death` header) the message is
published to the DLQ and acked, so one poison message cannot block the pipeline.
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal

from app.clients.cache import CacheClient, RedisCache
from app.clients.opensearch_engine import OpenSearchEngine
from app.clients.queue import DocumentEvent, RabbitMQPublisher, declare_topology
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.database import Database
from app.workers.processor import IndexingProcessor
from app.workers.reconciler import Reconciler

logger = logging.getLogger(__name__)


def death_count(message) -> int:
    deaths = (message.headers or {}).get("x-death") or []
    total = 0
    for entry in deaths:
        try:
            total += int(entry.get("count", 0))
        except Exception:
            continue
    return total


class IndexingConsumer:
    def __init__(self, settings) -> None:
        self._settings = settings
        self._database = Database(settings)
        self._engine = OpenSearchEngine(settings)
        self._cache = CacheClient(RedisCache(settings.redis_url, settings.redis_timeout_seconds))
        self._processor = IndexingProcessor(self._database, self._engine, self._cache)
        self._publisher = RabbitMQPublisher(settings)
        self._reconciler = Reconciler(self._database, self._publisher, settings)
        self._stopping = asyncio.Event()

    async def run(self) -> None:
        import aio_pika

        await self._engine.ensure_index()
        await self._publisher.connect()
        connection = await aio_pika.connect_robust(self._settings.rabbitmq_url)
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=self._settings.worker_prefetch)
        _, queue = await declare_topology(channel, self._settings)

        reconcile_task = asyncio.create_task(self._reconciler.run_forever())
        logger.info("indexing worker started", extra={"queue": self._settings.queue_name})

        try:
            async with queue.iterator() as messages:
                batch: list[tuple[object, DocumentEvent]] = []
                deadline = None
                while not self._stopping.is_set():
                    timeout = self._settings.worker_batch_linger_ms / 1000
                    try:
                        message = await asyncio.wait_for(messages.__anext__(), timeout=timeout)
                        try:
                            event = DocumentEvent.from_dict(json.loads(message.body))
                            batch.append((message, event))
                        except Exception:
                            logger.error("malformed event discarded", extra={"body": message.body[:200].decode(errors="replace")})
                            await message.ack()
                        if deadline is None:
                            deadline = asyncio.get_running_loop().time() + timeout
                    except TimeoutError:
                        pass
                    except StopAsyncIteration:
                        break

                    full = len(batch) >= self._settings.worker_batch_size
                    expired = deadline is not None and asyncio.get_running_loop().time() >= deadline
                    if batch and (full or expired):
                        await self._flush(batch, channel)
                        batch, deadline = [], None
                if batch:
                    await self._flush(batch, channel)
        finally:
            reconcile_task.cancel()
            await self._engine.close()
            await self._cache.close()
            await self._publisher.close()
            await connection.close()
            await self._database.close()

    async def _flush(self, batch, channel) -> None:
        events = [event for _, event in batch]
        outcome = await self._processor.process(events)
        failed = set(outcome.failed)
        for message, event in batch:
            if event.document_id in failed:
                if death_count(message) + 1 >= self._settings.max_index_attempts:
                    await self._to_dlq(channel, message, event)
                    await message.ack()
                else:
                    await message.nack(requeue=False)  # -> retry queue via DLX
            else:
                await message.ack()

    async def _to_dlq(self, channel, message, event: DocumentEvent) -> None:
        import aio_pika

        logger.error("event exhausted retries; routing to DLQ",
                     extra={"document_id": event.document_id, "event_type": event.type})
        exchange = await channel.declare_exchange(f"{self._settings.queue_exchange}.dlq",
                                                  aio_pika.ExchangeType.TOPIC, durable=True)
        await exchange.publish(
            aio_pika.Message(body=message.body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=event.type,
        )

    def stop(self) -> None:
        self._stopping.set()


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    consumer = IndexingConsumer(settings)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, consumer.stop)  # graceful drain: finish the batch, then exit
    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
