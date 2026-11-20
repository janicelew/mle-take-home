"""The RabbitMQ consumer: read `company-events`, apply batches to Postgres, acknowledge after the commit.

    uv run python -m similarco.consumer                  # run until stopped (Ctrl-C finishes the current batch)
    uv run python -m similarco.consumer --idle-exit 5    # stop once the queue has been empty for 5 seconds

Failure handling, in order of how often it happens:

* A message that cannot be understood is recorded in `rejected_messages` and acknowledged. It never loops.
* A database or broker hiccup requeues the batch and backs off. Nothing is lost and nothing is dropped.
* An unexpected error in a batch is retried one message at a time. A message that fails alone *while others in the
  same batch succeed* is set aside as a `processing_error`. If every message fails, the system is the problem rather
  than the data, so they are requeued instead of being thrown away.
"""

import argparse
import asyncio
import contextlib
import logging
import signal
import time
from dataclasses import replace

import aio_pika
import psycopg
from aio_pika.abc import AbstractIncomingMessage
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from similarco.config import Settings
from similarco.db import create_pool, ensure_schema
from similarco.embedding import EMBEDDING_DIM, Embedder, build_embedder
from similarco.events import Reason
from similarco.ingest import BatchStats, Delivery, process_batch, quarantine

log = logging.getLogger("similarco.consumer")

# Worth retrying: the cause is the environment, not the message.
TRANSIENT = (psycopg.OperationalError, psycopg.InterfaceError, PoolTimeout, aio_pika.exceptions.AMQPError)
_MAX_BACKOFF = 30.0


async def run_consumer(
    settings: Settings,
    embedder: Embedder,
    *,
    idle_exit: float | None = None,
    stop: asyncio.Event | None = None,
) -> BatchStats:
    """Consume until `stop` is set, or until the queue has been quiet for `idle_exit` seconds."""
    stop = stop or asyncio.Event()
    if embedder.dim != EMBEDDING_DIM:
        raise SystemExit(
            f"{embedder.model_name} makes {embedder.dim}-dimension vectors; schema.sql expects {EMBEDDING_DIM}"
        )

    await ensure_schema(settings.database_url)
    pool = await create_pool(settings.database_url, max_size=2)
    totals = BatchStats()
    try:
        await asyncio.to_thread(embedder.embed, ["warm-up"])  # load the model now, not during the first batch
        log.info("model ready: %s", embedder.model_name)
        connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=settings.batch_size * 2)
            queue = await channel.declare_queue(settings.queue, durable=True)  # exactly how the registry declares it
            inbox: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue()
            consumer_tag = await queue.consume(inbox.put)
            log.info("consuming %s", settings.queue)
            try:
                await _consume(pool, embedder, settings, inbox, totals, idle_exit, stop)
            finally:
                with contextlib.suppress(Exception):
                    await queue.cancel(consumer_tag)  # unacknowledged messages go back to the queue on close
    finally:
        await pool.close()
    log.info(
        "done: received=%d changed=%d embedded=%d rejected=%s",
        totals.received, totals.companies_changed, totals.texts_embedded, dict(totals.rejected),
    )  # fmt: skip
    return totals


async def _consume(
    pool: AsyncConnectionPool,
    embedder: Embedder,
    settings: Settings,
    inbox: "asyncio.Queue[AbstractIncomingMessage]",
    totals: BatchStats,
    idle_exit: float | None,
    stop: asyncio.Event,
) -> None:
    last_activity = time.monotonic()
    failures = 0
    while not stop.is_set():
        batch = await _collect(inbox, settings.batch_size, settings.flush_interval)
        if not batch:
            if idle_exit is not None and time.monotonic() - last_activity >= idle_exit:
                log.info("queue idle for %.0fs, stopping", idle_exit)
                return
            continue
        last_activity = time.monotonic()
        stats, healthy = await handle_batch(pool, embedder, batch)
        totals.add(stats)
        if healthy:
            failures = 0
            log.info(
                "batch of %d: changed=%d embedded=%d rejected=%s",
                stats.received, stats.companies_changed, stats.texts_embedded, dict(stats.rejected),
            )  # fmt: skip
        else:
            failures += 1
            delay = min(2.0**failures, _MAX_BACKOFF)
            log.warning("backing off %.0fs", delay)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)
            last_activity = time.monotonic()


async def _collect(
    inbox: "asyncio.Queue[AbstractIncomingMessage]", size: int, flush_interval: float
) -> list[AbstractIncomingMessage]:
    """Wait for a message, then take up to `size` more, waiting at most `flush_interval` for the batch to fill."""
    try:
        batch = [await asyncio.wait_for(inbox.get(), timeout=0.5)]
    except TimeoutError:
        return []
    deadline = time.monotonic() + flush_interval
    while len(batch) < size:
        try:
            batch.append(inbox.get_nowait())
            continue
        except asyncio.QueueEmpty:
            pass
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            batch.append(await asyncio.wait_for(inbox.get(), timeout=remaining))
        except TimeoutError:
            break
    return batch


async def handle_batch(
    pool: AsyncConnectionPool, embedder: Embedder, messages: list[AbstractIncomingMessage]
) -> tuple[BatchStats, bool]:
    """Apply one batch and settle every message. Returns the stats and whether the system looked healthy."""
    deliveries = [Delivery(m.body, m.message_id) for m in messages]
    try:
        stats = await process_batch(pool, embedder, deliveries)
    except TRANSIENT as exc:
        log.warning("temporary failure, requeueing %d messages: %s", len(messages), exc)
        await _requeue(messages)
        return BatchStats(), False
    except Exception:
        log.exception("batch of %d failed, retrying message by message", len(messages))
        return await _one_by_one(pool, embedder, messages, deliveries)
    await _ack(messages)  # only after the transaction has committed
    return stats, True


async def _one_by_one(
    pool: AsyncConnectionPool,
    embedder: Embedder,
    messages: list[AbstractIncomingMessage],
    deliveries: list[Delivery],
) -> tuple[BatchStats, bool]:
    total = BatchStats()
    failed: list[tuple[AbstractIncomingMessage, Delivery, Exception]] = []
    succeeded = 0
    for index, (message, delivery) in enumerate(zip(messages, deliveries, strict=True)):
        try:
            total.add(await process_batch(pool, embedder, [delivery]))
        except TRANSIENT as exc:
            log.warning("temporary failure, requeueing the rest of the batch: %s", exc)
            await _requeue(messages[index:])
            return total, False
        except Exception as exc:
            failed.append((message, delivery, exc))
            continue
        await _ack([message])
        succeeded += 1

    healthy = True
    for message, delivery, exc in failed:
        if not succeeded:  # nothing in this batch worked: suspect the system, keep the message
            await _requeue([message])
            healthy = False
            continue
        try:
            await quarantine(pool, delivery, exc)
        except Exception:
            log.exception("could not quarantine message %s, requeueing it", message.message_id)
            await _requeue([message])
            healthy = False
            continue
        log.error("set aside message %s: %s: %s", message.message_id, type(exc).__name__, exc)
        total.rejected[str(Reason.PROCESSING_ERROR)] += 1
        await _ack([message])
    return total, healthy


async def _ack(messages: list[AbstractIncomingMessage]) -> None:
    for message in messages:
        try:
            await message.ack()
        except Exception:
            # The channel dropped. The broker redelivers it, and applying it again changes nothing.
            log.warning("could not acknowledge message %s", message.message_id, exc_info=True)


async def _requeue(messages: list[AbstractIncomingMessage]) -> None:
    for message in messages:
        try:
            await message.nack(requeue=True)
        except Exception:
            log.warning("could not requeue message %s", message.message_id, exc_info=True)


async def _main(settings: Settings, idle_exit: float | None) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    embedder = build_embedder(settings.embedding_model, settings.model_cache_dir)
    await run_consumer(settings, embedder, idle_exit=idle_exit, stop=stop)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--idle-exit", type=float, metavar="SECONDS", help="stop after the queue is empty this long")
    parser.add_argument("--batch-size", type=int, help="messages per database transaction")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)-7s %(message)s")
    settings = Settings.from_env()
    if args.batch_size:
        settings = replace(settings, batch_size=args.batch_size)
    asyncio.run(_main(settings, args.idle_exit))


if __name__ == "__main__":
    main()
