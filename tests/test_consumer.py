"""The consumer against a real RabbitMQ and Postgres, on a private queue (never the real `company-events`)."""

import asyncio
import uuid
from dataclasses import replace

import aio_pika
import pytest
import pytest_asyncio

from similarco.consumer import run_consumer
from similarco.embedding import HashingEmbedder
from similarco.events import CompanyEvent, parse_event
from tests.conftest import delivery_from_line, feed_lines, message


class ExplodesOnPoison(HashingEmbedder):
    """Fails on one specific description, the way a bug on unexpected data would."""

    def embed(self, texts):
        if any("explode" in text for text in texts):
            raise ValueError("cannot embed this")
        return super().embed(texts)


class BrokenModel(HashingEmbedder):
    """Works for the warm-up call, then fails on everything: a problem with the system, not with any message."""

    def embed(self, texts):
        if list(texts) != ["warm-up"]:
            raise RuntimeError("model is broken")
        return super().embed(texts)


@pytest_asyncio.fixture
async def broker(settings):
    """A private durable queue, as the registry would declare it."""
    try:
        connection = await aio_pika.connect_robust(settings.rabbitmq_url, timeout=3)
    except Exception as exc:
        pytest.skip(f"RabbitMQ is not reachable ({exc!r}). Start it with `just up`.")
    channel = await connection.channel()
    queue = await channel.declare_queue(f"company-events-test-{uuid.uuid4().hex[:8]}", durable=True)
    yield channel, queue
    await channel.queue_delete(queue.name)
    await connection.close()


async def publish(channel, queue, deliveries) -> None:
    for delivery in deliveries:
        await channel.default_exchange.publish(
            aio_pika.Message(delivery.body, message_id=delivery.message_id), routing_key=queue.name
        )


async def ready(queue, expected: int, seconds: float = 5.0) -> int:
    """Messages waiting in the queue right now. Re-declares to get a fresh count (the first one is cached)."""
    count = -1
    for _ in range(int(seconds * 10)):
        await queue.declare()
        count = queue.declaration_result.message_count
        if count == expected:
            break
        await asyncio.sleep(0.1)
    return count


async def count(pool, table: str) -> int:
    async with pool.connection() as conn:
        return (await (await conn.execute(f"SELECT count(*) FROM {table}")).fetchone())[0]


@pytest.fixture
def consumer_settings(settings, test_database_url, broker):
    _, queue = broker
    return replace(settings, database_url=test_database_url, queue=queue.name, batch_size=50)


async def test_drains_the_queue_into_postgres(pool, broker, consumer_settings, embedder):
    channel, queue = broker
    lines = feed_lines()[:400]
    await publish(channel, queue, [delivery_from_line(line) for line in lines])
    assert await ready(queue, 400) == 400  # the helper really does see messages, so the 0 below means something

    totals = await run_consumer(consumer_settings, embedder, idle_exit=2)

    assert totals.received == 400
    expected = {e.registration_number for line in lines if isinstance(e := parse_event(line.encode()), CompanyEvent)}
    assert await count(pool, "companies") == len(expected)
    assert await ready(queue, 0) == 0  # everything was acknowledged, including the messages that were rejected


async def test_a_message_that_always_fails_is_set_aside_without_blocking_the_rest(pool, broker, consumer_settings):
    channel, queue = broker
    when = "2026-08-14T09:00:00+08:00"
    good = [message(f"SYN-00000{n}", when, companyName=f"Co {n}", natureOfBusiness="Cafe") for n in range(1, 6)]
    poison = message("SYN-000099", when, companyName="Poison", natureOfBusiness="please explode")
    await publish(channel, queue, [*good[:3], poison, *good[3:]])
    assert await ready(queue, 6) == 6

    await run_consumer(consumer_settings, ExplodesOnPoison(), idle_exit=2)

    assert await count(pool, "companies") == 5
    async with pool.connection() as conn:
        rows = await (await conn.execute("SELECT message_id, reason FROM rejected_messages")).fetchall()
    assert rows == [(poison.message_id, "processing_error")]
    assert await ready(queue, 0) == 0


async def test_when_everything_fails_nothing_is_thrown_away(pool, broker, consumer_settings):
    channel, queue = broker
    when = "2026-08-14T09:00:00+08:00"
    deliveries = [message(f"SYN-00000{n}", when, companyName=f"Co {n}", natureOfBusiness="Cafe") for n in range(1, 6)]
    await publish(channel, queue, deliveries)

    stop = asyncio.Event()
    task = asyncio.create_task(run_consumer(consumer_settings, BrokenModel(), stop=stop))
    await asyncio.sleep(2.5)
    stop.set()
    await asyncio.wait_for(task, timeout=30)

    assert await count(pool, "rejected_messages") == 0  # a broken model must not quarantine good data
    assert await count(pool, "companies") == 0
    assert await ready(queue, 5) == 5  # all back on the queue, to be retried when the system is healthy
