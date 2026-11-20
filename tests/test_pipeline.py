"""process_batch against a real Postgres: the guarantees the brief asks for, checked on the real feed."""

import random

import pytest

from similarco.embedding import HashingEmbedder
from similarco.events import Rejected, parse_event
from similarco.ingest import BatchStats, Delivery, process_batch
from tests.conftest import delivery_from_line, feed_lines, message, snapshot


class OtherModel(HashingEmbedder):
    model_name = "other-model"


class ExplodingEmbedder(HashingEmbedder):
    def embed(self, texts):
        raise RuntimeError("model crashed")


async def load(pool, embedder, deliveries: list[Delivery], batch_size: int = 64) -> BatchStats:
    total = BatchStats()
    for start in range(0, len(deliveries), batch_size):
        total.add(await process_batch(pool, embedder, deliveries[start : start + batch_size]))
    return total


def feed() -> list[Delivery]:
    return [delivery_from_line(line) for line in feed_lines()]


async def scalar(pool, sql: str):
    async with pool.connection() as conn:
        return (await (await conn.execute(sql)).fetchone())[0]


async def test_vectors_are_stored_in_the_row_not_in_a_toast_table(pool):
    """A search over out-of-line vectors measured 4 to 5 times slower. See the note in schema.sql."""
    storage = await scalar(
        pool, "SELECT attstorage FROM pg_attribute WHERE attrelid = 'companies'::regclass AND attname = 'embedding'"
    )
    assert storage == "p"


async def test_the_whole_feed_loads_and_no_company_is_lost(pool, embedder):
    stats = await load(pool, embedder, feed())

    rows = await snapshot(pool)
    assert [r["registration_number"] for r in rows] == [f"SYN-{n:06d}" for n in range(1, 2001)]
    assert sum(r["status"] == "struck_off" for r in rows) == 8
    assert all(r["embedding"] is not None for r in rows)  # even the 39 with no description can be recommended
    assert sum(r["embedding_basis"] == "fallback" for r in rows) == 39

    assert stats.received == 2518
    assert sum(stats.rejected.values()) == 21
    distinct_rejected = {
        m.message_id for line in feed_lines() if isinstance(m := parse_event(line.encode()), Rejected)
    }  # the six literal `null` lines are one message to the database: same body, same id
    assert await scalar(pool, "SELECT count(*) FROM rejected_messages") == len(distinct_rejected)


async def test_replaying_the_feed_changes_nothing(pool, embedder):
    await load(pool, embedder, feed())
    before = await snapshot(pool)
    last_write = await scalar(pool, "SELECT max(updated_at) FROM companies")

    stats = await load(pool, embedder, feed())

    assert await snapshot(pool) == before
    assert stats.companies_changed == 0 and stats.texts_embedded == 0  # and nothing was written or embedded
    assert await scalar(pool, "SELECT max(updated_at) FROM companies") == last_write


async def test_arrival_order_does_not_matter(pool, embedder):
    deliveries = feed()
    await load(pool, embedder, deliveries)
    expected = await snapshot(pool)

    shuffled = deliveries[:]
    random.Random(7).shuffle(shuffled)
    for order, batch_size in ((deliveries[::-1], 64), (shuffled, 17), (shuffled[::-1], 500)):
        async with pool.connection() as conn:
            await conn.execute("TRUNCATE companies, rejected_messages")
        await load(pool, embedder, order, batch_size)
        assert await snapshot(pool) == expected


async def test_the_newest_update_wins_even_if_it_arrives_first(pool, embedder):
    newer = message("SYN-000001", "2026-08-14T12:00:00+08:00", companyName="New Name", natureOfBusiness="Bakery")
    older = message("SYN-000001", "2026-08-14T09:00:00+08:00", companyName="Old Name", natureOfBusiness="Cafe")
    await process_batch(pool, embedder, [newer])
    await process_batch(pool, embedder, [older])
    [row] = await snapshot(pool)
    assert (row["company_name"], row["nature_of_business"]) == ("New Name", "Bakery")


async def test_the_same_company_twice_in_one_batch(pool, embedder):
    newer = message("SYN-000001", "2026-08-14T12:00:00+08:00", companyName="New", natureOfBusiness="Bakery")
    older = message("SYN-000001", "2026-08-14T09:00:00+08:00", companyName="Old", state="Johor")
    await process_batch(pool, embedder, [newer, older])  # one UPSERT per company, not per message
    [row] = await snapshot(pool)
    assert (row["company_name"], row["state"], row["nature_of_business"]) == ("New", "Johor", "Bakery")


async def test_only_changed_text_is_embedded(pool, embedder):
    first = await process_batch(
        pool, embedder, [message("SYN-000001", "2026-08-14T09:00:00+08:00", natureOfBusiness="Cafe")]
    )
    assert first.texts_embedded == 1

    state_only = message("SYN-000001", "2026-08-14T10:00:00+08:00", state="Johor")
    second = await process_batch(pool, embedder, [state_only])
    assert (second.companies_changed, second.texts_embedded) == (1, 0)

    new_text = message("SYN-000001", "2026-08-14T11:00:00+08:00", natureOfBusiness="Bakery")
    third = await process_batch(pool, embedder, [new_text])
    assert third.texts_embedded == 1


async def test_switching_models_re_embeds_on_replay(pool, embedder):
    msg = message("SYN-000001", "2026-08-14T09:00:00+08:00", natureOfBusiness="Cafe")
    await process_batch(pool, embedder, [msg])
    stats = await process_batch(pool, OtherModel(), [msg])
    assert stats.texts_embedded == 1
    assert await scalar(pool, "SELECT embedding_model FROM companies") == "other-model"


async def test_a_company_with_nothing_to_compare_is_kept_without_an_embedding(pool, embedder):
    only_struck_off = message("SYN-000009", "2026-08-14T09:00:00+08:00", type_="company-struck-off")
    await process_batch(pool, embedder, [only_struck_off])
    [row] = await snapshot(pool)
    assert row["registration_number"] == "SYN-000009"
    assert row["status"] == "struck_off" and row["embedding"] is None and row["embedding_basis"] is None


async def test_struck_off_before_the_upsert_still_ends_struck_off(pool, embedder):
    struck = message("SYN-000001", "2026-08-14T12:00:00+08:00", type_="company-struck-off")
    upsert = message("SYN-000001", "2026-08-14T09:00:00+08:00", companyName="Maju", natureOfBusiness="Cafe")
    await process_batch(pool, embedder, [struck])
    await process_batch(pool, embedder, [upsert])
    [row] = await snapshot(pool)
    assert (row["status"], row["company_name"]) == ("struck_off", "Maju")


async def test_rejections_are_recorded_once_however_often_they_arrive(pool, embedder):
    garbage = Delivery(b"{truncated", "same-id")
    for _ in range(3):
        await process_batch(pool, embedder, [garbage])
    assert await scalar(pool, "SELECT count(*) FROM rejected_messages") == 1
    assert await scalar(pool, "SELECT reason FROM rejected_messages") == "invalid_json"


async def test_a_failed_batch_leaves_nothing_behind(pool):
    batch = [Delivery(b"null", "bad"), message("SYN-000001", "2026-08-14T09:00:00+08:00", natureOfBusiness="Cafe")]
    with pytest.raises(RuntimeError):
        await process_batch(pool, ExplodingEmbedder(), batch)
    assert await scalar(pool, "SELECT count(*) FROM companies") == 0
    assert await scalar(pool, "SELECT count(*) FROM rejected_messages") == 0  # same transaction: all or nothing
