"""Apply a batch of raw queue messages to Postgres.

This is the consumer's whole job apart from talking to RabbitMQ, kept separate so it can be tested without a broker.
One call is one transaction: either every message in the batch is applied (or set aside as rejected), or none is, and
only then does the caller acknowledge them.
"""

import asyncio
import hashlib
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field

import numpy as np
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from similarco.embedding import Embedder, EmbeddingInput, embedding_input
from similarco.events import CompanyEvent, Reason, Rejected, parse_event
from similarco.merge import CompanyState, apply_events, versions_from_json, versions_to_json

_BODY_LIMIT = 10_000  # characters of a rejected message kept for inspection


@dataclass(frozen=True, slots=True)
class Delivery:
    """One message as it came off the queue."""

    body: bytes
    message_id: str | None


@dataclass(slots=True)
class BatchStats:
    received: int = 0
    rejected: Counter[str] = field(default_factory=Counter)
    companies_changed: int = 0
    texts_embedded: int = 0

    def add(self, other: "BatchStats") -> None:
        self.received += other.received
        self.rejected.update(other.rejected)
        self.companies_changed += other.companies_changed
        self.texts_embedded += other.texts_embedded


@dataclass(frozen=True, slots=True)
class _Plan:
    registration_number: str
    state: CompanyState
    state_changed: bool
    wanted: EmbeddingInput | None
    embedding_stale: bool


async def process_batch(pool: AsyncConnectionPool, embedder: Embedder, deliveries: Sequence[Delivery]) -> BatchStats:
    """Parse, merge, embed and store `deliveries` in one transaction. Raises if the database or model fails."""
    stats = BatchStats(received=len(deliveries))
    events_by_company: dict[str, list[CompanyEvent]] = defaultdict(list)
    rejected: list[tuple[Rejected, bytes]] = []
    for delivery in deliveries:
        parsed = parse_event(delivery.body, delivery.message_id)
        if isinstance(parsed, Rejected):
            rejected.append((parsed, delivery.body))
            stats.rejected[str(parsed.reason)] += 1
        else:
            events_by_company[parsed.registration_number].append(parsed)

    async with pool.connection() as conn:
        await _record_rejections(conn, rejected)
        if events_by_company:
            await _apply_events(conn, embedder, events_by_company, stats)
    return stats


async def quarantine(pool: AsyncConnectionPool, delivery: Delivery, error: BaseException) -> None:
    """Set aside a message that is well-formed but could not be processed, so it stops blocking the queue."""
    message_id = delivery.message_id or hashlib.sha256(delivery.body).hexdigest()
    rejection = Rejected(message_id, Reason.PROCESSING_ERROR, f"{type(error).__name__}: {error}")
    async with pool.connection() as conn:
        await _record_rejections(conn, [(rejection, delivery.body)])


async def _record_rejections(conn: AsyncConnection, rejected: Sequence[tuple[Rejected, bytes]]) -> None:
    if not rejected:
        return
    rows = [
        (r.message_id, str(r.reason), r.detail, body.decode("utf-8", "replace").replace("\x00", "�")[:_BODY_LIMIT])
        for r, body in rejected
    ]
    async with conn.cursor() as cur:
        await cur.executemany(
            "INSERT INTO rejected_messages (message_id, reason, detail, body) VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (message_id) DO NOTHING",
            rows,
        )


_LOCK_ROWS = """
    SELECT registration_number, company_name, nature_of_business, state, industry, status, field_versions,
           embedding_text_hash, embedding_model
    FROM companies
    WHERE registration_number = ANY(%s)
    ORDER BY registration_number
    FOR UPDATE
"""
_UPDATE_FIELDS = """
    UPDATE companies
    SET company_name = %s, nature_of_business = %s, state = %s, industry = %s, status = %s,
        field_versions = %s, updated_at = now()
    WHERE registration_number = %s
"""
_UPDATE_EMBEDDING = """
    UPDATE companies
    SET embedding = %s::vector, embedding_basis = %s, embedding_text_hash = %s, embedding_model = %s,
        updated_at = now()
    WHERE registration_number = %s
"""


async def _apply_events(
    conn: AsyncConnection,
    embedder: Embedder,
    events_by_company: dict[str, list[CompanyEvent]],
    stats: BatchStats,
) -> None:
    registration_numbers = sorted(events_by_company)  # a fixed order, so two consumers cannot deadlock
    # Make sure every row exists, then lock them, so a second consumer cannot interleave with this batch.
    await conn.execute(
        "INSERT INTO companies (registration_number) SELECT unnest(%s::text[]) ON CONFLICT DO NOTHING",
        (registration_numbers,),
    )
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_LOCK_ROWS, (registration_numbers,))
        rows = await cur.fetchall()

    plans = [_plan(row, events_by_company[row["registration_number"]], embedder.model_name) for row in rows]

    texts = list(dict.fromkeys(p.wanted.text for p in plans if p.embedding_stale and p.wanted))
    vectors: dict[str, np.ndarray] = {}
    if texts:  # CPU-bound, so off the event loop: the broker connection must keep sending heartbeats
        vectors = dict(zip(texts, await asyncio.to_thread(embedder.embed, texts), strict=True))

    field_rows, embedding_rows = [], []
    for p in plans:
        if p.state_changed:
            s = p.state
            field_rows.append((
                s.company_name, s.nature_of_business, s.state, s.industry, s.status,
                Jsonb(versions_to_json(s.versions)), p.registration_number,
            ))  # fmt: skip
        if p.embedding_stale:
            wanted = p.wanted
            embedding_rows.append((
                vectors[wanted.text] if wanted else None,
                wanted.basis if wanted else None,
                wanted.digest if wanted else None,
                embedder.model_name if wanted else None,
                p.registration_number,
            ))  # fmt: skip

    async with conn.cursor() as cur:
        if field_rows:
            await cur.executemany(_UPDATE_FIELDS, field_rows)
        if embedding_rows:
            await cur.executemany(_UPDATE_EMBEDDING, embedding_rows)

    stats.companies_changed += sum(1 for p in plans if p.state_changed or p.embedding_stale)
    stats.texts_embedded += len(texts)


def _plan(row: dict, events: list[CompanyEvent], model_name: str) -> _Plan:
    before = CompanyState(
        registration_number=row["registration_number"],
        company_name=row["company_name"],
        nature_of_business=row["nature_of_business"],
        state=row["state"],
        industry=row["industry"],
        status=row["status"],
        versions=versions_from_json(row["field_versions"]),
    )
    after = apply_events(before, events) or before
    wanted = embedding_input(
        nature_of_business=after.nature_of_business, industry=after.industry, company_name=after.company_name
    )
    wanted_digest = wanted.digest if wanted else None
    stale = wanted_digest != row["embedding_text_hash"] or (wanted is not None and row["embedding_model"] != model_name)
    return _Plan(row["registration_number"], after, after != before, wanted, stale)
