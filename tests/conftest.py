"""Shared fixtures.

Tests that need Postgres or RabbitMQ use a throwaway database (`similarco_test`) and a uniquely named queue, and skip
with a message if the service is not running (`just up`). They never touch the `companies` database or the real queue.
"""

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import httpx
import psycopg
import pytest
import pytest_asyncio
from psycopg.conninfo import make_conninfo

from similarco.api import create_app
from similarco.config import Settings
from similarco.db import create_pool, ensure_schema
from similarco.embedding import HashingEmbedder
from similarco.ingest import Delivery

FEED = Path(__file__).parent.parent / "data" / "feed.jsonl"
TEST_DATABASE = "similarco_test"


def feed_lines() -> list[str]:
    return FEED.read_text(encoding="utf-8").splitlines()


def delivery_from_line(line: str) -> Delivery:
    """A feed line as the producer publishes it: the id is a hash of the body."""
    return Delivery(line.encode(), hashlib.sha256(line.encode()).hexdigest())


def message(registration_number: str, updated_at: str, type_: str = "company-upserted", **data: object) -> Delivery:
    """A hand-made registry message."""
    payload: dict[str, object] = {"type": type_, "registrationNumber": registration_number, "updatedAt": updated_at}
    if data:
        payload["data"] = data
    return delivery_from_line(json.dumps(payload))


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings.from_env()


@pytest.fixture(scope="session")
def embedder() -> HashingEmbedder:
    return HashingEmbedder()


@pytest.fixture(scope="session")
def test_database_url(settings: Settings):
    admin_url = make_conninfo(settings.database_url, dbname="postgres")
    try:
        with psycopg.connect(admin_url, autocommit=True, connect_timeout=3) as conn:
            conn.execute(f"DROP DATABASE IF EXISTS {TEST_DATABASE} WITH (FORCE)")
            conn.execute(f"CREATE DATABASE {TEST_DATABASE}")
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres is not reachable ({exc}). Start it with `just up`.")
    yield make_conninfo(settings.database_url, dbname=TEST_DATABASE)
    with psycopg.connect(admin_url, autocommit=True) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {TEST_DATABASE} WITH (FORCE)")


@pytest_asyncio.fixture(scope="session")
async def _pool(test_database_url: str):
    await ensure_schema(test_database_url)
    pool = await create_pool(test_database_url, max_size=4)
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def pool(_pool):
    """A pool on an empty test database."""
    async with _pool.connection() as conn:
        await conn.execute("TRUNCATE companies, rejected_messages")
    return _pool


@pytest_asyncio.fixture
async def client(pool, settings: Settings, test_database_url: str):
    """The real API, served in-process on top of the (empty) test database."""
    app = create_app(replace(settings, database_url=test_database_url))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
            http.app = app  # type: ignore[attr-defined]
            yield http


async def snapshot(pool) -> list[dict]:
    """Everything a company row says about the business, in a stable order. Embeddings compared as text."""
    from psycopg.rows import dict_row

    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """SELECT registration_number, company_name, nature_of_business, state, industry, status,
                      field_versions, embedding_basis, embedding_text_hash, embedding_model,
                      embedding::text AS embedding
               FROM companies ORDER BY registration_number"""
        )
        return await cur.fetchall()
