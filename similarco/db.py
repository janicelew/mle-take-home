"""Database setup: the schema, and a connection pool that understands pgvector."""

from pathlib import Path
from typing import Final

import psycopg
from pgvector.psycopg import register_vector_async
from psycopg_pool import AsyncConnectionPool

from similarco.embedding import EMBEDDING_DIM

SCHEMA: Final = Path(__file__).with_name("schema.sql")
_SCHEMA_LOCK: Final = 727_001  # arbitrary; stops the consumer and the API creating the schema at the same moment


async def ensure_schema(database_url: str) -> None:
    """Create the extension and tables if they are missing. Idempotent and safe to run concurrently.

    Fails with instructions if an existing table was made for a different model's vector size, which is what
    happens to a database that predates a model change.
    """
    async with await psycopg.AsyncConnection.connect(database_url, autocommit=True) as conn:
        await conn.execute("SELECT pg_advisory_lock(%s)", (_SCHEMA_LOCK,))
        try:
            await conn.execute(SCHEMA.read_text(encoding="utf-8"))
            cursor = await conn.execute(
                "SELECT atttypmod FROM pg_attribute WHERE attrelid = 'companies'::regclass AND attname = 'embedding'"
            )
            (found,) = await cursor.fetchone()
        finally:
            await conn.execute("SELECT pg_advisory_unlock(%s)", (_SCHEMA_LOCK,))
    if found != EMBEDDING_DIM:
        raise RuntimeError(
            f"The companies table holds {found}-dimension vectors but this version makes {EMBEDDING_DIM}-dimension "
            "vectors (the embedding model changed). The table only holds data derived from the feed, so drop it "
            "(`docker compose exec postgres psql -U postgres -d companies -c 'DROP TABLE companies'`) and replay the "
            "feed (`just publish`, then `just drain`), or start clean with `just down` and `just up`."
        )


async def create_pool(
    database_url: str,
    *,
    min_size: int = 1,
    max_size: int = 4,
    statement_timeout_ms: int | None = None,
    autocommit: bool = False,
) -> AsyncConnectionPool:
    """Open a pool whose connections can send and receive `vector` values. Call `ensure_schema` first.

    `autocommit` is for read-only users (the API): it skips the BEGIN and COMMIT round trips around every request.
    """
    kwargs: dict[str, object] = {"autocommit": autocommit}
    if statement_timeout_ms:
        kwargs["options"] = f"-c statement_timeout={statement_timeout_ms}"
    pool = AsyncConnectionPool(
        database_url,
        min_size=min_size,
        max_size=max_size,
        timeout=5,
        kwargs=kwargs,
        configure=register_vector_async,
        open=False,
    )
    await pool.open(wait=True, timeout=15)
    return pool
