"""A database made for a different vector size must fail loudly, not on the first insert."""

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

from similarco.db import ensure_schema
from similarco.embedding import EMBEDDING_DIM

OLD_DATABASE = "similarco_test_old_dim"


@pytest.fixture
def old_database(settings):
    """A database whose `companies` table was created when the model made 384-dimension vectors."""
    admin_url = make_conninfo(settings.database_url, dbname="postgres")
    try:
        with psycopg.connect(admin_url, autocommit=True, connect_timeout=3) as admin:
            admin.execute(f"DROP DATABASE IF EXISTS {OLD_DATABASE} WITH (FORCE)")
            admin.execute(f"CREATE DATABASE {OLD_DATABASE}")
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres is not reachable ({exc}). Start it with `just up`.")
    url = make_conninfo(settings.database_url, dbname=OLD_DATABASE)
    with psycopg.connect(url, autocommit=True) as conn:
        conn.execute("CREATE EXTENSION vector")
        conn.execute("CREATE TABLE companies (registration_number text PRIMARY KEY, embedding vector(384))")
    yield url
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(f"DROP DATABASE IF EXISTS {OLD_DATABASE} WITH (FORCE)")


async def test_a_table_made_for_another_vector_size_is_rejected_with_instructions(old_database):
    with pytest.raises(RuntimeError) as error:
        await ensure_schema(old_database)
    message = str(error.value)
    assert "384-dimension" in message and f"{EMBEDDING_DIM}-dimension" in message
    assert "DROP TABLE companies" in message and "just drain" in message


async def test_a_table_made_for_the_current_size_is_accepted(pool, test_database_url):
    await ensure_schema(test_database_url)  # the test database was created by this version, so this must not raise
