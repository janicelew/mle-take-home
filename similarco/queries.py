"""The two reads behind the "Similar companies" panel."""

import numpy as np
from psycopg import AsyncConnection
from psycopg.rows import dict_row

_COMPANY = """
    SELECT registration_number, company_name, industry, state, nature_of_business, status, embedding_basis, embedding
    FROM companies
    WHERE registration_number = %s
"""

# Nearest active companies by cosine distance. Struck-off companies and companies with nothing to compare are
# never recommended. Many companies share an identical description (so identical vectors), and distance alone
# cannot order those; a stable hash of the pair does, so the same page always shows the same companies but no
# company is systematically favoured across pages.
_SIMILAR = """
    SELECT registration_number, company_name, industry, state, nature_of_business, status,
           1 - (embedding <=> %(vector)s) AS similarity
    FROM companies
    WHERE registration_number <> %(registration_number)s
      AND status = 'active'
      AND embedding IS NOT NULL
    ORDER BY embedding <=> %(vector)s, md5(registration_number || %(registration_number)s)
    LIMIT %(limit)s
"""


async def get_company(conn: AsyncConnection, registration_number: str) -> dict | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_COMPANY, (registration_number,))
        return await cur.fetchone()


async def find_similar(conn: AsyncConnection, registration_number: str, vector: np.ndarray, limit: int) -> list[dict]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_SIMILAR, {"vector": vector, "registration_number": registration_number, "limit": limit})
        return await cur.fetchall()
