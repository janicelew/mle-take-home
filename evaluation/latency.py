"""How fast is it, and what happens at 10x and 50x? Reproduces the latency numbers in DESIGN.md.

    uv run python -m evaluation.latency                       # the real API, then 20,000 and 100,000 synthetic rows
    uv run python -m evaluation.latency --rows 20000 50000    # choose the synthetic sizes
    uv run python -m evaluation.latency --skip-scale

Part 1 needs `just up` and a loaded database (`just publish` then `just drain`). It starts its own copy of the API.

Part 2 builds a scratch database (`similarco_bench`, dropped afterwards) of N synthetic companies and compares the
service's exact search with an HNSW index. The vectors are made from the real embeddings: by default half of the
rows repeat a real description exactly (as in the feed, where 2,000 companies share about 170 descriptions) and half
are a perturbed copy (a reworded description). `--duplicates 0` removes the exact repeats. That is a model of the
feed, not the feed itself; the point is the shape of the cost, not the third significant figure.
"""

import argparse
import asyncio
import os
import subprocess
import sys
import time

import httpx
import numpy as np
import psycopg
from pgvector.psycopg import register_vector
from psycopg.conninfo import make_conninfo

from evaluation.quality import labelled_descriptions
from similarco.config import Settings
from similarco.embedding import DEFAULT_MODEL, FastEmbedder

BENCH_DB = "similarco_bench"
PORT = 8765
K = 10


def percentiles(values_ms: list[float]) -> str:
    p50, p95, p99 = np.percentile(values_ms, [50, 95, 99])
    return f"{p50:.1f} | {p95:.1f} | {p99:.1f}"


# --- part 1: the API on the real data ---------------------------------------------------------------------------


def start_api(database_url: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "similarco.api:app", "--port", str(PORT), "--log-level", "warning"],
        env=os.environ | {"DATABASE_URL": database_url},
    )


async def timed_get(client: httpx.AsyncClient, reg: str, gate: asyncio.Semaphore, latencies: list[float]) -> None:
    async with gate:
        start = time.perf_counter()
        response = await client.get(f"/companies/{reg}/similar?limit=10")
        response.raise_for_status()
        latencies.append((time.perf_counter() - start) * 1000)


async def api_latency(settings: Settings, requests: int) -> None:
    with psycopg.connect(settings.database_url) as conn:
        regs = [r for (r,) in conn.execute("SELECT registration_number FROM companies WHERE embedding IS NOT NULL")]
    if not regs:
        print("The companies table is empty. Run `just publish` and `just drain` first.")
        return
    sample = [regs[i] for i in np.random.default_rng(0).integers(0, len(regs), requests)]

    server = start_api(settings.database_url)
    try:
        async with httpx.AsyncClient(base_url=f"http://localhost:{PORT}", timeout=10) as client:
            for _ in range(60):
                try:
                    if (await client.get("/healthz")).status_code == 200:
                        break
                except httpx.TransportError:
                    await asyncio.sleep(0.5)
            for reg in sample[:50]:  # warm up
                await client.get(f"/companies/{reg}/similar")

            print(
                f"\n### API on the real data ({len(regs)} companies, {requests} requests, limit=10, HTTP on localhost)"
            )
            print("| concurrent clients | p50 ms | p95 ms | p99 ms | requests/s |")
            print("|---|---|---|---|---|")
            for clients in (1, 8, 32):
                gate, latencies = asyncio.Semaphore(clients), []
                start = time.perf_counter()
                await asyncio.gather(*(timed_get(client, reg, gate, latencies) for reg in sample))
                print(f"| {clients} | {percentiles(latencies)} | {requests / (time.perf_counter() - start):.0f} |")
    finally:
        server.terminate()
        server.wait(timeout=10)


# --- part 2: exact search against HNSW at scale -------------------------------------------------------------------

EXACT = """
    SELECT id, embedding <=> %(v)s AS d FROM b
    WHERE active AND id <> %(id)s
    ORDER BY embedding <=> %(v)s, md5(id::text || %(id)s::text)
    LIMIT 10
"""
# The service's tie-break (md5) stops Postgres using the vector index, so at scale the index finds a pool of candidates
# and the tie-break orders only those.
ANN = """
    SELECT id, d FROM (
        SELECT id, embedding <=> %(v)s AS d FROM b WHERE active AND id <> %(id)s
        ORDER BY embedding <=> %(v)s LIMIT %(pool)s
    ) AS candidates
    ORDER BY d, md5(id::text || %(id)s::text)
    LIMIT 10
"""


def synthetic_vectors(rows: int, duplicate_share: float, seed: int = 0) -> np.ndarray:
    texts, _ = labelled_descriptions()
    base = np.array(FastEmbedder(DEFAULT_MODEL, Settings.from_env().model_cache_dir).embed(texts))
    rng = np.random.default_rng(seed)
    weights = 1 / np.arange(1, len(base) + 1) ** 0.8  # some descriptions are much more common than others
    pick = rng.choice(len(base), size=rows, p=weights / weights.sum())
    vectors = base[pick].copy()
    reworded = rng.random(rows) >= duplicate_share
    noise = rng.normal(size=(int(reworded.sum()), base.shape[1])).astype(np.float32)
    vectors[reworded] += 0.3 * noise / np.linalg.norm(noise, axis=1, keepdims=True)
    return (vectors / np.linalg.norm(vectors, axis=1, keepdims=True)).astype(np.float32)


def timed(conn: psycopg.Connection, sql: str, params: dict) -> tuple[list[tuple], float]:
    start = time.perf_counter()
    rows = conn.execute(sql, params).fetchall()
    return rows, (time.perf_counter() - start) * 1000


def scale_benchmark(settings: Settings, rows: int, queries: int, duplicate_share: float) -> None:
    admin_url = make_conninfo(settings.database_url, dbname="postgres")
    bench_url = make_conninfo(settings.database_url, dbname=BENCH_DB)
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(f"DROP DATABASE IF EXISTS {BENCH_DB} WITH (FORCE)")
        admin.execute(f"CREATE DATABASE {BENCH_DB}")
    try:
        with psycopg.connect(bench_url, autocommit=True) as conn:
            conn.execute("CREATE EXTENSION vector")
            register_vector(conn)
            vectors = synthetic_vectors(rows, duplicate_share)
            conn.execute(
                f"CREATE TABLE b (id int PRIMARY KEY, active bool NOT NULL, embedding vector({vectors.shape[1]}) NOT NULL)"
            )
            active = np.random.default_rng(1).random(rows) > 0.04  # a few percent struck off
            with (
                conn.cursor() as cur,
                cur.copy("COPY b (id, active, embedding) FROM STDIN WITH (FORMAT BINARY)") as copy,
            ):
                copy.set_types(["int4", "bool", "vector"])
                for i in range(rows):
                    copy.write_row((i, bool(active[i]), vectors[i]))
            conn.execute("ANALYZE b")

            ids = np.random.default_rng(2).choice(np.flatnonzero(active), size=queries, replace=False)
            params = [{"id": int(i), "v": vectors[i], "pool": 5 * K} for i in ids]

            exact = [timed(conn, EXACT, p) for p in params]
            exact_ms = [ms for _, ms in exact]
            kth = [rows_[-1][1] for rows_, _ in exact]  # each query's 10th-best distance

            print(f"\n### {rows:,} synthetic companies, {duplicate_share:.0%} exact duplicates, {queries} queries")
            print("| search | p50 ms | p95 ms | p99 ms | recall@10 | notes |")
            print("|---|---|---|---|---|---|")
            print(f"| exact scan (what the service does now) | {percentiles(exact_ms)} | 1.00 | no index |")

            conn.execute("SET maintenance_work_mem = '512MB'")
            conn.execute("SET max_parallel_maintenance_workers = 0")
            start = time.perf_counter()
            conn.execute(
                "CREATE INDEX ON b USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)"
            )
            build = time.perf_counter() - start
            size_mb = conn.execute("SELECT pg_relation_size('b_embedding_idx') / 1024 / 1024").fetchone()[0]
            conn.execute("SET hnsw.iterative_scan = relaxed_order")  # keep scanning if the active filter removes rows
            plan = "\n".join(r[0] for r in conn.execute("EXPLAIN " + ANN, params[0]).fetchall())
            used = "hnsw" in plan.lower() or "b_embedding_idx" in plan

            for ef in (40, 100, 200):
                conn.execute(f"SET hnsw.ef_search = {ef}")
                results = [timed(conn, ANN, p) for p in params]
                # Recall by distance, not by id: many companies are exact duplicates, so any of them is correct.
                hits = [
                    sum(d <= kth_d + 1e-6 for _, d in rows_) / K for (rows_, _), kth_d in zip(results, kth, strict=True)
                ]
                note = f"ef_search={ef}; index {size_mb} MB, built in {build:.0f}s" if ef == 40 else f"ef_search={ef}"
                note += "" if used else "; INDEX NOT USED"
                print(f"| HNSW | {percentiles([ms for _, ms in results])} | {np.mean(hits):.3f} | {note} |")
    finally:
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(f"DROP DATABASE IF EXISTS {BENCH_DB} WITH (FORCE)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, nargs="*", default=[20_000, 100_000], help="synthetic table sizes")
    parser.add_argument("--queries", type=int, default=200)
    parser.add_argument("--duplicates", type=float, default=0.5, help="share of rows that repeat a description exactly")
    parser.add_argument("--requests", type=int, default=1000, help="API requests per concurrency level")
    parser.add_argument("--skip-api", action="store_true")
    parser.add_argument("--skip-scale", action="store_true")
    args = parser.parse_args()
    settings = Settings.from_env()
    if not args.skip_api:
        asyncio.run(api_latency(settings, args.requests))
    if not args.skip_scale:
        for rows in args.rows:
            scale_benchmark(settings, rows, args.queries, args.duplicates)


if __name__ == "__main__":
    main()
