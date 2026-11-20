"""The service the portal calls.

    uv run uvicorn similarco.api:app --port 8000
    curl localhost:8000/companies/SYN-001550/similar?limit=4

What the portal gets when something goes wrong (always `{"error": {"code": ..., "message": ...}}`):

    404 company_not_found            we have never received that company
    422 invalid_registration_number  not a SYN-###### number (or `limit` is out of range)
    503 database_unavailable         with `Retry-After`; the panel is secondary, so the portal can hide it and move on
    500 internal_error               a bug; the details are in the service log, not in the response

A company we know but cannot compare (it has no description, industry or name) is not an error: 200 with an
empty `similar` list.
"""

import logging
import time
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg_pool import PoolTimeout
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from similarco.config import Settings
from similarco.db import create_pool, ensure_schema
from similarco.events import normalize_registration_number
from similarco.queries import find_similar, get_company

log = logging.getLogger("similarco.api")

DEFAULT_LIMIT = 10
MAX_LIMIT = 50
STATEMENT_TIMEOUT_MS = 3000


class _Model(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class Card(_Model):
    """What the portal needs to draw one card, and to link to that company's page."""

    registration_number: str
    company_name: str | None
    industry: str | None
    state: str | None
    nature_of_business: str | None
    status: str


class Company(Card):
    embedding_basis: str | None  # "description", "fallback" (industry and name only), or null (nothing to compare)


class SimilarCompany(Card):
    similarity: float  # cosine similarity, 1.0 is identical


class SimilarResponse(_Model):
    company: Company
    similar: list[SimilarCompany]


class ApiError(Exception):
    def __init__(self, status_code: int, code: str, message: str, headers: dict[str, str] | None = None) -> None:
        super().__init__(message)
        self.status_code, self.code, self.message, self.headers = status_code, code, message, headers


def _error(status_code: int, code: str, message: str, headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse({"error": {"code": code, "message": message}}, status_code=status_code, headers=headers)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await ensure_schema(settings.database_url)
        app.state.pool = await create_pool(
            settings.database_url,
            min_size=2,
            max_size=10,
            statement_timeout_ms=STATEMENT_TIMEOUT_MS,
            autocommit=True,  # the API only reads
        )
        try:
            yield
        finally:
            await app.state.pool.close()

    app = FastAPI(title="SimilarCo", version="1.0", lifespan=lifespan)

    @app.middleware("http")
    async def server_timing(request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        response.headers["Server-Timing"] = f"app;dur={(time.perf_counter() - start) * 1000:.1f}"
        return response

    @app.exception_handler(ApiError)
    async def _api_error(_: Request, exc: ApiError) -> JSONResponse:
        return _error(exc.status_code, exc.code, exc.message, exc.headers)

    @app.exception_handler(RequestValidationError)
    async def _invalid(_: Request, exc: RequestValidationError) -> JSONResponse:
        problems = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors())
        return _error(422, "invalid_request", problems)

    @app.exception_handler(psycopg.Error)
    @app.exception_handler(PoolTimeout)
    async def _database_down(_: Request, exc: Exception) -> JSONResponse:
        log.error("database error: %s: %s", type(exc).__name__, exc)
        return _error(503, "database_unavailable", "The database is not available right now.", {"Retry-After": "2"})

    @app.exception_handler(Exception)
    async def _unexpected(_: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled error", exc_info=exc)
        return _error(500, "internal_error", "Something went wrong.")

    @app.get("/healthz")
    async def healthz(request: Request) -> dict[str, str]:
        async with request.app.state.pool.connection() as conn:
            await conn.execute("SELECT 1")
        return {"status": "ok"}

    @app.get(
        "/companies/{registration_number}/similar",
        response_model=SimilarResponse,
        responses={404: {"description": "company_not_found"}, 503: {"description": "database_unavailable"}},
    )
    async def similar_companies(
        registration_number: str,
        request: Request,
        limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT, description="How many companies to return"),
    ) -> SimilarResponse:
        normalized = normalize_registration_number(registration_number)
        if normalized is None:
            raise ApiError(422, "invalid_registration_number", f"{registration_number!r} is not a SYN-###### number")

        async with request.app.state.pool.connection() as conn:
            company = await get_company(conn, normalized)
            if company is None:
                raise ApiError(404, "company_not_found", f"No company {normalized}")
            vector = company.pop("embedding")
            neighbours = await find_similar(conn, normalized, vector, limit) if vector is not None else []

        return SimilarResponse(
            company=Company.model_validate(company),
            similar=[
                SimilarCompany.model_validate(row | {"similarity": round(row["similarity"], 4)}) for row in neighbours
            ],
        )

    return app


app = create_app()
