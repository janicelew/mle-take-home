"""The service the portal calls, served in-process on a small hand-made dataset."""

import pytest

from similarco.ingest import process_batch
from tests.conftest import message

WHEN = "2026-08-14T09:00:00+08:00"


@pytest.fixture
async def seeded(pool, embedder):
    cafe = {"natureOfBusiness": "Cafe and coffee shop operator", "industry": "Food & Beverage"}
    coffee = {"natureOfBusiness": "Coffee shop and cafe", "industry": "Food & Beverage"}
    gloves = {"natureOfBusiness": "Rubber gloves manufacturing", "industry": "Manufacturing"}
    deliveries = [
        message("SYN-000001", WHEN, companyName="Kopi Satu", state="Selangor", **cafe),
        message("SYN-000002", WHEN, companyName="Kopi Dua", state="Johor", **coffee),
        message("SYN-000003", WHEN, companyName="Glove Co", state="Perak", **gloves),
        message("SYN-000004", WHEN, companyName="Kopi Mati", state="Kedah", **cafe),
        message("SYN-000004", "2026-08-14T10:00:00+08:00", type_="company-struck-off"),
        message("SYN-000005", WHEN, companyName="Nameless Logistics", industry="Logistics"),  # no description
        message("SYN-000006", WHEN, type_="company-struck-off"),  # known only as struck off: nothing to compare
        message("SYN-000007", WHEN, companyName="Kopi Tiga", state="Perak", **cafe),
        message("SYN-000008", WHEN, companyName="Kopi Empat", state="Sabah", **cafe),
    ]
    await process_batch(pool, embedder, deliveries)


async def test_returns_the_most_similar_first_with_what_a_card_needs(client, seeded):
    response = await client.get("/companies/SYN-000001/similar")
    assert response.status_code == 200
    body = response.json()

    assert body["company"] == {
        "registrationNumber": "SYN-000001", "companyName": "Kopi Satu", "industry": "Food & Beverage",
        "state": "Selangor", "natureOfBusiness": "Cafe and coffee shop operator", "status": "active",
        "embeddingBasis": "description",
    }  # fmt: skip
    similar = body["similar"]
    assert all(
        {"registrationNumber", "companyName", "industry", "state", "similarity"} <= card.keys() for card in similar
    )
    scores = [card["similarity"] for card in similar]
    assert scores == sorted(scores, reverse=True)
    assert {c["registrationNumber"] for c in similar[:2]} == {"SYN-000007", "SYN-000008"}  # identical description
    assert similar[2]["registrationNumber"] == "SYN-000002"  # then the coffee shop that words it differently
    assert similar[-1]["registrationNumber"] in {"SYN-000003", "SYN-000005"}  # unrelated companies come last


async def test_never_recommends_itself_or_a_struck_off_company(client, seeded):
    ids = [
        c["registrationNumber"] for c in (await client.get("/companies/SYN-000001/similar?limit=50")).json()["similar"]
    ]
    assert "SYN-000001" not in ids
    assert "SYN-000004" not in ids  # struck off
    assert "SYN-000006" not in ids  # nothing to compare
    assert "SYN-000005" in ids  # but the company with no description is still recommendable


async def test_limit_is_respected(client, seeded):
    assert len((await client.get("/companies/SYN-000001/similar?limit=2")).json()["similar"]) == 2


async def test_registration_numbers_are_normalised(client, seeded):
    for spelling in ("syn-000001", "SYN000001", "%20SYN-000001%20"):
        assert (await client.get(f"/companies/{spelling}/similar")).status_code == 200


async def test_the_same_page_always_shows_the_same_companies(client, seeded):
    first = (await client.get("/companies/SYN-000007/similar?limit=2")).json()
    again = (await client.get("/companies/SYN-000007/similar?limit=2")).json()
    assert first == again  # ties between identical descriptions are broken by a stable hash, not by chance


async def test_a_struck_off_company_still_has_a_page_with_active_companies_on_it(client, seeded):
    body = (await client.get("/companies/SYN-000004/similar")).json()
    assert body["company"]["status"] == "struck_off"
    assert body["similar"] and all(c["status"] == "active" for c in body["similar"])


async def test_a_company_with_only_industry_and_name_gets_recommendations(client, seeded):
    body = (await client.get("/companies/SYN-000005/similar")).json()
    assert body["company"]["embeddingBasis"] == "fallback"
    assert body["similar"]


async def test_a_company_with_nothing_to_compare_gets_an_empty_list_not_an_error(client, seeded):
    response = await client.get("/companies/SYN-000006/similar")
    assert response.status_code == 200
    assert response.json()["similar"] == []
    assert response.json()["company"]["embeddingBasis"] is None


async def test_unknown_company_is_404(client, seeded):
    response = await client.get("/companies/SYN-999999/similar")
    assert response.status_code == 404
    assert response.json() == {"error": {"code": "company_not_found", "message": "No company SYN-999999"}}


@pytest.mark.parametrize("path", ["/companies/hello/similar", "/companies/SYN-12/similar", "/companies/123/similar"])
async def test_malformed_registration_number_is_422(client, path):
    response = await client.get(path)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_registration_number"


@pytest.mark.parametrize("limit", [0, -1, 51, "abc"])
async def test_limit_out_of_range_is_422(client, seeded, limit):
    response = await client.get(f"/companies/SYN-000001/similar?limit={limit}")
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"


async def test_database_down_is_503_with_retry_after(client, seeded):
    await client.app.state.pool.close()
    response = await client.get("/companies/SYN-000001/similar")
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "database_unavailable"
    assert response.headers["Retry-After"]


async def test_health_and_timing_header(client):
    response = await client.get("/healthz")
    assert response.status_code == 200 and response.json() == {"status": "ok"}
    assert response.headers["Server-Timing"].startswith("app;dur=")
