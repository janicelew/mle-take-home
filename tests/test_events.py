"""parse_event: every kind of mess found in the feed, one case at a time."""

import hashlib
import json
from datetime import UTC, datetime

import pytest

from similarco.events import (
    UNKNOWN_TIME,
    CompanyEvent,
    Reason,
    Rejected,
    collapse_repeats,
    normalize_registration_number,
    normalize_state,
    parse_event,
    parse_timestamp,
)

# A real line from the feed, cut off mid-string. Five of these arrive.
TRUNCATED = (
    b'{"type": "company-upserted", "registrationNumber": "SYN-000509", '
    b'"updatedAt": "2026-08-14T09:15:13+08:00", "data": {"companyName": "Bes'
)


def parse(payload: object, message_id: str | None = "m1") -> CompanyEvent | Rejected:
    return parse_event(json.dumps(payload).encode(), message_id)


def upsert(**overrides: object) -> dict:
    base = {
        "type": "company-upserted",
        "registrationNumber": "SYN-000123",
        "updatedAt": "2026-08-14T09:00:00+08:00",
        "data": {"companyName": "Maju Sdn. Bhd.", "natureOfBusiness": "Cafe", "state": "Selangor", "industry": "F&B"},
    }
    return base | overrides


def accepted(payload: object) -> CompanyEvent:
    result = parse(payload)
    assert isinstance(result, CompanyEvent), result
    return result


def test_a_clean_message_is_accepted():
    event = accepted(upsert())
    assert event.registration_number == "SYN-000123"
    assert event.kind == "upserted"
    assert event.updated_at == datetime(2026, 8, 14, 1, 0, tzinfo=UTC)
    assert event.field_values() == {
        "status": "active",
        "company_name": "Maju Sdn. Bhd.",
        "nature_of_business": "Cafe",
        "state": "Selangor",
        "industry": "F&B",
    }


@pytest.mark.parametrize("raw", ["SYN-000123", " SYN-000123 ", "syn-000123", "SYN000123", "Syn 000123"])
def test_registration_number_variants_are_the_same_company(raw: str):
    assert normalize_registration_number(raw) == "SYN-000123"
    assert accepted(upsert(registrationNumber=raw)).registration_number == "SYN-000123"


@pytest.mark.parametrize("raw", [119, 1756, "SYN-12", "ABC-000123", "SYN-0001234", "", True, ["SYN-000123"]])
def test_unusable_registration_numbers_are_rejected_not_guessed(raw: object):
    result = parse(upsert(registrationNumber=raw))
    assert isinstance(result, Rejected)
    assert result.reason is Reason.INVALID_REGISTRATION_NUMBER


def test_missing_registration_number_is_rejected():
    payload = upsert()
    del payload["registrationNumber"]
    result = parse(payload)
    assert isinstance(result, Rejected) and result.reason is Reason.MISSING_REGISTRATION_NUMBER
    result = parse(upsert(registrationNumber=None))
    assert isinstance(result, Rejected) and result.reason is Reason.MISSING_REGISTRATION_NUMBER


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-08-14T09:00:00+08:00", datetime(2026, 8, 14, 1, 0, tzinfo=UTC)),
        ("2026-08-14T01:00:00Z", datetime(2026, 8, 14, 1, 0, tzinfo=UTC)),
        ("2026-08-14T09:00:00", datetime(2026, 8, 14, 1, 0, tzinfo=UTC)),  # no offset: Malaysia time, like the feed
        (1786671038000, datetime(2026, 8, 14, 1, 30, 38, tzinfo=UTC)),  # epoch milliseconds
        (1786671038, datetime(2026, 8, 14, 1, 30, 38, tzinfo=UTC)),  # epoch seconds
    ],
)
def test_timestamps_in_every_format_in_the_feed(raw: object, expected: datetime):
    assert parse_timestamp(raw) == expected
    assert accepted(upsert(updatedAt=raw)).updated_at == expected


@pytest.mark.parametrize("raw", [None, "yesterday", "", True, [], {}])
def test_unreadable_timestamp_keeps_the_company_but_loses_every_comparison(raw: object):
    assert parse_timestamp(raw) is None
    assert accepted(upsert(updatedAt=raw)).updated_at == UNKNOWN_TIME


def test_missing_timestamp_is_not_an_error():
    payload = upsert()
    del payload["updatedAt"]
    assert accepted(payload).updated_at == UNKNOWN_TIME


@pytest.mark.parametrize("raw", ["KUALA LUMPUR", "kuala lumpur", " Kuala   Lumpur ", "Kuala Lumpur"])
def test_state_casing_is_normalised(raw: str):
    assert normalize_state(raw) == "Kuala Lumpur"


def test_unknown_state_is_kept_not_dropped():
    assert normalize_state("narnia") == "Narnia"
    assert normalize_state("Penang") == "Pulau Pinang"
    assert normalize_state("   ") is None


def test_repeated_description_is_collapsed():
    assert collapse_repeats("Trading Trading Trading") == "Trading"
    assert collapse_repeats("Management consulting for SMEs " * 50) == "Management consulting for SMEs"
    assert collapse_repeats("Trading and trading") == "Trading and trading"
    assert collapse_repeats("Cafe") == "Cafe"
    event = accepted(upsert(data={"natureOfBusiness": "Software development " * 100}))
    assert event.nature_of_business == "Software development"


@pytest.mark.parametrize("data", [None, "oops", [], 42, {}])
def test_missing_or_broken_data_still_creates_the_company(data: object):
    event = accepted(upsert(data=data))
    assert event.field_values() == {"status": "active"}


def test_blank_and_non_text_fields_mean_no_information():
    event = accepted(upsert(data={"companyName": "A", "natureOfBusiness": "   ", "state": None, "industry": 7}))
    assert event.field_values() == {"status": "active", "company_name": "A"}


def test_extra_keys_are_ignored():
    event = accepted(upsert(source="partner-api", ingestedBy="ssm-batch", surprise=[1, 2]))
    assert event.registration_number == "SYN-000123"


def test_struck_off_only_asserts_the_status():
    payload = {
        "type": "company-struck-off",
        "registrationNumber": "SYN-000123",
        "updatedAt": "2026-08-14T09:00:00+08:00",
    }
    event = accepted(payload)
    assert event.kind == "struck_off"
    assert event.field_values() == {"status": "struck_off"}
    assert accepted(payload | {"data": {"companyName": "ignored"}}).field_values() == {"status": "struck_off"}


def test_unknown_event_type_is_rejected():
    for value in ("company-renamed", None, 5):
        result = parse(upsert(type=value))
        assert isinstance(result, Rejected) and result.reason is Reason.UNKNOWN_TYPE


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (TRUNCATED, Reason.INVALID_JSON),
        (b"", Reason.INVALID_JSON),
        (b"not json", Reason.INVALID_JSON),
        (b"\xff\xfe\x00", Reason.INVALID_ENCODING),
        (b"null", Reason.NOT_AN_OBJECT),
        (b"[1, 2]", Reason.NOT_AN_OBJECT),
        (b'"text"', Reason.NOT_AN_OBJECT),
    ],
)  # fmt: skip
def test_garbage_is_rejected_with_a_reason_and_never_raises(body: bytes, reason: Reason):
    result = parse_event(body, "m1")
    assert isinstance(result, Rejected)
    assert result.reason is reason
    assert result.message_id == "m1"


def test_message_id_falls_back_to_a_hash_of_the_body():
    body = json.dumps(upsert()).encode()
    event = parse_event(body)
    assert isinstance(event, CompanyEvent)
    assert event.message_id == hashlib.sha256(body).hexdigest()
    rejected = parse_event(b"null")
    assert isinstance(rejected, Rejected) and rejected.message_id == hashlib.sha256(b"null").hexdigest()
