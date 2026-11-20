"""Turn one raw `company-events` message into a validated event, or say why it can't be used.

Everything here is a pure function: no I/O, no clock, no database. The registry feed is unvalidated, so this
module is where the mess is cleaned up once and the rest of the service can trust what it gets.
"""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from enum import StrEnum
from typing import Final, Literal

MALAYSIA_TIME: Final = timezone(timedelta(hours=8))
# Used when an event has no usable timestamp. The event is still accepted (so the company is not lost), but it
# loses against any event that does have one.
UNKNOWN_TIME: Final = datetime(1970, 1, 1, tzinfo=UTC)

EventKind = Literal["upserted", "struck_off"]

_EVENT_TYPES: Final[dict[str, EventKind]] = {
    "company-upserted": "upserted",
    "company-struck-off": "struck_off",
}
_REGISTRATION_NUMBER: Final = re.compile(r"SYN[-\s]?(\d{6})", re.IGNORECASE)

_STATE_NAMES: Final = (
    "Johor", "Kedah", "Kelantan", "Kuala Lumpur", "Labuan", "Melaka", "Negeri Sembilan", "Pahang", "Perak",
    "Perlis", "Pulau Pinang", "Putrajaya", "Sabah", "Sarawak", "Selangor", "Terengganu",
)  # fmt: skip
_STATES: Final = {name.casefold(): name for name in _STATE_NAMES} | {"penang": "Pulau Pinang", "malacca": "Melaka"}


class Reason(StrEnum):
    """Why a message was set aside instead of being applied."""

    INVALID_ENCODING = "invalid_encoding"
    INVALID_JSON = "invalid_json"
    NOT_AN_OBJECT = "not_an_object"
    UNKNOWN_TYPE = "unknown_type"
    MISSING_REGISTRATION_NUMBER = "missing_registration_number"
    INVALID_REGISTRATION_NUMBER = "invalid_registration_number"
    PROCESSING_ERROR = "processing_error"


@dataclass(frozen=True, slots=True)
class Rejected:
    """A message that cannot be applied to any company."""

    message_id: str
    reason: Reason
    detail: str


@dataclass(frozen=True, slots=True)
class CompanyEvent:
    """One validated change to one company. `None` means the message said nothing about that field."""

    registration_number: str
    kind: EventKind
    updated_at: datetime  # always timezone-aware UTC
    message_id: str
    company_name: str | None = None
    nature_of_business: str | None = None
    state: str | None = None
    industry: str | None = None

    @property
    def version(self) -> tuple[datetime, str]:
        """Orders events: newest `updatedAt` first, and the message id breaks exact ties the same way everywhere."""
        return (self.updated_at, self.message_id)

    def field_values(self) -> dict[str, str]:
        """The fields this event asserts, by column name. Fields the message left out are not included."""
        if self.kind == "struck_off":
            return {"status": "struck_off"}
        values = {
            "status": "active",
            "company_name": self.company_name,
            "nature_of_business": self.nature_of_business,
            "state": self.state,
            "industry": self.industry,
        }
        return {name: value for name, value in values.items() if value is not None}


def normalize_registration_number(value: object) -> str | None:
    """`' syn-000123 '`, `'SYN000123'` and `'SYN-000123'` are the same company. Anything else is not a number."""
    if not isinstance(value, str):
        return None
    match = _REGISTRATION_NUMBER.fullmatch(value.strip())
    return f"SYN-{match.group(1)}" if match else None


def parse_timestamp(value: object) -> datetime | None:
    """Parse the feed's `updatedAt` into UTC, or return None if it can't be read.

    Accepts ISO 8601 with an offset or `Z`, ISO 8601 without a timezone (assumed Malaysia time, +08:00, like the
    rest of the feed), and epoch numbers in milliseconds (or seconds, if the number is small).
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        seconds = value / 1000 if abs(value) >= 1e11 else value
        try:
            return datetime.fromtimestamp(seconds, UTC)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip())
        except ValueError:
            return None
        return (parsed if parsed.tzinfo else parsed.replace(tzinfo=MALAYSIA_TIME)).astimezone(UTC)
    return None


def clean_text(value: object) -> str | None:
    """Collapse whitespace; blank or non-text values mean 'no information'."""
    if not isinstance(value, str):
        return None
    return " ".join(value.split()) or None


def collapse_repeats(text: str) -> str:
    """`'Trading Trading Trading'` -> `'Trading'`. A few feed descriptions repeat one phrase hundreds of times."""
    words = text.split()
    for size in range(1, len(words) // 2 + 1):
        if len(words) % size == 0 and words == words[:size] * (len(words) // size):
            return " ".join(words[:size])
    return text


def normalize_state(value: object) -> str | None:
    """`'KUALA LUMPUR'` and `'kuala lumpur'` are both `'Kuala Lumpur'`. Unknown values are title-cased, not dropped."""
    cleaned = clean_text(value)
    if cleaned is None:
        return None
    return _STATES.get(cleaned.casefold(), cleaned.title())


def normalize_description(value: object) -> str | None:
    cleaned = clean_text(value)
    return collapse_repeats(cleaned) if cleaned else None


def parse_event(body: bytes, message_id: str | None = None) -> CompanyEvent | Rejected:
    """Validate one message body. Never raises: anything unusable comes back as `Rejected` with a reason."""
    mid = message_id or hashlib.sha256(body).hexdigest()

    def reject(reason: Reason, detail: str) -> Rejected:
        return Rejected(mid, reason, detail)

    try:
        payload = json.loads(body.decode("utf-8"))
    except UnicodeDecodeError as exc:
        return reject(Reason.INVALID_ENCODING, str(exc))
    except json.JSONDecodeError as exc:
        return reject(Reason.INVALID_JSON, str(exc))
    if not isinstance(payload, dict):
        return reject(Reason.NOT_AN_OBJECT, f"expected a JSON object, got {type(payload).__name__}")

    kind = _EVENT_TYPES.get(payload.get("type")) if isinstance(payload.get("type"), str) else None
    if kind is None:
        return reject(Reason.UNKNOWN_TYPE, f"type={payload.get('type')!r}")

    raw_registration = payload.get("registrationNumber")
    if raw_registration is None:
        return reject(Reason.MISSING_REGISTRATION_NUMBER, "registrationNumber is missing")
    registration = normalize_registration_number(raw_registration)
    if registration is None:
        return reject(Reason.INVALID_REGISTRATION_NUMBER, f"registrationNumber={raw_registration!r}")

    updated_at = parse_timestamp(payload.get("updatedAt")) or UNKNOWN_TIME
    data = payload.get("data")
    data = data if isinstance(data, dict) and kind == "upserted" else {}

    return CompanyEvent(
        registration_number=registration,
        kind=kind,
        updated_at=updated_at,
        message_id=mid,
        company_name=clean_text(data.get("companyName")),
        nature_of_business=normalize_description(data.get("natureOfBusiness")),
        state=normalize_state(data.get("state")),
        industry=clean_text(data.get("industry")),
    )
