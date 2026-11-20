"""How events combine into a company's current state.

Delivery is at-least-once and out of order, so the rule has to give the same answer whatever order the events
arrive in, and however many times each arrives. It does that field by field:

    each field holds the value from the newest event that said something about that field.

"Newest" is `(updatedAt, message_id)`, so even two events with the same timestamp resolve the same way everywhere.
A partial event (say, only a company name) therefore never erases a description another event provided, and a late
older event can still fill in a field nobody newer has mentioned.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime

from similarco.events import CompanyEvent

Version = tuple[datetime, str]


@dataclass(frozen=True, slots=True)
class CompanyState:
    registration_number: str
    company_name: str | None = None
    nature_of_business: str | None = None
    state: str | None = None
    industry: str | None = None
    status: str = "active"
    versions: Mapping[str, Version] = field(default_factory=dict)  # field name -> version of the event it came from


def apply_event(state: CompanyState | None, event: CompanyEvent) -> CompanyState:
    """Return the state after `event`. Applying the same event again, or an older one, changes nothing it shouldn't."""
    state = state or CompanyState(event.registration_number)
    changes: dict[str, str] = {}
    versions = dict(state.versions)
    for name, value in event.field_values().items():
        current = versions.get(name)
        if current is None or event.version > current:
            changes[name] = value
            versions[name] = event.version
    return replace(state, versions=versions, **changes) if changes else state


def apply_events(state: CompanyState | None, events: Iterable[CompanyEvent]) -> CompanyState | None:
    for event in events:
        state = apply_event(state, event)
    return state


def versions_to_json(versions: Mapping[str, Version]) -> dict[str, list[str]]:
    return {name: [when.isoformat(), message_id] for name, (when, message_id) in versions.items()}


def versions_from_json(raw: Mapping[str, list[str]]) -> dict[str, Version]:
    return {name: (datetime.fromisoformat(when), message_id) for name, (when, message_id) in raw.items()}
