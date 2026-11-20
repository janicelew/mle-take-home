"""The real feed, end to end through the pure logic: what a registry replay is known to contain."""

from collections import Counter, defaultdict

from similarco.embedding import embedding_input
from similarco.events import UNKNOWN_TIME, CompanyEvent, Reason, Rejected, parse_event
from similarco.merge import apply_events
from tests.conftest import feed_lines


def parse_feed() -> tuple[list[CompanyEvent], list[Rejected]]:
    events, rejected = [], []
    for line in feed_lines():
        parsed = parse_event(line.encode())
        (rejected if isinstance(parsed, Rejected) else events).append(parsed)
    return events, rejected  # type: ignore[return-value]


def test_every_line_is_either_an_event_or_a_reason():
    events, rejected = parse_feed()
    assert len(events) + len(rejected) == len(feed_lines()) == 2518


def test_exactly_the_known_bad_messages_are_rejected():
    _, rejected = parse_feed()
    assert Counter(r.reason for r in rejected) == {
        Reason.INVALID_JSON: 5,  # truncated mid-string
        Reason.NOT_AN_OBJECT: 6,  # the literal `null`
        Reason.MISSING_REGISTRATION_NUMBER: 7,
        Reason.INVALID_REGISTRATION_NUMBER: 3,  # bare integers, company name "X"
    }


def test_all_two_thousand_companies_survive():
    events, _ = parse_feed()
    assert {e.registration_number for e in events} == {f"SYN-{n:06d}" for n in range(1, 2001)}


def test_seven_messages_have_no_usable_timestamp_and_are_still_accepted():
    events, _ = parse_feed()
    assert sum(e.updated_at == UNKNOWN_TIME for e in events) == 7


def test_final_state_of_the_feed():
    events, _ = parse_feed()
    by_company: dict[str, list[CompanyEvent]] = defaultdict(list)
    for e in events:
        by_company[e.registration_number].append(e)
    states = [apply_events(None, group) for group in by_company.values()]

    assert sum(s.status == "struck_off" for s in states) == 8
    assert all(s.company_name for s in states)
    assert {s.state for s in states if s.state} <= {
        "Johor", "Kedah", "Kelantan", "Kuala Lumpur", "Labuan", "Melaka", "Negeri Sembilan", "Pahang", "Perak",
        "Perlis", "Pulau Pinang", "Putrajaya", "Sabah", "Sarawak", "Selangor", "Terengganu",
    }  # fmt: skip

    inputs = [
        embedding_input(nature_of_business=s.nature_of_business, industry=s.industry, company_name=s.company_name)
        for s in states
    ]
    basis = Counter(i.basis if i else None for i in inputs)
    # 39 companies never had a description: they are embedded from industry and name rather than dropped.
    assert basis == {"description": 1961, "fallback": 39}
