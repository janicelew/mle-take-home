"""How events combine. The property that matters: the answer does not depend on arrival order or repeats."""

import itertools
import random
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pytest

from similarco.events import CompanyEvent, Rejected, parse_event
from similarco.merge import CompanyState, apply_event, apply_events, versions_from_json, versions_to_json
from tests.conftest import feed_lines

T0 = datetime(2026, 8, 14, 1, 0, tzinfo=UTC)


def event(minutes: int, mid: str = "m", kind: str = "upserted", **fields: str) -> CompanyEvent:
    return CompanyEvent("SYN-000001", kind, T0 + timedelta(minutes=minutes), mid, **fields)  # type: ignore[arg-type]


def final(*events: CompanyEvent) -> CompanyState:
    state = apply_events(None, events)
    assert state is not None
    return state


def test_newest_event_wins_whatever_order_they_arrive_in():
    old, new = event(1, "a", nature_of_business="old"), event(2, "b", nature_of_business="new")
    assert final(old, new).nature_of_business == "new"
    assert final(new, old).nature_of_business == "new"


def test_a_partial_event_does_not_erase_what_it_does_not_mention():
    full = event(1, "a", company_name="Maju", nature_of_business="Cafe", state="Johor")
    name_only = event(2, "b", company_name="Maju Baru")
    state = final(full, name_only)
    assert (state.company_name, state.nature_of_business, state.state) == ("Maju Baru", "Cafe", "Johor")


def test_an_older_event_still_fills_a_gap_a_newer_one_left():
    newer_without_description = event(5, "b", company_name="Maju")
    older_with_description = event(1, "a", nature_of_business="Cafe")
    for order in itertools.permutations([newer_without_description, older_with_description]):
        assert final(*order).nature_of_business == "Cafe"


def test_each_field_follows_its_own_newest_event():
    # The description is asserted at t=1 and t=3; the t=2 event says nothing about it. Order must not matter.
    events = [
        event(1, "a", nature_of_business="one"),
        event(2, "b", state="Johor"),
        event(3, "c", nature_of_business="three"),
    ]
    for order in itertools.permutations(events):
        state = final(*order)
        assert (state.nature_of_business, state.state) == ("three", "Johor")


def test_applying_an_event_twice_changes_nothing():
    once = final(event(1, "a", company_name="Maju"))
    assert apply_event(once, event(1, "a", company_name="Maju")) == once


def test_equal_timestamps_resolve_the_same_way_in_either_order():
    a, b = event(1, "aaa", company_name="A"), event(1, "bbb", company_name="B")
    assert final(a, b) == final(b, a)
    assert final(a, b).company_name == "B"  # the larger message id wins, deterministically


def test_struck_off_stays_struck_off_until_something_newer_says_otherwise():
    upsert = event(1, "a", company_name="Maju")
    struck = event(2, "b", kind="struck_off")
    late_old_upsert = event(1, "c", company_name="Maju Lama")
    revived = event(3, "d", company_name="Maju")
    assert final(struck, upsert).status == "struck_off"  # struck-off arrived first
    assert final(upsert, struck, late_old_upsert).status == "struck_off"
    assert final(upsert, struck, revived).status == "active"


def test_a_company_known_only_from_a_struck_off_event_exists():
    state = final(event(1, "a", kind="struck_off"))
    assert state.status == "struck_off" and state.company_name is None


def test_an_event_without_a_timestamp_never_beats_one_with_a_timestamp():
    from similarco.events import UNKNOWN_TIME

    undated = CompanyEvent("SYN-000001", "upserted", UNKNOWN_TIME, "u", company_name="Undated")
    dated = event(1, "d", company_name="Dated")
    assert final(undated, dated).company_name == "Dated"
    assert final(dated, undated).company_name == "Dated"
    assert final(undated).company_name == "Undated"  # but it is not lost if nothing better exists


def test_versions_survive_a_round_trip_through_json():
    state = final(event(1, "a", company_name="Maju", state="Johor"))
    assert versions_from_json(versions_to_json(state.versions)) == state.versions


def test_state_is_the_same_in_any_order_for_every_company_in_the_real_feed():
    """The strongest check there is: replay each real company's messages in many orders, with repeats."""
    events_by_company: dict[str, list[CompanyEvent]] = defaultdict(list)
    for line in feed_lines():
        parsed = parse_event(line.encode())
        if not isinstance(parsed, Rejected):
            events_by_company[parsed.registration_number].append(parsed)

    rng = random.Random(2026)
    multi = {reg: events for reg, events in events_by_company.items() if len(events) > 1}
    assert len(multi) > 300  # the feed really does have hundreds of companies with several messages
    for reg, events in multi.items():
        expected = final(*events)
        for _ in range(8):
            shuffled = events + rng.sample(events, k=rng.randint(0, len(events)))  # some repeated
            rng.shuffle(shuffled)
            assert final(*shuffled) == expected, reg
        assert final(*reversed(events)) == expected, reg


@pytest.mark.parametrize("count", [2, 3, 4, 5])
def test_every_permutation_gives_one_answer(count: int):
    events = [event(i, f"m{i}", company_name=f"name{i}", **({"state": "Johor"} if i % 2 else {})) for i in range(count)]
    results = [final(*order) for order in itertools.permutations(events)]
    assert all(result == results[0] for result in results)
