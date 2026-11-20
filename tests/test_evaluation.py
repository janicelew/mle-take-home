"""The hand-made test data behind the model comparison must really come from the feed."""

from evaluation.quality import DISTRACTORS, PAIRS, labelled_descriptions, language


def test_every_pair_and_distractor_is_a_real_description_in_the_feed():
    texts, _ = labelled_descriptions()
    for text in [*(en for en, _ in PAIRS), *(ms for _, ms in PAIRS), *DISTRACTORS]:
        assert any(description.startswith(text) for description in texts), f"not in the feed: {text!r}"


def test_every_description_in_the_feed_has_a_known_language():
    texts, _ = labelled_descriptions()
    assert [t for t in texts if language(t) == "other"] == []


def test_the_two_languages_are_not_confused():
    assert language("Operating cafes and coffee shops in Kuantan") == "en"
    assert language("Mengusahakan kedai kopi dan kafe di Kuantan") == "ms"
    assert language("Penghantaran bungkusan ke seluruh Kuching") == "ms"
