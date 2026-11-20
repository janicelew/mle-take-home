"""What gets embedded, and (marked slow) whether the real model does what DESIGN.md claims."""

import numpy as np
import pytest

from similarco.config import Settings
from similarco.embedding import (
    EMBEDDING_DIM,
    MAX_EMBEDDING_CHARS,
    FastEmbedder,
    HashingEmbedder,
    embedding_input,
)


def test_a_description_is_embedded_as_is():
    result = embedding_input(nature_of_business="Cafe", industry="Food & Beverage", company_name="Maju")
    assert result is not None and (result.text, result.basis) == ("Cafe", "description")


def test_without_a_description_industry_and_name_stand_in():
    result = embedding_input(nature_of_business=None, industry="Logistics", company_name="Maju Logistik")
    assert result is not None and (result.text, result.basis) == ("Logistics: Maju Logistik", "fallback")
    result = embedding_input(nature_of_business=None, industry=None, company_name="Maju")
    assert result is not None and result.basis == "fallback"


def test_with_nothing_to_go_on_there_is_nothing_to_embed():
    assert embedding_input(nature_of_business=None, industry=None, company_name=None) is None


def test_long_text_is_truncated():
    result = embedding_input(nature_of_business="x" * 5000, industry=None, company_name=None)
    assert result is not None and len(result.text) == MAX_EMBEDDING_CHARS


def test_the_digest_changes_with_the_text_and_the_basis_but_not_the_company():
    a = embedding_input(nature_of_business="Cafe", industry=None, company_name="One")
    b = embedding_input(nature_of_business="Cafe", industry="Retail", company_name="Two")
    c = embedding_input(nature_of_business="Bakery", industry=None, company_name="One")
    d = embedding_input(nature_of_business=None, industry=None, company_name="Cafe")
    assert a and b and c and d
    assert a.digest == b.digest  # same text: the vector can be reused
    assert len({a.digest, c.digest, d.digest}) == 3  # d has the same words as a but a different basis


def test_hashing_embedder_is_deterministic_unit_length_and_shares_words():
    embedder = HashingEmbedder()
    cafe, cafe_again, coffee_cafe, gloves = embedder.embed(
        ["Cafe and coffee", "Cafe and coffee", "Coffee cafe", "Rubber gloves"]
    )
    assert cafe.shape == (EMBEDDING_DIM,) and cafe.dtype == np.float32
    assert np.array_equal(cafe, cafe_again)
    assert abs(float(np.linalg.norm(cafe)) - 1) < 1e-5
    assert float(cafe @ coffee_cafe) > float(cafe @ gloves)


@pytest.mark.slow
def test_the_real_model_puts_malay_next_to_its_english_twin():
    pairs = [
        ("Kerja-kerja kejuruteraan awam jalan dan saliran", "Civil engineering works for roads and drainage"),
        ("Perunding pengurusan untuk PKS", "Management consulting for SMEs"),
        ("Pengeluaran sarung tangan getah untuk eksport", "Production of rubber gloves for export"),
        ("Pengangkutan dan penyimpanan rantaian sejuk", "Cold chain transport and storage of frozen goods"),
        ("Operasi kafe dan kedai kopi", "Operating cafes and coffee shops"),
        ("Perkhidmatan penghantaran bungkusan batu terakhir", "Last-mile parcel delivery"),
    ]
    embedder = FastEmbedder(cache_dir=Settings.from_env().model_cache_dir)
    assert embedder.dim == EMBEDDING_DIM
    malay = np.array(embedder.embed([m for m, _ in pairs]))
    english = np.array(embedder.embed([e for _, e in pairs]))
    assert (np.argmax(malay @ english.T, axis=1) == np.arange(len(pairs))).sum() >= 5
