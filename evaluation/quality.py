"""Compare embedding models on the real feed. Reproduces the model table in DESIGN.md.

    uv run python -m evaluation.quality                      # the models that are already on disk (fast)
    uv run python -m evaluation.quality --all                # also the larger ones: downloads each, tests it, deletes it
    uv run python -m evaluation.quality --all --only mpnet   # a chosen few (substring of the label)

No database needed. It reads data/feed.jsonl, applies the consumer's own parsing and merging, and measures:

1. Industry agreement: for each distinct description, how many of its 5 nearest *other* descriptions have the same
   industry? Overall, and again with neighbours restricted to the other language. The feed labels industry and similar
   businesses mostly share one, so this is a proxy; it undercounts, because a bakery and a cafe are similar and carry
   different labels.
2. Translation: the feed has most business descriptions in both English and Malay. For 31 of them I matched the Malay
   text to its English twin by hand (PAIRS below; a test checks each one really is in the feed). Does the Malay text
   find its twin among 43 English candidates?
3. Speed on this CPU, because 20,000 descriptions that are all different have to be embedded once.

The rule for replacing the current model is printed with the result, and was fixed before the larger models ran.
"""

import argparse
import shutil
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from similarco.config import Settings
from similarco.embedding import DEFAULT_MODEL, HASHING_MODEL, build_embedder
from similarco.events import CompanyEvent, parse_event
from similarco.merge import apply_events

FEED = Path("data/feed.jsonl")
DOWNLOADS = Path(".cache/eval_models")

# (English, Malay): the same business written in both languages, as it appears in the feed. Cities are left out
# except where the template is nothing but a city (parcel delivery).
PAIRS = [
    ("Durian orchard and fruit export", "Kebun durian dan eksport buah-buahan"),
    ("Oil palm plantation and fresh fruit bunch supply", "Ladang kelapa sawit dan pembekalan buah tandan segar"),
    ("Poultry farming and egg production", "Penternakan ayam dan pengeluaran telur"),
    ("Civil engineering works for roads and drainage", "Kerja-kerja kejuruteraan awam jalan dan saliran"),
    ("General contractor for residential and commercial buildings", "Kontraktor am bangunan kediaman dan komersial"),
    ("Renovation and interior fit-out for offices", "Pengubahsuaian dan kerja dalaman pejabat"),
    ("Private kindergarten and childcare", "Tadika swasta dan taska"),
    ("Tuition centre for primary and secondary students", "Pusat tuisyen untuk pelajar sekolah rendah dan menengah"),
    ("Bakery producing bread, cakes and pastries for retail outlets", "Pengeluaran roti dan kek untuk kedai runcit"),
    ("Halal catering for corporate events and weddings", "Perkhidmatan katering makanan halal untuk majlis"),
    ("Operating cafes and coffee shops", "Mengusahakan kedai kopi dan kafe"),
    ("Restaurant serving Malaysian and Chinese cuisine", "Restoran masakan Melayu dan Cina"),
    ("Community pharmacy and health supplements", "Farmasi komuniti dan makanan tambahan"),
    ("Home nursing care for the elderly", "Perkhidmatan jagaan warga emas di rumah"),
    ("General practice clinic", "Klinik perubatan am"),
    ("Boutique hotel", "Hotel butik"),
    ("Homestay and chalet operations by the beach", "Pengusaha homestay dan chalet tepi pantai"),
    ("Travel agency for umrah and inbound tourism", "Agensi pelancongan umrah dan pelancongan masuk"),
    ("Cold chain transport and storage of frozen goods", "Perkhidmatan pengangkutan dan penyimpanan barangan sejuk beku"),
    ("Freight forwarding and customs brokerage", "Agensi penghantaran fret dan kastam"),
    ("Last-mile parcel delivery across Seremban", "Penghantaran bungkusan ke seluruh Seremban"),
    ("Manufacture of furniture from rubberwood", "Pembuatan perabot daripada kayu getah"),
    ("Manufacture of plastic packaging for food products", "Pengilangan bungkusan plastik untuk produk makanan"),
    ("Production of rubber gloves for export", "Pengeluaran sarung tangan getah untuk eksport"),
    ("Accounting, tax and company secretarial services", "Perkhidmatan perakaunan, cukai dan kesetiausahaan syarikat"),
    ("Management consulting for SMEs", "Perunding pengurusan untuk PKS"),
    ("Mini market selling groceries and household goods", "Kedai runcit barangan keperluan harian"),
    ("Online retail of baby products and toys", "Perniagaan dalam talian produk bayi"),
    ("Retail of mobile phones and accessories", "Penjualan telefon bimbit dan aksesori"),
    ("Software development for small business accounting", "Pembangunan perisian perakaunan untuk perniagaan kecil"),
    ("IT support and network installation for offices", "Sokongan IT dan pemasangan rangkaian pejabat"),
]  # fmt: skip

# English businesses in the feed with no Malay twin. They are extra wrong answers a model has to avoid.
DISTRACTORS = [
    "Hydroponic vegetable farming for supermarkets",
    "Supply and installation of steel structures",
    "Professional training courses in accounting and finance",
    "Wholesale distribution of frozen food to restaurants",
    "Dental clinic and orthodontic services",
    "Trucking services for construction materials",
    "Precision machining of metal parts for electronics",
    "Retail of textiles and baju kurung",
    "Architectural design and planning services",
    "Recruitment and staffing agency",
    "Cloud hosting and managed IT services",
    "Mobile app development for e-commerce clients",
]

ENGLISH_STARTS = [en.removesuffix(" Seremban") for en, _ in PAIRS] + DISTRACTORS
MALAY_STARTS = [ms.removesuffix(" Seremban") for _, ms in PAIRS]

# The rule for replacing the current model, fixed before the larger models were run.
MIN_TEXTS_PER_SECOND = 20  # 20,000 distinct descriptions should embed in well under 20 minutes on a laptop
MARGIN = 0.03  # about the noise on 169 descriptions; a smaller win is not worth a bigger model or a new schema


@dataclass(frozen=True)
class Candidate:
    label: str
    model: str
    prefix: str = ""  # some models expect an instruction in front of the text; this is the one for similarity
    large: bool = False  # downloaded for the test and deleted afterwards


CANDIDATES = [
    Candidate("hashed bag-of-words (baseline)", HASHING_MODEL),
    Candidate("bge-small-en-v1.5 (English only)", "BAAI/bge-small-en-v1.5"),
    Candidate("paraphrase-multilingual-MiniLM-L12-v2 (previous choice)", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"),
    Candidate("paraphrase-multilingual-mpnet-base-v2 (current)", DEFAULT_MODEL),
    Candidate("potion-multilingual-128M", "minishlab/potion-multilingual-128M", large=True),
    Candidate("embeddinggemma-300m", "google/embeddinggemma-300m", prefix="task: sentence similarity | query: ", large=True),
    Candidate("Qwen3-Embedding-0.6B (int8)", "Qwen/Qwen3-Embedding-0.6B-Q", large=True),
    Candidate("multilingual-e5-large", "intfloat/multilingual-e5-large", prefix="query: ", large=True),
]  # fmt: skip


@dataclass
class Result:
    label: str
    dim: int = 0
    size_gb: float | None = None
    texts_per_second: float = 0.0
    agreement: tuple[float, float, float] = (0.0, 0.0, 0.0)  # mean, low, high of the 95% interval
    cross_language: tuple[float, float, float] = (0.0, 0.0, 0.0)
    top1: int = 0
    top3: int = 0
    error: str = ""
    extra: dict = field(default_factory=dict)

    @property
    def composite(self) -> float:
        return (self.agreement[0] + self.cross_language[0] + self.top1 / len(PAIRS)) / 3


def labelled_descriptions() -> tuple[list[str], list[str]]:
    """Distinct final descriptions and the industry most companies with that description have."""
    from collections import Counter, defaultdict

    events: dict[str, list[CompanyEvent]] = defaultdict(list)
    for line in FEED.read_text(encoding="utf-8").splitlines():
        parsed = parse_event(line.encode())
        if isinstance(parsed, CompanyEvent):
            events[parsed.registration_number].append(parsed)
    industries: dict[str, Counter] = defaultdict(Counter)
    for group in events.values():
        state = apply_events(None, group)
        if state and state.nature_of_business and state.industry:
            industries[state.nature_of_business][state.industry] += 1
    texts = sorted(industries)
    return texts, [industries[t].most_common(1)[0][0] for t in texts]


def language(text: str) -> str:
    if any(text.startswith(start) for start in MALAY_STARTS):
        return "ms"
    if any(text.startswith(start) for start in ENGLISH_STARTS):
        return "en"
    return "other"


def per_query_precision(vectors: np.ndarray, labels: np.ndarray, queries: np.ndarray, pool: np.ndarray, k: int = 5):
    """For each query, the share of its k nearest neighbours in `pool` (never itself) that have its industry."""
    sims = vectors[queries] @ vectors[pool].T
    sims[queries[:, None] == pool[None, :]] = -np.inf
    nearest = np.argpartition(-sims, k, axis=1)[:, :k]
    return (labels[pool][nearest] == labels[queries][:, None]).mean(axis=1)


def mean_with_interval(values: np.ndarray, rng: np.random.Generator, rounds: int = 2000) -> tuple[float, float, float]:
    boots = rng.choice(values, size=(rounds, len(values))).mean(axis=1)
    low, high = np.percentile(boots, [2.5, 97.5])
    return float(values.mean()), float(low), float(high)


def evaluate(candidate: Candidate, texts: list[str], labels: np.ndarray, languages: np.ndarray, settings: Settings):
    result = Result(candidate.label)
    cache = DOWNLOADS / candidate.model.replace("/", "_") if candidate.large else settings.model_cache_dir
    try:
        embedder = build_embedder(candidate.model, cache)
        result.dim = embedder.dim

        def embed(batch: list[str]) -> np.ndarray:
            return np.array(embedder.embed([candidate.prefix + text for text in batch]))

        vectors = embed(texts)  # the first call also loads (and, for large models, downloads) the model
        everyone = np.arange(len(texts))
        rng = np.random.default_rng(0)
        result.extra["agreement"] = per_query_precision(vectors, labels, everyone, everyone)
        result.agreement = mean_with_interval(result.extra["agreement"], rng)

        malay, english = np.flatnonzero(languages == "ms"), np.flatnonzero(languages == "en")
        across = np.concatenate([
            per_query_precision(vectors, labels, malay, english),
            per_query_precision(vectors, labels, english, malay),
        ])  # fmt: skip
        result.extra["cross"] = across
        result.cross_language = mean_with_interval(across, rng)

        english_pool = [en for en, _ in PAIRS] + DISTRACTORS
        sims = embed([ms for _, ms in PAIRS]) @ embed(english_pool).T
        position = (-sims).argsort(axis=1).argsort(axis=1)[np.arange(len(PAIRS)), np.arange(len(PAIRS))]
        result.top1, result.top3 = int((position == 0).sum()), int((position < 3).sum())

        timing = [f"{text} #{i}" for i, text in enumerate((texts * 2)[:200])]  # unique, so no cache can help
        start = time.perf_counter()
        embed(timing)
        result.texts_per_second = len(timing) / (time.perf_counter() - start)
    except Exception as exc:  # a model that will not load is a result too
        result.error = f"{type(exc).__name__}: {exc}"[:200]
    finally:
        if candidate.large and cache.exists():
            shutil.rmtree(cache, ignore_errors=True)
    return result


def paired_difference(a: np.ndarray, b: np.ndarray, rng: np.random.Generator, rounds: int = 5000):
    """How much better is `a` than `b` on the same queries, and how sure are we? Returns mean, low, high, share > 0."""
    differences = a - b
    boots = rng.choice(differences, size=(rounds, len(differences))).mean(axis=1)
    low, high = np.percentile(boots, [2.5, 97.5])
    return float(differences.mean()), float(low), float(high), float((boots > 0).mean())


def size_in_gb(model: str) -> float | None:
    from fastembed import TextEmbedding

    return next((m.get("size_in_GB") for m in TextEmbedding.list_supported_models() if m["model"] == model), None)


def interval(triple: tuple[float, float, float]) -> str:
    return f"{triple[0]:.3f} ({triple[1]:.2f}-{triple[2]:.2f})"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--all", action="store_true", help="also download and test the larger models")
    parser.add_argument("--only", nargs="*", default=[], help="keep only candidates whose label contains one of these")
    args = parser.parse_args()
    settings = Settings.from_env()

    texts, labels_list = labelled_descriptions()
    languages = np.array([language(t) for t in texts])
    labels = np.array(labels_list)
    print(f"{len(texts)} distinct labelled descriptions: {(languages == 'en').sum()} English, "
          f"{(languages == 'ms').sum()} Malay, {(languages == 'other').sum()} unclassified")  # fmt: skip
    print(f"{len(PAIRS)} Malay-English pairs, {len(PAIRS) + len(DISTRACTORS)} English candidates per Malay text\n")

    chosen = [
        c for c in CANDIDATES if (args.all or not c.large) and (not args.only or any(o in c.label for o in args.only))
    ]
    results = []
    for candidate in chosen:
        print(f"testing {candidate.label} ...", file=sys.stderr, flush=True)
        result = evaluate(candidate, texts, labels, languages, settings)
        result.size_gb = size_in_gb(candidate.model) if candidate.model != HASHING_MODEL else 0.0
        results.append(result)
        print(f"   done: {result.error or f'{result.texts_per_second:.0f} texts/s'}", file=sys.stderr, flush=True)

    print(
        "| model | dim | size | texts/s | industry agreement (95% range) | cross-language agreement | Malay finds English twin: top-1 / top-3 (of 31) | score |"
    )
    print("|---|---|---|---|---|---|---|---|")
    for r in results:
        if r.error:
            print(f"| {r.label} | | | | could not run: {r.error} | | | |")
            continue
        size = "-" if r.size_gb in (None, 0.0) else f"{r.size_gb:.2f} GB"
        print(f"| {r.label} | {r.dim} | {size} | {r.texts_per_second:.0f} | {interval(r.agreement)} | "
              f"{interval(r.cross_language)} | {r.top1} / {r.top3} | {r.composite:.3f} |")  # fmt: skip

    ran = [r for r in results if not r.error]
    current = next((r for r in ran if "(current)" in r.label), None)
    print(f"\nRULE: replace the current model only if another runs at {MIN_TEXTS_PER_SECOND}+ texts/s here AND beats its "
          f"score (the average of the three quality columns) by more than {MARGIN}.")  # fmt: skip
    if current:
        fast = [r for r in ran if r.texts_per_second >= MIN_TEXTS_PER_SECOND]
        best = max(fast, key=lambda r: r.composite)
        slower = [r.label for r in ran if r.texts_per_second < MIN_TEXTS_PER_SECOND and r.composite > best.composite]
        if best is current or best.composite - current.composite <= MARGIN:
            print(
                f"RESULT: keep {current.label} (best eligible: {best.label}, score {best.composite:.3f} vs {current.composite:.3f})."
            )
        else:
            print(f"RESULT: switch to {best.label} (score {best.composite:.3f} vs {current.composite:.3f}).")
        if slower:
            print(f"        Higher scores but too slow on this CPU: {', '.join(slower)}.")

        previous = next((r for r in ran if "(previous choice)" in r.label), None)
        if previous:
            rng = np.random.default_rng(1)
            print(f"\nIs the current model really better than the previous choice? (same queries, {5000} resamples)")
            for name, key in (("industry agreement", "agreement"), ("cross-language agreement", "cross")):
                mean, low, high, share = paired_difference(current.extra[key], previous.extra[key], rng)
                print(
                    f"   {name}: {mean:+.3f} (95% range {low:+.3f} to {high:+.3f}); better in {share:.0%} of resamples"
                )


if __name__ == "__main__":
    main()
