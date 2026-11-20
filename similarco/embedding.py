"""What gets embedded for a company, and the models that do it.

The default model is multilingual because about a quarter of the feed's descriptions are in Malay, which an
English-only model such as bge-small places almost at random (see DESIGN.md for the measurements).
"""

import hashlib
import re
import zlib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal, Protocol

import numpy as np

EMBEDDING_DIM: Final = 768  # must match the `vector(768)` column in schema.sql; db.py checks that it does
DEFAULT_MODEL: Final = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"
HASHING_MODEL: Final = "hashing-bow"  # placeholder: no download, no semantics beyond shared words
MAX_EMBEDDING_CHARS: Final = 512  # the model reads at most 128 tokens anyway
_CACHE_LIMIT: Final = 20_000

Basis = Literal["description", "fallback"]


@dataclass(frozen=True, slots=True)
class EmbeddingInput:
    text: str
    basis: Basis

    @property
    def digest(self) -> str:
        """Identifies what was embedded, so unchanged text is never embedded twice."""
        return hashlib.sha256(f"{self.basis}\n{self.text}".encode()).hexdigest()


def embedding_input(
    *, nature_of_business: str | None, industry: str | None, company_name: str | None
) -> EmbeddingInput | None:
    """The business description when there is one. Otherwise industry and name, so the company can still be found.

    A company with none of these is stored without an embedding: there is nothing to compare.
    """
    if nature_of_business:
        return EmbeddingInput(nature_of_business[:MAX_EMBEDDING_CHARS], "description")
    parts = [part for part in (industry, company_name) if part]
    if parts:
        return EmbeddingInput(": ".join(parts)[:MAX_EMBEDDING_CHARS], "fallback")
    return None


class Embedder(Protocol):
    @property
    def model_name(self) -> str: ...

    @property
    def dim(self) -> int: ...

    def embed(self, texts: Sequence[str]) -> list[np.ndarray]:
        """One unit-length float32 vector per text, in order."""
        ...


def _unit(vector: np.ndarray) -> np.ndarray:
    norm = float(np.linalg.norm(vector))
    return (vector / norm if norm else vector).astype(np.float32)


class HashingEmbedder:
    """Hashed bag-of-words. Deterministic and offline, for tests and as a documented placeholder."""

    model_name = HASHING_MODEL
    dim = EMBEDDING_DIM

    def embed(self, texts: Sequence[str]) -> list[np.ndarray]:
        vectors = []
        for text in texts:
            vector = np.zeros(self.dim, dtype=np.float32)
            for token in re.findall(r"\w+", text.lower()):
                vector[zlib.crc32(token.encode()) % self.dim] += 1
            vectors.append(_unit(vector))
        return vectors


class FastEmbedder:
    """An open-source ONNX model running on the CPU through fastembed. Loaded on first use."""

    def __init__(self, model_name: str = DEFAULT_MODEL, cache_dir: Path | None = None) -> None:
        self._model_name = model_name
        self._cache_dir = cache_dir
        self._model = None
        self._cache: dict[str, np.ndarray] = {}

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def dim(self) -> int:
        from fastembed import TextEmbedding

        supported = {m["model"]: m["dim"] for m in TextEmbedding.list_supported_models()}
        if self._model_name not in supported:
            raise ValueError(f"{self._model_name!r} is not a model fastembed supports (or use {HASHING_MODEL!r})")
        return supported[self._model_name]

    def embed(self, texts: Sequence[str]) -> list[np.ndarray]:
        missing = [text for text in dict.fromkeys(texts) if text not in self._cache]
        if missing:
            if len(self._cache) + len(missing) > _CACHE_LIMIT:
                self._cache.clear()
            for text, vector in zip(missing, self._load().embed(missing), strict=True):
                self._cache[text] = _unit(np.asarray(vector))
        return [self._cache[text] for text in texts]

    def _load(self):
        if self._model is None:
            from fastembed import TextEmbedding

            cache = str(self._cache_dir) if self._cache_dir else None
            self._model = TextEmbedding(self._model_name, cache_dir=cache)
        return self._model


def build_embedder(model_name: str = DEFAULT_MODEL, cache_dir: Path | None = None) -> Embedder:
    if model_name == HASHING_MODEL:
        return HashingEmbedder()
    return FastEmbedder(model_name, cache_dir)
