"""Settings, read from the environment. Defaults match docker-compose.yml and the producer."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from similarco.embedding import DEFAULT_MODEL

# The registry team owns this queue: durable, no extra arguments.
QUEUE: Final = "company-events"
DEFAULT_BATCH_SIZE: Final = 64
DEFAULT_MODEL_CACHE: Final = ".cache/fastembed"


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    rabbitmq_url: str
    queue: str = QUEUE
    embedding_model: str = DEFAULT_MODEL
    model_cache_dir: Path = Path(DEFAULT_MODEL_CACHE)
    batch_size: int = DEFAULT_BATCH_SIZE
    flush_interval: float = 0.25  # seconds to wait for a batch to fill once it has started

    @classmethod
    def from_env(cls) -> "Settings":
        postgres_port = os.environ.get("POSTGRES_PORT", "5432")
        rabbitmq_port = os.environ.get("RABBITMQ_PORT", "5672")
        return cls(
            database_url=os.environ.get(
                "DATABASE_URL", f"postgresql://postgres:postgres@localhost:{postgres_port}/companies"
            ),
            rabbitmq_url=os.environ.get("RABBITMQ_URL", f"amqp://guest:guest@localhost:{rabbitmq_port}/"),
            queue=os.environ.get("QUEUE", QUEUE),
            embedding_model=os.environ.get("EMBEDDING_MODEL", DEFAULT_MODEL),
            model_cache_dir=Path(os.environ.get("MODEL_CACHE_DIR", DEFAULT_MODEL_CACHE)),
            batch_size=int(os.environ.get("BATCH_SIZE", DEFAULT_BATCH_SIZE)),
        )
