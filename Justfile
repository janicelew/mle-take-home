set shell := ["bash", "-euo", "pipefail", "-c"]

default: setup

# Install the dependencies (the producer's, the consumer's, the service's and the test tools).
setup:
    uv sync --locked

# Start Postgres (with pgvector) and RabbitMQ, and wait until both are healthy.
up:
    docker compose up -d --wait

# Stop everything and delete the volumes.
down:
    docker compose down -v

# Replay the registry feed onto the company-events queue.
publish:
    uv run python -m producer.publish

# Run the consumer until you press Ctrl-C. The first run downloads the embedding model (about 1 GB).
consumer:
    uv run python -m similarco.consumer

# Consume everything currently on the queue, then exit once it has been quiet for 5 seconds.
drain:
    uv run python -m similarco.consumer --idle-exit 5

# Serve the "Similar companies" API on http://localhost:8000 (interactive docs at /docs).
api:
    uv run uvicorn similarco.api:app --port 8000

# Run the tests (needs `just up`; tests that need a service skip with a message if it is not running).
test:
    uv run pytest

# Also run the tests that use the real embedding model.
test-all:
    uv run pytest -m ""

# Lint and check formatting.
lint:
    uv run ruff check .
    uv run ruff format --check .

# Compare the embedding models on the feed (used for the numbers in DESIGN.md).
eval-quality:
    uv run python -m evaluation.quality

# Test all eight embedding models. Downloads about 6 GB in total, one model at a time, deleting each afterwards.
eval-models-all:
    uv run python -m evaluation.quality --all

# Measure API latency, and exact search against an HNSW index at a larger scale (see DESIGN.md).
eval-latency:
    uv run python -m evaluation.latency
