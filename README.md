# SimilarCo: Company Similarity Recommender

SimilarCo powers a **"Similar companies"** panel for a company directory. When a user opens a company's page, SimilarCo returns the companies whose business activities are most alike, so the user can discover peers, suppliers and competitors in the same industry.

It is an event-driven service: company updates stream in through a message queue, each company's business description is turned into a semantic embedding, and similarity search runs inside PostgreSQL with pgvector.

> Built from a take-home brief by OneCredit's Data & AI team. All data is synthetic. The decisions behind it, what was measured, and what is left are in [DESIGN.md](DESIGN.md).

## How it works

```mermaid
flowchart LR
    P[Registry feed producer] -- company-events --> Q[(RabbitMQ)]
    Q --> C[Consumer]
    C -- embed descriptions --> E[Embedding model]
    C --> DB[(PostgreSQL + pgvector)]
    W[Web portal] --> S[Recommendation API]
    S -- similarity search --> DB
```

1. **Ingest:** the registry publishes every company change to the `company-events` queue in RabbitMQ.
2. **Embed:** the consumer reads each event and converts the company's *nature of business* into a vector with an open-source multilingual model running locally on CPU.
3. **Store:** the company and its vector are saved in PostgreSQL using the pgvector extension.
4. **Recommend:** the API takes a company's registration number and returns its nearest neighbours, most similar first, with the details needed to draw each card.

### Real-world data challenges handled

The incoming feed is unvalidated and merged from several sources. All of these are in the sample feed, and each has a test:

- **Duplicate deliveries:** the queue delivers at least once, so the same message can arrive more than once. Replaying the whole feed changes nothing.
- **Out-of-order updates:** an older change can arrive after a newer one. Each event's `updatedAt` decides which version wins, field by field, so the result is the same in any arrival order.
- **Messy records:** truncated JSON, `null` lines, missing or malformed registration numbers (`syn-000123`, ` SYN-000123 `, bare integers), four timestamp formats, inconsistent state casing, missing fields, empty and endlessly repeated descriptions, and mixed English and Malay text.
- **No lost companies:** all 2,000 companies in the feed appear on their own page and in others' recommendations. Messages that cannot be applied are kept in a `rejected_messages` table with the reason, not dropped.

## Tech stack

| Area | Tools |
|---|---|
| Language | Python 3.13, managed with uv |
| Messaging | RabbitMQ 3.13 (`aio-pika`) |
| Database | PostgreSQL 16 + pgvector (`psycopg` 3) |
| Embeddings | `paraphrase-multilingual-mpnet-base-v2` (768 dimensions) through fastembed, on CPU |
| API | FastAPI + uvicorn |
| Infrastructure | Docker Compose, just |

Python 3.13, not the scaffold's 3.14: the embedding runtime (`onnxruntime`) has no wheels for Python 3.14 on Intel Macs. 3.13 works everywhere. See [DESIGN.md](DESIGN.md#9-python-313-not-314).

## Getting started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (must be running)
- [uv](https://docs.astral.sh/uv/) (installs Python 3.13 for you)
- [just](https://just.systems/)
- Internet access on the first consumer run, to download the embedding model (about 1 GB, cached in `.cache/`)

### Start everything

```bash
just setup     # install Python dependencies
just up        # start PostgreSQL (with pgvector) and RabbitMQ
just publish   # replay the company feed onto the queue (2,518 messages)
just drain     # consume the queue into Postgres, then exit (about 20 seconds once the model is cached)
just api       # serve the API on http://localhost:8000
```

Then, in another terminal:

```bash
curl "localhost:8000/companies/SYN-001550/similar?limit=4"
```

| Service | Address | Login |
|---|---|---|
| API | http://localhost:8000 (interactive docs at `/docs`) | none |
| PostgreSQL | `localhost:5432`, database `companies` | `postgres` / `postgres` |
| RabbitMQ UI | http://localhost:15672 | `guest` / `guest` |

Ports can be changed with `POSTGRES_PORT`, `RABBITMQ_PORT` and `RABBITMQ_UI_PORT` (set them before `just up`; the consumer, API and tests read the same variables).

Each `just publish` resends the whole feed. The consumer does not mind: replays change nothing. To start from a clean state, run `just down` and then `just up`.

### Run the consumer

```bash
just drain      # process what is on the queue now, then exit after 5 quiet seconds
just consumer   # or run until Ctrl-C (finishes the current batch, then stops)
```

The consumer creates the tables on startup, so there is no separate migration step. Messages are acknowledged only after their batch is committed, so stopping it at any moment loses nothing.

To run without downloading a model (offline, or for a quick look), set `EMBEDDING_MODEL=hashing-bow`. That is a hashed bag-of-words placeholder with no real semantics; do not judge the recommendations by it.

### Run the recommendation API

```bash
just api
```

`GET /companies/{registrationNumber}/similar?limit=10` returns the company and its nearest neighbours, most similar first:

```json
{
  "company": {"registrationNumber": "SYN-001550", "companyName": "Sinar Teguh Bakery Sdn. Bhd.", "industry": "Food & Beverage",
              "state": "Selangor", "natureOfBusiness": "Operating cafes and coffee shops in the Klang Valley",
              "status": "active", "embeddingBasis": "description"},
  "similar": [
    {"registrationNumber": "SYN-000539", "companyName": "Seri Sejahtera Food Sdn. Bhd.", "industry": "Food & Beverage",
     "state": "Johor", "natureOfBusiness": "...", "status": "active", "similarity": 1.0}
  ]
}
```

`limit` is 1 to 50 (default 10). Errors always look like `{"error": {"code": "...", "message": "..."}}`:

| Status | `code` | When |
|---|---|---|
| 404 | `company_not_found` | the company has never been received |
| 422 | `invalid_registration_number` / `invalid_request` | not a `SYN-######` number, or `limit` out of range |
| 503 | `database_unavailable` | the database cannot be reached; has `Retry-After` |
| 500 | `internal_error` | a bug; details are in the service log only |

A company that exists but cannot be compared (no description, industry or name) returns 200 with an empty `similar` list. `GET /healthz` checks the database.

### Run the tests

```bash
just test       # unit tests, plus tests that use Postgres and RabbitMQ if they are running (`just up`)
just test-all   # also the test that downloads and runs the real embedding model
just lint       # ruff check and format
```

The tests that need services use a throwaway database (`similarco_test`) and a uniquely named queue, never the real `companies` database or the real `company-events` queue. If a service is not running they skip with a message that says so.

### Reproduce the measurements in DESIGN.md

```bash
just eval-quality   # embedding models compared on the feed (the ones already on disk; `just eval-models-all` tests eight models and downloads about 6 GB, one at a time)
just eval-latency   # API latency, and exact search against HNSW on 20,000 and 100,000 synthetic rows (a few minutes)
```

## Configuration

All optional. Defaults match `docker-compose.yml`.

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:$POSTGRES_PORT/companies` | Postgres |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:$RABBITMQ_PORT/` | RabbitMQ (same as the producer) |
| `QUEUE` | `company-events` | queue to consume |
| `EMBEDDING_MODEL` | `sentence-transformers/paraphrase-multilingual-mpnet-base-v2` | another 768-dimension fastembed model, or `hashing-bow`. A different vector size also needs `EMBEDDING_DIM` and `schema.sql` changed; the service refuses to start on a table of the wrong size and says how to fix it |
| `MODEL_CACHE_DIR` | `.cache/fastembed` | where the model is stored |
| `BATCH_SIZE` | `64` | messages per database transaction |

## Project structure

```
data/feed.jsonl        Synthetic feed: 2,518 messages about 2,000 companies (includes duplicates)
producer/publish.py    Replays the feed onto the company-events queue (provided)
docker-compose.yml     PostgreSQL + pgvector and RabbitMQ (provided)
Justfile               Shortcut commands
similarco/
  events.py            Parse and clean one raw message, or say why it cannot be used (pure)
  merge.py             How events combine into a company's state; order-independent (pure)
  embedding.py         What is embedded, and the embedding models
  ingest.py            Apply a batch of messages to Postgres in one transaction
  consumer.py          The RabbitMQ consumer: batching, acks, retries, shutdown
  queries.py, api.py   The similarity search and the HTTP service
  schema.sql, db.py    Tables, and a pgvector-aware connection pool
tests/                 Unit tests, plus Postgres and RabbitMQ integration tests
evaluation/            Scripts behind the numbers in DESIGN.md
DESIGN.md              Decisions, assumptions, measurements, scaling plan and what is left
```

## Notes

- All company data is synthetic. `SYN-` registration numbers are not real SSM numbers.
- On some Intel Macs, RabbitMQ fails to start with `.erlang.cookie: eacces`. A local, gitignored `docker-compose.override.yml` that mounts `/var/lib/rabbitmq` as tmpfs fixes it; queue data then does not survive a container restart, so re-run `just publish` if needed. Even with the override it was intermittent on one machine (about half of cold starts failed, and an immediate second `just up` worked), so if `just up` reports that error, run it again.
