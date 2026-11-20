-- Safe to run repeatedly. The consumer and the API both run it at startup.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS companies (
    registration_number text PRIMARY KEY,                 -- normalised: SYN-000123
    company_name        text,
    nature_of_business  text,
    state               text,
    industry            text,
    status              text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'struck_off')),
    -- Which event each field came from: {"state": ["2026-08-14T01:00:00+00:00", "<message id>"], ...}.
    -- Lets a late, older event fill a gap without overwriting something newer. See merge.py.
    field_versions      jsonb NOT NULL DEFAULT '{}'::jsonb,
    -- NULL when there is nothing to embed (no description, industry or name).
    embedding           vector(768),
    embedding_basis     text CHECK (embedding_basis IN ('description', 'fallback')),
    embedding_text_hash text,                             -- what was embedded; unchanged text is not re-embedded
    embedding_model     text,                             -- a different model means every row must be re-embedded
    first_seen_at       timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now() -- when this service last changed the row
);

-- Keep the vector in the row. A row is about 3.6 kB, over Postgres's 2 kB limit, so by default it moves the vector
-- into a separate TOAST table, and then every distance in a scan pays an extra lookup: measured 67-86 ms against
-- 14-16 ms for the same exact scan over 2,000 rows. Only applies to rows written after this runs; see DESIGN.md.
ALTER TABLE companies ALTER COLUMN embedding SET STORAGE PLAIN;

-- Messages that cannot be applied to any company. Kept for inspection, then acknowledged so they never loop.
CREATE TABLE IF NOT EXISTS rejected_messages (
    message_id  text PRIMARY KEY,
    reason      text NOT NULL,
    detail      text NOT NULL,
    body        text,
    received_at timestamptz NOT NULL DEFAULT now()
);

-- No vector index on purpose: at this size (2,000 rows) an exact scan takes about 15 ms and gives exact answers.
-- DESIGN.md has the measurements, and what to do when that stops being true (about 150 ms at 20,000 rows).
