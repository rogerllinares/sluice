-- Sluice durable queue schema. Applied idempotently on backend start.
--
-- seq gives a total enqueue order (created_at alone is not unique under fast
-- inserts, so FIFO would be non-deterministic). status drives the state
-- machine queued -> running -> done | dead; locked_at stamps the reservation so
-- the visibility-timeout reaper can reclaim a crashed worker's in-flight job.
-- attempts counts reservations and is the SINGLE authoritative retry counter
-- (F5 reconciliation): Nack reads it to decide retry-vs-dead-letter.
--
-- F5 columns:
--   available_at      gates redelivery — a Nacked job is set into the future by
--                     the exponential-backoff delay; Dequeue ignores rows whose
--                     available_at is still in the future.
--   idempotency_key   UNIQUE (when present) so a duplicate Enqueue is a no-op —
--                     at-least-once delivery becomes exactly-once effects.
--   dead_lettered_at  stamps when a job exceeded max-retries (status='dead').

CREATE TABLE IF NOT EXISTS jobs (
    seq             BIGINT GENERATED ALWAYS AS IDENTITY,
    id              TEXT PRIMARY KEY,
    payload         BYTEA NOT NULL,
    status          TEXT NOT NULL DEFAULT 'queued',  -- queued | running | done | dead
    attempts        INT  NOT NULL DEFAULT 0,
    locked_at       TIMESTAMPTZ,
    available_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    idempotency_key TEXT,
    dead_lettered_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotent enqueue: at most one live row per key. A partial unique index keeps
-- the constraint scoped to present keys (NULL keys are never deduped).
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
    ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Dequeue scans for the oldest eligible row by (status, available_at, seq); this
-- index keeps that scan cheap as the table grows.
CREATE INDEX IF NOT EXISTS idx_jobs_dequeue ON jobs (status, available_at, seq);
