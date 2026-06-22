-- Sluice durable queue schema (F2). Applied idempotently on backend start.
--
-- seq gives a total enqueue order (created_at alone is not unique under fast
-- inserts, so FIFO would be non-deterministic). status drives the state
-- machine queued -> running -> done; locked_at stamps the reservation so the
-- visibility-timeout reaper can reclaim a crashed worker's in-flight job.
-- attempts counts reservations (for F5 retry caps). idempotency_key and
-- backoff columns arrive in F5 when their tests demand them.

CREATE TABLE IF NOT EXISTS jobs (
    seq        BIGINT GENERATED ALWAYS AS IDENTITY,
    id         TEXT PRIMARY KEY,
    payload    BYTEA NOT NULL,
    status     TEXT NOT NULL DEFAULT 'queued',  -- queued | running | done | dead
    attempts   INT  NOT NULL DEFAULT 0,
    locked_at  TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dequeue scans for the oldest eligible row by (status, seq); this index keeps
-- that scan cheap as the table grows.
CREATE INDEX IF NOT EXISTS idx_jobs_dequeue ON jobs (status, seq);
