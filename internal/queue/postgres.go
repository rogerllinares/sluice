package queue

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

const (
	// defaultVisibilityTimeout is how long a reserved job stays invisible to
	// other workers before it is assumed crashed and made eligible again.
	defaultVisibilityTimeout = 30 * time.Second
	// defaultPollInterval is how often Dequeue retries when the queue is empty.
	defaultPollInterval = 50 * time.Millisecond
)

// Postgres is the durable Queue backend (ADR-001). It stores jobs in a single
// table and reserves them with SELECT ... FOR UPDATE SKIP LOCKED, so any number
// of concurrent workers contend on the table without ever reserving the same
// row. A visibility timeout makes a crashed worker's in-flight job visible
// again, which is what gives at-least-once delivery. The durable attempts
// column is the single authoritative retry counter (F5).
type Postgres struct {
	pool              *pgxpool.Pool
	visibilityTimeout time.Duration
	pollInterval      time.Duration
	maxAttempts       int
	baseBackoff       time.Duration
	maxBackoff        time.Duration
	queueDepth        int
}

// Option configures a Postgres backend.
type Option func(*Postgres)

// WithVisibilityTimeout sets how long a reserved job stays invisible before it
// is treated as crashed and redelivered. Default: 30s.
func WithVisibilityTimeout(d time.Duration) Option {
	return func(p *Postgres) { p.visibilityTimeout = d }
}

// WithMaxAttempts sets how many deliveries a job gets before the next Nack
// dead-letters it. Default: 3.
func WithMaxAttempts(n int) Option {
	return func(p *Postgres) { p.maxAttempts = n }
}

// WithBackoff sets the base and max retry-backoff delays. Default: 500ms base,
// 30s max.
func WithBackoff(base, max time.Duration) Option {
	return func(p *Postgres) { p.baseBackoff, p.maxBackoff = base, max }
}

// WithQueueDepth sets the advisory bound Submit sheds at: while the queued
// backlog holds n or more jobs, Submit returns ErrQueueFull. 0 (the default)
// means unbounded — a durable table has no natural capacity, so shedding is
// opt-in. The bound is advisory under concurrency: two Submits can read the
// same backlog count and both insert (MVCC snapshots do not serialize the
// count), overshooting by a request or two. That is acceptable for load
// shedding, whose job is stopping runaway backlog growth, not enforcing an
// exact ceiling.
func WithQueueDepth(n int) Option {
	return func(p *Postgres) { p.queueDepth = n }
}

// NewPostgres opens a connection pool to dsn, applies the schema, and returns a
// ready backend. The caller owns Close.
func NewPostgres(ctx context.Context, dsn string, opts ...Option) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	p := &Postgres{
		pool:              pool,
		visibilityTimeout: defaultVisibilityTimeout,
		pollInterval:      defaultPollInterval,
		maxAttempts:       defaultMaxAttempts,
		baseBackoff:       defaultBaseBackoff,
		maxBackoff:        defaultMaxBackoff,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p, nil
}

// Close releases the connection pool.
func (p *Postgres) Close() { p.pool.Close() }

// pgPayload maps a nil payload to an empty byte slice. A payload-less job is
// legal, but the column is NOT NULL and pgx would send nil as SQL NULL.
func pgPayload(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

// Enqueue inserts a job in the 'queued' state. Because this is a plain INSERT
// in the caller's transaction scope, a job can be enqueued atomically with the
// business data it concerns (transactional enqueue, no outbox needed).
//
// When the job carries an IdempotencyKey, ON CONFLICT DO NOTHING against the
// partial unique index makes a duplicate enqueue a silent no-op — an
// at-least-once producer retrying its enqueue never creates a second job. A nil
// idempotency_key is stored NULL, which the partial index never deduplicates.
func (p *Postgres) Enqueue(ctx context.Context, job Job) error {
	var key *string
	if job.IdempotencyKey != "" {
		key = &job.IdempotencyKey
	}
	_, err := p.pool.Exec(ctx,
		`INSERT INTO jobs (id, payload, idempotency_key) VALUES ($1, $2, $3)
		 ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`,
		job.ID, pgPayload(job.Payload), key)
	if err != nil {
		return fmt.Errorf("enqueue %q: %w", job.ID, err)
	}
	return nil
}

// submitSQL is the bounded insert: the job lands only while the queued backlog
// is below the bound ($4). Rows that are running, done, or dead do not count —
// the bound gates waiting work, mirroring the in-memory buffer. The ON CONFLICT
// clause keeps the idempotent-enqueue behaviour: a duplicate key inserts
// nothing (0 rows), which Submit then distinguishes from a shed.
const submitSQL = `
INSERT INTO jobs (id, payload, idempotency_key)
SELECT $1, $2, $3
WHERE (SELECT count(*) FROM jobs WHERE status = 'queued') < $4
ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`

// Submit is the non-blocking enqueue path over the durable backend: it inserts
// the job while the queued backlog is below the advisory WithQueueDepth bound
// and sheds (ErrQueueFull) otherwise. Without a bound it is a durable Enqueue.
// Dedupe takes precedence over capacity: a duplicate of an already-stored
// idempotency key reports success — the job exists, which is exactly-once
// effects — even when the backlog is full.
func (p *Postgres) Submit(ctx context.Context, job Job) error {
	if p.queueDepth <= 0 {
		return p.Enqueue(ctx, job)
	}
	var key *string
	if job.IdempotencyKey != "" {
		key = &job.IdempotencyKey
	}
	tag, err := p.pool.Exec(ctx, submitSQL, job.ID, pgPayload(job.Payload), key, p.queueDepth)
	if err != nil {
		return fmt.Errorf("submit %q: %w", job.ID, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	// Zero rows: either the capacity gate held the insert back or the
	// idempotency conflict swallowed it. Check the key before reporting a shed.
	if key != nil {
		var dup bool
		if err := p.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM jobs WHERE idempotency_key = $1)`,
			*key).Scan(&dup); err != nil {
			return fmt.Errorf("submit %q: %w", job.ID, err)
		}
		if dup {
			return nil
		}
	}
	return ErrQueueFull
}

// SubmitWait is the blocking enqueue path: the backpressure counterpart to
// Submit's shedding. It retries the bounded Submit every pollInterval until a
// backlog slot frees (mirroring Dequeue's polling contract), or returns
// ctx.Err() when the caller gives up first.
func (p *Postgres) SubmitWait(ctx context.Context, job Job) error {
	for {
		err := p.Submit(ctx, job)
		if !errors.Is(err, ErrQueueFull) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(p.pollInterval):
		}
	}
}

// dequeueSQL reserves the oldest eligible job in one atomic statement. A row is
// eligible when it is queued and its backoff has elapsed (available_at <= now),
// OR it is running but its lease expired (the worker that held it is assumed
// crashed — this is the reaper, inlined). The inner SELECT ... FOR UPDATE SKIP
// LOCKED skips rows other workers are reserving right now, so concurrent
// dequeues never collide. $1 is the visibility timeout as a Postgres interval
// literal. Dead rows are never eligible.
const dequeueSQL = `
UPDATE jobs
SET status = 'running', locked_at = now(), attempts = attempts + 1
WHERE id = (
    SELECT id FROM jobs
    WHERE (status = 'queued' AND available_at <= now())
       OR (status = 'running' AND locked_at < now() - $1::interval)
    ORDER BY seq
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, payload, attempts`

// Dequeue reserves the next available job for a single worker, blocking until
// one is ready or ctx is done. It honours the same blocking contract as the
// in-memory backend by polling: when no row is eligible it waits pollInterval,
// then retries, returning ctx.Err() if ctx is cancelled meanwhile.
func (p *Postgres) Dequeue(ctx context.Context) (Job, error) {
	// Milliseconds is the resolution floor: a sub-millisecond visibility
	// timeout rounds to "0 milliseconds", which would make every running job
	// look expired and be re-reserved immediately. Timeouts are always >= 1ms
	// in practice, so this is a documented floor, not a runtime guard.
	leaseInterval := fmt.Sprintf("%d milliseconds", p.visibilityTimeout.Milliseconds())
	for {
		var job Job
		err := p.pool.QueryRow(ctx, dequeueSQL, leaseInterval).
			Scan(&job.ID, &job.Payload, &job.Attempts)
		switch {
		case err == nil:
			return job, nil
		case errors.Is(err, pgx.ErrNoRows):
			select {
			case <-ctx.Done():
				return Job{}, ctx.Err()
			case <-time.After(p.pollInterval):
			}
		default:
			return Job{}, fmt.Errorf("dequeue: %w", err)
		}
	}
}

// Ack marks a reserved job as successfully processed. Called only AFTER the
// handler returns nil.
//
// It is intentionally idempotent: a row that is already 'done' (double-ack, or
// the visibility-timeout reaper already requeued it) is a no-op, not an error.
// This is also the guarantee boundary of at-least-once delivery — if the lease
// expired and another worker re-reserved this job, this Ack still marks it done,
// so a duplicate effect is possible. Handlers must be idempotent (idempotency
// key, added in F5) to turn at-least-once delivery into exactly-once *effects*.
func (p *Postgres) Ack(ctx context.Context, job Job) error {
	if _, err := p.pool.Exec(ctx,
		`UPDATE jobs SET status = 'done' WHERE id = $1`, job.ID); err != nil {
		return fmt.Errorf("ack %q: %w", job.ID, err)
	}
	return nil
}

// Nack reports a failed delivery and decides the job's fate from its
// authoritative attempts count (F5 — the single retry-accounting site). At or
// past maxAttempts the row moves to status='dead' (the DLQ — queryable via
// DeadLettered, never redelivered). Otherwise it returns to 'queued' with
// available_at pushed into the future by an exponential-backoff-with-jitter
// delay, so the next Dequeue only picks it up once the backoff elapses. Like
// Ack it is idempotent — nacking an unknown row is a no-op.
func (p *Postgres) Nack(ctx context.Context, job Job) error {
	if job.Attempts >= p.maxAttempts {
		if _, err := p.pool.Exec(ctx,
			`UPDATE jobs SET status = 'dead', locked_at = NULL, dead_lettered_at = now()
			 WHERE id = $1`, job.ID); err != nil {
			return fmt.Errorf("dead-letter %q: %w", job.ID, err)
		}
		return nil
	}
	delay := backoffDelay(job.Attempts, p.baseBackoff, p.maxBackoff)
	backoffInterval := fmt.Sprintf("%d milliseconds", delay.Milliseconds())
	if _, err := p.pool.Exec(ctx,
		`UPDATE jobs SET status = 'queued', locked_at = NULL,
		 available_at = now() + $2::interval WHERE id = $1`,
		job.ID, backoffInterval); err != nil {
		return fmt.Errorf("nack %q: %w", job.ID, err)
	}
	return nil
}

// DeadLettered returns the jobs that exceeded maxAttempts (status='dead'),
// oldest first. It is the queryable dead-letter view — these jobs are never
// redelivered; an operator inspects or replays them out of band.
func (p *Postgres) DeadLettered(ctx context.Context) ([]Job, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT id, payload, attempts FROM jobs WHERE status = 'dead' ORDER BY seq`)
	if err != nil {
		return nil, fmt.Errorf("query dead-letter: %w", err)
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.Payload, &j.Attempts); err != nil {
			return nil, fmt.Errorf("scan dead-letter row: %w", err)
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dead-letter rows: %w", err)
	}
	return out, nil
}
