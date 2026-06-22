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
// again, which is what gives at-least-once delivery.
type Postgres struct {
	pool              *pgxpool.Pool
	visibilityTimeout time.Duration
	pollInterval      time.Duration
}

// Option configures a Postgres backend.
type Option func(*Postgres)

// WithVisibilityTimeout sets how long a reserved job stays invisible before it
// is treated as crashed and redelivered. Default: 30s.
func WithVisibilityTimeout(d time.Duration) Option {
	return func(p *Postgres) { p.visibilityTimeout = d }
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
	}
	for _, opt := range opts {
		opt(p)
	}
	return p, nil
}

// Close releases the connection pool.
func (p *Postgres) Close() { p.pool.Close() }

// Enqueue inserts a job in the 'queued' state. Because this is a plain INSERT
// in the caller's transaction scope, a job can be enqueued atomically with the
// business data it concerns (transactional enqueue, no outbox needed).
func (p *Postgres) Enqueue(ctx context.Context, job Job) error {
	_, err := p.pool.Exec(ctx,
		`INSERT INTO jobs (id, payload) VALUES ($1, $2)`,
		job.ID, job.Payload)
	if err != nil {
		return fmt.Errorf("enqueue %q: %w", job.ID, err)
	}
	return nil
}

// dequeueSQL reserves the oldest eligible job in one atomic statement. A row is
// eligible when it is queued, OR it is running but its lease expired (the
// worker that held it is assumed crashed — this is the reaper, inlined). The
// inner SELECT ... FOR UPDATE SKIP LOCKED skips rows other workers are
// reserving right now, so concurrent dequeues never collide. $1 is the
// visibility timeout as a Postgres interval literal.
const dequeueSQL = `
UPDATE jobs
SET status = 'running', locked_at = now(), attempts = attempts + 1
WHERE id = (
    SELECT id FROM jobs
    WHERE status = 'queued'
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

// Nack returns a reserved job to the queue for redelivery. Retry caps,
// exponential backoff, and the dead-letter path are layered on in F5; here it
// simply clears the lease so the next Dequeue picks it up again. Like Ack it is
// idempotent — nacking an unknown or already-queued row is a no-op.
func (p *Postgres) Nack(ctx context.Context, job Job) error {
	if _, err := p.pool.Exec(ctx,
		`UPDATE jobs SET status = 'queued', locked_at = NULL WHERE id = $1`, job.ID); err != nil {
		return fmt.Errorf("nack %q: %w", job.ID, err)
	}
	return nil
}
