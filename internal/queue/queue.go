package queue

import (
	"context"
	"errors"
)

// Job is the unit of work moving through the queue.
//
// ID and Payload were pinned by F1. Attempts was added by F2: the durable
// backend increments it on every reservation, so a redelivered job (crashed
// worker, visibility timeout elapsed) carries its delivery count. IdempotencyKey
// was added by F5 for enqueue-time dedupe.
type Job struct {
	ID      string
	Payload []byte

	// Attempts is the number of times this job has been reserved for
	// processing. 0 before the first Dequeue; incremented on each reservation.
	// This is the SINGLE authoritative delivery counter (F5 reconciliation):
	// the backend owns it, and Nack reads it to decide retry-vs-dead-letter.
	// The worker pool no longer counts attempts itself.
	Attempts int

	// IdempotencyKey, when non-empty, makes Enqueue dedupe: a second Enqueue
	// carrying the same key is a no-op, so an at-least-once producer that
	// retries an enqueue does not create a duplicate job. Empty = never deduped.
	IdempotencyKey string
}

// ErrQueueFull is returned by the non-blocking enqueue path (Memory.Submit) when
// a bounded queue is at capacity, so the API can map it to HTTP 429 in F4.
var ErrQueueFull = errors.New("queue full")

// Queue is the contract every backend implements. The signatures were finalised
// against the F1 tests; the durable backends (F2 Postgres, optional Redis) and
// the F5 retry/DLQ work implement the same interface unchanged.
type Queue interface {
	// Enqueue adds a job. On a full bounded queue this returns ErrQueueFull
	// (non-blocking shedding); the blocking variant lives on the producer API.
	Enqueue(ctx context.Context, job Job) error // TODO(F1)

	// Dequeue reserves the next available job for a single worker.
	Dequeue(ctx context.Context) (Job, error) // TODO(F1)

	// Ack marks a reserved job as successfully processed. Called AFTER work.
	Ack(ctx context.Context, job Job) error // TODO(F1)

	// Nack reports that processing failed and lets the backend decide the
	// job's fate from its authoritative Attempts count: below MaxAttempts it
	// schedules a redelivery after an exponential-backoff-with-jitter delay;
	// at or past MaxAttempts it dead-letters the job (no more redelivery). This
	// is the single retry-accounting site (F5) — the worker pool only reports
	// success (Ack) or failure (Nack); it does not count attempts.
	Nack(ctx context.Context, job Job) error
}
