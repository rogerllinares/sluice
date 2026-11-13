package queue

import (
	"context"
	"errors"
)

// Job is the unit of work moving through the queue.
//
// Attempts is owned by the backend: the durable
// backend increments it on every reservation, so a redelivered job (crashed
// worker, visibility timeout elapsed) carries its delivery count. IdempotencyKey
// enables enqueue-time dedupe.
type Job struct {
	ID      string
	Payload []byte

	// Attempts is the number of times this job has been reserved for
	// processing. 0 before the first Dequeue; incremented on each reservation.
	// This is the SINGLE authoritative delivery counter:
	// the backend owns it, and Nack reads it to decide retry-vs-dead-letter.
	// The worker pool no longer counts attempts itself.
	Attempts int

	// IdempotencyKey, when non-empty, makes Enqueue dedupe: a second Enqueue
	// carrying the same key is a no-op, so an at-least-once producer that
	// retries an enqueue does not create a duplicate job. Empty = never deduped.
	IdempotencyKey string
}

// ErrQueueFull is returned by the non-blocking enqueue path (Memory.Submit) when
// a bounded queue is at capacity, so the API can map it to HTTP 429.
var ErrQueueFull = errors.New("queue full")

// Queue is the contract every backend implements (in-memory, Postgres, optional
// Redis); the worker pool and the API only ever see this interface.
type Queue interface {
	// Enqueue adds a job. On a full bounded queue this returns ErrQueueFull
	// (non-blocking shedding); the blocking variant lives on the producer API.
	Enqueue(ctx context.Context, job Job) error

	// Dequeue reserves the next available job for a single worker.
	Dequeue(ctx context.Context) (Job, error)

	// Ack marks a reserved job as successfully processed. Called AFTER work.
	Ack(ctx context.Context, job Job) error

	// Nack reports that processing failed and lets the backend decide the
	// job's fate from its authoritative Attempts count: below MaxAttempts it
	// schedules a redelivery after an exponential-backoff-with-jitter delay;
	// at or past MaxAttempts it dead-letters the job (no more redelivery). This
	// is the single retry-accounting site — the worker pool only reports
	// success (Ack) or failure (Nack); it does not count attempts.
	Nack(ctx context.Context, job Job) error
}
