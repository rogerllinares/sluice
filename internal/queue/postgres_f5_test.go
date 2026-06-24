package queue

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// postgres_f5_test.go specifies the F5 reliability semantics on the durable
// backend: idempotent enqueue (UNIQUE idempotency key), bounded retries with
// backoff (available_at delay), and the dead-letter path (status='dead'). These
// are integration tests against a real Postgres via testcontainers, so they run
// in CI (Docker required); locally they skip if no Docker daemon is reachable.

// TestPostgresEnqueueDedupesIdempotencyKey proves enqueue-time dedupe: a second
// enqueue with the same idempotency key does not create a second row.
func TestPostgresEnqueueDedupesIdempotencyKey(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	job := Job{ID: "job-1", Payload: []byte("x"), IdempotencyKey: "order-42"}
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	dup := Job{ID: "job-2", Payload: []byte("y"), IdempotencyKey: "order-42"}
	if err := q.Enqueue(ctx, dup); err != nil {
		t.Fatalf("duplicate Enqueue should be a no-op, got: %v", err)
	}

	got := dequeueWithin(t, q, 5*time.Second)
	if got.ID != "job-1" || !bytes.Equal(got.Payload, []byte("x")) {
		t.Fatalf("dequeued %+v, want the first job (job-1/x)", got)
	}
	if err := q.Ack(ctx, got); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// No second row should be dequeueable.
	empty, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := q.Dequeue(empty); err == nil {
		t.Fatal("a second job existed after idempotent dedupe; want only one")
	}
}

// TestPostgresNackDeadLettersAfterMaxAttempts proves the DLQ path: a job nacked
// at MaxAttempts moves to status='dead' and is no longer dequeueable, and shows
// up in DeadLettered().
func TestPostgresNackDeadLettersAfterMaxAttempts(t *testing.T) {
	q := newTestQueue(t,
		WithMaxAttempts(2),
		WithBackoff(5*time.Millisecond, 20*time.Millisecond),
	)
	ctx := context.Background()

	if err := q.Enqueue(ctx, Job{ID: "poison", Payload: []byte("x")}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Attempt 1: dequeue (attempts->1), nack -> retry scheduled (below max).
	first := dequeueWithin(t, q, 5*time.Second)
	if first.Attempts != 1 {
		t.Fatalf("attempt 1 Attempts=%d, want 1", first.Attempts)
	}
	if err := q.Nack(ctx, first); err != nil {
		t.Fatalf("Nack 1: %v", err)
	}

	// Attempt 2: redelivered after backoff (attempts->2), nack -> dead-letter.
	second := dequeueWithin(t, q, 5*time.Second)
	if second.Attempts != 2 {
		t.Fatalf("attempt 2 Attempts=%d, want 2", second.Attempts)
	}
	if err := q.Nack(ctx, second); err != nil {
		t.Fatalf("Nack 2: %v", err)
	}

	// The job must now be dead, not redelivered.
	empty, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if _, err := q.Dequeue(empty); err == nil {
		t.Fatal("a dead-lettered job was redelivered; want none")
	}

	dead, err := q.DeadLettered(ctx)
	if err != nil {
		t.Fatalf("DeadLettered: %v", err)
	}
	if len(dead) != 1 || dead[0].ID != "poison" {
		t.Fatalf("DeadLettered = %+v, want exactly [poison]", dead)
	}
	if dead[0].Attempts != 2 {
		t.Errorf("dead job Attempts=%d, want 2", dead[0].Attempts)
	}
}

// TestPostgresNackBackoffDelaysRedelivery proves the backoff is real: right
// after a nack the job is NOT immediately dequeueable (available_at is in the
// future), but becomes dequeueable once the delay passes.
func TestPostgresNackBackoffDelaysRedelivery(t *testing.T) {
	q := newTestQueue(t,
		WithMaxAttempts(5),
		WithBackoff(800*time.Millisecond, 5*time.Second),
	)
	ctx := context.Background()

	if err := q.Enqueue(ctx, Job{ID: "slow-retry", Payload: []byte("x")}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	first := dequeueWithin(t, q, 5*time.Second)
	if err := q.Nack(ctx, first); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	// Immediately after nack the backoff (>=800ms) should keep it invisible.
	tooSoon, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := q.Dequeue(tooSoon); err == nil {
		t.Fatal("job redelivered before its backoff elapsed; want it delayed")
	}

	// After the backoff window it must reappear.
	got := dequeueWithin(t, q, 5*time.Second)
	if got.ID != "slow-retry" {
		t.Fatalf("redelivered %q, want slow-retry", got.ID)
	}
	if got.Attempts != 2 {
		t.Errorf("redelivery Attempts=%d, want 2", got.Attempts)
	}
}
