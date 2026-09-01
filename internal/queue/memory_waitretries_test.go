package queue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rogerllinares/sluice/internal/queue"
)

// memory_waitretries_test.go specifies the ctx-aware WaitRetries contract: the
// final drain step of a graceful shutdown must share the shutdown's deadline,
// so a pending long backoff timer can never pin the process past its ceiling.

// TestMemoryWaitRetriesReturnsImmediatelyWhenIdle: with nothing pending,
// WaitRetries is a no-op, not a wait.
func TestMemoryWaitRetriesReturnsImmediatelyWhenIdle(t *testing.T) {
	q := queue.NewMemory(4)
	start := time.Now()
	if err := q.WaitRetries(context.Background()); err != nil {
		t.Fatalf("WaitRetries on an idle queue = %v, want nil", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("WaitRetries on an idle queue took %v, want immediate return", elapsed)
	}
}

// TestMemoryWaitRetriesHonoursContext: a pending backoff far longer than the
// caller's deadline must not pin the wait — WaitRetries returns ctx.Err() at
// the deadline. This is the shutdown-ceiling guarantee.
func TestMemoryWaitRetriesHonoursContext(t *testing.T) {
	q := queue.NewMemory(4,
		queue.WithMemoryMaxAttempts(5),
		queue.WithMemoryBackoff(10*time.Second, 20*time.Second),
	)
	ctx := context.Background()

	if err := q.Enqueue(ctx, queue.Job{ID: "slow-retry"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if err := q.Nack(ctx, job); err != nil { // schedules a >=10s backoff timer
		t.Fatalf("Nack: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = q.WaitRetries(waitCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitRetries with a pending 10s backoff and a 100ms ctx = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("WaitRetries took %v to honour a 100ms ctx, want prompt return", elapsed)
	}
}

// TestMemoryWaitRetriesDrainsPendingRetry: with a short backoff pending,
// WaitRetries blocks until the redelivery lands, and the job is dequeueable
// afterwards — the deterministic drain the tests and the shutdown path rely on.
func TestMemoryWaitRetriesDrainsPendingRetry(t *testing.T) {
	q := queue.NewMemory(4,
		queue.WithMemoryMaxAttempts(5),
		queue.WithMemoryBackoff(5*time.Millisecond, 20*time.Millisecond),
	)
	ctx := context.Background()

	if err := q.Enqueue(ctx, queue.Job{ID: "fast-retry"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if err := q.Nack(ctx, job); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	if err := q.WaitRetries(ctx); err != nil {
		t.Fatalf("WaitRetries = %v, want nil once the retry lands", err)
	}

	redelivered, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue after WaitRetries: %v", err)
	}
	if redelivered.ID != "fast-retry" || redelivered.Attempts != 2 {
		t.Errorf("redelivered job = %+v, want fast-retry with Attempts=2", redelivered)
	}
}
