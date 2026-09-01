package queue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// postgres_submit_test.go specifies the non-blocking Submit / blocking
// SubmitWait pair on the durable backend, so the HTTP producer edge can shed
// (429) or exert backpressure over Postgres exactly as it does over the
// in-memory queue. A durable table has no natural capacity, so the bound is
// opt-in (WithQueueDepth) and advisory: it gates on the queued backlog, and
// concurrent submits may overshoot by a request or two — good enough to stop
// runaway backlog growth, which is what shedding is for.

// TestPostgresSubmitUnboundedByDefault: without WithQueueDepth, Submit never
// sheds — it behaves as a durable Enqueue.
func TestPostgresSubmitUnboundedByDefault(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := q.Submit(ctx, Job{ID: fmt.Sprintf("u-%d", i)}); err != nil {
			t.Fatalf("Submit #%d on unbounded backend = %v, want nil", i, err)
		}
	}
}

// TestPostgresSubmitShedsAtCapacity: with WithQueueDepth(n), the n+1th Submit
// against an untouched backlog returns ErrQueueFull instead of inserting.
func TestPostgresSubmitShedsAtCapacity(t *testing.T) {
	const depth = 2
	q := newTestQueue(t, WithQueueDepth(depth))
	ctx := context.Background()

	for i := 0; i < depth; i++ {
		if err := q.Submit(ctx, Job{ID: fmt.Sprintf("fill-%d", i)}); err != nil {
			t.Fatalf("Submit #%d returned error before the bound was reached: %v", i, err)
		}
	}

	err := q.Submit(ctx, Job{ID: "overflow"})
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Submit past the bound = %v, want ErrQueueFull", err)
	}
}

// TestPostgresSubmitRoomAfterDequeue: the bound gates the queued backlog, not
// lifetime inserts — reserving a job (queued -> running) frees a slot.
func TestPostgresSubmitRoomAfterDequeue(t *testing.T) {
	q := newTestQueue(t, WithQueueDepth(1))
	ctx := context.Background()

	if err := q.Submit(ctx, Job{ID: "first"}); err != nil {
		t.Fatalf("first Submit = %v, want nil", err)
	}
	if err := q.Submit(ctx, Job{ID: "second"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("second Submit = %v, want ErrQueueFull", err)
	}

	dequeueWithin(t, q, 2*time.Second)

	if err := q.Submit(ctx, Job{ID: "third"}); err != nil {
		t.Errorf("Submit after Dequeue freed the backlog = %v, want nil", err)
	}
}

// TestPostgresSubmitDedupePrecedesCapacity: a duplicate of an already-stored
// idempotency key is a success (the job exists — exactly-once effects), even
// when the backlog is at its bound. Only a genuinely new job is shed.
func TestPostgresSubmitDedupePrecedesCapacity(t *testing.T) {
	q := newTestQueue(t, WithQueueDepth(1))
	ctx := context.Background()

	if err := q.Submit(ctx, Job{ID: "a", IdempotencyKey: "k"}); err != nil {
		t.Fatalf("Submit with key = %v, want nil", err)
	}
	if err := q.Submit(ctx, Job{ID: "a-retry", IdempotencyKey: "k"}); err != nil {
		t.Fatalf("duplicate-key Submit at capacity = %v, want nil (dedupe wins over the bound)", err)
	}
	if err := q.Submit(ctx, Job{ID: "b"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("new-job Submit at capacity = %v, want ErrQueueFull", err)
	}

	// Exactly one row landed: the original. The dedupe must not have inserted.
	job := dequeueWithin(t, q, 2*time.Second)
	if job.ID != "a" {
		t.Errorf("dequeued %q, want the original job %q", job.ID, "a")
	}
	empty, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if got, err := q.Dequeue(empty); err == nil {
		t.Errorf("a second job (%q) was inserted despite the dedupe", got.ID)
	}
}

// TestPostgresSubmitWaitBlocksUntilSlotFrees is the backpressure contract over
// the durable backend: on a full backlog SubmitWait blocks (polling), then
// succeeds once a Dequeue frees a slot.
func TestPostgresSubmitWaitBlocksUntilSlotFrees(t *testing.T) {
	q := newTestQueue(t, WithQueueDepth(1))
	ctx := context.Background()

	if err := q.Submit(ctx, Job{ID: "fill"}); err != nil {
		t.Fatalf("Submit to fill the backlog = %v, want nil", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- q.SubmitWait(context.Background(), Job{ID: "waiter"})
	}()

	// While the backlog is at its bound SubmitWait must block, not return.
	select {
	case err := <-done:
		t.Fatalf("SubmitWait returned %v on a full backlog, want it to block", err)
	case <-time.After(300 * time.Millisecond):
	}

	dequeueWithin(t, q, 2*time.Second)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SubmitWait after a slot freed = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("SubmitWait did not unblock within 3s after a slot freed")
	}
}

// TestPostgresSubmitWaitRespectsContext: a blocked SubmitWait honours ctx and
// returns its error instead of polling forever.
func TestPostgresSubmitWaitRespectsContext(t *testing.T) {
	q := newTestQueue(t, WithQueueDepth(1))

	if err := q.Submit(context.Background(), Job{ID: "fill"}); err != nil {
		t.Fatalf("Submit to fill the backlog = %v, want nil", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := q.SubmitWait(ctx, Job{ID: "waiter"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SubmitWait with an expiring ctx = %v, want context.DeadlineExceeded", err)
	}
}
