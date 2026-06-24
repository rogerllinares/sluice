package queue_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rogerllinares/sluice/internal/queue"
)

// memory_f5_test.go specifies the F5 reliability semantics on the in-memory
// backend: idempotent enqueue (dedupe on key), bounded retries with backoff,
// and the dead-letter path once a job is past max retries. The durable backend
// proves the same contract against real Postgres (postgres_f5_test.go); the
// in-memory backend lets these run fast without Docker.

// drainDeadLetter polls the backend's dead-letter list until it has n entries or
// the deadline passes, so tests do not race the backoff goroutine.
func waitForDeadLetter(t *testing.T, q *queue.Memory, n int, within time.Duration) []queue.Job {
	t.Helper()
	deadline := time.After(within)
	for {
		dl := q.DeadLettered()
		if len(dl) >= n {
			return dl
		}
		select {
		case <-deadline:
			t.Fatalf("dead-letter has %d entries, want %d within %v", len(dl), n, within)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestMemoryEnqueueDedupesIdempotencyKey is the exactly-once-effects seam: two
// enqueues carrying the same idempotency key create a single job. The second
// enqueue is a no-op (no error, no second delivery).
func TestMemoryEnqueueDedupesIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	q := queue.NewMemory(4)

	job := queue.Job{ID: "job-1", Payload: []byte("x"), IdempotencyKey: "order-42"}
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	// Same key, different ID/payload: must be deduped (no second job).
	dup := queue.Job{ID: "job-2", Payload: []byte("y"), IdempotencyKey: "order-42"}
	if err := q.Enqueue(ctx, dup); err != nil {
		t.Fatalf("duplicate Enqueue should be a no-op, got error: %v", err)
	}

	got, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if got.ID != "job-1" {
		t.Fatalf("dequeued %q, want job-1 (the first of the deduped pair)", got.ID)
	}

	// No second job should exist.
	empty, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := q.Dequeue(empty); err == nil {
		t.Fatal("a second job existed after idempotent dedupe; want only one")
	}
}

// TestMemoryEnqueueWithoutKeyNeverDedupes pins that jobs without an idempotency
// key are never deduped (the key is opt-in): two keyless jobs are two jobs.
func TestMemoryEnqueueWithoutKeyNeverDedupes(t *testing.T) {
	ctx := context.Background()
	q := queue.NewMemory(4)

	if err := q.Enqueue(ctx, queue.Job{ID: "a"}); err != nil {
		t.Fatalf("Enqueue a: %v", err)
	}
	if err := q.Enqueue(ctx, queue.Job{ID: "b"}); err != nil {
		t.Fatalf("Enqueue b: %v", err)
	}
	for _, want := range []string{"a", "b"} {
		got, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("Dequeue: %v", err)
		}
		if got.ID != want {
			t.Fatalf("dequeued %q, want %q", got.ID, want)
		}
	}
}

// TestMemoryNackRetriesThenDeadLetters is the core retry/DLQ contract: a job
// nacked up to MaxAttempts is redelivered (carrying an incremented Attempts)
// each time, then on the final nack it is dead-lettered instead of redelivered.
func TestMemoryNackRetriesThenDeadLetters(t *testing.T) {
	ctx := context.Background()
	// MaxAttempts=3: deliveries 1,2,3 then dead-letter. Tiny backoff so the
	// test is fast.
	q := queue.NewMemory(4,
		queue.WithMemoryMaxAttempts(3),
		queue.WithMemoryBackoff(5*time.Millisecond, 20*time.Millisecond),
	)

	if err := q.Enqueue(ctx, queue.Job{ID: "flaky", Payload: []byte("x")}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	for attempt := 1; attempt <= 3; attempt++ {
		got, err := dequeueWithinMem(t, q, 2*time.Second)
		if err != nil {
			t.Fatalf("Dequeue attempt %d: %v", attempt, err)
		}
		if got.ID != "flaky" {
			t.Fatalf("attempt %d dequeued %q, want flaky", attempt, got.ID)
		}
		if got.Attempts != attempt {
			t.Fatalf("attempt %d has Attempts=%d, want %d", attempt, got.Attempts, attempt)
		}
		if err := q.Nack(ctx, got); err != nil {
			t.Fatalf("Nack attempt %d: %v", attempt, err)
		}
	}

	// After the 3rd nack (Attempts==MaxAttempts) the job must be dead-lettered,
	// not redelivered.
	dl := waitForDeadLetter(t, q, 1, 2*time.Second)
	if dl[0].ID != "flaky" {
		t.Fatalf("dead-lettered %q, want flaky", dl[0].ID)
	}
	if dl[0].Attempts != 3 {
		t.Errorf("dead-lettered job Attempts=%d, want 3", dl[0].Attempts)
	}

	// And it must NOT be redelivered.
	empty, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, err := q.Dequeue(empty); err == nil {
		t.Fatal("a dead-lettered job was redelivered; want none")
	}
}

// TestMemoryNackBelowMaxRedelivers pins that a job under MaxAttempts is
// redelivered (not dead-lettered) after its backoff delay.
func TestMemoryNackBelowMaxRedelivers(t *testing.T) {
	ctx := context.Background()
	q := queue.NewMemory(4,
		queue.WithMemoryMaxAttempts(5),
		queue.WithMemoryBackoff(5*time.Millisecond, 20*time.Millisecond),
	)

	if err := q.Enqueue(ctx, queue.Job{ID: "retry-me"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	first, err := dequeueWithinMem(t, q, 2*time.Second)
	if err != nil {
		t.Fatalf("first Dequeue: %v", err)
	}
	if err := q.Nack(ctx, first); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	second, err := dequeueWithinMem(t, q, 2*time.Second)
	if err != nil {
		t.Fatalf("redelivery Dequeue: %v", err)
	}
	if second.ID != "retry-me" {
		t.Fatalf("redelivered %q, want retry-me", second.ID)
	}
	if second.Attempts != 2 {
		t.Errorf("redelivery Attempts=%d, want 2", second.Attempts)
	}
	if len(q.DeadLettered()) != 0 {
		t.Errorf("job under MaxAttempts was dead-lettered, want none")
	}
}

// dequeueWithinMem reserves a job from the in-memory queue, bounded by d.
func dequeueWithinMem(t *testing.T, q *queue.Memory, d time.Duration) (queue.Job, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return q.Dequeue(ctx)
}

// TestMemoryConcurrentSubmitDedupesSameKey pins the idempotency guarantee under
// concurrency: many goroutines Submit jobs sharing one idempotency key at once,
// and at most one is enqueued. Run under -race to catch the check-and-record
// TOCTOU window. (The losers either shed (ErrQueueFull) or are deduped (nil);
// either way no second job exists.)
func TestMemoryConcurrentSubmitDedupesSameKey(t *testing.T) {
	const racers = 50
	q := queue.NewMemory(racers) // room for all, so dedupe (not shedding) is what bounds it

	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_ = q.Submit(queue.Job{ID: "j", IdempotencyKey: "same-key"})
		}(i)
	}
	wg.Wait()

	// Exactly one job must have made it onto the queue.
	ctx := context.Background()
	if _, err := q.Dequeue(ctx); err != nil {
		t.Fatalf("expected exactly one job enqueued, first Dequeue failed: %v", err)
	}
	empty, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if got, err := q.Dequeue(empty); err == nil {
		t.Fatalf("a second job with the same key was enqueued (%q); want exactly one", got.ID)
	}
}

// TestMemoryRetryDropsToDeadLetterWhenBufferFull covers the documented
// limitation: if the buffer is full when a backoff retry fires, the job is
// dead-lettered (not silently lost). We use depth=1, keep the slot occupied,
// nack a job below MaxAttempts, and assert it lands in the dead-letter list.
func TestMemoryRetryDropsToDeadLetterWhenBufferFull(t *testing.T) {
	ctx := context.Background()
	q := queue.NewMemory(1,
		queue.WithMemoryMaxAttempts(5),
		queue.WithMemoryBackoff(5*time.Millisecond, 20*time.Millisecond),
	)

	// Dequeue the job to retry (frees the slot, attempts=1).
	if err := q.Enqueue(ctx, queue.Job{ID: "retry"}); err != nil {
		t.Fatalf("Enqueue retry: %v", err)
	}
	job, err := dequeueWithinMem(t, q, 2*time.Second)
	if err != nil {
		t.Fatalf("Dequeue retry: %v", err)
	}
	// Occupy the single slot so the retry has nowhere to land.
	if err := q.Submit(queue.Job{ID: "blocker"}); err != nil {
		t.Fatalf("Submit blocker: %v", err)
	}
	// Nack the under-max job: its retry timer will fire and find no room.
	if err := q.Nack(ctx, job); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	dl := waitForDeadLetter(t, q, 1, 2*time.Second)
	if dl[0].ID != "retry" {
		t.Fatalf("dead-lettered %q, want retry (dropped because buffer was full)", dl[0].ID)
	}
}
