package queue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rogerllinares/sluice/internal/queue"
)

// memory_test.go specifies the in-memory bounded-buffered-channel queue (F1).
//
// Type under test: queue.Memory, constructed via queue.NewMemory(depth int).
// It implements the queue.Queue interface (Enqueue / Dequeue / Ack / Nack) and
// additionally exposes a non-blocking Submit that sheds load with ErrQueueFull
// when the bounded buffer is full — the seam every later backend (Postgres,
// Redis) and the F4 backpressure work build on.

// TestMemoryImplementsQueue is a compile-time assertion that *Memory satisfies
// the Queue interface. If the interface and the implementation drift apart this
// fails to build — which is the cheapest possible test.
func TestMemoryImplementsQueue(t *testing.T) {
	var _ queue.Queue = queue.NewMemory(1)
}

// TestMemoryRoundTrip covers the core contract: a job enqueued comes back out of
// Dequeue intact, and Ack marks it done without error. Table-driven over a few
// job shapes so the round-trip is exercised for more than one payload.
func TestMemoryRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		job  queue.Job
	}{
		{name: "simple id", job: queue.Job{ID: "job-1"}},
		{name: "with payload", job: queue.Job{ID: "job-2", Payload: []byte("hello")}},
		{name: "empty payload", job: queue.Job{ID: "job-3", Payload: []byte{}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			q := queue.NewMemory(4)

			if err := q.Enqueue(ctx, tc.job); err != nil {
				t.Fatalf("Enqueue(%+v) returned error: %v", tc.job, err)
			}

			got, err := q.Dequeue(ctx)
			if err != nil {
				t.Fatalf("Dequeue() returned error: %v", err)
			}
			if got.ID != tc.job.ID {
				t.Errorf("Dequeue() ID = %q, want %q", got.ID, tc.job.ID)
			}
			if string(got.Payload) != string(tc.job.Payload) {
				t.Errorf("Dequeue() Payload = %q, want %q", got.Payload, tc.job.Payload)
			}

			if err := q.Ack(ctx, got); err != nil {
				t.Errorf("Ack(%+v) returned error: %v", got, err)
			}
		})
	}
}

// TestMemoryFIFOOrder pins that the bounded buffered channel preserves
// enqueue order — a property callers rely on and the channel gives for free.
func TestMemoryFIFOOrder(t *testing.T) {
	ctx := context.Background()
	q := queue.NewMemory(3)
	want := []string{"a", "b", "c"}

	for _, id := range want {
		if err := q.Enqueue(ctx, queue.Job{ID: id}); err != nil {
			t.Fatalf("Enqueue(%q) returned error: %v", id, err)
		}
	}

	for _, id := range want {
		got, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("Dequeue() returned error: %v", err)
		}
		if got.ID != id {
			t.Errorf("Dequeue() ID = %q, want %q (FIFO order broken)", got.ID, id)
		}
	}
}

// TestMemorySubmitShedsWhenFull is the backpressure seam. With QueueDepth=N the
// non-blocking Submit accepts exactly N jobs, then returns ErrQueueFull instead
// of blocking. F4 maps ErrQueueFull -> HTTP 429; here we only assert the error
// type exists and is returned the moment the bounded buffer is full.
func TestMemorySubmitShedsWhenFull(t *testing.T) {
	const depth = 2
	q := queue.NewMemory(depth)

	// Fill the buffer to its bound.
	for i := 0; i < depth; i++ {
		if err := q.Submit(context.Background(), queue.Job{ID: "fill"}); err != nil {
			t.Fatalf("Submit #%d returned error before buffer was full: %v", i, err)
		}
	}

	// One more must be shed, not block.
	err := q.Submit(context.Background(), queue.Job{ID: "overflow"})
	if !errors.Is(err, queue.ErrQueueFull) {
		t.Fatalf("Submit() on full queue = %v, want ErrQueueFull", err)
	}
}

// TestMemorySubmitWaitBlocksUntilSlotFrees is the backpressure counterpart to
// shedding: on a full queue SubmitWait does NOT return ErrQueueFull, it blocks
// until a worker dequeues and frees a slot, then succeeds. We fill the buffer,
// launch a SubmitWait in a goroutine, assert it is still blocked, then Dequeue
// once and assert it unblocks with nil.
func TestMemorySubmitWaitBlocksUntilSlotFrees(t *testing.T) {
	ctx := context.Background()
	const depth = 1
	q := queue.NewMemory(depth)

	// Fill the buffer to its bound.
	if err := q.Submit(ctx, queue.Job{ID: "fill"}); err != nil {
		t.Fatalf("Submit to fill buffer returned error: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- q.SubmitWait(ctx, queue.Job{ID: "waiter"})
	}()

	// While the buffer is full SubmitWait must block, not return.
	select {
	case err := <-done:
		t.Fatalf("SubmitWait returned %v on a full queue, want it to block", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Freeing a slot must unblock SubmitWait with a successful enqueue.
	if _, err := q.Dequeue(ctx); err != nil {
		t.Fatalf("Dequeue() returned error: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SubmitWait returned %v after a slot freed, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SubmitWait did not unblock within 2s after a slot freed")
	}
}

// TestMemorySubmitWaitRespectsContext pins that a blocked SubmitWait honours
// ctx: when the context is cancelled (here via timeout) before a slot frees, it
// returns the context error instead of blocking forever.
func TestMemorySubmitWaitRespectsContext(t *testing.T) {
	const depth = 1
	q := queue.NewMemory(depth)

	// Fill the buffer so SubmitWait has to wait.
	if err := q.Submit(context.Background(), queue.Job{ID: "fill"}); err != nil {
		t.Fatalf("Submit to fill buffer returned error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := q.SubmitWait(ctx, queue.Job{ID: "waiter"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SubmitWait on a full queue with an expiring ctx = %v, want context.DeadlineExceeded", err)
	}
}

// TestMemorySubmitRoomAfterDequeue checks the bound is dynamic, not a one-shot
// counter: dequeuing frees a slot so a subsequent Submit succeeds again.
func TestMemorySubmitRoomAfterDequeue(t *testing.T) {
	ctx := context.Background()
	const depth = 1
	q := queue.NewMemory(depth)

	if err := q.Submit(ctx, queue.Job{ID: "first"}); err != nil {
		t.Fatalf("first Submit returned error: %v", err)
	}
	if err := q.Submit(ctx, queue.Job{ID: "second"}); !errors.Is(err, queue.ErrQueueFull) {
		t.Fatalf("second Submit = %v, want ErrQueueFull", err)
	}

	if _, err := q.Dequeue(ctx); err != nil {
		t.Fatalf("Dequeue() returned error: %v", err)
	}

	if err := q.Submit(ctx, queue.Job{ID: "third"}); err != nil {
		t.Errorf("Submit after Dequeue freed a slot = %v, want nil", err)
	}
}
