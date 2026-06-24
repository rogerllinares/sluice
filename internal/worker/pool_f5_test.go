package worker_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rogerllinares/sluice/internal/queue"
	"github.com/rogerllinares/sluice/internal/worker"
)

// pool_f5_test.go pins the F5 retry-accounting reconciliation: the pool no
// longer counts attempts itself. On handler failure it simply Nacks, and the
// backend (which owns the authoritative Attempts count) decides redelivery vs
// dead-letter. So a permanently failing job runs exactly MaxAttempts times and
// then lands in the dead-letter list — driven by the backend, not the pool.

// TestPoolFailingJobDeadLettersAfterMaxAttempts proves the single-sourced retry
// cap: with MaxAttempts=3 the handler sees a failing job exactly 3 times, then
// the backend dead-letters it (the pool stops seeing it).
func TestPoolFailingJobDeadLettersAfterMaxAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	const maxAttempts = 3
	q := queue.NewMemory(4,
		queue.WithMemoryMaxAttempts(maxAttempts),
		queue.WithMemoryBackoff(5*time.Millisecond, 20*time.Millisecond),
	)

	var runs int64
	handler := func(_ context.Context, job queue.Job) error {
		if job.ID == "always-fails" {
			atomic.AddInt64(&runs, 1)
			return errFailing
		}
		return nil
	}

	pool := worker.New(q, handler, worker.Config{NumWorkers: 2})
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := q.Enqueue(ctx, queue.Job{ID: "always-fails"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Wait until the backend has dead-lettered the job.
	deadline := time.After(3 * time.Second)
	for len(q.DeadLettered()) == 0 {
		select {
		case <-deadline:
			t.Fatalf("job not dead-lettered; ran %d times", atomic.LoadInt64(&runs))
		case <-time.After(5 * time.Millisecond):
		}
	}

	// Stop the workers, then drain any pending backoff timer deterministically
	// (instead of sleeping) before asserting the final delivery count — the
	// terminal nack dead-letters without scheduling a new timer, so once
	// retries drain no further delivery can occur.
	cancel()
	pool.Wait()
	q.WaitRetries()

	if got := atomic.LoadInt64(&runs); got != maxAttempts {
		t.Errorf("handler ran %d times, want exactly %d (MaxAttempts)", got, maxAttempts)
	}
	dl := q.DeadLettered()
	if len(dl) != 1 || dl[0].ID != "always-fails" {
		t.Errorf("dead-letter = %+v, want exactly [always-fails]", dl)
	}
}

var errFailing = errorString("handler always fails")

type errorString string

func (e errorString) Error() string { return string(e) }
