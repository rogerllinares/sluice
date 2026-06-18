package worker_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rogerllinares/sluice/internal/queue"
	"github.com/rogerllinares/sluice/internal/worker"
)

// pool_test.go specifies the minimal worker loop (F1 slice / F3 seam).
//
// Given a queue.Queue and a Handler, a submitted job must be processed exactly
// once and acked. We use the in-memory queue as the concrete backend and a
// handler that records which job IDs it saw.

// TestWorkerProcessesJobOnce is the end-to-end F1 slice: enqueue -> worker pool
// drains it -> handler runs exactly once -> job is acked. The handler signals a
// channel so the test waits for actual processing instead of sleeping.
func TestWorkerProcessesJobOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := queue.NewMemory(4)

	var (
		mu        sync.Mutex
		seen      []string
		processed = make(chan struct{}, 1)
	)
	handler := func(_ context.Context, job queue.Job) error {
		mu.Lock()
		seen = append(seen, job.ID)
		mu.Unlock()
		processed <- struct{}{}
		return nil
	}

	pool := worker.New(q, handler, worker.Config{NumWorkers: 1})
	if pool == nil {
		t.Fatal("worker.New returned nil")
	}
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("pool.Start returned error: %v", err)
	}

	if err := q.Enqueue(ctx, queue.Job{ID: "only-job"}); err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}

	select {
	case <-processed:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not invoked within 2s")
	}

	// Give any erroneous second delivery a chance to show up, then assert
	// the job was handled exactly once.
	select {
	case <-processed:
		t.Fatal("handler invoked more than once for a single job")
	case <-time.After(100 * time.Millisecond):
	}

	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if len(got) != 1 || got[0] != "only-job" {
		t.Fatalf("handler saw %v, want exactly [only-job]", got)
	}
}

// TestWorkerProcessesEveryJobExactlyOnce submits a batch and asserts each job
// is handled exactly once across the pool — the load-bearing "exactly once"
// guarantee of the worker loop over the in-memory queue.
func TestWorkerProcessesEveryJobExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 50
	q := queue.NewMemory(n)

	var (
		mu     sync.Mutex
		counts = make(map[string]int)
		total  int64
		done   = make(chan struct{})
	)
	handler := func(_ context.Context, job queue.Job) error {
		mu.Lock()
		counts[job.ID]++
		mu.Unlock()
		if atomic.AddInt64(&total, 1) == n {
			close(done)
		}
		return nil
	}

	pool := worker.New(q, handler, worker.Config{NumWorkers: 4})
	if pool == nil {
		t.Fatal("worker.New returned nil")
	}
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("pool.Start returned error: %v", err)
	}

	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := "job-" + string(rune('A'+i%26)) + "-" + time.Duration(i).String()
		ids = append(ids, id)
		if err := q.Enqueue(ctx, queue.Job{ID: id}); err != nil {
			t.Fatalf("Enqueue(%q) returned error: %v", id, err)
		}
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("only %d/%d jobs processed within 3s", atomic.LoadInt64(&total), n)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, id := range ids {
		if counts[id] != 1 {
			t.Errorf("job %q processed %d times, want exactly 1", id, counts[id])
		}
	}
}
