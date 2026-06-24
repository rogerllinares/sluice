package worker_test

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rogerllinares/sluice/internal/queue"
	"github.com/rogerllinares/sluice/internal/worker"
	"go.uber.org/goleak"
)

// TestMain runs every test in this package under goleak: a worker goroutine
// that outlives ctx cancellation fails the suite. This is the F3 zero-leak
// guarantee enforced for the whole package, not just one test.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// TestPoolProcessesConcurrentlyWithNWorkers proves the pool actually runs
// NumWorkers handlers at the same time. Each handler parks until the test
// releases it, so all `workers` must be in flight simultaneously — a pool that
// spawned fewer goroutines would never reach `workers` starts and time out.
func TestPoolProcessesConcurrentlyWithNWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	const workers = 4
	q := queue.NewMemory(workers)

	var inFlight, maxInFlight int64
	started := make(chan struct{}, workers)
	release := make(chan struct{})

	handler := func(_ context.Context, _ queue.Job) error {
		cur := atomic.AddInt64(&inFlight, 1)
		for {
			old := atomic.LoadInt64(&maxInFlight)
			if cur <= old || atomic.CompareAndSwapInt64(&maxInFlight, old, cur) {
				break
			}
		}
		started <- struct{}{}
		<-release
		atomic.AddInt64(&inFlight, -1)
		return nil
	}

	pool := worker.New(q, handler, worker.Config{NumWorkers: workers})
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for i := 0; i < workers; i++ {
		if err := q.Enqueue(ctx, queue.Job{ID: "j"}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	for i := 0; i < workers; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d/%d handlers ran concurrently", atomic.LoadInt64(&inFlight), workers)
		}
	}
	close(release)

	if got := atomic.LoadInt64(&maxInFlight); got != workers {
		t.Errorf("max concurrent in-flight = %d, want %d", got, workers)
	}

	cancel()
	pool.Wait()
}

// TestPoolDefaultsToNumCPU pins the "NumWorkers <= 0 means NumCPU" default.
func TestPoolDefaultsToNumCPU(t *testing.T) {
	q := queue.NewMemory(1)
	handler := func(context.Context, queue.Job) error { return nil }

	pool := worker.New(q, handler, worker.Config{NumWorkers: 0})
	if got, want := pool.Size(), runtime.NumCPU(); got != want {
		t.Errorf("default pool size = %d, want NumCPU = %d", got, want)
	}
}

// TestPoolContainsPanickingJob proves a panicking job cannot shrink the pool:
// every normal job is still processed, and the poison job is delivered exactly
// MaxAttempts times (here 2 = original + 1 retry) then dead-lettered by the
// backend instead of looping forever. F5 moved the retry cap into the backend
// (single authoritative Attempts count); the pool just Nacks on failure.
func TestPoolContainsPanickingJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	const normal = 5
	q := queue.NewMemory(normal+1,
		queue.WithMemoryMaxAttempts(2),
		queue.WithMemoryBackoff(5*time.Millisecond, 20*time.Millisecond),
	)

	var processed, poisonRuns int64
	done := make(chan struct{})

	handler := func(_ context.Context, job queue.Job) error {
		if job.ID == "poison" {
			atomic.AddInt64(&poisonRuns, 1)
			panic("boom")
		}
		if atomic.AddInt64(&processed, 1) == normal {
			close(done)
		}
		return nil
	}

	pool := worker.New(q, handler, worker.Config{NumWorkers: 2})
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := q.Enqueue(ctx, queue.Job{ID: "poison"}); err != nil {
		t.Fatalf("Enqueue poison: %v", err)
	}
	for i := 0; i < normal; i++ {
		if err := q.Enqueue(ctx, queue.Job{ID: "ok"}); err != nil {
			t.Fatalf("Enqueue ok: %v", err)
		}
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("only %d/%d normal jobs processed — pool shrank?", atomic.LoadInt64(&processed), normal)
	}

	deadline := time.After(2 * time.Second)
	for atomic.LoadInt64(&poisonRuns) < 2 {
		select {
		case <-deadline:
			t.Fatalf("poison ran %d times, want it to reach 2 (1 retry)", atomic.LoadInt64(&poisonRuns))
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Give any erroneous 3rd attempt a chance to surface before asserting.
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt64(&poisonRuns); got != 2 {
		t.Errorf("poison ran %d times, want exactly 2 (original + 1 retry)", got)
	}

	cancel()
	pool.Wait()
}

// TestPoolNoGoroutineLeak documents the cancel -> Wait -> drained pattern; the
// package-level goleak (TestMain) is what actually asserts no worker goroutine
// outlived the pool.
func TestPoolNoGoroutineLeak(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	q := queue.NewMemory(4)

	var processed int64
	firstDone := make(chan struct{})
	var once sync.Once
	handler := func(context.Context, queue.Job) error {
		atomic.AddInt64(&processed, 1)
		once.Do(func() { close(firstDone) })
		return nil
	}

	pool := worker.New(q, handler, worker.Config{NumWorkers: 3})
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	for i := 0; i < 4; i++ {
		if err := q.Enqueue(ctx, queue.Job{ID: "j"}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	// Wait for at least one job to actually be processed before cancelling.
	// Cancelling immediately races the dequeue: a worker can observe ctx.Done
	// before pulling any job, which made this test flaky under suite-wide
	// timing pressure (the cancel-vs-dequeue race). Synchronising on real work
	// removes the race without weakening the leak guarantee (goleak in TestMain
	// still asserts every worker goroutine exits after cancel).
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("no job processed within 2s")
	}

	cancel()
	pool.Wait()

	if atomic.LoadInt64(&processed) == 0 {
		t.Error("expected at least one job processed before cancel")
	}
}
