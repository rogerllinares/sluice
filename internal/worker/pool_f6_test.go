package worker_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rogerllinares/sluice/internal/queue"
	"github.com/rogerllinares/sluice/internal/worker"
)

// pool_f6_test.go pins the F6 graceful-shutdown contract: Shutdown stops the
// pool accepting new jobs, lets the in-flight job finish within a hard ceiling,
// then returns; a job that hangs past the ceiling cannot pin the process (force
// path); and a full start -> drain -> stop cycle leaks no goroutines (the
// package-level goleak in TestMain asserts that).

// TestShutdownDrainsInFlightJob proves the graceful path: a job already running
// when Shutdown is called is allowed to finish, and Shutdown returns nil (clean
// drain within the ceiling). After Shutdown returns, every worker goroutine has
// exited (goleak in TestMain would fail otherwise).
func TestShutdownDrainsInFlightJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := queue.NewMemory(4)

	running := make(chan struct{}) // closed when the handler has started
	release := make(chan struct{}) // test releases the handler
	var finished atomic.Bool

	var startedOnce atomic.Bool
	handler := func(_ context.Context, _ queue.Job) error {
		if startedOnce.CompareAndSwap(false, true) {
			close(running)
		}
		<-release
		finished.Store(true)
		return nil
	}

	pool := worker.New(q, handler, worker.Config{NumWorkers: 2})
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := q.Enqueue(ctx, queue.Job{ID: "in-flight"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Wait until the handler is genuinely in flight before shutting down.
	select {
	case <-running:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	// Shutdown in a goroutine; it must block until the in-flight handler returns.
	shutdownDone := make(chan error, 1)
	go func() {
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		shutdownDone <- pool.Shutdown(sctx)
	}()

	// Shutdown must NOT return while the job is still running.
	select {
	case <-shutdownDone:
		t.Fatal("Shutdown returned before the in-flight job finished")
	case <-time.After(150 * time.Millisecond):
	}

	close(release) // let the in-flight handler finish

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown returned error on clean drain: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after the in-flight job finished")
	}

	if !finished.Load() {
		t.Error("in-flight job did not finish during drain")
	}
}

// TestShutdownRejectsNewSubmitsAfterDrainStart proves the pool stops accepting
// new work once Shutdown has started: a job enqueued after Shutdown completes is
// not processed (the workers have stopped pulling).
func TestShutdownStopsPullingNewJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := queue.NewMemory(8)

	var processed atomic.Int64
	handler := func(context.Context, queue.Job) error {
		processed.Add(1)
		return nil
	}

	pool := worker.New(q, handler, worker.Config{NumWorkers: 2})
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer scancel()
	if err := pool.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	// After Shutdown, the pool has stopped. A newly enqueued job must not be
	// picked up by any worker.
	if err := q.Enqueue(ctx, queue.Job{ID: "after-shutdown"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := processed.Load(); got != 0 {
		t.Errorf("processed %d jobs after shutdown, want 0", got)
	}
}

// TestShutdownReturnsWithinCeilingWhenJobHangs proves the force path: a handler
// that hangs forever cannot pin the process. Shutdown must return (with a
// non-nil error) once the hard ceiling elapses, and cancel the in-flight job's
// context so the worker can exit and the suite stays leak-free.
func TestShutdownReturnsWithinCeilingWhenJobHangs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := queue.NewMemory(4)

	running := make(chan struct{})
	var startedOnce atomic.Bool
	// The handler hangs until its context is cancelled — modelling a job that
	// ignores graceful drain. The force path must cancel this context.
	handler := func(hctx context.Context, _ queue.Job) error {
		if startedOnce.CompareAndSwap(false, true) {
			close(running)
		}
		<-hctx.Done()
		return hctx.Err()
	}

	pool := worker.New(q, handler, worker.Config{NumWorkers: 1})
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := q.Enqueue(ctx, queue.Job{ID: "hangs"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case <-running:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	const ceiling = 200 * time.Millisecond
	sctx, scancel := context.WithTimeout(context.Background(), ceiling)
	defer scancel()

	start := time.Now()
	err := pool.Shutdown(sctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Shutdown returned nil when a job hung past the ceiling, want an error")
	}
	// It must return shortly after the ceiling, not hang forever.
	if elapsed > ceiling+2*time.Second {
		t.Errorf("Shutdown took %v, want it to return shortly after the %v ceiling", elapsed, ceiling)
	}

	// The force path must have cancelled the handler's context so the worker
	// goroutine exits — wait for it so goleak (TestMain) stays green.
	pool.Wait()
}

// TestShutdownIsIdempotent proves a second Shutdown (and Shutdown before Start)
// is a safe no-op — no panic, no double-close, no hang — so a SIGTERM handler
// that races itself or a defer-and-explicit pair cannot crash the process.
func TestShutdownIsIdempotent(t *testing.T) {
	// Shutdown before Start: no-op, nil.
	unstarted := worker.New(queue.NewMemory(1), func(context.Context, queue.Job) error { return nil }, worker.Config{NumWorkers: 1})
	if err := unstarted.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Start returned %v, want nil", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := queue.NewMemory(4)
	pool := worker.New(q, func(context.Context, queue.Job) error { return nil }, worker.Config{NumWorkers: 2})
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer scancel()
	if err := pool.Shutdown(sctx); err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}
	// Second Shutdown after the pool already drained must not panic or hang.
	if err := pool.Shutdown(sctx); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

// TestStopAbortsImmediately proves the abort-now path: Stop cancels in-flight
// work immediately (does not wait for a graceful drain) and the workers exit.
func TestStopAbortsImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := queue.NewMemory(4)

	running := make(chan struct{})
	var startedOnce atomic.Bool
	handler := func(hctx context.Context, _ queue.Job) error {
		if startedOnce.CompareAndSwap(false, true) {
			close(running)
		}
		<-hctx.Done() // a graceful drain would never cancel this; Stop does
		return hctx.Err()
	}

	pool := worker.New(q, handler, worker.Config{NumWorkers: 1})
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := q.Enqueue(ctx, queue.Job{ID: "running"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case <-running:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	done := make(chan struct{})
	go func() {
		pool.Stop()
		pool.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not abort the in-flight job and drain the pool")
	}
}
