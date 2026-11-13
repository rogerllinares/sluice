// Package worker contains the worker pool that drains the queue.
//
// The Pool spawns a fixed number of goroutines (NumWorkers, default NumCPU)
// that each run a for-select loop pulling jobs from a queue.Queue. The pool has
// two independent knobs from the queue's QueueDepth: concurrency is the number
// of workers, depth is how many jobs can wait. Each job runs under a context so
// shutdown cancels in-flight work; a per-job deferred recover keeps one bad job
// from shrinking the pool. Shutdown drains in-flight work within a hard ceiling
// with no goroutine leaks (see ROADMAP F3/F6).
//
// F3 added the WaitGroup-tracked drain, the NumCPU default, and a per-job
// recover (a panicking job is contained, not a dead worker). F5 reconciled the
// retry accounting: the pool no longer counts attempts. On failure it just
// Nacks, and the backend — which owns the single authoritative Attempts count —
// decides redelivery (with backoff) vs dead-letter. Graceful Shutdown stays
// TODO until F6. Behaviour is built test-first.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"

	"github.com/rogerllinares/sluice/internal/queue"
)

// ErrShutdownTimeout is returned by Shutdown when the hard ceiling (the ctx
// deadline) elapses before every in-flight job has drained. Shutdown forces the
// remaining workers to abort (cancelling their job contexts) before returning
// this, so the process can exit instead of being pinned by a hung job.
var ErrShutdownTimeout = errors.New("worker: shutdown ceiling exceeded, forced abort")

// Config holds the pool's knobs. Logger receives Ack/Nack failures; nil means
// slog.Default().
type Config struct {
	NumWorkers int
	Logger     *slog.Logger
}

// Pool is a fixed-size set of workers draining a queue.Queue.
//
// The WaitGroup is wired in F3 (Start adds, work signals on exit, Wait blocks).
// F6 adds graceful shutdown via two derived contexts created in Start:
//
//   - dequeueCtx gates pulling NEW jobs. Shutdown cancels it so idle workers
//     (blocked in Dequeue) exit at once and busy workers stop pulling once their
//     current job returns — that is the graceful drain.
//   - jobCtx is the context handed to the handler (in-flight work). Graceful
//     drain never cancels it, so a running job finishes. The force path
//     (ceiling exceeded, or Stop) cancels it so a hung job cannot pin the pool.
//
// The two contexts are INDEPENDENT siblings, both derived from the parent ctx —
// crucially jobCtx is NOT a child of dequeueCtx. If it were, cancelling
// dequeueCtx (the graceful "stop pulling" signal) would cascade down and cancel
// the in-flight handler too, so a hung job would unblock and Shutdown could
// never observe the ceiling. Keeping them independent lets a graceful drain stop
// new pulls while leaving in-flight work running. A full abort therefore cancels
// BOTH (Stop, and the Shutdown force path); cancelling the shared parent ctx
// (e.g. the SIGTERM context) cancels both at once.
type Pool struct {
	q   queue.Queue
	h   Handler
	cfg Config
	wg  sync.WaitGroup

	// mu guards the cancel funcs: Start writes them while Shutdown/Stop may read
	// them from another goroutine. The contexts themselves are handed to each
	// worker as arguments, so workers never read shared fields.
	mu sync.Mutex
	// stopDequeue cancels dequeueCtx (stop pulling new jobs — graceful drain).
	// abort cancels jobCtx (cancel in-flight work — force path / Stop).
	// Both are set by Start; nil means never started (Shutdown/Stop are no-ops).
	stopDequeue context.CancelFunc
	abort       context.CancelFunc
}

// Handler processes a single job. Returning an error triggers Nack
// (retry/backoff or DLQ); nil triggers Ack.
//
// TODO(F3/F5): finalise the handler signature against the first failing tests.
type Handler func(ctx context.Context, job queue.Job) error

// New constructs a Pool over the given queue and handler. A NumWorkers of 0 (or
// negative) defaults to runtime.NumCPU().
func New(q queue.Queue, h Handler, cfg Config) *Pool {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = runtime.NumCPU()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Pool{q: q, h: h, cfg: cfg}
}

// Size reports the number of worker goroutines the pool runs (NumWorkers after
// the NumCPU default is applied).
func (p *Pool) Size() int { return p.cfg.NumWorkers }

// Start spawns NumWorkers goroutines that drain the queue. It derives two
// contexts from ctx so shutdown is graceful by default: dequeueCtx (cancelled to
// stop pulling new jobs) and jobCtx (handed to handlers; cancelled only to force
// in-flight work to abort). Cancelling the parent ctx — e.g. the SIGTERM context
// from signal.NotifyContext — still aborts everything immediately.
func (p *Pool) Start(ctx context.Context) {
	// Independent siblings off ctx (NOT jobCtx off dequeueCtx) so the graceful
	// "stop pulling" cancel does not also abort in-flight handlers.
	dequeueCtx, stopDequeue := context.WithCancel(ctx)
	jobCtx, abort := context.WithCancel(ctx)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopDequeue, p.abort = stopDequeue, abort
	// Add under mu: Shutdown only Waits after seeing the cancel funcs set here.
	p.wg.Add(p.cfg.NumWorkers)
	for i := 0; i < p.cfg.NumWorkers; i++ {
		go p.work(dequeueCtx, jobCtx)
	}
}

// cancels returns the cancel funcs Start set (both nil before Start).
func (p *Pool) cancels() (stopDequeue, abort context.CancelFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stopDequeue, p.abort
}

// Wait blocks until every worker goroutine has returned (after ctx is
// cancelled). It lets callers — and the zero-leak tests — confirm the pool
// drained instead of leaking goroutines. F6 builds graceful Shutdown on top.
func (p *Pool) Wait() { p.wg.Wait() }

// work is a single worker's drain loop: Dequeue -> handle -> Ack/Nack. It pulls
// new jobs with dequeueCtx (cancelled on Shutdown — the worker then exits once
// its current job, if any, returns) and runs the handler with jobCtx (cancelled
// only by the force path / Stop, so a graceful drain lets in-flight work
// finish). It signals the WaitGroup on exit so Wait can observe a clean drain.
func (p *Pool) work(dequeueCtx, jobCtx context.Context) {
	defer p.wg.Done()
	for {
		job, err := p.q.Dequeue(dequeueCtx)
		if err != nil {
			return
		}
		if err := p.handle(jobCtx, job); err != nil {
			// Delivery failed (handler error or panic). The pool does not count
			// attempts or decide the cap — it just reports the failure. The
			// backend reads the job's authoritative Attempts and either
			// redelivers after a backoff or dead-letters it past MaxAttempts.
			p.report("nack", job, p.q.Nack(jobCtx, job))
			continue
		}
		p.report("ack", job, p.q.Ack(jobCtx, job))
	}
}

// report logs a failed Ack/Nack: the backend did not record the outcome, so the
// job may be redelivered (at-least-once) or sit until its lease expires.
func (p *Pool) report(op string, job queue.Job, err error) {
	if err != nil {
		p.cfg.Logger.Error("worker: "+op+" failed", "job", job.ID, "err", err)
	}
}

// handle runs the handler with a per-job recover so a panicking job becomes an
// error instead of killing the worker goroutine (which would shrink the pool).
func (p *Pool) handle(ctx context.Context, job queue.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("job %s panicked: %v", job.ID, r)
		}
	}()
	return p.h(ctx, job)
}

// Shutdown drains the pool gracefully within a hard ceiling. It cancels
// dequeueCtx so workers stop pulling new jobs (idle workers exit at once; a busy
// worker finishes its current job, then its next Dequeue returns ctx.Err and it
// exits), then waits for every worker to return. If ctx (the ceiling) fires
// first, it forces the remaining in-flight jobs to abort (cancel jobCtx), waits
// for the now-cancelled workers to exit so no goroutine leaks, and returns
// ErrShutdownTimeout. A clean drain returns nil.
//
// Shutdown before Start, or a double Shutdown, is a no-op (nil cancels guarded).
func (p *Pool) Shutdown(ctx context.Context) error {
	stopDequeue, abort := p.cancels()
	if stopDequeue == nil {
		return nil // never started
	}
	stopDequeue() // stop pulling new jobs — begin the graceful drain

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil // every worker drained within the ceiling
	case <-ctx.Done():
		// Ceiling hit: a job is hanging. Force in-flight work to abort so the
		// process is not pinned, then wait for the workers to actually exit
		// (keeps the pool leak-free) before reporting the timeout.
		abort()
		p.wg.Wait()
		return ErrShutdownTimeout
	}
}

// Stop aborts the pool immediately: the abort-now counterpart to Shutdown's
// graceful drain. It cancels both contexts — stopDequeue (idle workers stop
// pulling) and abort (in-flight handlers are cancelled) — so a worker neither
// finishes its current job nor blocks on the next Dequeue; it exits. Callers
// Wait afterwards to confirm the workers exited. Stop before Start is a no-op.
func (p *Pool) Stop() {
	stopDequeue, abort := p.cancels()
	if stopDequeue == nil {
		return // never started
	}
	stopDequeue()
	abort()
}
