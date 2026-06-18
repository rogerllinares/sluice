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
// F1 implements the minimal drain loop (New, Start, work); per-job recover and
// graceful Shutdown stay TODO until their F3/F6 tests demand them. Behaviour is
// built test-first.
package worker

import (
	"context"

	"github.com/rogerllinares/sluice/internal/queue"
)

// Config holds the pool's two independent knobs.
//
// Only NumWorkers is pinned by the F1 slice; per-job timeout and shutdown-ceiling
// fields are added in F3/F6 when their tests demand them.
type Config struct {
	NumWorkers int
}

// Pool is a fixed-size set of workers draining a queue.Queue.
//
// Shutdown lifecycle state (WaitGroup, accepting flag, done channel) is wired in
// F3/F6; F1 only needs enough to spawn the draining goroutines.
type Pool struct {
	q   queue.Queue
	h   Handler
	cfg Config
}

// Handler processes a single job. Returning an error triggers Nack
// (retry/backoff or DLQ); nil triggers Ack.
//
// TODO(F3/F5): finalise the handler signature against the first failing tests.
type Handler func(ctx context.Context, job queue.Job) error

// New constructs a Pool over the given queue and handler.
func New(q queue.Queue, h Handler, cfg Config) *Pool {
	return &Pool{q: q, h: h, cfg: cfg}
}

// Start spawns NumWorkers goroutines that drain the queue until ctx is
// cancelled. Each worker pulls one job, runs the handler, and Acks on success
// (Nacks on error) — so a dequeued job is delivered to the handler exactly once.
func (p *Pool) Start(ctx context.Context) error {
	for i := 0; i < p.cfg.NumWorkers; i++ {
		go p.work(ctx)
	}
	return nil
}

// work is a single worker's drain loop: Dequeue -> handler -> Ack/Nack, exiting
// when ctx is cancelled (Dequeue then returns ctx.Err()).
func (p *Pool) work(ctx context.Context) {
	for {
		job, err := p.q.Dequeue(ctx)
		if err != nil {
			return
		}
		if err := p.h(ctx, job); err != nil {
			_ = p.q.Nack(ctx, job)
			continue
		}
		_ = p.q.Ack(ctx, job)
	}
}

// Shutdown stops accepting new work and drains in-flight jobs within ctx's
// deadline (the hard ceiling), returning an error if the deadline is hit.
//
// TODO(F6): implement graceful drain (accepting=false atomic, close jobs from
//           the single owner, wg.Wait in a goroutine, select done vs ctx.Done).
func (p *Pool) Shutdown(ctx context.Context) error {
	_ = ctx
	// TODO(F6).
	return nil
}
