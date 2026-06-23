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
// F3 adds the WaitGroup-tracked drain, the NumCPU default, a per-job recover (a
// panicking job is contained, not a dead worker), and a provisional retry cap
// (see maxAttempts). Graceful Shutdown stays TODO until F6. Behaviour is built
// test-first.
package worker

import (
	"context"
	"fmt"
	"runtime"
	"sync"

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
// The WaitGroup is wired in F3 (Start adds, work signals on exit, Wait blocks).
// The accepting flag + done channel for graceful Shutdown arrive in F6.
type Pool struct {
	q   queue.Queue
	h   Handler
	cfg Config
	wg  sync.WaitGroup
}

// Handler processes a single job. Returning an error triggers Nack
// (retry/backoff or DLQ); nil triggers Ack.
//
// TODO(F3/F5): finalise the handler signature against the first failing tests.
type Handler func(ctx context.Context, job queue.Job) error

// maxAttempts caps how many times a job is delivered before the pool gives up:
// the original attempt plus one retry. This is the F3-provisional retry limit,
// counted pool-side; F5 reconciles it into proper max-retries + backoff + DLQ
// across both backends (the durable backend already increments Attempts on each
// Dequeue, so the counts converge there).
const maxAttempts = 2

// New constructs a Pool over the given queue and handler. A NumWorkers of 0 (or
// negative) defaults to runtime.NumCPU().
func New(q queue.Queue, h Handler, cfg Config) *Pool {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = runtime.NumCPU()
	}
	return &Pool{q: q, h: h, cfg: cfg}
}

// Size reports the number of worker goroutines the pool runs (NumWorkers after
// the NumCPU default is applied).
func (p *Pool) Size() int { return p.cfg.NumWorkers }

// Start spawns NumWorkers goroutines that drain the queue until ctx is
// cancelled. Each worker pulls one job, runs the handler, and Acks on success
// (Nacks on error) — so a dequeued job is delivered to the handler exactly once.
func (p *Pool) Start(ctx context.Context) error {
	p.wg.Add(p.cfg.NumWorkers)
	for i := 0; i < p.cfg.NumWorkers; i++ {
		go p.work(ctx)
	}
	return nil
}

// Wait blocks until every worker goroutine has returned (after ctx is
// cancelled). It lets callers — and the zero-leak tests — confirm the pool
// drained instead of leaking goroutines. F6 builds graceful Shutdown on top.
func (p *Pool) Wait() { p.wg.Wait() }

// work is a single worker's drain loop: Dequeue -> handle -> Ack/Nack, exiting
// when ctx is cancelled (Dequeue then returns ctx.Err()). It signals the
// WaitGroup on exit so Wait can observe a clean drain.
func (p *Pool) work(ctx context.Context) {
	defer p.wg.Done()
	for {
		job, err := p.q.Dequeue(ctx)
		if err != nil {
			return
		}
		if err := p.handle(ctx, job); err != nil {
			// Delivery failed (handler error or panic): count the attempt and
			// retry until maxAttempts, then drop (Ack) so a poison job cannot
			// loop forever. F5 turns the drop into a DLQ route.
			job.Attempts++
			if job.Attempts >= maxAttempts {
				_ = p.q.Ack(ctx, job) // give up: drop
			} else {
				_ = p.q.Nack(ctx, job) // retry
			}
			continue
		}
		_ = p.q.Ack(ctx, job)
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
