package queue

import (
	"context"
	"sync"
	"time"
)

// Memory is the in-memory queue backend: a bounded buffered channel sized to a
// fixed depth. It is the simplest Queue implementation and the seam the durable
// backends (Postgres, Redis) and the worker pool are validated against. F5 adds
// the reliability semantics it shares with the durable backend: idempotent
// enqueue (dedupe on key), bounded retries with exponential backoff, and a
// dead-letter list for jobs past MaxAttempts.
//
// Unlike Postgres, the in-memory backend has no durable row to carry Attempts,
// so a dequeued job's Attempts field is the source of truth in flight and the
// re-enqueued copy carries the incremented count.
type Memory struct {
	jobs chan Job

	maxAttempts int
	baseBackoff time.Duration
	maxBackoff  time.Duration

	mu       sync.Mutex
	seenKeys map[string]struct{} // idempotency keys already enqueued
	dead     []Job               // dead-lettered jobs (past MaxAttempts)

	// pendingRetries counts in-flight backoff timers, and retriesIdle is
	// closed whenever that count is zero (and replaced when it leaves zero).
	// Both are guarded by mu. This lets WaitRetries select "no pending
	// redeliveries" against a caller's ctx — a WaitGroup cannot be waited on
	// with a deadline, and its Add-concurrent-with-Wait contract is exactly
	// the Nack-vs-shutdown race this design removes.
	pendingRetries int
	retriesIdle    chan struct{}
}

// MemoryOption configures a Memory backend.
type MemoryOption func(*Memory)

// WithMemoryMaxAttempts sets how many deliveries a job gets before the next
// Nack dead-letters it. Default: 3. (Named distinctly from the Postgres
// WithMaxAttempts because the two backends take different option types.)
func WithMemoryMaxAttempts(n int) MemoryOption {
	return func(m *Memory) { m.maxAttempts = n }
}

// WithMemoryBackoff sets the base and max retry-backoff delays. Default:
// 500ms base, 30s max.
func WithMemoryBackoff(base, max time.Duration) MemoryOption {
	return func(m *Memory) { m.baseBackoff, m.maxBackoff = base, max }
}

// NewMemory returns a Memory whose buffer holds at most depth jobs.
func NewMemory(depth int, opts ...MemoryOption) *Memory {
	idle := make(chan struct{})
	close(idle) // no retries pending yet: born idle
	m := &Memory{
		jobs:        make(chan Job, depth),
		maxAttempts: defaultMaxAttempts,
		baseBackoff: defaultBaseBackoff,
		maxBackoff:  defaultMaxBackoff,
		seenKeys:    make(map[string]struct{}),
		retriesIdle: idle,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Enqueue adds a job, blocking until a buffer slot is free or ctx is done. If
// the job carries an IdempotencyKey already seen, the enqueue is a no-op — this
// is the dedupe that turns an at-least-once producer into exactly-once effects.
func (m *Memory) Enqueue(ctx context.Context, job Job) error {
	if m.duplicate(job.IdempotencyKey) {
		return nil
	}
	select {
	case m.jobs <- job:
		return nil
	case <-ctx.Done():
		m.forget(job.IdempotencyKey) // enqueue failed: let a retry try again
		return ctx.Err()
	}
}

// duplicate records a non-empty key and reports whether it was already present.
func (m *Memory) duplicate(key string) bool {
	if key == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.seenKeys[key]; ok {
		return true
	}
	m.seenKeys[key] = struct{}{}
	return false
}

// forget drops a key so a failed enqueue can be retried.
func (m *Memory) forget(key string) {
	if key == "" {
		return
	}
	m.mu.Lock()
	delete(m.seenKeys, key)
	m.mu.Unlock()
}

// Dequeue returns the next job in FIFO order, blocking until one is available or
// ctx is done. Attempts is incremented here so the in-flight job carries its
// delivery count (the single authoritative counter for this backend).
func (m *Memory) Dequeue(ctx context.Context) (Job, error) {
	select {
	case job := <-m.jobs:
		job.Attempts++
		return job, nil
	case <-ctx.Done():
		return Job{}, ctx.Err()
	}
}

// Ack marks a job done. For the in-memory backend a successful Dequeue already
// removed the job from the buffer, so there is nothing to release.
func (m *Memory) Ack(_ context.Context, _ Job) error {
	return nil
}

// Nack reports a failed delivery and decides the job's fate from its Attempts
// count: at or past MaxAttempts it is dead-lettered (recorded, never
// redelivered); otherwise it is redelivered after a backoff delay. The
// redelivery is scheduled on a timer so the buffer slot is not held during the
// wait; the re-enqueued copy carries the current Attempts so the count keeps
// climbing across retries.
func (m *Memory) Nack(_ context.Context, job Job) error {
	if job.Attempts >= m.maxAttempts {
		m.mu.Lock()
		m.dead = append(m.dead, job)
		m.mu.Unlock()
		return nil
	}
	delay := backoffDelay(job.Attempts, m.baseBackoff, m.maxBackoff)
	// Re-enqueue after the backoff on a tracked timer goroutine: the retry must
	// outlive the Nack call, so it cannot borrow Nack's ctx. The pending count
	// lets WaitRetries observe when every scheduled redelivery has landed,
	// which keeps the redelivery race out of the goleak/shutdown path.
	m.retryScheduled()
	time.AfterFunc(delay, func() {
		defer m.retryFinished()
		select {
		case m.jobs <- job:
		default:
			// Buffer full at retry time: drop into the dead-letter list rather
			// than block a timer goroutine. At-least-once is preserved up to
			// capacity; documented as a known in-memory limitation (Postgres,
			// the durable backend, has no such bound).
			m.mu.Lock()
			m.dead = append(m.dead, job)
			m.mu.Unlock()
		}
	})
	return nil
}

// retryScheduled registers a pending backoff timer. When the count leaves
// zero, the idle channel is replaced so waiters only observe a later idle
// instant.
func (m *Memory) retryScheduled() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingRetries == 0 {
		m.retriesIdle = make(chan struct{})
	}
	m.pendingRetries++
}

// retryFinished retires a pending backoff timer, signalling idle at zero.
func (m *Memory) retryFinished() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingRetries--
	if m.pendingRetries == 0 {
		close(m.retriesIdle)
	}
}

// WaitRetries blocks until every scheduled backoff redelivery has fired
// (either re-enqueued or, if the buffer was full, dead-lettered), or until ctx
// is done — so a pending long backoff can never pin a shutdown past its
// ceiling. Tests use it to drain pending retries deterministically instead of
// sleeping; the composition root uses it as the final drain step.
func (m *Memory) WaitRetries(ctx context.Context) error {
	m.mu.Lock()
	idle := m.retriesIdle
	m.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DeadLettered returns a snapshot of jobs that exceeded MaxAttempts. It is the
// in-memory analogue of querying the Postgres 'dead' status — read-only, never
// redelivered.
func (m *Memory) DeadLettered() []Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Job, len(m.dead))
	copy(out, m.dead)
	return out
}

// Submit is the non-blocking enqueue path: it adds a job if a slot is free,
// otherwise sheds load by returning ErrQueueFull instead of blocking. It honours
// the same idempotency dedupe as Enqueue. The send itself never blocks, so ctx
// only gates entry: a cancelled request is not enqueued.
func (m *Memory) Submit(ctx context.Context, job Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.duplicate(job.IdempotencyKey) {
		return nil
	}
	select {
	case m.jobs <- job:
		return nil
	default:
		m.forget(job.IdempotencyKey) // shed: do not claim the key was enqueued
		return ErrQueueFull
	}
}

// SubmitWait is the blocking enqueue path: the backpressure counterpart to
// Submit's shedding. It blocks until a buffer slot frees and the job is
// enqueued, or until ctx is cancelled/times out (returning ctx.Err()). Callers
// that prefer to wait under load use this; callers that prefer to shed use
// Submit.
func (m *Memory) SubmitWait(ctx context.Context, job Job) error {
	return m.Enqueue(ctx, job)
}
