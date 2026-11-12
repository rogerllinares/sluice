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
	seenKeys map[string]struct{}      // idempotency keys already enqueued
	inFlight map[string]chan struct{} // keys whose first enqueue is still sending; closed when it settles
	dead     []Job                    // dead-lettered jobs (past MaxAttempts)

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

// NewMemory returns a Memory whose buffer holds at most depth jobs. A depth
// below 1 is clamped to 1: an unbuffered channel would shed every Submit.
func NewMemory(depth int, opts ...MemoryOption) *Memory {
	depth = max(depth, 1)
	idle := make(chan struct{})
	close(idle) // no retries pending yet: born idle
	m := &Memory{
		jobs:        make(chan Job, depth),
		maxAttempts: defaultMaxAttempts,
		baseBackoff: defaultBaseBackoff,
		maxBackoff:  defaultMaxBackoff,
		seenKeys:    make(map[string]struct{}),
		inFlight:    make(map[string]chan struct{}),
		retriesIdle: idle,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Enqueue adds a job, blocking until a buffer slot is free or ctx is done. If
// the job carries an IdempotencyKey already enqueued, the enqueue is a no-op —
// this is the dedupe that turns an at-least-once producer into exactly-once
// effects. A duplicate that arrives while the first enqueue of its key is
// still blocked waits for that outcome instead of being told "enqueued" early.
func (m *Memory) Enqueue(ctx context.Context, job Job) error {
	for {
		dup, wait := m.claim(job.IdempotencyKey)
		if dup {
			return nil
		}
		if wait == nil {
			break
		}
		select {
		case <-wait: // the first caller settled: enqueued (dup next loop) or failed (we claim)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case m.jobs <- job:
		m.settle(job.IdempotencyKey, true)
		return nil
	case <-ctx.Done():
		m.settle(job.IdempotencyKey, false) // enqueue failed: let a retry try again
		return ctx.Err()
	}
}

// claim reports whether key was already enqueued (dup). Otherwise, if another
// caller is still sending the same key, it returns that caller's settle channel
// to wait on; else it takes the key in flight for this caller (wait == nil).
// An empty key never dedupes.
func (m *Memory) claim(key string) (dup bool, wait <-chan struct{}) {
	if key == "" {
		return false, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.seenKeys[key]; ok {
		return true, nil
	}
	if ch, ok := m.inFlight[key]; ok {
		return false, ch
	}
	m.inFlight[key] = make(chan struct{})
	return false, nil
}

// settle ends a claim: an enqueued key becomes seen, a failed one is released.
func (m *Memory) settle(key string, enqueued bool) {
	if key == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if enqueued {
		m.seenKeys[key] = struct{}{}
	}
	close(m.inFlight[key])
	delete(m.inFlight, key)
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
// climbing across retries. A full buffer at retry time delays the retry; it
// never dead-letters a job that has attempts left.
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
	m.redeliver(job, delay)
	return nil
}

// redeliver re-enqueues job after delay. If the buffer is still full when the
// timer fires, it waits another delay and tries again instead of dead-lettering
// a job that has attempts left; the retry stays pending (WaitRetries keeps
// waiting) until it lands, so a shutdown ceiling still bounds the wait.
func (m *Memory) redeliver(job Job, delay time.Duration) {
	time.AfterFunc(delay, func() {
		select {
		case m.jobs <- job:
			m.retryFinished()
		default:
			m.redeliver(job, delay)
		}
	})
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
// (re-enqueued; a full buffer only delays it), or until ctx
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
// the same idempotency dedupe as Enqueue; while another caller is still sending
// the same key, Submit cannot know the outcome without blocking, so it sheds
// (ErrQueueFull = retry later). ctx only gates entry: a cancelled request is
// not enqueued.
func (m *Memory) Submit(ctx context.Context, job Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dup, wait := m.claim(job.IdempotencyKey)
	if dup {
		return nil
	}
	if wait != nil {
		return ErrQueueFull
	}
	select {
	case m.jobs <- job:
		m.settle(job.IdempotencyKey, true)
		return nil
	default:
		m.settle(job.IdempotencyKey, false) // shed: do not claim the key was enqueued
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
