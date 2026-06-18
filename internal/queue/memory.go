package queue

import "context"

// Memory is the in-memory queue backend: a bounded buffered channel sized to a
// fixed depth. It is the simplest Queue implementation and the seam the durable
// backends (Postgres, Redis) and the F4 backpressure work are validated against.
type Memory struct {
	jobs chan Job
}

// NewMemory returns a Memory whose buffer holds at most depth jobs.
func NewMemory(depth int) *Memory {
	return &Memory{jobs: make(chan Job, depth)}
}

// Enqueue adds a job, blocking until a buffer slot is free or ctx is done.
func (m *Memory) Enqueue(ctx context.Context, job Job) error {
	select {
	case m.jobs <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Dequeue returns the next job in FIFO order, blocking until one is available or
// ctx is done.
func (m *Memory) Dequeue(ctx context.Context) (Job, error) {
	select {
	case job := <-m.jobs:
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

// Nack returns a job for retry by re-enqueuing it. Retry/backoff and DLQ
// semantics are layered on in F5.
func (m *Memory) Nack(ctx context.Context, job Job) error {
	return m.Enqueue(ctx, job)
}

// Submit is the non-blocking enqueue path: it adds a job if a slot is free,
// otherwise sheds load by returning ErrQueueFull instead of blocking.
func (m *Memory) Submit(job Job) error {
	select {
	case m.jobs <- job:
		return nil
	default:
		return ErrQueueFull
	}
}
