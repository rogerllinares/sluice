// Package worker contains the worker pool that drains the queue.
//
// The Pool spawns a fixed number of goroutines (NumWorkers, default NumCPU)
// that each run a for-select loop pulling jobs from a queue.Queue. The pool has
// two independent knobs from the queue's QueueDepth: concurrency is the number
// of workers, depth is how many jobs can wait. Each job runs under a context so
// shutdown cancels in-flight work; a per-job deferred recover keeps one bad job
// from shrinking the pool. Shutdown drains in-flight work within a hard ceiling
// with no goroutine leaks.
//
// The pool does not count attempts. On failure it just Nacks, and the backend —
// which owns the single authoritative Attempts count — decides redelivery (with
// backoff) vs dead-letter.
package worker
