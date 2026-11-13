// Package queue defines the core queue contract for Sluice.
//
// The Queue interface is the single seam every backend implements: an in-memory
// bounded buffered channel, a durable PostgreSQL backend using
// SELECT ... FOR UPDATE SKIP LOCKED, and optionally a Redis Streams adapter.
// Keeping every backend behind one interface is what lets the worker pool, the
// API, and the tests stay backend-agnostic.
//
// Responsibilities of an implementation:
//   - Enqueue: accept a Job (transactionally, where the backend supports it).
//   - Dequeue/Reserve: hand a job to exactly one worker and lease it (so a
//     crashed worker's job becomes visible again after a visibility timeout).
//   - Ack: mark a reserved job done — only ever called AFTER processing.
//   - Nack: return a job for retry (attempts++, backoff) or send it to the DLQ.
//   - Deadletter: move a job past max-retries to the dead-letter queue/table.
//
// The contract lives in queue.go, the in-memory backend in memory.go and the
// durable one in postgres.go.
package queue
