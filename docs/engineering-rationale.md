# Sluice — Engineering Rationale

## Resumen

One page on *why* Sluice is built the way it is: the design choices behind a
concurrent Go job-queue, the three core trade-offs (backpressure vs
load-shedding, at-least-once vs exactly-once, retry vs dead-letter), and an
explicit map from each feature to the CS topics it demonstrates.

## Contenido

### Why this design

Sluice's spine is the simplest primitive that satisfies every MVP constraint at
once: a **fixed worker pool over a bounded buffered channel**, with durability
delegated to **PostgreSQL `FOR UPDATE SKIP LOCKED`** (full rationale and the
rejected alternatives — Redis Streams, NATS JetStream — in ADR-001). Two design
commitments shape everything else:

1. **Hand-roll the concurrency and locking code.** `riverqueue/river` is
   production-grade Postgres-queue prior art, but delegating to it would hide the
   exact code that makes this a portfolio piece. River is referenced in the
   README, never imported.
2. **Two independent knobs, not one.** `NumWorkers` (concurrency, default
   `NumCPU`) and `QueueDepth` (how many jobs may wait) are orthogonal. Conflating
   them is a common junior mistake; separating them is what makes the
   backpressure story tunable and measurable.

The bounded channel is the whole point: an *unbounded* queue hides overload until
the process OOMs. A bounded one turns overload into an explicit, observable
signal (`queue_depth` pinned at its bound) that the producer edge can act on.

### Core trade-offs

**Backpressure vs load-shedding.** Same cause (producer faster than pool), two
responses. *Backpressure* = make the producer wait (`SubmitWait(ctx)` blocks on a
full channel) — correct when the producer can slow down and no work may be
dropped. *Load-shedding* = refuse fast (`Submit` does a non-blocking
`select`/`default` -> `ErrQueueFull` -> HTTP `429` + `Retry-After`) — correct at a
request edge where blocking would exhaust connections and a client can retry.
Sluice offers both and lets the caller choose; the HTTP enqueue path defaults to
shedding so a burst returns `429` instead of stalling the server.

**At-least-once vs exactly-once.** Exactly-once *delivery* is effectively
unattainable across a crash boundary, so Sluice targets **at-least-once
delivery** (ack only *after* processing; a visibility-timeout reaper re-queues a
crashed worker's job) and makes handlers **idempotent** (idempotency key /
upsert). At-least-once + idempotent handler = **exactly-once *effects***. The
wording matters: claiming "exactly-once delivery" reads junior; "exactly-once
effects via idempotency" reads correct.

**Retry/backoff vs dead-letter.** Transient failures (timeout, flaky dependency)
should retry with **exponential backoff + jitter** (jitter avoids a synchronized
retry stampede). But infinite retry on a *permanently* poisoned job blocks the
pool forever, so after `max_retries` the job moves to a **dead-letter queue** —
isolating the bad job, keeping the pool flowing, and preserving the failure for
inspection instead of silently dropping it.

### Feature -> CS topic map (DTU framing)

| Feature | CS topic | What it demonstrates |
|---|---|---|
| Fixed worker pool (`NumWorkers` goroutines, `sync.WaitGroup`) | **Concurrency** | Bounded parallelism; goroutine lifecycle; no leaks |
| Bounded buffered channel as the queue | **Data Structures** + **Concurrency** | Channel as a thread-safe bounded FIFO; capacity as a control knob |
| `context.Context` cancellation fanned to all workers | **Concurrency** | Cooperative cancellation; structured shutdown |
| Backpressure (`SubmitWait`) vs shedding (`Submit` -> `429`) | **Concurrency** + **Networks** | Flow control; protecting a server under overload |
| Per-job `recover` containment | **Concurrency** | Fault isolation — one panic can't shrink the pool |
| Graceful shutdown (drain within a hard ceiling) | **Concurrency** | Signal handling; deadline-bounded drain; no close-of-closed panics |
| Postgres `FOR UPDATE SKIP LOCKED` dequeue | **Data Structures** + **Networks** | Row-level locking; concurrent consumers without double-dequeue |
| At-least-once + visibility-timeout reaper | **Networks / distributed systems** | Delivery semantics; lease/visibility timeout; crash recovery |
| Idempotency keys -> exactly-once effects | **Networks / distributed systems** | Deduplication; reasoning about duplicate delivery |
| Retry with exponential backoff + jitter | **Networks / distributed systems** | Failure handling; avoiding retry stampedes |
| Dead-letter queue + job status state machine | **Data Structures** | Explicit state machine (`queued -> running -> done/failed/dead`) |
| Prometheus metrics + `slog` | **Networks** | Observability; making internal state externally measurable |

### Status / scaling notes

Skeleton stage (kickoff 2026-06-17); Go toolchain pending install. The Postgres
queue has a throughput ceiling (table bloat, lock/CPU contention under heavy
`SKIP LOCKED`) — documented as a scaling boundary (partitioning / VACUUM /
archiving), irrelevant at portfolio scale. NATS JetStream is noted as future
work; the Redis Streams adapter is optional (decision deferred — see STATE.md D2).

---

> TODO: once F1 lands, link the durability-demo script and a captured load-test
> metrics snapshot (depth pinned, `429`s observed) from here and the README.
