# Sluice

[![CI](https://github.com/rogerllinares/sluice/actions/workflows/ci.yml/badge.svg)](https://github.com/rogerllinares/sluice/actions/workflows/ci.yml)

A concurrent **job-queue in Go**: a fixed worker pool drains a bounded buffered
channel, the producer edge applies **backpressure** and **load-shedding**, and
durable **at-least-once** delivery is backed by PostgreSQL (`FOR UPDATE SKIP
LOCKED`). Idempotent handlers turn at-least-once delivery into exactly-once
*effects*. Failed jobs retry with exponential backoff and jitter, then dead-letter.
Shutdown is graceful (SIGINT/SIGTERM drain within a hard ceiling, no goroutine
leaks). The point of the project is to *prove* these guarantees — kill a worker
mid-job and watch redelivery; `docker compose down && up` and watch the queue
survive — not just assert them.

## Architecture

```
                 backpressure / load-shedding
                 (non-blocking Submit -> ErrQueueFull -> HTTP 429 + Retry-After)
                          |
   ┌───────────────┐      v      ┌──────────────────────┐     ┌──────────────────┐
   │  Producer API │ ──────────► │   Queue [backend]    │ ──► │   Worker Pool    │
   │ (HTTP enqueue)│   Enqueue   │  bounded channel /   │     │ NumWorkers       │
   └───────────────┘             │  Postgres SKIP LOCKED│     │ goroutines       │
                                 └──────────────────────┘     └────────┬─────────┘
                                                                       │ process(ctx)
                                                                       v
                                                      ┌────────────────────────────┐
                                                      │   ack   │  retry/backoff   │
                                                      │ (done)  │  (attempts++,    │
                                                      │         │   exp + jitter)  │
                                                      │         │                  │
                                                      │         └──► DLQ ──────────┤
                                                      │   (past max retries)       │
                                                      └────────────────────────────┘
```

- **Backpressure / load-shedding at the API edge:** when the bounded queue is
  full, `Submit` returns `ErrQueueFull` instead of blocking (mapped to HTTP
  `429` + `Retry-After`); `SubmitWait(ctx)` is the blocking variant for callers
  that prefer to wait. The queue pins at `QueueDepth` rather than growing memory.
- **At-least-once + DLQ:** ack happens *after* processing; a visibility-timeout
  reaper re-queues jobs from crashed workers; jobs past max retries land in the
  dead-letter queue.

## Features

- Enqueue API over a bounded buffered channel (in-memory first, Postgres-durable next).
- Independent, configurable knobs: `NumWorkers` (default `NumCPU`) and `QueueDepth`.
- Backpressure (blocking `SubmitWait`) **and** load-shedding (non-blocking `Submit` -> `429`).
- Durable at-least-once via Postgres `FOR UPDATE SKIP LOCKED` + visibility-timeout reaper.
- Idempotency keys -> exactly-once *effects* (never "exactly-once delivery").
- Max-retries with exponential backoff + jitter; dead-letter queue.
- Graceful shutdown on SIGINT/SIGTERM (drain within a hard ceiling, no leaks/panics).
- Observability: Prometheus `/metrics` (`queue_depth`, `jobs_in_flight`, counters,
  duration histogram) + `slog` structured logging.
- Load test that *visibly* triggers shedding (depth pins, `429`s observed).
- Optional Redis Streams adapter behind the same `Queue` interface (NATS is future work).

> Prior art: [`riverqueue/river`](https://github.com/riverqueue/river) is a
> production-grade Postgres queue. Sluice **hand-rolls** the `SKIP LOCKED`
> concurrency code — River is a reference, not a dependency.

## How to run

> **Status:** F1 landed — the in-memory bounded-channel queue and the worker
> pool are implemented and tested (`go test ./...` is green). The Postgres-durable
> backend (F2), HTTP edge (F4), retries/DLQ (F5), and observability (F7) are next;
> the commands below describe the full intended surface.

```bash
go test ./...               # full TDD suite (green today)
docker compose up -d        # Postgres (+ Redis if the adapter ships) — needed from F2
go run ./cmd/sluice         # start queue + worker pool + /metrics (from F4)
# durability demo (TODO: script):
#   docker kill <worker>          -> redelivery after the visibility timeout
#   docker compose down && up     -> queue state persisted
```

## Docs

- [Engineering rationale](docs/engineering-rationale.md) — why this design, the
  backpressure / at-least-once / DLQ trade-offs, and the backend choice
  (Postgres + `FOR UPDATE SKIP LOCKED`).
