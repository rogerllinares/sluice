// Command sluice wires the job-queue together and runs it until interrupted.
//
// This file is a wiring SKELETON only. The real logic (config loading, queue
// construction, worker pool, HTTP API, graceful shutdown) is implemented later
// via strict TDD in the internal/* packages — main() only composes them.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// One root context cancelled on SIGINT/SIGTERM, fanned out to every
	// subsystem (API server + worker pool) so shutdown is a single signal.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// TODO(F0): load configuration (NumWorkers default NumCPU, QueueDepth,
	//           timeouts, backend DSN) from env/flags.

	// TODO(F1/F2): construct the Queue (in-memory bounded channel first, then
	//              the Postgres SKIP LOCKED backend) behind the queue.Queue interface.

	// TODO(F3): build and start the worker pool draining the queue.

	// TODO(F4/F7): build and start the HTTP API server (enqueue + load shedding,
	//              plus the /metrics endpoint).

	// TODO(F6): block until ctx is cancelled (signal received), then run a
	//           graceful shutdown that drains in-flight work within a hard ceiling.
	<-ctx.Done()

	// TODO(F6): graceful shutdown — Shutdown(ctx) on the pool + API, wait for
	//           drain or the deadline, then exit with the right status code.
}
