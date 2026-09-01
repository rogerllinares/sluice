// Command sluice wires the job-queue together and runs it until interrupted.
//
// This file is the composition root: it constructs the queue, the worker pool,
// and the HTTP API, then blocks until SIGINT/SIGTERM. The behaviour of each
// piece is built and tested in the internal/* packages; main() only composes
// them. Graceful drain on shutdown is F6 (the existing TODO markers stay).
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rogerllinares/sluice/internal/api"
	"github.com/rogerllinares/sluice/internal/queue"
	"github.com/rogerllinares/sluice/internal/worker"
)

const (
	// queueDepth bounds the in-memory buffer: once full, Submit sheds (429).
	queueDepth = 100
	// httpAddr is the producer edge the enqueue handler is served on.
	httpAddr = ":8080"
	// shutdownCeiling is the hard ceiling on graceful shutdown: in-flight work
	// gets at most this long to drain before the process forces an abort and
	// exits. It bounds how long a SIGTERM can take (e.g. under an orchestrator's
	// own termination grace period).
	shutdownCeiling = 20 * time.Second
)

func main() {
	// One root context cancelled on SIGINT/SIGTERM, fanned out to every
	// subsystem (API server + worker pool) so shutdown is a single signal.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// TODO(F0): load configuration (NumWorkers default NumCPU, QueueDepth,
	//           timeouts, backend DSN) from env/flags.

	// In-memory bounded-channel queue (F1). The Postgres SKIP LOCKED backend
	// (F2) implements the same queue.Queue interface and drops in here later.
	q := queue.NewMemory(queueDepth)

	// Worker pool draining the queue. A no-op handler is enough to demonstrate
	// the backpressure story (the pool drains; the API sheds when it cannot
	// keep up). F5 gives jobs real, idempotent handlers.
	pool := worker.New(q, func(_ context.Context, job queue.Job) error {
		slog.Info("processed job", "id", job.ID)
		return nil
	}, worker.Config{})
	if err := pool.Start(ctx); err != nil {
		log.Fatalf("start worker pool: %v", err)
	}

	// HTTP producer edge: POST /enqueue -> Submit -> 202, or 429 + Retry-After
	// when the bounded queue is full (load shedding made visible).
	srv, err := api.NewServer(q)
	if err != nil {
		log.Fatalf("construct api server: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /enqueue", srv.EnqueueHandler)
	httpServer := &http.Server{Addr: httpAddr, Handler: mux}

	go func() {
		slog.Info("http server listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	// Block until a signal cancels ctx.
	<-ctx.Done()
	slog.Info("shutdown signal received, draining", "ceiling", shutdownCeiling)

	// Stop listening for the signal so a second Ctrl-C aborts the process the
	// hard way (the OS default) instead of being swallowed mid-drain.
	stop()

	// Graceful shutdown within a single hard ceiling shared by both subsystems.
	// Order: stop the producer edge first (HTTP), so no new jobs arrive while
	// the pool drains the ones already queued; then drain the pool; then wait
	// for any in-flight backoff retries to land so none are stranded.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownCeiling)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("http server graceful shutdown", "err", err)
	}

	if err := pool.Shutdown(shutdownCtx); err != nil {
		// Ceiling exceeded: a job hung past the deadline and was force-aborted.
		// The process still exits (a hung job must not pin it) but non-zero so
		// an orchestrator sees the unclean drain.
		slog.Error("worker pool drain exceeded ceiling", "err", err)
		if errors.Is(err, worker.ErrShutdownTimeout) {
			os.Exit(1)
		}
	}

	// Drain in-flight backoff retries so a redelivery timer is not stranded.
	q.WaitRetries()
	slog.Info("shutdown complete")
}
