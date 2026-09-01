// Command sluice wires the job-queue together and runs it until interrupted.
//
// This file is the composition root: it loads configuration from the
// environment, selects the backend (durable Postgres when SLUICE_DATABASE_URL
// is set, in-memory otherwise), constructs the worker pool and the HTTP API,
// then blocks until SIGINT/SIGTERM and drains everything within one hard
// ceiling. The behaviour of each piece is built and tested in the internal/*
// packages; main() only composes them.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/rogerllinares/sluice/internal/api"
	"github.com/rogerllinares/sluice/internal/config"
	"github.com/rogerllinares/sluice/internal/queue"
	"github.com/rogerllinares/sluice/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("sluice exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	// One root context cancelled on SIGINT/SIGTERM: the single shutdown signal.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	logger := slog.Default()

	// Backend selection. waitRetries is the Memory-only final drain step: its
	// pending backoff timers live in-process, while a Postgres retry is a
	// durable row that simply waits for the next process to pick it up.
	var q queue.Queue
	waitRetries := func(context.Context) error { return nil }
	if cfg.DatabaseURL != "" {
		pg, err := queue.NewPostgres(ctx, cfg.DatabaseURL,
			queue.WithQueueDepth(cfg.QueueDepth),
			queue.WithVisibilityTimeout(cfg.VisibilityTimeout),
			queue.WithMaxAttempts(cfg.MaxAttempts),
			queue.WithBackoff(cfg.BaseBackoff, cfg.MaxBackoff),
		)
		if err != nil {
			return fmt.Errorf("connect durable backend: %w", err)
		}
		defer pg.Close()
		q = pg
		logger.Info("backend: postgres (durable, at-least-once)",
			"queue_depth", cfg.QueueDepth, "visibility_timeout", cfg.VisibilityTimeout)
	} else {
		mem := queue.NewMemory(cfg.QueueDepth,
			queue.WithMemoryMaxAttempts(cfg.MaxAttempts),
			queue.WithMemoryBackoff(cfg.BaseBackoff, cfg.MaxBackoff),
		)
		q = mem
		waitRetries = mem.WaitRetries
		logger.Info("backend: in-memory (jobs do not survive a restart)",
			"queue_depth", cfg.QueueDepth)
	}

	// The pool gets its own lifetime context — NOT the signal context.
	// Shutdown is driven exclusively by pool.Shutdown below: wiring the signal
	// context here would cancel dequeueCtx AND jobCtx the instant SIGTERM
	// arrives, aborting in-flight jobs and making the graceful drain
	// unreachable.
	pool := worker.New(q, newDemoHandler(logger), worker.Config{NumWorkers: cfg.NumWorkers})
	if err := pool.Start(context.Background()); err != nil {
		return fmt.Errorf("start worker pool: %v", err)
	}
	logger.Info("worker pool started", "workers", pool.Size())

	// HTTP producer edge: POST /enqueue -> Submit -> 202, or 429 + Retry-After
	// when the bounded queue is full (load shedding made visible). /healthz is
	// the readiness probe the demo script polls before enqueueing.
	srv, err := api.NewServer(q)
	if err != nil {
		pool.Stop()
		pool.Wait()
		return fmt.Errorf("construct api server: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /enqueue", srv.EnqueueHandler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}

	// Serve in a goroutine, reporting a listen failure into serveErr so run
	// can unwind cleanly (deferred cleanup included) instead of exiting from a
	// non-main goroutine.
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		// The producer edge died (e.g. port in use): abort the pool and exit.
		pool.Stop()
		pool.Wait()
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	logger.Info("shutdown signal received, draining", "ceiling", cfg.ShutdownCeiling)

	// Stop listening for the signal so a second Ctrl-C aborts the process the
	// hard way (the OS default) instead of being swallowed mid-drain.
	stop()

	// One hard ceiling shared by every drain step, in order: stop the producer
	// edge (no new jobs arrive), drain the pool (in-flight work finishes or is
	// force-aborted at the deadline), then wait for any in-memory backoff
	// retries to land. Every step honours the same deadline, so a SIGTERM is
	// bounded by the ceiling no matter where the drain gets stuck.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownCeiling)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("http server graceful shutdown", "err", err)
	}
	if err := pool.Shutdown(shutdownCtx); err != nil {
		// Ceiling exceeded: a job hung past the deadline and was force-aborted.
		// Exit non-zero so an orchestrator sees the unclean drain.
		return fmt.Errorf("worker pool drain: %w", err)
	}
	if err := waitRetries(shutdownCtx); err != nil {
		return fmt.Errorf("drain pending retries: %w", err)
	}
	logger.Info("shutdown complete")
	return nil
}
