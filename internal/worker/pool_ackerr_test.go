package worker_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rogerllinares/sluice/internal/queue"
	"github.com/rogerllinares/sluice/internal/worker"
)

// failingAcks is the in-memory queue with Ack and Nack that always fail, so the
// test can see whether the pool reports those errors or drops them.
type failingAcks struct{ *queue.Memory }

func (failingAcks) Ack(context.Context, queue.Job) error  { return errors.New("ack store down") }
func (failingAcks) Nack(context.Context, queue.Job) error { return errors.New("nack store down") }

// syncBuffer guards the log buffer: workers write while the test reads.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestPoolLogsAckAndNackErrors: a failed Ack/Nack means the backend did not
// record the outcome (the job may be redelivered or stuck), so the pool must log
// it at Error level with the job ID instead of discarding it.
func TestPoolLogsAckAndNackErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := failingAcks{queue.NewMemory(4)}
	logs := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	handler := func(_ context.Context, job queue.Job) error {
		if job.ID == "bad" {
			return errors.New("handler failed")
		}
		return nil
	}

	pool := worker.New(q, handler, worker.Config{NumWorkers: 1, Logger: logger})
	pool.Start(ctx)
	for _, id := range []string{"good", "bad"} {
		if err := q.Enqueue(ctx, queue.Job{ID: id}); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		out := logs.String()
		if strings.Contains(out, "ack store down") && strings.Contains(out, "nack store down") {
			if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "job=good") ||
				!strings.Contains(out, "job=bad") {
				t.Fatalf("want ERROR lines naming both jobs, got:\n%s", out)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Ack/Nack errors were not logged, got:\n%s", logs.String())
}
