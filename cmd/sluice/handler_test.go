package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/rogerllinares/sluice/internal/queue"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// TestDemoHandlerSucceedsOnEmptyAndNonJSONPayloads: the handler is a demo
// surface, not a validator — any payload shape processes successfully.
func TestDemoHandlerSucceedsOnEmptyAndNonJSONPayloads(t *testing.T) {
	h := newDemoHandler(discardLogger())
	for _, payload := range [][]byte{nil, {}, []byte("not json at all")} {
		if err := h(context.Background(), queue.Job{ID: "j", Payload: payload}); err != nil {
			t.Errorf("handler with payload %q = %v, want nil", payload, err)
		}
	}
}

// TestDemoHandlerFailsOnRequest: {"fail":true} drives the retry/DLQ path.
func TestDemoHandlerFailsOnRequest(t *testing.T) {
	h := newDemoHandler(discardLogger())
	err := h(context.Background(), queue.Job{ID: "j", Payload: []byte(`{"fail":true}`)})
	if err == nil {
		t.Fatal("handler with fail:true = nil, want an error")
	}
}

// TestDemoHandlerSleepHonoursContext: a sleeping job is in-flight work that
// the shutdown force path must be able to abort — the sleep selects on ctx.
func TestDemoHandlerSleepHonoursContext(t *testing.T) {
	h := newDemoHandler(discardLogger())
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := h(ctx, queue.Job{ID: "j", Payload: []byte(`{"sleep_ms":10000}`)})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled sleeping handler = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("handler took %v to honour a 50ms ctx, want prompt return", elapsed)
	}
}

// TestDemoHandlerSleeps: sleep_ms actually holds the job in flight, which is
// what makes "kill the worker mid-job" demonstrable.
func TestDemoHandlerSleeps(t *testing.T) {
	h := newDemoHandler(discardLogger())
	start := time.Now()
	if err := h(context.Background(), queue.Job{ID: "j", Payload: []byte(`{"sleep_ms":80}`)}); err != nil {
		t.Fatalf("sleeping handler = %v, want nil", err)
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Errorf("handler returned after %v, want >= 80ms", elapsed)
	}
}
