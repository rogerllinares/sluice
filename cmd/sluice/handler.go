package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/rogerllinares/sluice/internal/queue"
	"github.com/rogerllinares/sluice/internal/worker"
)

// demoPayload is the optional JSON contract the demo handler understands. It
// exists so the queue's guarantees are observable from outside: a job that
// takes real time can be killed mid-flight (redelivery becomes visible), and a
// job that fails on purpose walks the retry/backoff path into the DLQ.
type demoPayload struct {
	// SleepMS holds the job in flight for this many milliseconds.
	SleepMS int `json:"sleep_ms"`
	// Fail makes every delivery of the job return an error.
	Fail bool `json:"fail"`
}

// newDemoHandler returns the handler the sluice binary runs. A payload that is
// empty or not JSON processes immediately (success) — the handler never
// rejects a job for its shape; it is a demo surface, not a validator.
func newDemoHandler(logger *slog.Logger) worker.Handler {
	return func(ctx context.Context, job queue.Job) error {
		var p demoPayload
		if len(job.Payload) > 0 {
			// Best-effort decode: non-JSON payloads keep the zero value.
			_ = json.Unmarshal(job.Payload, &p)
		}
		if p.SleepMS > 0 {
			select {
			case <-time.After(time.Duration(p.SleepMS) * time.Millisecond):
			case <-ctx.Done():
				// Shutdown's force path (or Stop) cancelled in-flight work.
				return ctx.Err()
			}
		}
		if p.Fail {
			return fmt.Errorf("job %s: payload asked this delivery to fail (attempt %d)", job.ID, job.Attempts)
		}
		logger.Info("processed job", "id", job.ID, "attempts", job.Attempts, "slept_ms", p.SleepMS)
		return nil
	}
}
