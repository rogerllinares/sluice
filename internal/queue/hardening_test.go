package queue

import (
	"context"
	"testing"
	"time"
)

// A zero or negative backoff (e.g. WithMemoryBackoff(0, 0)) must not panic in
// rand.Int63n: it means "retry immediately".
func TestBackoffZeroBaseDoesNotPanic(t *testing.T) {
	for _, base := range []time.Duration{0, -time.Second} {
		if got := backoffDelay(3, base, base); got != 0 {
			t.Fatalf("backoffDelay(3, %v, %v) = %v, want 0", base, base, got)
		}
	}
}

// NewMemory(depth < 1) used to build an unbuffered channel, so every Submit
// shed with ErrQueueFull. It is clamped to a one-slot buffer.
func TestNewMemoryClampsDepthToOne(t *testing.T) {
	for _, depth := range []int{0, -5} {
		m := NewMemory(depth)
		if err := m.Submit(context.Background(), Job{ID: "a"}); err != nil {
			t.Fatalf("NewMemory(%d).Submit: %v, want nil", depth, err)
		}
		if err := m.Submit(context.Background(), Job{ID: "b"}); err != ErrQueueFull {
			t.Fatalf("NewMemory(%d) second Submit: %v, want ErrQueueFull", depth, err)
		}
	}
}
