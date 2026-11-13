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

// A retry whose backoff fires while the buffer is full used to be dead-lettered
// even with attempts left. It now waits another backoff and tries again.
func TestNackRetryWaitsForRoomInsteadOfDeadLettering(t *testing.T) {
	ctx := context.Background()
	m := NewMemory(1, WithMemoryMaxAttempts(3), WithMemoryBackoff(time.Millisecond, time.Millisecond))
	if err := m.Submit(ctx, Job{ID: "filler"}); err != nil {
		t.Fatal(err)
	}
	if err := m.Nack(ctx, Job{ID: "retry", Attempts: 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // several backoffs fire against the full buffer
	if dead := m.DeadLettered(); len(dead) != 0 {
		t.Fatalf("dead-lettered %v with attempts left", dead)
	}
	if job, _ := m.Dequeue(ctx); job.ID != "filler" {
		t.Fatalf("first dequeue = %q, want filler", job.ID)
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := m.WaitRetries(waitCtx); err != nil {
		t.Fatalf("retry never landed: %v", err)
	}
	if job, _ := m.Dequeue(ctx); job.ID != "retry" || job.Attempts != 2 {
		t.Fatalf("redelivered %q attempts %d, want retry with 2", job.ID, job.Attempts)
	}
}

// TOCTOU: a duplicate key used to count as "already enqueued" from the moment
// the first caller claimed it, before its send succeeded. If that send then
// failed, the duplicate had already been told nil and the job was lost.
func TestDuplicateWhileFirstEnqueueIsBlockedIsNotReportedEnqueued(t *testing.T) {
	ctx := context.Background()
	m := NewMemory(1)
	if err := m.Submit(ctx, Job{ID: "filler"}); err != nil {
		t.Fatal(err)
	}
	firstCtx, cancelFirst := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	go func() { firstDone <- m.Enqueue(firstCtx, Job{ID: "a", IdempotencyKey: "k"}) }()
	time.Sleep(20 * time.Millisecond) // the first caller holds the key, blocked on the full buffer

	if err := m.Submit(ctx, Job{ID: "b", IdempotencyKey: "k"}); err != ErrQueueFull {
		t.Fatalf("Submit duplicate while first is in flight = %v, want ErrQueueFull", err)
	}
	dupCtx, cancelDup := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelDup()
	dupDone := make(chan error, 1)
	go func() { dupDone <- m.Enqueue(dupCtx, Job{ID: "c", IdempotencyKey: "k"}) }()
	time.Sleep(20 * time.Millisecond)
	cancelFirst() // the first send fails: the key was never enqueued
	if err := <-firstDone; err == nil {
		t.Fatal("first Enqueue succeeded on a full buffer")
	}
	if err := <-dupDone; err == nil {
		t.Fatal("duplicate Enqueue reported nil, but no job with key k is in the queue")
	}
}
