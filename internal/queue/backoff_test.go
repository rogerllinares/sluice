package queue

import (
	"testing"
	"time"
)

// backoff_test.go specifies the shared retry-backoff schedule (F5). The same
// function is used by both backends so the retry timing is single-sourced.

// TestBackoffGrowsExponentially pins that the base delay (before jitter) doubles
// each attempt: attempt 1 -> base, 2 -> 2*base, 3 -> 4*base, capped at maxDelay.
func TestBackoffGrowsExponentially(t *testing.T) {
	base := 100 * time.Millisecond
	maxDelay := 10 * time.Second

	tests := []struct {
		attempt  int
		wantBase time.Duration
	}{
		{attempt: 1, wantBase: 100 * time.Millisecond},
		{attempt: 2, wantBase: 200 * time.Millisecond},
		{attempt: 3, wantBase: 400 * time.Millisecond},
		{attempt: 4, wantBase: 800 * time.Millisecond},
	}
	for _, tc := range tests {
		// Jitter adds at most 100% of the base, so the result is in
		// [wantBase, 2*wantBase). We assert the floor and the ceiling.
		got := backoffDelay(tc.attempt, base, maxDelay)
		if got < tc.wantBase {
			t.Errorf("backoffDelay(attempt=%d) = %v, want >= %v", tc.attempt, got, tc.wantBase)
		}
		if got >= 2*tc.wantBase {
			t.Errorf("backoffDelay(attempt=%d) = %v, want < %v (jitter ceiling)", tc.attempt, got, 2*tc.wantBase)
		}
	}
}

// TestBackoffIsCapped pins that the exponential growth never exceeds maxDelay
// (plus its jitter band), so a long-lived retry loop cannot schedule absurd
// delays.
func TestBackoffIsCapped(t *testing.T) {
	base := 1 * time.Second
	maxDelay := 4 * time.Second

	// attempt 10 would be 2^9 * 1s = 512s uncapped; must clamp to maxDelay.
	got := backoffDelay(10, base, maxDelay)
	if got < maxDelay {
		t.Errorf("backoffDelay(attempt=10) = %v, want >= cap %v", got, maxDelay)
	}
	if got >= 2*maxDelay {
		t.Errorf("backoffDelay(attempt=10) = %v, want < %v (capped + jitter ceiling)", got, 2*maxDelay)
	}
}

// TestBackoffFirstAttempt pins the attempt<=1 edge: the very first retry still
// gets at least the base delay (no zero-delay hot loop).
func TestBackoffFirstAttempt(t *testing.T) {
	base := 50 * time.Millisecond
	if got := backoffDelay(1, base, time.Minute); got < base {
		t.Errorf("backoffDelay(attempt=1) = %v, want >= base %v", got, base)
	}
	// attempt 0 (shouldn't happen, but must not panic or go negative)
	if got := backoffDelay(0, base, time.Minute); got < 0 {
		t.Errorf("backoffDelay(attempt=0) = %v, want non-negative", got)
	}
}
