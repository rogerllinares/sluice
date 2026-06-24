package queue

import (
	"math/rand"
	"time"
)

const (
	// defaultBaseBackoff is the first-retry delay; each further attempt doubles
	// it (before jitter), up to defaultMaxBackoff.
	defaultBaseBackoff = 500 * time.Millisecond
	// defaultMaxBackoff caps the exponential growth so a long retry loop cannot
	// schedule absurd delays.
	defaultMaxBackoff = 30 * time.Second
	// defaultMaxAttempts is how many deliveries a job gets before the next Nack
	// dead-letters it instead of redelivering.
	defaultMaxAttempts = 3
)

// backoffDelay returns the retry delay for the given attempt number using
// capped exponential backoff with full jitter. The base doubles per attempt
// (attempt 1 -> base, 2 -> 2*base, ...), clamps at maxDelay, then adds a random
// jitter of up to 100% of that capped value. Full jitter spreads retries so a
// thundering herd of jobs that all failed at once do not all retry in lockstep.
// The result is in [capped, 2*capped).
func backoffDelay(attempt int, base, maxDelay time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// Exponential growth: base * 2^(attempt-1), guarding against shift overflow
	// by clamping to maxDelay as soon as we exceed it.
	capped := base
	for i := 1; i < attempt; i++ {
		capped *= 2
		if capped >= maxDelay {
			capped = maxDelay
			break
		}
	}
	if capped > maxDelay {
		capped = maxDelay
	}
	// Full jitter: a random amount in [0, capped) added to the capped base.
	jitter := time.Duration(rand.Int63n(int64(capped)))
	return capped + jitter
}
