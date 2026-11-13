package queue_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain runs every queue test under goleak: a retry timer, a reaper or a
// blocked Dequeue that outlives its test fails the package, the same guarantee
// the worker package already has.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
