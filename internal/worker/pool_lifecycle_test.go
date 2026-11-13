package worker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rogerllinares/sluice/internal/queue"
	"github.com/rogerllinares/sluice/internal/worker"
)

// Start cannot fail, so it returns nothing: an always-nil error only taught
// callers to write dead error handling. Compile-time check of the signature.
var _ func(context.Context) = (*worker.Pool)(nil).Start

// TestLifecycleCallsRaceFree runs Start against Shutdown and Stop from other
// goroutines. Before the fix, Start wrote the pool's contexts and cancel funcs
// while Shutdown/Stop read them unsynchronised: `go test -race` (CI) flags that.
// The pool must also end up stopped and leak-free whichever call wins.
func TestLifecycleCallsRaceFree(t *testing.T) {
	for i := 0; i < 50; i++ {
		pool := worker.New(queue.NewMemory(1), func(context.Context, queue.Job) error { return nil },
			worker.Config{NumWorkers: 2})

		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); pool.Start(context.Background()) }()
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = pool.Shutdown(ctx)
		}()
		go func() { defer wg.Done(); pool.Stop() }()
		wg.Wait()

		pool.Stop() // Start may have won the race: stop it so no worker outlives the test
		pool.Wait()
	}
}
