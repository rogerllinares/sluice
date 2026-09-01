package queue

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// postgres_test.go specifies the durable backend (F2). The integration tests
// run against a real Postgres spun up per-suite via testcontainers, so they
// require a running Docker daemon. The locking guarantee under test is
// SELECT ... FOR UPDATE SKIP LOCKED: concurrent workers never reserve the same
// row, and a crashed worker's in-flight job becomes visible again after the
// visibility timeout.

// requireDocker gates the integration tests on a healthy Docker provider.
// On a machine without a running daemon the tests skip with a clear message
// (go test ./... stays green for reviewers without Docker); in CI the daemon
// is part of the contract, so the same condition fails loudly instead — the
// suite must never silently shrink where it is authoritative.
func requireDocker(t *testing.T) {
	t.Helper()
	unavailable := func(reason any) {
		if os.Getenv("CI") != "" {
			t.Fatalf("docker daemon unavailable in CI (integration tests must run here): %v", reason)
		}
		t.Skipf("skipping integration test: docker daemon unavailable: %v", reason)
	}
	// GetProvider can panic on exotic misconfigurations (see the upstream
	// SkipIfProviderIsNotHealthy helper); treat that the same as an error.
	defer func() {
		if r := recover(); r != nil {
			unavailable(r)
		}
	}()
	provider, err := testcontainers.ProviderDocker.GetProvider()
	if err == nil {
		err = provider.Health(context.Background())
	}
	if err != nil {
		unavailable(err)
	}
}

// newTestContainer starts an ephemeral Postgres and returns its DSN. Cleanup is
// registered on t.
func newTestContainer(t *testing.T) string {
	t.Helper()
	requireDocker(t)
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("sluice"),
		postgres.WithUsername("sluice"),
		postgres.WithPassword("sluice"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return dsn
}

// newTestQueue starts a container and returns a ready *Postgres wired to it.
func newTestQueue(t *testing.T, opts ...Option) *Postgres {
	t.Helper()
	q, err := NewPostgres(context.Background(), newTestContainer(t), opts...)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	t.Cleanup(q.Close)
	return q
}

// dequeueWithin reserves a job, failing the test if none is ready within d.
func dequeueWithin(t *testing.T, q *Postgres, d time.Duration) Job {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	job, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	return job
}

// TestPostgresImplementsQueue is a compile-time check that the durable backend
// satisfies the same seam as the in-memory one.
func TestPostgresImplementsQueue(t *testing.T) {
	var _ Queue = (*Postgres)(nil)
}

// TestPostgresRoundTrip proves enqueue -> dequeue -> ack, and that an acked job
// is gone (a short Dequeue then finds nothing).
func TestPostgresRoundTrip(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	if err := q.Enqueue(ctx, Job{ID: "job-1", Payload: []byte("hello")}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got := dequeueWithin(t, q, 5*time.Second)
	if got.ID != "job-1" || !bytes.Equal(got.Payload, []byte("hello")) {
		t.Fatalf("Dequeue returned %+v, want id=job-1 payload=hello", got)
	}
	if err := q.Ack(ctx, got); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	empty, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := q.Dequeue(empty); err == nil {
		t.Fatal("Dequeue found a job after ack; queue should be empty")
	}
}

// TestPostgresFIFOOrder asserts a single consumer drains jobs in enqueue order.
func TestPostgresFIFOOrder(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	ids := []string{"a", "b", "c"}
	for _, id := range ids {
		if err := q.Enqueue(ctx, Job{ID: id, Payload: []byte(id)}); err != nil {
			t.Fatalf("Enqueue(%q): %v", id, err)
		}
	}
	for _, want := range ids {
		got := dequeueWithin(t, q, 5*time.Second)
		if got.ID != want {
			t.Fatalf("FIFO violated: got %q, want %q", got.ID, want)
		}
		if err := q.Ack(ctx, got); err != nil {
			t.Fatalf("Ack(%q): %v", got.ID, err)
		}
	}
}

// TestPostgresExactlyOnceUnderConcurrency is the star test: many workers drain
// a batch concurrently and every job is reserved exactly once — the proof that
// FOR UPDATE SKIP LOCKED prevents double-dequeue.
func TestPostgresExactlyOnceUnderConcurrency(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	const n = 50
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("job-%02d", i)
		if err := q.Enqueue(ctx, Job{ID: id, Payload: []byte("x")}); err != nil {
			t.Fatalf("Enqueue(%q): %v", id, err)
		}
	}

	var (
		mu     sync.Mutex
		counts = make(map[string]int)
		wg     sync.WaitGroup
	)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				dctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				job, err := q.Dequeue(dctx)
				cancel()
				if err != nil { // queue drained -> Dequeue times out
					return
				}
				mu.Lock()
				counts[job.ID]++
				mu.Unlock()
				if err := q.Ack(ctx, job); err != nil {
					t.Errorf("Ack(%q): %v", job.ID, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if len(counts) != n {
		t.Fatalf("saw %d distinct jobs, want %d", len(counts), n)
	}
	for id, c := range counts {
		if c != 1 {
			t.Errorf("job %q reserved %d times, want exactly 1", id, c)
		}
	}
}

// TestPostgresRedeliveryAfterVisibilityTimeout proves the lease/reaper: a
// reserved-but-unacked job (crashed worker) becomes visible again after the
// visibility timeout, with attempts incremented.
func TestPostgresRedeliveryAfterVisibilityTimeout(t *testing.T) {
	q := newTestQueue(t, WithVisibilityTimeout(1*time.Second))
	ctx := context.Background()

	if err := q.Enqueue(ctx, Job{ID: "crash-job", Payload: []byte("x")}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	first := dequeueWithin(t, q, 5*time.Second)
	if first.Attempts != 1 {
		t.Fatalf("first reserve attempts=%d, want 1", first.Attempts)
	}
	// Simulate a crash: never ack. Wait past the visibility timeout.
	time.Sleep(1500 * time.Millisecond)

	second := dequeueWithin(t, q, 5*time.Second)
	if second.ID != "crash-job" {
		t.Fatalf("redelivered %q, want crash-job", second.ID)
	}
	if second.Attempts != 2 {
		t.Fatalf("redelivery attempts=%d, want 2", second.Attempts)
	}
}

// TestPostgresNackRequeues proves Nack returns a job for redelivery.
func TestPostgresNackRequeues(t *testing.T) {
	q := newTestQueue(t)
	ctx := context.Background()

	if err := q.Enqueue(ctx, Job{ID: "retry-job", Payload: []byte("x")}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	first := dequeueWithin(t, q, 5*time.Second)
	if err := q.Nack(ctx, first); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	second := dequeueWithin(t, q, 5*time.Second)
	if second.ID != "retry-job" {
		t.Fatalf("got %q after nack, want retry-job", second.ID)
	}
	if second.Attempts != 2 {
		t.Fatalf("attempts after nack+redeliver=%d, want 2", second.Attempts)
	}
}

// TestPostgresDurabilityAcrossReconnect proves the queue state lives in
// Postgres, not in process memory: a new pool against the same database still
// sees an enqueued job.
func TestPostgresDurabilityAcrossReconnect(t *testing.T) {
	dsn := newTestContainer(t)
	ctx := context.Background()

	q1, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres(q1): %v", err)
	}
	if err := q1.Enqueue(ctx, Job{ID: "durable", Payload: []byte("x")}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	q1.Close()

	q2, err := NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres(q2): %v", err)
	}
	defer q2.Close()

	got := dequeueWithin(t, q2, 5*time.Second)
	if got.ID != "durable" {
		t.Fatalf("after reconnect got %q, want durable", got.ID)
	}
}
