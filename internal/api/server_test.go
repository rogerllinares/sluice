package api_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/rogerllinares/sluice/internal/api"
	"github.com/rogerllinares/sluice/internal/queue"
)

// server_test.go specifies the HTTP producer edge (F4): the enqueue endpoint
// where load shedding becomes visible. The handler decodes a job from the
// request body, calls the non-blocking Submit, and maps a full queue to
// HTTP 429 + Retry-After instead of blocking. Success is 202 Accepted; a
// malformed body is 400.

// TestEnqueueHandlerAccepts is the happy path: a well-formed job is submitted to
// a queue with room and the handler returns 202 Accepted.
func TestEnqueueHandlerAccepts(t *testing.T) {
	q := queue.NewMemory(4)
	srv := mustNewServer(t, q)

	body := []byte(`{"id":"job-1","payload":"aGVsbG8="}`)
	req := httptest.NewRequest(http.MethodPost, "/enqueue", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	srv.EnqueueHandler(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("EnqueueHandler status = %d, want %d (202)", rec.Code, http.StatusAccepted)
	}

	// The job must actually be on the queue (dequeueable).
	got, err := q.Dequeue(context.Background())
	if err != nil {
		t.Fatalf("Dequeue after accept returned error: %v", err)
	}
	if got.ID != "job-1" {
		t.Errorf("dequeued job ID = %q, want %q", got.ID, "job-1")
	}
}

// TestEnqueueHandlerShedsWhenFull is the load-shedding contract: when the
// bounded queue is full, Submit returns ErrQueueFull and the handler maps it to
// 429 Too Many Requests with a Retry-After header — never blocking.
func TestEnqueueHandlerShedsWhenFull(t *testing.T) {
	const depth = 1
	q := queue.NewMemory(depth)
	srv := mustNewServer(t, q)

	// Fill the queue to its bound so the next Submit sheds.
	if err := q.Submit(context.Background(), queue.Job{ID: "fill"}); err != nil {
		t.Fatalf("Submit to fill queue returned error: %v", err)
	}

	body := []byte(`{"id":"overflow","payload":""}`)
	req := httptest.NewRequest(http.MethodPost, "/enqueue", bytes.NewReader(body))
	rec := httptest.NewRecorder()

	srv.EnqueueHandler(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("EnqueueHandler on full queue status = %d, want %d (429)", rec.Code, http.StatusTooManyRequests)
	}
	retryAfter := rec.Header().Get("Retry-After")
	if retryAfter == "" {
		t.Fatal("429 response missing Retry-After header")
	}
	if _, err := strconv.Atoi(retryAfter); err != nil {
		t.Errorf("Retry-After = %q, want an integer number of seconds: %v", retryAfter, err)
	}
}

// TestEnqueueHandlerRejectsBadBody pins input validation: a malformed JSON body
// is a client error (400), not a 500 or a silent enqueue.
func TestEnqueueHandlerRejectsBadBody(t *testing.T) {
	q := queue.NewMemory(4)
	srv := mustNewServer(t, q)

	req := httptest.NewRequest(http.MethodPost, "/enqueue", bytes.NewReader([]byte("not json")))
	rec := httptest.NewRecorder()

	srv.EnqueueHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("EnqueueHandler with bad body status = %d, want %d (400)", rec.Code, http.StatusBadRequest)
	}
}

// TestEnqueueHandlerRejectsEmptyID pins that an empty job ID is a client error
// (400), not a silent 202. F5 builds idempotency on the ID, so an empty ID must
// not slip onto the queue.
func TestEnqueueHandlerRejectsEmptyID(t *testing.T) {
	q := queue.NewMemory(4)
	srv := mustNewServer(t, q)

	req := httptest.NewRequest(http.MethodPost, "/enqueue", bytes.NewReader([]byte(`{"id":"","payload":""}`)))
	rec := httptest.NewRecorder()

	srv.EnqueueHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("EnqueueHandler with empty id status = %d, want %d (400)", rec.Code, http.StatusBadRequest)
	}
	if _, err := q.Dequeue(contextWithImmediateCancel()); err == nil {
		t.Error("a job with an empty ID was enqueued, want none")
	}
}

// contextWithImmediateCancel returns an already-cancelled context so a Dequeue
// on an empty queue returns immediately (ctx error) instead of blocking.
func contextWithImmediateCancel() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// TestNewServerRejectsNonSheddingBackend pins fail-fast wiring: a queue that
// cannot shed (no non-blocking Submit) is a misconfiguration that must surface
// as a construction error — not a panic, and not a 500 under load.
func TestNewServerRejectsNonSheddingBackend(t *testing.T) {
	srv, err := api.NewServer(nonSheddingQueue{})
	if err == nil {
		t.Fatal("NewServer accepted a non-shedding backend, want a construction error")
	}
	if srv != nil {
		t.Error("NewServer returned a non-nil server alongside the error")
	}
}

// TestEnqueueHandlerDedupesIdempotencyKey pins that idempotency is reachable
// from the wire: two requests carrying the same idempotency_key both succeed
// (dedupe is a success, not an error) but only one job lands on the queue.
func TestEnqueueHandlerDedupesIdempotencyKey(t *testing.T) {
	q := queue.NewMemory(4)
	srv := mustNewServer(t, q)

	for i := 0; i < 2; i++ {
		body := []byte(`{"id":"job-` + strconv.Itoa(i) + `","payload":"","idempotency_key":"same-key"}`)
		req := httptest.NewRequest(http.MethodPost, "/enqueue", bytes.NewReader(body))
		rec := httptest.NewRecorder()

		srv.EnqueueHandler(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("request %d with idempotency_key: status = %d, want %d (202)", i, rec.Code, http.StatusAccepted)
		}
	}

	if _, err := q.Dequeue(context.Background()); err != nil {
		t.Fatalf("Dequeue of the deduped job returned error: %v", err)
	}
	if got, err := q.Dequeue(contextWithImmediateCancel()); err == nil {
		t.Errorf("duplicate idempotency_key enqueued a second job (%q), want dedupe", got.ID)
	}
}

// mustNewServer fails the test on a wiring error so the handler tests stay
// focused on handler behaviour.
func mustNewServer(t *testing.T, q queue.Queue) *api.Server {
	t.Helper()
	srv, err := api.NewServer(q)
	if err != nil {
		t.Fatalf("api.NewServer: %v", err)
	}
	return srv
}

// submitterCapability mirrors the capability NewServer asserts. Every shipped
// backend must satisfy it — the durable backend joining the HTTP edge is the
// whole point of the seam.
type submitterCapability interface {
	Submit(ctx context.Context, job queue.Job) error
}

var (
	_ submitterCapability = (*queue.Memory)(nil)
	_ submitterCapability = (*queue.Postgres)(nil)
)

// nonSheddingQueue implements queue.Queue but NOT the non-blocking Submit, so it
// cannot shed load — exactly the backend NewServer must reject.
type nonSheddingQueue struct{}

func (nonSheddingQueue) Enqueue(context.Context, queue.Job) error   { return nil }
func (nonSheddingQueue) Dequeue(context.Context) (queue.Job, error) { return queue.Job{}, nil }
func (nonSheddingQueue) Ack(context.Context, queue.Job) error       { return nil }
func (nonSheddingQueue) Nack(context.Context, queue.Job) error      { return nil }
