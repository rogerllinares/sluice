package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/rogerllinares/sluice/internal/queue"
)

// defaultRetryAfterSeconds is the Retry-After hint sent with a 429 when the
// queue sheds load. It is a coarse client-side backoff suggestion, not a
// guarantee about when a slot frees.
const defaultRetryAfterSeconds = 1

// Server is the HTTP producer edge over a shedding-capable queue. It owns no
// goroutines; it just maps HTTP requests onto the queue's non-blocking Submit
// path so that a full queue sheds (429) rather than blocking the request.
type Server struct {
	q                 submitter
	retryAfterSeconds int
}

// NewServer constructs a Server over the given queue. The queue must support
// non-blocking shedding (Submit); a backend that cannot shed is a wiring error
// surfaced as a construction error — never as a 500 under load.
func NewServer(q queue.Queue) (*Server, error) {
	sub, ok := q.(submitter)
	if !ok {
		return nil, fmt.Errorf("api.NewServer: queue %T does not support non-blocking Submit (cannot shed load)", q)
	}
	return &Server{q: sub, retryAfterSeconds: defaultRetryAfterSeconds}, nil
}

// submitter is the non-blocking enqueue path the handler needs. The Queue
// interface is intentionally backend-agnostic and does not include Submit, so
// the server depends on this narrow capability — every shedding-capable
// backend (Memory's bounded buffer, Postgres' advisory WithQueueDepth bound)
// satisfies it. The ctx carries the request's cancellation and deadline into
// the enqueue path.
type submitter interface {
	Submit(ctx context.Context, job queue.Job) error
}

// enqueueRequest is the JSON shape accepted by EnqueueHandler. Payload is a
// base64 string on the wire (encoding/json's standard []byte encoding). An
// optional idempotency_key makes the enqueue deduplicating: a producer that
// retries the request never creates a second job.
type enqueueRequest struct {
	ID             string `json:"id"`
	Payload        []byte `json:"payload"`
	IdempotencyKey string `json:"idempotency_key"`
}

// EnqueueHandler accepts a job over HTTP and enqueues it via the non-blocking
// Submit. A full queue is mapped to 429 + Retry-After (load shedding) rather
// than blocking the request; a malformed body is 400; success is 202.
func (s *Server) EnqueueHandler(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.ID == "" {
		// ID is the idempotency seam F5 builds on; never enqueue an empty one.
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	err := s.q.Submit(r.Context(), queue.Job{
		ID:             req.ID,
		Payload:        req.Payload,
		IdempotencyKey: req.IdempotencyKey,
	})
	switch {
	case errors.Is(err, queue.ErrQueueFull):
		w.Header().Set("Retry-After", strconv.Itoa(s.retryAfterSeconds))
		http.Error(w, "queue full, retry later", http.StatusTooManyRequests)
	case err != nil:
		http.Error(w, "enqueue failed", http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusAccepted)
	}
}
