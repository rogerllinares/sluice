package api

import (
	"encoding/json"
	"errors"
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
// non-blocking shedding (Submit); a backend that cannot shed is a wiring error,
// so NewServer panics at construction rather than failing with a 500 under load.
func NewServer(q queue.Queue) *Server {
	sub, ok := q.(submitter)
	if !ok {
		panic("api.NewServer: queue does not support non-blocking Submit (cannot shed load)")
	}
	return &Server{q: sub, retryAfterSeconds: defaultRetryAfterSeconds}
}

// submitter is the non-blocking enqueue path the handler needs. The Queue
// interface is intentionally backend-agnostic and does not include Submit
// (shedding is a Memory-level concern), so the server depends on this narrow
// capability — every shedding-capable backend can satisfy it.
type submitter interface {
	Submit(job queue.Job) error
}

// enqueueRequest is the JSON shape accepted by EnqueueHandler. Payload is a
// base64 string on the wire (encoding/json's standard []byte encoding).
type enqueueRequest struct {
	ID      string `json:"id"`
	Payload []byte `json:"payload"`
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

	err := s.q.Submit(queue.Job{ID: req.ID, Payload: req.Payload})
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
