// Package api exposes the HTTP producer edge for Sluice.
//
// The enqueue endpoint is where backpressure and load-shedding become visible:
// it calls the queue's non-blocking enqueue path and, when the bounded queue is
// full (queue.ErrQueueFull), responds with HTTP 429 + Retry-After instead of
// blocking the process or growing memory. A blocking variant (SubmitWait) is
// offered for callers that prefer to wait. This package also hosts the
// /metrics endpoint (see ROADMAP F4/F7).
//
// This file is a SKELETON: handler stub signatures with TODOs, no logic. The
// behaviour is built test-first.
package api

import (
	"net/http"

	"github.com/rogerllinares/sluice/internal/queue"
)

// Server holds the dependencies the HTTP handlers need (the queue, config).
//
// TODO(F4): hold a queue.Queue and any limits (Retry-After hint); wire test-first.
type Server struct {
	// TODO(F4): fields (queue, config).
}

// NewServer constructs the HTTP server over the given queue.
//
// TODO(F4): implement test-first.
func NewServer(q queue.Queue) *Server {
	_ = q
	// TODO(F4): build the Server.
	return nil
}

// EnqueueHandler accepts a job over HTTP and enqueues it, mapping a full queue
// to 429 + Retry-After (load shedding) rather than blocking.
//
// TODO(F4): decode the request, build a queue.Job, call Enqueue; on
//           queue.ErrQueueFull write 429 + Retry-After; on success write 202.
func (s *Server) EnqueueHandler(w http.ResponseWriter, r *http.Request) {
	_ = w
	_ = r
	// TODO(F4).
}
