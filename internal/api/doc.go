// Package api exposes the HTTP producer edge for Sluice.
//
// The enqueue endpoint is where backpressure and load-shedding become visible:
// it calls the queue's non-blocking enqueue path (Submit) and, when the bounded
// queue is full (queue.ErrQueueFull), responds with HTTP 429 + Retry-After
// instead of blocking the process or growing memory. A blocking variant
// (queue.SubmitWait) is offered for callers that prefer to wait. This package
// also hosts the /metrics endpoint in F7.
//
// The HTTP Server lives in server.go; behaviour is built test-first.
package api
