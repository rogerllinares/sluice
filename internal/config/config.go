// Package config loads Sluice's runtime configuration from the environment:
// one SLUICE_* variable per knob, every knob with a default, malformed values
// rejected with an error naming the variable. Keeping the parsing here leaves
// the composition root (cmd/sluice) a straight-line reader of a typed struct.
package config

import (
	"fmt"
	"strconv"
	"time"
)

// Config carries every runtime knob of the sluice binary.
type Config struct {
	// DatabaseURL selects the backend: set means durable Postgres, empty means
	// the in-memory bounded channel (jobs do not survive a restart).
	DatabaseURL string

	// QueueDepth bounds the backlog — jobs waiting, not jobs in flight. The
	// in-memory buffer holds at most this many; the durable backend sheds
	// (429) once the queued backlog reaches it.
	QueueDepth int

	// NumWorkers sizes the worker pool. 0 lets the pool default to NumCPU.
	NumWorkers int

	// HTTPAddr is the listen address of the producer edge.
	HTTPAddr string

	// ShutdownCeiling bounds the whole graceful drain: HTTP shutdown, pool
	// drain, and pending-retry wait all share this single deadline.
	ShutdownCeiling time.Duration

	// VisibilityTimeout is how long a reserved job stays invisible before the
	// durable backend assumes its worker crashed and redelivers it.
	VisibilityTimeout time.Duration

	// MaxAttempts is how many deliveries a job gets before the next failure
	// dead-letters it.
	MaxAttempts int

	// BaseBackoff and MaxBackoff shape the capped exponential retry delay.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// Load reads the configuration from getenv — injectable for tests; pass
// os.Getenv in production. Unset or empty variables keep their defaults.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		DatabaseURL:       getenv("SLUICE_DATABASE_URL"),
		QueueDepth:        100,
		NumWorkers:        0,
		HTTPAddr:          ":8080",
		ShutdownCeiling:   20 * time.Second,
		VisibilityTimeout: 30 * time.Second,
		MaxAttempts:       3,
		BaseBackoff:       500 * time.Millisecond,
		MaxBackoff:        30 * time.Second,
	}
	if v := getenv("SLUICE_HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
	for _, f := range []struct {
		name string
		dst  *int
	}{
		{"SLUICE_QUEUE_DEPTH", &cfg.QueueDepth},
		{"SLUICE_NUM_WORKERS", &cfg.NumWorkers},
		{"SLUICE_MAX_ATTEMPTS", &cfg.MaxAttempts},
	} {
		if err := loadInt(getenv, f.name, f.dst); err != nil {
			return Config{}, err
		}
	}
	for _, f := range []struct {
		name string
		dst  *time.Duration
	}{
		{"SLUICE_SHUTDOWN_CEILING", &cfg.ShutdownCeiling},
		{"SLUICE_VISIBILITY_TIMEOUT", &cfg.VisibilityTimeout},
		{"SLUICE_BASE_BACKOFF", &cfg.BaseBackoff},
		{"SLUICE_MAX_BACKOFF", &cfg.MaxBackoff},
	} {
		if err := loadDuration(getenv, f.name, f.dst); err != nil {
			return Config{}, err
		}
	}
	return cfg, nil
}

// loadInt parses name as a non-negative integer into dst; empty keeps the
// default.
func loadInt(getenv func(string) string, name string, dst *int) error {
	v := getenv(name)
	if v == "" {
		return nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("%s: %q is not an integer", name, v)
	}
	if n < 0 {
		return fmt.Errorf("%s: %d must not be negative", name, n)
	}
	*dst = n
	return nil
}

// loadDuration parses name as a Go duration ("20s", "500ms") into dst; empty
// keeps the default.
func loadDuration(getenv func(string) string, name string, dst *time.Duration) error {
	v := getenv(name)
	if v == "" {
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("%s: %q is not a duration (examples: \"20s\", \"500ms\")", name, v)
	}
	if d < 0 {
		return fmt.Errorf("%s: %s must not be negative", name, d)
	}
	*dst = d
	return nil
}
