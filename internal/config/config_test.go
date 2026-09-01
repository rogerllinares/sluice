package config

import (
	"strings"
	"testing"
	"time"
)

// env builds a getenv func over a map, mirroring how Load consumes os.Getenv.
func env(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(nil))
	if err != nil {
		t.Fatalf("Load with an empty environment returned error: %v", err)
	}

	want := Config{
		DatabaseURL:       "",
		QueueDepth:        100,
		NumWorkers:        0,
		HTTPAddr:          ":8080",
		ShutdownCeiling:   20 * time.Second,
		VisibilityTimeout: 30 * time.Second,
		MaxAttempts:       3,
		BaseBackoff:       500 * time.Millisecond,
		MaxBackoff:        30 * time.Second,
	}
	if cfg != want {
		t.Errorf("Load defaults = %+v, want %+v", cfg, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"SLUICE_DATABASE_URL":       "postgres://u:p@host:5432/db",
		"SLUICE_QUEUE_DEPTH":        "5",
		"SLUICE_NUM_WORKERS":        "2",
		"SLUICE_HTTP_ADDR":          ":9090",
		"SLUICE_SHUTDOWN_CEILING":   "5s",
		"SLUICE_VISIBILITY_TIMEOUT": "1500ms",
		"SLUICE_MAX_ATTEMPTS":       "7",
		"SLUICE_BASE_BACKOFF":       "100ms",
		"SLUICE_MAX_BACKOFF":        "2s",
	}))
	if err != nil {
		t.Fatalf("Load with overrides returned error: %v", err)
	}

	want := Config{
		DatabaseURL:       "postgres://u:p@host:5432/db",
		QueueDepth:        5,
		NumWorkers:        2,
		HTTPAddr:          ":9090",
		ShutdownCeiling:   5 * time.Second,
		VisibilityTimeout: 1500 * time.Millisecond,
		MaxAttempts:       7,
		BaseBackoff:       100 * time.Millisecond,
		MaxBackoff:        2 * time.Second,
	}
	if cfg != want {
		t.Errorf("Load overrides = %+v, want %+v", cfg, want)
	}
}

// TestLoadRejectsMalformedValues pins the error contract: a bad value fails
// loudly and the error names the offending variable, so an operator can fix
// the deployment without reading Go source.
func TestLoadRejectsMalformedValues(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"SLUICE_QUEUE_DEPTH", "many"},
		{"SLUICE_QUEUE_DEPTH", "-1"},
		{"SLUICE_NUM_WORKERS", "2.5"},
		{"SLUICE_MAX_ATTEMPTS", "-3"},
		{"SLUICE_SHUTDOWN_CEILING", "20"}, // a bare number is not a duration
		{"SLUICE_VISIBILITY_TIMEOUT", "soon"},
		{"SLUICE_BASE_BACKOFF", "-500ms"},
		{"SLUICE_MAX_BACKOFF", "half a minute"},
	}
	for _, tc := range cases {
		t.Run(tc.name+"="+tc.value, func(t *testing.T) {
			_, err := Load(env(map[string]string{tc.name: tc.value}))
			if err == nil {
				t.Fatalf("Load accepted %s=%q, want an error", tc.name, tc.value)
			}
			if !strings.Contains(err.Error(), tc.name) {
				t.Errorf("error %q does not name the offending variable %s", err, tc.name)
			}
		})
	}
}
