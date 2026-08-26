package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestHealthcheck(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	newMux().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}
}

func TestReadinessReflectsDatabaseCheck(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want int
	}{{"ready", nil, http.StatusOK}, {"unavailable", errors.New("down"), http.StatusServiceUnavailable}} {
		t.Run(test.name, func(t *testing.T) {
			handler := newOperationalMux(http.NotFoundHandler(), func(context.Context) error { return test.err })
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if response.Code != test.want {
				t.Fatalf("status=%d want=%d", response.Code, test.want)
			}
		})
	}
}

func TestResolveExplicitWorkerIdentity(t *testing.T) {
	t.Setenv("WORKER_ID", "worker-instance-123")
	t.Setenv("WORKER_NAME", "payments-worker")

	identity, err := resolveWorkerIdentity()
	if err != nil {
		t.Fatalf("resolve worker identity: %v", err)
	}
	if identity.InstanceID != "worker-instance-123" {
		t.Fatalf("unexpected instance id %q", identity.InstanceID)
	}
	if identity.Name != "payments-worker" {
		t.Fatalf("unexpected worker name %q", identity.Name)
	}
}

func TestResolveFallbackWorkerIdentity(t *testing.T) {
	t.Setenv("WORKER_ID", "")
	t.Setenv("WORKER_NAME", "")

	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("resolve hostname: %v", err)
	}
	identity, err := resolveWorkerIdentity()
	if err != nil {
		t.Fatalf("resolve worker identity: %v", err)
	}
	if !strings.HasPrefix(identity.InstanceID, hostname+"-") {
		t.Fatalf("instance id %q does not start with hostname %q", identity.InstanceID, hostname)
	}
	if len(identity.InstanceID) <= len(hostname)+1 {
		t.Fatalf("instance id %q has no random process suffix", identity.InstanceID)
	}
	if identity.Name != hostname {
		t.Fatalf("expected hostname %q as name, got %q", hostname, identity.Name)
	}
}

func TestResolveHeartbeatConfiguration(t *testing.T) {
	t.Setenv("WORKER_HEARTBEAT_INTERVAL", "100ms")
	t.Setenv("WORKER_STALE_AFTER", "300ms")
	t.Setenv("WORKER_DEAD_AFTER", "700ms")
	t.Setenv("WORKER_HEARTBEAT_TIMEOUT", "50ms")

	configuration, err := resolveHeartbeatConfig()
	if err != nil {
		t.Fatalf("resolve heartbeat configuration: %v", err)
	}
	if configuration.Interval != 100*time.Millisecond ||
		configuration.StaleAfter != 300*time.Millisecond ||
		configuration.DeadAfter != 700*time.Millisecond ||
		configuration.Timeout != 50*time.Millisecond {
		t.Fatalf("unexpected heartbeat configuration: %#v", configuration)
	}
}

func TestResolveLeaseConfiguration(t *testing.T) {
	t.Setenv("WORKER_TASK_LEASE_DURATION", "500ms")
	t.Setenv("WORKER_TASK_LEASE_RENEW_INTERVAL", "100ms")
	t.Setenv("WORKER_TASK_LEASE_RENEW_TIMEOUT", "50ms")

	configuration, err := resolveLeaseConfig()
	if err != nil {
		t.Fatalf("resolve lease configuration: %v", err)
	}
	if configuration.Duration != 500*time.Millisecond ||
		configuration.RenewInterval != 100*time.Millisecond ||
		configuration.RenewTimeout != 50*time.Millisecond {
		t.Fatalf("unexpected lease configuration: %#v", configuration)
	}
}

func TestResolveRetryConfiguration(t *testing.T) {
	t.Setenv("TASK_RETRY_BASE_DELAY", "100ms")
	t.Setenv("TASK_RETRY_MAX_DELAY", "400ms")
	t.Setenv("TASK_RETRY_JITTER", "0")
	configuration, err := resolveRetryConfig()
	if err != nil {
		t.Fatalf("resolve retry configuration: %v", err)
	}
	if configuration.BaseDelay != 100*time.Millisecond ||
		configuration.MaxDelay != 400*time.Millisecond ||
		configuration.Jitter != 0 {
		t.Fatalf("unexpected retry configuration: %+v", configuration)
	}
}
