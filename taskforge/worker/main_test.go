package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestHealthcheck(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	newMux().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
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
