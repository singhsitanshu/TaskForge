package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWorkerMetricsExposeBoundedCountersAndHistograms(t *testing.T) {
	metrics := New()
	metrics.TaskClaimed(5*time.Millisecond, 20*time.Millisecond)
	metrics.Execution(10 * time.Millisecond)
	metrics.TaskCompleted("success")
	metrics.Attempt("succeeded")
	metrics.Heartbeat("success")
	metrics.LeaseRenewal("lost", time.Millisecond)
	metrics.RetryScheduled(2 * time.Second)
	metrics.TaskTotalLatency(3 * time.Second)

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("status=%d content-type=%q", response.Code, response.Header().Get("Content-Type"))
	}
	body := response.Body.String()
	for _, expected := range []string{
		`taskforge_worker_tasks_claimed_total 1`,
		`taskforge_worker_tasks_completed_total{outcome="success"} 1`,
		`taskforge_task_attempts_total{outcome="succeeded"} 1`,
		`taskforge_worker_heartbeats_total{outcome="success"} 1`,
		`taskforge_worker_lease_renewals_total{outcome="lost"} 1`,
		`taskforge_task_queue_wait_seconds_count 1`,
		`taskforge_task_execution_duration_seconds_count 1`,
		`taskforge_task_total_latency_seconds_count 1`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("missing %q", expected)
		}
	}
	if strings.Contains(body, "task_id=") || strings.Contains(body, "worker_id=") {
		t.Fatal("high-cardinality identity leaked into metric labels")
	}
}
