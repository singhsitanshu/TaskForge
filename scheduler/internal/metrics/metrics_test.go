package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"taskforge/scheduler/internal/domain"
)

func TestSnapshotValuesReplaceRatherThanAccumulate(t *testing.T) {
	metrics := New()
	metrics.SetSnapshot(domain.GlobalSnapshot{
		TaskCounts:      map[string]int64{"QUEUED": 10, "RUNNING": 4},
		WorkerCounts:    map[string]int64{"ACTIVE": 2, "STALE": 1, "DEAD": 3},
		RunningAttempts: 4, EligibleTasks: 10, ExpiredRunningLeases: 1,
	})
	metrics.SetSnapshot(domain.GlobalSnapshot{
		TaskCounts:      map[string]int64{"QUEUED": 7},
		WorkerCounts:    map[string]int64{"ACTIVE": 1},
		RunningAttempts: 0, EligibleTasks: 6, ExpiredRunningLeases: 0,
	})
	body := scrape(t, metrics)
	for _, expected := range []string{
		`taskforge_tasks_current{status="QUEUED"} 7`,
		`taskforge_tasks_current{status="RUNNING"} 0`,
		`taskforge_workers_current{liveness="ACTIVE"} 1`,
		`taskforge_workers_current{liveness="DEAD"} 0`,
		`taskforge_tasks_eligible_for_claim 6`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("missing %q in metrics:\n%s", expected, body)
		}
	}
}

func TestSchedulerCounterAndHistogramSemantics(t *testing.T) {
	metrics := New()
	metrics.RecoveryBatch(10*time.Millisecond, nil)
	metrics.Recovered("requeued", 2*time.Second)
	metrics.TaskTotalLatency(3 * time.Second)
	metrics.RetryBatch(5*time.Millisecond, nil)
	metrics.RetryPromoted(time.Second)
	body := scrape(t, metrics)
	for _, expected := range []string{
		`taskforge_task_recoveries_total{outcome="requeued"} 1`,
		`taskforge_task_attempts_total{outcome="abandoned"} 1`,
		`taskforge_recovery_lag_seconds_count 1`,
		`taskforge_task_total_latency_seconds_count 1`,
		`taskforge_task_retries_promoted_total 1`,
		`taskforge_retry_lateness_seconds_count 1`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("missing %q in metrics", expected)
		}
	}
}

func scrape(t *testing.T, metrics *Metrics) string {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status=%d", response.Code)
	}
	if !strings.Contains(response.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("unexpected content type %q", response.Header().Get("Content-Type"))
	}
	return response.Body.String()
}
