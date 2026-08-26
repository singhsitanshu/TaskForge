package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestRunConcurrentUniqueSubmissions(t *testing.T) {
	var mutex sync.Mutex
	keys := make(map[string]struct{})
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		mutex.Lock()
		keys[request.Header.Get("Idempotency-Key")] = struct{}{}
		mutex.Unlock()
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(response).Encode(map[string]string{"id": request.Header.Get("Idempotency-Key")})
	}))
	defer server.Close()

	benchmarkResult, err := run(context.Background(), configuration{
		URL: server.URL, Operation: "submit", Count: 20, Concurrency: 5,
		TaskType: "test.noop", Payload: `{}`, Queue: "default", MaxAttempts: 1,
		KeyMode: "unique", KeyPrefix: "test", Timeout: defaultTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	if benchmarkResult.Successes != 20 || benchmarkResult.DistinctTaskIDs != 20 || len(keys) != 20 {
		t.Fatalf("unexpected result: %+v keys=%d", benchmarkResult, len(keys))
	}
}

func TestPercentile(t *testing.T) {
	values := []float64{5, 1, 4, 2, 3}
	if got := percentile(values, 0.50); got != 3 {
		t.Fatalf("p50=%v", got)
	}
	if got := percentile(values, 1); got != 5 {
		t.Fatalf("max=%v", got)
	}
}

const defaultTimeout = 30_000_000_000
