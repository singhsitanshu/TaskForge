package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type configuration struct {
	URL         string
	Operation   string
	Count       int
	Concurrency int
	Rate        float64
	TaskType    string
	Payload     string
	Queue       string
	MaxAttempts int
	KeyMode     string
	KeyPrefix   string
	TaskID      string
	Timeout     time.Duration
	Output      string
}

type taskResponse struct {
	ID string `json:"id"`
}

type observation struct {
	Status  int
	Latency time.Duration
	TaskID  string
	Error   string
}

type percentiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

type result struct {
	StartedAt       time.Time      `json:"started_at"`
	FinishedAt      time.Time      `json:"finished_at"`
	DurationSeconds float64        `json:"duration_seconds"`
	Operation       string         `json:"operation"`
	RequestCount    int            `json:"request_count"`
	Successes       int            `json:"successes"`
	Failures        int            `json:"failures"`
	RequestsPerSec  float64        `json:"requests_per_second"`
	StatusCounts    map[string]int `json:"status_counts"`
	ErrorCounts     map[string]int `json:"error_counts"`
	DistinctTaskIDs int            `json:"distinct_task_ids"`
	FirstTaskID     string         `json:"first_task_id,omitempty"`
	LatencyMS       percentiles    `json:"latency_ms"`
	Configuration   map[string]any `json:"configuration"`
}

func main() {
	config := parseFlags()
	if err := validate(config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	benchmarkResult, err := run(context.Background(), config)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoded, err := json.MarshalIndent(benchmarkResult, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	encoded = append(encoded, '\n')
	if config.Output == "" || config.Output == "-" {
		_, _ = os.Stdout.Write(encoded)
		return
	}
	if err := os.WriteFile(config.Output, encoded, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseFlags() configuration {
	config := configuration{}
	flag.StringVar(&config.URL, "url", "http://localhost:8000", "TaskForge API base URL")
	flag.StringVar(&config.Operation, "operation", "submit", "submit, list, or get")
	flag.IntVar(&config.Count, "count", 1, "total request count")
	flag.IntVar(&config.Concurrency, "concurrency", 1, "maximum in-flight requests")
	flag.Float64Var(&config.Rate, "rate", 0, "paced arrival rate per second; zero is closed-loop")
	flag.StringVar(&config.TaskType, "task-type", "test.noop", "submitted task type")
	flag.StringVar(&config.Payload, "payload", `{}`, "submitted JSON object payload")
	flag.StringVar(&config.Queue, "queue", "default", "submitted queue")
	flag.IntVar(&config.MaxAttempts, "max-attempts", 3, "submitted maximum attempts")
	flag.StringVar(&config.KeyMode, "key-mode", "unique", "idempotency key mode: none, unique, or same")
	flag.StringVar(&config.KeyPrefix, "key-prefix", "tf012", "idempotency key or unique-key prefix")
	flag.StringVar(&config.TaskID, "task-id", "", "task ID for get operations")
	flag.DurationVar(&config.Timeout, "timeout", 30*time.Second, "per-request timeout")
	flag.StringVar(&config.Output, "output", "-", "JSON output path or - for stdout")
	flag.Parse()
	return config
}

func validate(config configuration) error {
	if config.Count < 1 || config.Concurrency < 1 {
		return errors.New("count and concurrency must be positive")
	}
	if config.Rate < 0 {
		return errors.New("rate cannot be negative")
	}
	if config.Operation != "submit" && config.Operation != "list" && config.Operation != "get" {
		return errors.New("operation must be submit, list, or get")
	}
	if config.Operation == "get" && config.TaskID == "" {
		return errors.New("task-id is required for get operations")
	}
	if config.KeyMode != "none" && config.KeyMode != "unique" && config.KeyMode != "same" {
		return errors.New("key-mode must be none, unique, or same")
	}
	if config.MaxAttempts < 1 || config.MaxAttempts > 100 {
		return errors.New("max-attempts must be between 1 and 100")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(config.Payload), &payload); err != nil {
		return fmt.Errorf("payload must be a JSON object: %w", err)
	}
	return nil
}

func run(ctx context.Context, config configuration) (result, error) {
	client := &http.Client{
		Timeout: config.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        config.Concurrency * 2,
			MaxIdleConnsPerHost: config.Concurrency,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	jobs := make(chan int)
	observations := make(chan observation, config.Concurrency)
	var workers sync.WaitGroup
	for worker := 0; worker < config.Concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for sequence := range jobs {
				observations <- execute(ctx, client, config, sequence)
			}
		}()
	}

	startedAt := time.Now().UTC()
	go func() {
		defer close(jobs)
		var ticker *time.Ticker
		if config.Rate > 0 {
			interval := time.Duration(float64(time.Second) / config.Rate)
			if interval < time.Microsecond {
				interval = time.Microsecond
			}
			ticker = time.NewTicker(interval)
			defer ticker.Stop()
		}
		for sequence := 0; sequence < config.Count; sequence++ {
			if ticker != nil && sequence > 0 {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
			select {
			case <-ctx.Done():
				return
			case jobs <- sequence:
			}
		}
	}()
	go func() {
		workers.Wait()
		close(observations)
	}()

	all := make([]observation, 0, config.Count)
	for item := range observations {
		all = append(all, item)
	}
	finishedAt := time.Now().UTC()
	return summarize(config, startedAt, finishedAt, all), nil
}

func execute(ctx context.Context, client *http.Client, config configuration, sequence int) observation {
	request, err := buildRequest(ctx, config, sequence)
	if err != nil {
		return observation{Error: err.Error()}
	}
	started := time.Now()
	response, err := client.Do(request)
	latency := time.Since(started)
	if err != nil {
		return observation{Latency: latency, Error: classifyError(err)}
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return observation{Status: response.StatusCode, Latency: latency, Error: "read response: " + err.Error()}
	}
	item := observation{Status: response.StatusCode, Latency: latency}
	if response.StatusCode >= 200 && response.StatusCode < 300 && config.Operation == "submit" {
		var task taskResponse
		if err := json.Unmarshal(body, &task); err != nil {
			item.Error = "decode response: " + err.Error()
		} else {
			item.TaskID = task.ID
		}
	}
	return item
}

func buildRequest(ctx context.Context, config configuration, sequence int) (*http.Request, error) {
	baseURL := strings.TrimRight(config.URL, "/")
	if config.Operation == "list" {
		return http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/tasks?limit=100", nil)
	}
	if config.Operation == "get" {
		return http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/tasks/"+config.TaskID, nil)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(config.Payload), &payload); err != nil {
		return nil, err
	}
	body, err := json.Marshal(map[string]any{
		"task_type":    config.TaskType,
		"payload":      payload,
		"queue":        config.Queue,
		"max_attempts": config.MaxAttempts,
	})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/tasks", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	switch config.KeyMode {
	case "same":
		request.Header.Set("Idempotency-Key", config.KeyPrefix)
	case "unique":
		request.Header.Set("Idempotency-Key", config.KeyPrefix+"-"+strconv.Itoa(sequence))
	}
	return request, nil
}

func summarize(config configuration, startedAt, finishedAt time.Time, observations []observation) result {
	latencies := make([]float64, 0, len(observations))
	statusCounts := make(map[string]int)
	errorCounts := make(map[string]int)
	taskIDs := make(map[string]struct{})
	successes := 0
	firstTaskID := ""
	for _, item := range observations {
		latencies = append(latencies, float64(item.Latency)/float64(time.Millisecond))
		if item.Status != 0 {
			statusCounts[strconv.Itoa(item.Status)]++
		}
		if item.Error != "" {
			errorCounts[item.Error]++
		}
		if item.Status >= 200 && item.Status < 300 && item.Error == "" {
			successes++
		}
		if item.TaskID != "" {
			if firstTaskID == "" {
				firstTaskID = item.TaskID
			}
			taskIDs[item.TaskID] = struct{}{}
		}
	}
	duration := finishedAt.Sub(startedAt).Seconds()
	requestsPerSecond := 0.0
	if duration > 0 {
		requestsPerSecond = float64(len(observations)) / duration
	}
	return result{
		StartedAt:       startedAt,
		FinishedAt:      finishedAt,
		DurationSeconds: duration,
		Operation:       config.Operation,
		RequestCount:    len(observations),
		Successes:       successes,
		Failures:        len(observations) - successes,
		RequestsPerSec:  requestsPerSecond,
		StatusCounts:    statusCounts,
		ErrorCounts:     errorCounts,
		DistinctTaskIDs: len(taskIDs),
		FirstTaskID:     firstTaskID,
		LatencyMS: percentiles{
			P50: percentile(latencies, 0.50),
			P95: percentile(latencies, 0.95),
			P99: percentile(latencies, 0.99),
			Max: percentile(latencies, 1.00),
		},
		Configuration: map[string]any{
			"count": config.Count, "concurrency": config.Concurrency, "rate": config.Rate,
			"task_type": config.TaskType, "payload": config.Payload, "queue": config.Queue,
			"max_attempts": config.MaxAttempts, "key_mode": config.KeyMode,
			"key_prefix": config.KeyPrefix,
		},
	}
}

func percentile(values []float64, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	index := int(float64(len(sorted)-1) * quantile)
	return sorted[index]
}

func classifyError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return err.Error()
}
