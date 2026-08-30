package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	shortBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	taskBuckets  = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600}
)

type Metrics struct {
	registry           *prometheus.Registry
	tasksClaimed       prometheus.Counter
	tasksCompleted     *prometheus.CounterVec
	attempts           *prometheus.CounterVec
	heartbeats         *prometheus.CounterVec
	leaseRenewals      *prometheus.CounterVec
	retriesScheduled   prometheus.Counter
	claimDuration      prometheus.Histogram
	executionDuration  prometheus.Histogram
	queueWait          prometheus.Histogram
	leaseRenewDuration prometheus.Histogram
	retryDelay         prometheus.Histogram
	taskTotalLatency   prometheus.Histogram
}

func New() *Metrics {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		registry: registry,
		tasksClaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "taskforge_worker_tasks_claimed_total",
			Help: "Tasks successfully claimed by this worker process.",
		}),
		tasksCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_worker_tasks_completed_total",
			Help: "Durable worker task outcomes.",
		}, []string{"outcome"}),
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_task_attempts_total",
			Help: "Durable execution attempt outcomes.",
		}, []string{"outcome"}),
		heartbeats: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_worker_heartbeats_total",
			Help: "Worker heartbeat operation outcomes.",
		}, []string{"outcome"}),
		leaseRenewals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_worker_lease_renewals_total",
			Help: "Task lease renewal outcomes.",
		}, []string{"outcome"}),
		retriesScheduled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "taskforge_task_retries_scheduled_total",
			Help: "Retryable failures that durably scheduled another attempt.",
		}),
		claimDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "taskforge_worker_claim_duration_seconds",
			Help:    "Successful PostgreSQL claim transaction latency.",
			Buckets: shortBuckets,
		}),
		executionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "taskforge_task_execution_duration_seconds",
			Help:    "Handler invocation duration through return or cancellation.",
			Buckets: taskBuckets,
		}),
		queueWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "taskforge_task_queue_wait_seconds",
			Help:    "Database claim time minus the task's latest queued_at timestamp.",
			Buckets: taskBuckets,
		}),
		leaseRenewDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "taskforge_worker_lease_renew_duration_seconds",
			Help:    "PostgreSQL lease renewal operation latency.",
			Buckets: shortBuckets,
		}),
		retryDelay: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "taskforge_retry_delay_seconds",
			Help:    "Calculated delay for durably scheduled task retries.",
			Buckets: taskBuckets,
		}),
		taskTotalLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "taskforge_task_total_latency_seconds",
			Help:    "Task creation-to-terminal latency.",
			Buckets: taskBuckets,
		}),
	}
	registry.MustRegister(
		metrics.tasksClaimed,
		metrics.tasksCompleted,
		metrics.attempts,
		metrics.heartbeats,
		metrics.leaseRenewals,
		metrics.retriesScheduled,
		metrics.claimDuration,
		metrics.executionDuration,
		metrics.queueWait,
		metrics.leaseRenewDuration,
		metrics.retryDelay,
		metrics.taskTotalLatency,
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	return metrics
}

func (metrics *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{})
}

func (metrics *Metrics) TaskClaimed(claimDuration, queueWait time.Duration) {
	metrics.tasksClaimed.Inc()
	metrics.claimDuration.Observe(nonNegative(claimDuration))
	metrics.queueWait.Observe(nonNegative(queueWait))
}

func (metrics *Metrics) Execution(duration time.Duration) {
	metrics.executionDuration.Observe(nonNegative(duration))
}

func (metrics *Metrics) TaskCompleted(outcome string) {
	metrics.tasksCompleted.WithLabelValues(outcome).Inc()
}

func (metrics *Metrics) Attempt(outcome string) {
	metrics.attempts.WithLabelValues(outcome).Inc()
}

func (metrics *Metrics) Heartbeat(outcome string) {
	metrics.heartbeats.WithLabelValues(outcome).Inc()
}

func (metrics *Metrics) LeaseRenewal(outcome string, duration time.Duration) {
	metrics.leaseRenewals.WithLabelValues(outcome).Inc()
	metrics.leaseRenewDuration.Observe(nonNegative(duration))
}

func (metrics *Metrics) LeaseLost() {
	metrics.leaseRenewals.WithLabelValues("lost").Inc()
}

func (metrics *Metrics) RetryScheduled(delay time.Duration) {
	metrics.retriesScheduled.Inc()
	metrics.retryDelay.Observe(nonNegative(delay))
}

func (metrics *Metrics) TaskTotalLatency(duration time.Duration) {
	metrics.taskTotalLatency.Observe(nonNegative(duration))
}

func nonNegative(duration time.Duration) float64 {
	if duration < 0 {
		return 0
	}
	return duration.Seconds()
}
