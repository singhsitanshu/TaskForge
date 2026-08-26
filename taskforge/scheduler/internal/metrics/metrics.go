package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"taskforge/scheduler/internal/domain"
)

var (
	shortBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}
	lagBuckets   = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900}
	taskBuckets  = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600}
	taskStatuses = []string{"QUEUED", "LEASED", "RUNNING", "RETRYING", "SUCCEEDED", "FAILED", "CANCELLED"}
	livenesses   = []string{"ACTIVE", "STALE", "DEAD"}
)

type Metrics struct {
	registry                *prometheus.Registry
	recoveries              *prometheus.CounterVec
	attempts                *prometheus.CounterVec
	recoveryErrors          prometheus.Counter
	retryPromotions         prometheus.Counter
	retryPromotionErrors    prometheus.Counter
	stateCollectionErrors   prometheus.Counter
	recoveryBatchDuration   prometheus.Histogram
	retryBatchDuration      prometheus.Histogram
	stateCollectionDuration prometheus.Histogram
	recoveryLag             prometheus.Histogram
	retryLateness           prometheus.Histogram
	taskTotalLatency        prometheus.Histogram
	lastRecoverySuccess     prometheus.Gauge
	lastRetryScanSuccess    prometheus.Gauge
	lastStateCollection     prometheus.Gauge
	tasksCurrent            *prometheus.GaugeVec
	workersCurrent          *prometheus.GaugeVec
	runningAttempts         prometheus.Gauge
	eligibleTasks           prometheus.Gauge
	expiredRunningLeases    prometheus.Gauge
}

func New() *Metrics {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		registry: registry,
		recoveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_task_recoveries_total",
			Help: "Expired lease recovery outcomes.",
		}, []string{"outcome"}),
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskforge_task_attempts_total",
			Help: "Durable execution attempt outcomes.",
		}, []string{"outcome"}),
		recoveryErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "taskforge_scheduler_recovery_errors_total",
			Help: "Expired lease recovery scan failures.",
		}),
		retryPromotions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "taskforge_task_retries_promoted_total",
			Help: "Due retry tasks promoted back to QUEUED.",
		}),
		retryPromotionErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "taskforge_scheduler_retry_promotion_errors_total",
			Help: "Retry promotion scan failures.",
		}),
		stateCollectionErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "taskforge_scheduler_state_collection_errors_total",
			Help: "Database-backed global state sampling failures.",
		}),
		recoveryBatchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "taskforge_scheduler_recovery_batch_duration_seconds",
			Help: "Expired lease recovery batch duration.", Buckets: shortBuckets,
		}),
		retryBatchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "taskforge_scheduler_retry_batch_duration_seconds",
			Help: "Retry promotion batch duration.", Buckets: shortBuckets,
		}),
		stateCollectionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "taskforge_scheduler_state_collection_duration_seconds",
			Help: "Database-backed global state sampling duration.", Buckets: shortBuckets,
		}),
		recoveryLag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "taskforge_recovery_lag_seconds",
			Help: "Recovery observation time minus expired lease timestamp.", Buckets: lagBuckets,
		}),
		retryLateness: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "taskforge_retry_lateness_seconds",
			Help: "Retry promotion observation time minus scheduled_at.", Buckets: lagBuckets,
		}),
		taskTotalLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "taskforge_task_total_latency_seconds",
			Help: "Task creation-to-terminal latency.", Buckets: taskBuckets,
		}),
		lastRecoverySuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "taskforge_scheduler_last_successful_recovery_timestamp_seconds",
			Help: "Unix timestamp of the last successful recovery scan.",
		}),
		lastRetryScanSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "taskforge_scheduler_last_successful_retry_scan_timestamp_seconds",
			Help: "Unix timestamp of the last successful retry promotion scan.",
		}),
		lastStateCollection: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "taskforge_scheduler_last_successful_state_collection_timestamp_seconds",
			Help: "Unix timestamp of the last successful database state sample.",
		}),
		tasksCurrent: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "taskforge_tasks_current", Help: "Sampled task rows by durable status.",
		}, []string{"status"}),
		workersCurrent: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "taskforge_workers_current", Help: "Sampled workers by derived liveness.",
		}, []string{"liveness"}),
		runningAttempts: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "taskforge_attempts_running", Help: "Sampled RUNNING attempt rows.",
		}),
		eligibleTasks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "taskforge_tasks_eligible_for_claim",
			Help: "Sampled QUEUED tasks due now with attempts remaining.",
		}),
		expiredRunningLeases: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "taskforge_expired_running_leases",
			Help: "Sampled RUNNING tasks whose ownership lease has expired.",
		}),
	}
	registry.MustRegister(
		metrics.recoveries, metrics.attempts, metrics.recoveryErrors,
		metrics.retryPromotions, metrics.retryPromotionErrors,
		metrics.stateCollectionErrors, metrics.recoveryBatchDuration,
		metrics.retryBatchDuration, metrics.stateCollectionDuration,
		metrics.recoveryLag, metrics.retryLateness, metrics.taskTotalLatency,
		metrics.lastRecoverySuccess,
		metrics.lastRetryScanSuccess, metrics.lastStateCollection,
		metrics.tasksCurrent, metrics.workersCurrent, metrics.runningAttempts,
		metrics.eligibleTasks, metrics.expiredRunningLeases,
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	return metrics
}

func (metrics *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{})
}

func (metrics *Metrics) RecoveryBatch(duration time.Duration, err error) {
	metrics.recoveryBatchDuration.Observe(nonNegative(duration))
	if err != nil {
		metrics.recoveryErrors.Inc()
		return
	}
	metrics.lastRecoverySuccess.SetToCurrentTime()
}

func (metrics *Metrics) Recovered(outcome string, lag time.Duration) {
	metrics.recoveries.WithLabelValues(outcome).Inc()
	metrics.attempts.WithLabelValues("abandoned").Inc()
	metrics.recoveryLag.Observe(nonNegative(lag))
}

func (metrics *Metrics) TaskTotalLatency(duration time.Duration) {
	metrics.taskTotalLatency.Observe(nonNegative(duration))
}

func (metrics *Metrics) RetryBatch(duration time.Duration, err error) {
	metrics.retryBatchDuration.Observe(nonNegative(duration))
	if err != nil {
		metrics.retryPromotionErrors.Inc()
		return
	}
	metrics.lastRetryScanSuccess.SetToCurrentTime()
}

func (metrics *Metrics) RetryPromoted(lateness time.Duration) {
	metrics.retryPromotions.Inc()
	metrics.retryLateness.Observe(nonNegative(lateness))
}

func (metrics *Metrics) StateCollection(duration time.Duration, err error) {
	metrics.stateCollectionDuration.Observe(nonNegative(duration))
	if err != nil {
		metrics.stateCollectionErrors.Inc()
		return
	}
	metrics.lastStateCollection.SetToCurrentTime()
}

func (metrics *Metrics) SetSnapshot(snapshot domain.GlobalSnapshot) {
	for _, status := range taskStatuses {
		metrics.tasksCurrent.WithLabelValues(status).Set(float64(snapshot.TaskCounts[status]))
	}
	for _, liveness := range livenesses {
		metrics.workersCurrent.WithLabelValues(liveness).Set(
			float64(snapshot.WorkerCounts[liveness]),
		)
	}
	metrics.runningAttempts.Set(float64(snapshot.RunningAttempts))
	metrics.eligibleTasks.Set(float64(snapshot.EligibleTasks))
	metrics.expiredRunningLeases.Set(float64(snapshot.ExpiredRunningLeases))
}

func nonNegative(duration time.Duration) float64 {
	if duration < 0 {
		return 0
	}
	return duration.Seconds()
}
