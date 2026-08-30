import time

from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest

API_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5)
TASK_LATENCY_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600)


class ApiMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.requests = Counter(
            "taskforge_api_requests_total",
            "Normalized API requests by method, route, and status class.",
            ("method", "route", "status_class"),
            registry=self.registry,
        )
        self.request_duration = Histogram(
            "taskforge_api_request_duration_seconds",
            "API request latency by method and normalized route.",
            ("method", "route"),
            buckets=API_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.submissions = Counter(
            "taskforge_task_submissions_total",
            "Task submission outcomes.",
            ("outcome",),
            registry=self.registry,
        )
        self.cancellations = Counter(
            "taskforge_task_cancellations_total",
            "Task cancellation outcomes.",
            ("outcome",),
            registry=self.registry,
        )
        self.task_total_latency = Histogram(
            "taskforge_task_total_latency_seconds",
            "Task creation-to-terminal latency.",
            buckets=TASK_LATENCY_BUCKETS,
            registry=self.registry,
        )

    def record_submission(self, outcome: str) -> None:
        self.submissions.labels(outcome=outcome).inc()

    def record_cancellation(self, outcome: str) -> None:
        self.cancellations.labels(outcome=outcome).inc()

    def observe_task_total_latency(self, seconds: float) -> None:
        self.task_total_latency.observe(max(0.0, seconds))

    def observe_request(
        self,
        *,
        method: str,
        route: str,
        status_code: int,
        started_at: float,
    ) -> None:
        status_class = f"{status_code // 100}xx"
        self.requests.labels(
            method=method,
            route=route,
            status_class=status_class,
        ).inc()
        self.request_duration.labels(method=method, route=route).observe(
            max(0.0, time.perf_counter() - started_at)
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)
