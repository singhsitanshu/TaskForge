from app.metrics import ApiMetrics


def test_independent_registries_do_not_duplicate_registration() -> None:
    first = ApiMetrics()
    second = ApiMetrics()
    first.record_submission("created")
    second.record_submission("replayed")
    first_text = first.render().decode()
    second_text = second.render().decode()
    assert 'taskforge_task_submissions_total{outcome="created"} 1.0' in first_text
    assert 'taskforge_task_submissions_total{outcome="replayed"} 1.0' in second_text
    assert "task_id" not in first_text


def test_api_histogram_and_normalized_labels() -> None:
    metrics = ApiMetrics()
    metrics.observe_request(
        method="GET",
        route="/tasks/{task_id}",
        status_code=200,
        started_at=0.0,
    )
    text = metrics.render().decode()
    assert 'route="/tasks/{task_id}"' in text
    assert 'status_class="2xx"' in text
    assert "taskforge_api_request_duration_seconds_count" in text
