import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_prometheus_uses_dns_discovery_for_scalable_services() -> None:
    configuration = (ROOT / "monitoring/prometheus/prometheus.yml").read_text()
    assert 'names: ["worker"]' in configuration
    assert 'names: ["scheduler"]' in configuration
    assert configuration.count("dns_sd_configs:") == 2
    assert "worker-1" not in configuration
    assert "scheduler-1" not in configuration


def test_grafana_dashboard_and_datasource_are_provisioned() -> None:
    dashboard = json.loads(
        (ROOT / "monitoring/grafana/dashboards/taskforge-overview.json").read_text()
    )
    assert dashboard["uid"] == "taskforge-operations"
    titles = {panel["title"] for panel in dashboard["panels"]}
    assert {"Overview", "Latency", "Reliability", "Workers", "API"} <= titles
    datasource = (
        ROOT / "monitoring/grafana/provisioning/datasources/prometheus.yml"
    ).read_text()
    provider = (
        ROOT / "monitoring/grafana/provisioning/dashboards/taskforge.yml"
    ).read_text()
    assert "uid: taskforge-prometheus" in datasource
    assert "http://prometheus:9090" in datasource
    assert "/var/lib/grafana/dashboards" in provider


def test_custom_metric_names_use_taskforge_prefix_and_standard_suffixes() -> None:
    sources = [
        ROOT / "api/app/metrics.py",
        ROOT / "worker/internal/metrics/metrics.go",
        ROOT / "scheduler/internal/metrics/metrics.go",
    ]
    metric_names: set[str] = set()
    for source in sources:
        metric_names.update(
            re.findall(r'["\'](taskforge_[a-z0-9_]+)["\']', source.read_text())
        )
    assert metric_names
    assert all(name.startswith("taskforge_") for name in metric_names)
    for name in metric_names:
        if any(word in name for word in ("duration", "latency", "wait", "lag", "delay", "lateness")):
            assert name.endswith("_seconds"), name
        if name.endswith("total"):
            assert name.endswith("_total"), name
