#!/usr/bin/env python3
"""Generate dependency-free SVG plots from a TF-012 results.json file."""

from __future__ import annotations

import argparse
import html
import json
import pathlib
import statistics
from typing import Any

COLORS = ("#2563eb", "#dc2626", "#16a34a", "#9333ea", "#ea580c")


def value(item: dict[str, Any], path: str) -> float | None:
    current: Any = item
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return float(current) if isinstance(current, int | float) else None


def grouped(
    results: list[dict[str, Any]],
    scenario: str,
    x_field: str,
    y_field: str,
) -> list[tuple[float, float]]:
    buckets: dict[float, list[float]] = {}
    for item in results:
        if item.get("scenario") != scenario:
            continue
        if "classification" in item and (
            item.get("classification") != "PUBLIC" or not item.get("valid")
        ):
            continue
        x = value(item, x_field)
        y = value(item, y_field)
        if x is not None and y is not None:
            buckets.setdefault(x, []).append(y)
    return sorted((x, statistics.median(values)) for x, values in buckets.items())


def line_plot(
    path: pathlib.Path,
    title: str,
    x_label: str,
    y_label: str,
    series: list[tuple[str, list[tuple[float, float]]]],
) -> None:
    width, height = 900, 520
    left, right, top, bottom = 95, 35, 55, 75
    points = [point for _, values in series for point in values]
    if not points:
        points = [(0, 0), (1, 1)]
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = 0.0, max(ys) * 1.1 if max(ys) > 0 else 1.0
    if x_min == x_max:
        x_max = x_min + 1

    def sx(number: float) -> float:
        return left + (number - x_min) / (x_max - x_min) * (width - left - right)

    def sy(number: float) -> float:
        return height - bottom - (number - y_min) / (y_max - y_min) * (height - top - bottom)

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{width / 2}" y="30" text-anchor="middle" font-family="sans-serif" font-size="20">{html.escape(title)}</text>',
    ]
    for step in range(6):
        number = y_min + (y_max - y_min) * step / 5
        y = sy(number)
        svg.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}" stroke="#e5e7eb"/>'
        )
        svg.append(
            f'<text x="{left - 10}" y="{y + 4:.1f}" text-anchor="end" font-family="sans-serif" font-size="12">{number:.2f}</text>'
        )
    svg.extend(
        [
            f'<line x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" stroke="#111827"/>',
            f'<line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" stroke="#111827"/>',
            f'<text x="{width / 2}" y="{height - 20}" text-anchor="middle" font-family="sans-serif" font-size="14">{html.escape(x_label)}</text>',
            f'<text transform="translate(20 {height / 2}) rotate(-90)" text-anchor="middle" font-family="sans-serif" font-size="14">{html.escape(y_label)}</text>',
        ]
    )
    for index, (name, values) in enumerate(series):
        color = COLORS[index % len(COLORS)]
        coordinates = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in values)
        if coordinates:
            svg.append(
                f'<polyline points="{coordinates}" fill="none" stroke="{color}" stroke-width="3"/>'
            )
        for x, y in values:
            svg.append(f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="4" fill="{color}"/>')
            svg.append(
                f'<text x="{sx(x):.1f}" y="{height - bottom + 20}" text-anchor="middle" font-family="sans-serif" font-size="11">{x:g}</text>'
            )
        legend_x = left + index * 180
        svg.append(
            f'<line x1="{legend_x}" y1="{height - 48}" x2="{legend_x + 25}" y2="{height - 48}" stroke="{color}" stroke-width="3"/>'
        )
        svg.append(
            f'<text x="{legend_x + 32}" y="{height - 44}" font-family="sans-serif" font-size="12">{html.escape(name)}</text>'
        )
    svg.append("</svg>")
    path.write_text("\n".join(svg) + "\n")


def efficiency_points(results: list[dict[str, Any]], scenario: str) -> list[tuple[float, float]]:
    throughput = grouped(results, scenario, "workers", "raw.processing_throughput_per_second")
    if not throughput:
        return []
    baseline = next((y for x, y in throughput if x == 1), throughput[0][1])
    return [(workers, measured / (baseline * workers)) for workers, measured in throughput]


def recovery_points(results: list[dict[str, Any]]) -> list[tuple[float, float]]:
    points = []
    for item in results:
        if item.get("scenario") == "recovery_storm":
            if "classification" in item and (
                item.get("classification") != "PUBLIC" or not item.get("valid")
            ):
                continue
            lag = value(item, "raw.recovery_lag_p95_seconds")
            if lag is not None:
                points.append((float(item["kill_percentage"]), lag))
    return sorted(points)


def prometheus_points(
    results: list[dict[str, Any]], scenario: str, x_field: str, metric: str
) -> list[tuple[float, float]]:
    buckets: dict[float, list[float]] = {}
    for item in results:
        if item.get("scenario") != scenario:
            continue
        if "classification" in item and (
            item.get("classification") != "PUBLIC" or not item.get("valid")
        ):
            continue
        x = value(item, x_field)
        reconciled = (
            item.get("prometheus_reconciliation", {})
            .get("histograms", {})
            .get(metric.removesuffix("_p95"), {})
            .get("prometheus")
        )
        if x is not None and isinstance(reconciled, int | float):
            buckets.setdefault(x, []).append(float(reconciled))
            continue
        rows = item.get("prometheus_after", {}).get(metric, [])
        if x is None or not isinstance(rows, list) or not rows:
            continue
        try:
            y = float(rows[0]["value"][1])
        except (KeyError, IndexError, TypeError, ValueError):
            continue
        buckets.setdefault(x, []).append(y)
    return sorted((x, statistics.median(values)) for x, values in buckets.items())


def resource_points(results: list[dict[str, Any]]) -> list[tuple[float, float]]:
    points = []
    for item in results:
        if item.get("scenario") != "noop_scaling":
            continue
        if "classification" in item and (
            item.get("classification") != "PUBLIC" or not item.get("valid")
        ):
            continue
        cpu = value(item, "resources.postgres.cpu_percent_max")
        if cpu is not None:
            points.append((float(item["workers"]), cpu))
    buckets: dict[float, list[float]] = {}
    for x, y in points:
        buckets.setdefault(x, []).append(y)
    return sorted((x, statistics.median(values)) for x, values in buckets.items())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("results", type=pathlib.Path)
    parser.add_argument("--output-dir", type=pathlib.Path)
    arguments = parser.parse_args()
    document = json.loads(arguments.results.read_text())
    results = document["results"]
    output = arguments.output_dir or arguments.results.parent / "plots"
    output.mkdir(parents=True, exist_ok=True)

    plot_specs: list[tuple[str, str, str, str, list[tuple[str, list[tuple[float, float]]]]]] = [
        (
            "01-processing-throughput.svg",
            "Processing throughput scaling",
            "Workers",
            "Tasks/s",
            [
                (
                    "noop",
                    grouped(
                        results, "noop_scaling", "workers", "raw.processing_throughput_per_second"
                    ),
                ),
                (
                    "I/O 50 ms",
                    grouped(
                        results, "io50_scaling", "workers", "raw.processing_throughput_per_second"
                    ),
                ),
                (
                    "CPU",
                    grouped(
                        results, "cpu_scaling", "workers", "raw.processing_throughput_per_second"
                    ),
                ),
            ],
        ),
        (
            "02-scaling-efficiency.svg",
            "Horizontal scaling efficiency",
            "Workers",
            "Efficiency",
            [
                ("noop", efficiency_points(results, "noop_scaling")),
                ("I/O 50 ms", efficiency_points(results, "io50_scaling")),
                ("CPU", efficiency_points(results, "cpu_scaling")),
            ],
        ),
        (
            "03-queue-p95.svg",
            "Queue wait p95",
            "Workers",
            "Seconds",
            [
                ("noop", grouped(results, "noop_scaling", "workers", "raw.queue_p95_seconds")),
                ("I/O 50 ms", grouped(results, "io50_scaling", "workers", "raw.queue_p95_seconds")),
                ("CPU", grouped(results, "cpu_scaling", "workers", "raw.queue_p95_seconds")),
            ],
        ),
        (
            "04-claim-p95.svg",
            "Prometheus claim latency p95",
            "Workers",
            "Seconds",
            [
                ("noop", prometheus_points(results, "noop_scaling", "workers", "claim_p95")),
            ],
        ),
        (
            "05-api-throughput.svg",
            "API submission throughput",
            "Concurrency",
            "Requests/s",
            [
                (
                    "POST /tasks",
                    grouped(
                        results,
                        "api_throughput",
                        "submission.configuration.concurrency",
                        "submission.requests_per_second",
                    ),
                ),
            ],
        ),
        (
            "06-arrival-saturation.svg",
            "Offered versus completed rate",
            "Offered tasks/s",
            "Tasks/s",
            [
                (
                    "completed",
                    grouped(
                        results,
                        "arrival_saturation",
                        "offered_rate_per_second",
                        "raw.processing_throughput_per_second",
                    ),
                ),
            ],
        ),
        (
            "07-scheduler-scaling.svg",
            "Retry scheduler scaling",
            "Schedulers",
            "Tasks/s",
            [
                (
                    "fail once",
                    grouped(
                        results,
                        "scheduler_retry_scaling",
                        "schedulers",
                        "raw.processing_throughput_per_second",
                    ),
                ),
            ],
        ),
        (
            "08-recovery.svg",
            "Recovery total-latency p95",
            "Killed workers (%)",
            "Seconds",
            [
                ("recovered", recovery_points(results)),
            ],
        ),
        (
            "09-retry-lateness.svg",
            "Retry lateness p95",
            "Schedulers",
            "Seconds",
            [
                (
                    "raw PostgreSQL",
                    grouped(
                        results,
                        "scheduler_retry_scaling",
                        "schedulers",
                        "raw.retry_lateness_p95_seconds",
                    ),
                ),
                (
                    "Prometheus",
                    prometheus_points(
                        results, "scheduler_retry_scaling", "schedulers", "retry_lateness_p95"
                    ),
                ),
            ],
        ),
        (
            "10-postgres-cpu.svg",
            "PostgreSQL CPU during noop scaling",
            "Workers",
            "CPU (%)",
            [
                ("postgres", resource_points(results)),
            ],
        ),
    ]
    manifest = []
    for filename, title, x_label, y_label, series in plot_specs:
        line_plot(output / filename, title, x_label, y_label, series)
        manifest.append({"file": filename, "title": title, "series": [name for name, _ in series]})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
