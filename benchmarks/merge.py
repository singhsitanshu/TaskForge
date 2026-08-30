#!/usr/bin/env python3
"""Merge independently reset benchmark suites without rewriting source evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib

from benchmarks.run import aggregate, enrich_raw, summarize_resources, write_csv


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("base", type=pathlib.Path)
    parser.add_argument("supplement", type=pathlib.Path)
    parser.add_argument("--replace-scenario", required=True)
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    arguments = parser.parse_args()

    base = json.loads(arguments.base.read_text())
    supplement = json.loads(arguments.supplement.read_text())
    replacement = [
        item for item in supplement["results"] if item["scenario"] == arguments.replace_scenario
    ]
    if not replacement:
        raise SystemExit(f"supplement has no {arguments.replace_scenario} results")
    if base.get("profile") != supplement.get("profile"):
        raise SystemExit("profiles differ; refusing to merge incomparable results")

    combined = dict(base)
    combined["results"] = [
        item for item in base["results"] if item["scenario"] != arguments.replace_scenario
    ] + replacement
    for item in combined["results"]:
        if item.get("raw") and item.get("count"):
            enrich_raw(item["raw"], int(item["count"]))
        if item.get("resource_samples"):
            item["resources"] = summarize_resources(item["resource_samples"])
    combined["summaries"] = aggregate(combined["results"])
    combined["errors"] = []
    combined["all_correctness_passed"] = all(
        item.get("correctness", {}).get("passed", False) for item in combined["results"]
    )
    combined["merged_at"] = dt.datetime.now(dt.UTC).isoformat()
    combined["provenance"] = {
        "base": str(arguments.base),
        "supplement": str(arguments.supplement),
        "replaced_scenario": arguments.replace_scenario,
        "source_files_are_immutable": True,
    }
    arguments.output_dir.mkdir(parents=True, exist_ok=True)
    result_path = arguments.output_dir / "results.json"
    result_path.write_text(json.dumps(combined, indent=2) + "\n")
    write_csv(arguments.output_dir / "results.csv", combined["results"])
    print(result_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
