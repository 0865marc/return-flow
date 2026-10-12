"""Offline timing analysis of a completed two-case k6 benchmark.

Usage: python3 completion.py RESULTS_ROOT [--output PATH]
Only reads saved files and writes local JSON; no API, broker or database access.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from statistics import fmean


def read_json(path: Path):
    return json.loads(path.read_text())


def statistics(values: list[float]) -> dict:
    values = sorted(values)

    def percentile(fraction):
        if not values:
            return None
        index = (len(values) - 1) * fraction
        lo, hi = math.floor(index), math.ceil(index)
        return values[lo] + (values[hi] - values[lo]) * (index - lo)

    return {
        "count": len(values),
        "mean": fmean(values) if values else None,
        "p50": percentile(0.5),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "min": values[0] if values else None,
        "max": values[-1] if values else None,
    }


def timestamp_ms(raw: str) -> float:
    value = datetime.fromisoformat(raw)
    if value.tzinfo is None:
        raise ValueError(f"Timestamp lacks timezone: {raw!r}")
    return value.timestamp() * 1000


def iso_from_ms(value: float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat()


def analyze_case(directory: Path) -> dict:
    report = read_json(directory / "report.json")
    snapshot = read_json(directory / "snapshot.json")
    summary = read_json(directory / "k6-summary.json")
    plan = read_json(directory / "plan.json")
    diagnostic = summary["admission_diagnostic"]
    counts = diagnostic["counts"]
    timing = diagnostic["timing"]
    settings = diagnostic["configuration"]
    start = timing["scenario_started_epoch_ms"]
    if start is None:
        raise ValueError(f"Missing scenario start marker in {directory}")
    duration = settings["submission_duration_seconds"]
    rate = settings["target_requests_per_second"]
    cutoff = start + duration * 1000
    attempted = counts["requests_attempted"]
    if not 0 <= attempted <= len(plan):
        raise ValueError(f"Invalid attempted/plan counts in {directory}")
    # k6 may schedule one boundary tick at exactly duration. The script's
    # out-of-range guard then returns without a POST; it is not an API failure.
    guarded_boundary_iterations = counts["plan_errors"] if (
        attempted == counts["planned"] == len(plan)
        and counts["iterations_started"] == attempted + counts["plan_errors"]
        and counts["iterations_returned"] == counts["iterations_started"]
        and counts["http_requests_builtin"] == attempted
        and counts["accepted"] == attempted
        and counts["failed_iterations"] == counts["plan_errors"]
        and counts["transport_failures"] == 0
        and counts["unexpected_statuses"] == 0
        and counts["invalid_bodies"] == 0
    ) else 0
    if counts["plan_errors"] != guarded_boundary_iterations:
        raise ValueError(f"Plan errors prevent reliable ID reconciliation in {directory}")
    if attempted != report["counts"]["requests_attempted"]:
        raise ValueError(f"Report disagrees with k6 request count in {directory}")
    # One local k6 instance; the script maps each started iteration to this prefix.
    submitted = {row["command_id"]: row for row in plan[:attempted]}
    if len(submitted) != attempted:
        raise ValueError(f"Duplicate IDs in submitted plan in {directory}")
    observed = {row["command_id"]: row for row in snapshot}
    if len(observed) != len(snapshot):
        raise ValueError(f"Duplicate IDs in final snapshot in {directory}")

    terminal = []
    invalid_terminal_timestamps = []
    unexpected = set(observed) - set(submitted)
    for command_id, item in observed.items():
        if command_id not in submitted or item["status"] not in {"completed", "failed"}:
            continue
        try:
            ended = timestamp_ms(item["completed_at"])
        except (ValueError, TypeError, KeyError):
            invalid_terminal_timestamps.append(command_id)
            continue
        terminal.append((command_id, item, ended))

    terminal_by_cutoff = [row for row in terminal if row[2] <= cutoff]
    completed_by_cutoff = [row for row in terminal_by_cutoff if row[1]["status"] == "completed"]
    last = max((row[2] for row in terminal), default=None)
    all_terminal = len(terminal) == attempted and not unexpected and attempted > 0
    missing = set(submitted) - set(observed)
    jitter = summary["metrics"].get("diag_start_offset_error_ms", {}).get("values")
    estimate_valid = (
        bool(timing.get("index_schedule_estimate_valid")) or guarded_boundary_iterations > 0
    ) and (
        counts["dropped_iterations"] == 0
        and attempted == counts["planned"]
        and counts["plan_errors"] == guarded_boundary_iterations
    )
    approximate_completion = None
    if estimate_valid:
        durations = [
            ended - (start + submitted[command_id]["index"] * 1000 / rate)
            for command_id, item, ended in terminal
            if item["status"] == "completed"
        ]
        approximate_completion = {
            "method": "database_completed_at_minus_scenario_start_plus_plan_index_over_rate",
            "milliseconds": statistics(durations),
            "negative_values": sum(value < 0 for value in durations),
            "scope": "Successful commands with terminal timestamps; unresolved commands are excluded.",
            "timing_error": (
                "Estimated duration includes scheduling delay shown in diag_start_offset_error_ms. "
                "No individual send timestamps or client/database UTC offset calibration exist. "
                "completed_at is written before transaction COMMIT, so this is not exact end-to-end completion latency."
            ),
        }

    return {
        "mode": report["mode"],
        "directory": str(directory),
        "scenario_started_at": iso_from_ms(start),
        "submission_ended_at": iso_from_ms(cutoff),
        "submission_duration_seconds": duration,
        "requests_attempted": attempted,
        "accepted_http_responses": counts["accepted"],
        "dropped_arrivals": counts["dropped_iterations"],
        "guarded_boundary_iterations_without_http_request": guarded_boundary_iterations,
        "terminal_by_submission_end": len(terminal_by_cutoff),
        "completed_by_submission_end": len(completed_by_cutoff),
        "failed_by_submission_end": len(terminal_by_cutoff) - len(completed_by_cutoff),
        "attempted_awaiting_terminal_at_submission_end": attempted - len(terminal_by_cutoff),
        "terminal_after_submission_end": len(terminal) - len(terminal_by_cutoff),
        "observed_terminal_commands": len(terminal),
        "all_attempted_commands_terminal": all_terminal,
        "missing_attempted_in_snapshot": len(missing),
        "attempted_without_terminal_timestamp_in_snapshot": attempted - len(terminal),
        "invalid_terminal_timestamps": len(invalid_terminal_timestamps),
        "unexpected_snapshot_commands": len(unexpected),
        "last_observed_terminal_at": iso_from_ms(last),
        "seconds_from_scenario_start_to_last_observed_terminal": None if last is None else (last - start) / 1000,
        "observed_tail_seconds_after_submission_end": None if last is None else max(0, (last - cutoff) / 1000),
        "seconds_from_scenario_start_until_all_attempted_terminal": (last - start) / 1000 if all_terminal else None,
        "start_offset_error_ms": jitter,
        "index_schedule_estimate_valid": estimate_valid,
        "approximate_planned_send_to_completed": approximate_completion,
        "approximation_omitted_reason": None if estimate_valid else (
            "Dropped arrivals, incomplete offered load or invalid schedule metadata prevent mapping each request to index/rate."
        ),
        "notes": [
            "Counts at submission end are reconstructed offline from the final DB snapshot; no state was polled during load.",
            "Awaiting terminal includes every attempted request without completed_at by cutoff, including HTTP requests still in flight; it is not RabbitMQ queue length.",
            "Terminal includes successful and failed commands; completed counts only successful commands.",
            "completed_at precedes COMMIT. A row may have become committed or its HTTP response may have arrived after this timestamp.",
            "Client scenario time and DB timestamps share host UTC but their offset was not calibrated; cutoff and tail inherit this uncertainty.",
            "If unresolved commands remain, the last observed terminal is not the time to finish all submitted work.",
            "A guarded boundary iteration is a scheduler tick outside the finite plan: the script returned before POST; it is not an API error or an extra submitted command.",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_root", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    root = args.results_root.resolve()
    result = {
        "methodology": "offline_final_snapshot_and_single_scenario_start_marker",
        "results": [analyze_case(root / mode) for mode in ("postgres_pool", "event_driven")],
    }
    output = args.output or root / "completion-timing.json"
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
