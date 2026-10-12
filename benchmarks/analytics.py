"""Offline reconciliation. No network, database access, or clock sampling here."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
import json
import math
from statistics import fmean
from typing import Any
from uuid import UUID


COMMAND_TYPES = {
    "create": "create_delivery",
    "deliver": "ensure_delivery_delivered",
    "return": "request_return",
}


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) else None


def _statistics(values: list[float]) -> dict:
    """Percentiles use linear interpolation at (n - 1) * percentile."""
    ordered = sorted(values)

    def percentile(fraction: float) -> float | None:
        if not ordered:
            return None
        position = (len(ordered) - 1) * fraction
        lower = math.floor(position)
        upper = math.ceil(position)
        return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)

    return {
        "count": len(ordered),
        "min": ordered[0] if ordered else None,
        "mean": fmean(ordered) if ordered else None,
        "p50": percentile(0.50),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "max": ordered[-1] if ordered else None,
    }


def _object(value: Any) -> dict | None:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    try:
        parsed = json.loads(value)
    except (ValueError, TypeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _is_uuid(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        UUID(value)
    except ValueError:
        return False
    return True


def _result_errors(result: dict | None, operation: str, delivery_id: Any) -> list[str]:
    if result is None:
        return ["result_is_not_an_object"]
    errors = []
    if not _is_uuid(result.get("id")):
        errors.append("invalid_entity_id")
    expected_status = {"create": "pending", "deliver": "delivered", "return": "requested"}
    if operation not in expected_status:
        return errors + ["unknown_operation"]
    if result.get("status") != expected_status[operation]:
        errors.append("unexpected_entity_status")
    if operation == "deliver" and result.get("id") != delivery_id:
        errors.append("delivery_id_mismatch")
    if operation == "return" and result.get("delivery_id") != delivery_id:
        errors.append("delivery_id_mismatch")
    return errors


def _response_errors(record: dict, mode: str, operation: str) -> tuple[dict | None, list[str]]:
    if record.get("response_truncated"):
        return None, ["response_truncated"]
    body = _object(record.get("response_body"))
    if body is None:
        return None, ["response_is_not_a_json_object"]
    if mode == "event_driven":
        errors = []
        if body.get("command_id") != record.get("command_id"):
            errors.append("command_id_mismatch")
        if body.get("status") != "accepted":
            errors.append("unexpected_acceptance_status")
        return body, errors
    if mode == "postgres_pool":
        return body, _result_errors(body, operation, record.get("delivery_id"))
    return body, ["unknown_mode"]


def _database_duration(row: dict) -> tuple[float | None, str | None]:
    values = [row.get("created_at"), row.get("completed_at")]
    if any(value is None for value in values):
        return None, "missing_timestamps"
    try:
        dates = [
            value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
            for value in values
        ]
        if any(date.utcoffset() is None for date in dates):
            return None, "invalid_timestamps"
        duration = (dates[1] - dates[0]).total_seconds() * 1000
    except (ValueError, TypeError, AttributeError, OverflowError):
        return None, "invalid_timestamps"
    if duration < 0:
        return None, "negative_duration"
    return duration, None


def _rate(count: int, duration: Any) -> float | None:
    denominator = _number(duration)
    return count / denominator if denominator is not None and denominator > 0 else None


def build_report(execution: dict, snapshot: list[dict], lifecycle: dict) -> dict:
    """Reconcile client receipts with one snapshot taken after producer shutdown.

    Counts distinguish HTTP requests from unique persisted commands. A missing
    command is an unobserved outcome at the cutoff, never proof of lost work.
    All latencies use one clock: client monotonic HTTP durations or differences
    of two database timestamps. The latter precede COMMIT and are not end-to-end.
    """
    records = execution.get("records", [])
    mode = execution.get("mode")
    window = execution.get("execution", {})
    counts = dict.fromkeys(
        (
            "planned", "sent", "dropped", "unique_sent_commands",
            "http_expected_status", "http_unexpected_status", "http_transport_errors",
            "http_missing_status", "http_invalid_body", "http_success",
            "completed", "failed", "pending", "missing", "unknown",
            "completed_despite_http_failure", "completed_despite_invalid_http_body",
        ),
        0,
    )
    counts["planned"] = len(records)
    issues: list[dict] = []
    http_failures: list[dict] = []
    sent: dict[str, list[dict]] = {}
    receipt_results: dict[int, tuple[str, dict | None]] = {}
    drop_reasons: Counter = Counter()
    http_statuses: Counter = Counter()
    http_values: list[float] = []
    http_exclusions: Counter = Counter()
    unknown_outcomes = 0

    def issue(command_id: Any, code: str, **detail: Any) -> None:
        issues.append({"command_id": command_id, "code": code, **detail})

    for index, record in enumerate(records):
        command_id = record.get("command_id")
        if record.get("outcome") == "dropped":
            counts["dropped"] += 1
            drop_reasons[record.get("drop_reason") or "unspecified"] += 1
            continue
        if record.get("outcome") != "sent":
            unknown_outcomes += 1
            issue(command_id, "unknown_submission_outcome")
            continue
        counts["sent"] += 1
        if not isinstance(command_id, str) or not command_id:
            issue(command_id, "missing_command_id", index=record.get("index", index))
            # Keep every invalid receipt distinct for reconciliation.
            command_id = f"__missing_command_id_{index}"
        sent.setdefault(command_id, []).append(record)
        latency = _number(record.get("latency_ms"))
        if latency is None:
            http_exclusions["missing_or_invalid"] += 1
        elif latency < 0:
            http_exclusions["negative_duration"] += 1
        else:
            http_values.append(latency)
        status = record.get("http_status")
        if status is not None:
            http_statuses[str(status)] += 1
        body = None
        errors = []
        if record.get("transport_error"):
            counts["http_transport_errors"] += 1
            receipt_outcome = "transport_error"
        elif status is None:
            counts["http_missing_status"] += 1
            receipt_outcome = "missing_status"
        elif status != record.get("expected_http_status"):
            counts["http_unexpected_status"] += 1
            receipt_outcome = "unexpected_status"
        else:
            counts["http_expected_status"] += 1
            operation = record.get("operation", execution.get("operation"))
            body, errors = _response_errors(record, mode, operation)
            if errors:
                counts["http_invalid_body"] += 1
                receipt_outcome = "invalid_body"
            else:
                counts["http_success"] += 1
                receipt_outcome = "success"
        receipt_results[id(record)] = (receipt_outcome, body)
        if receipt_outcome != "success":
            http_failures.append({
                "index": record.get("index", index),
                "command_id": record.get("command_id"),
                "outcome": receipt_outcome,
                "http_status": status,
                "expected_http_status": record.get("expected_http_status"),
                "transport_error": record.get("transport_error"),
                "validation_errors": errors,
            })

    counts["unique_sent_commands"] = len(sent)
    observed: dict[str, dict] = {}
    for row in snapshot:
        command_id = row.get("command_id")
        if command_id not in sent:
            issue(command_id, "snapshot_contains_unsent_command")
            continue
        if command_id in observed:
            issue(command_id, "duplicate_snapshot_command")
            continue
        observed[command_id] = row

    cohorts: dict[str, list] = {
        "pending_command_ids": [],
        "missing_command_ids": [],
        "unknown_command_ids": [],
        "failed_commands": [],
    }
    db_values: dict[str, list[float]] = {"completed": [], "failed": []}
    db_exclusions: dict[str, Counter] = {"completed": Counter(), "failed": Counter()}
    business = {"valid_completed": 0, "invalid_completed": 0, "unaudited_completed": 0}

    for command_id, receipts in sent.items():
        record = receipts[0]
        operation = record.get("operation", execution.get("operation"))
        if len(receipts) > 1:
            issue(command_id, "duplicate_submitted_command_id", requests=len(receipts))
        row = observed.get(command_id)
        if row is None:
            counts["missing"] += 1
            cohorts["missing_command_ids"].append(command_id)
            continue
        type_valid = row.get("type") == COMMAND_TYPES.get(operation)
        if not type_valid:
            issue(command_id, "command_type_mismatch", expected=COMMAND_TYPES.get(operation), actual=row.get("type"))
        state = row.get("status")
        if state not in ("completed", "failed", "pending"):
            counts["unknown"] += 1
            cohorts["unknown_command_ids"].append(command_id)
            continue
        counts[state] += 1
        if state == "pending":
            cohorts["pending_command_ids"].append(command_id)
            continue
        duration, exclusion = _database_duration(row)
        if exclusion:
            db_exclusions[state][exclusion] += 1
        else:
            db_values[state].append(duration)
        if state == "failed":
            cohorts["failed_commands"].append({
                "command_id": command_id,
                "error_code": row.get("error_code"),
                "error": row.get("error"),
            })
            continue

        result = _object(row.get("result"))
        result_errors = _result_errors(result, operation, record.get("delivery_id"))
        for code in result_errors:
            issue(command_id, "persisted_" + code)
        if row.get("business_valid") is False:
            business["invalid_completed"] += 1
            issue(command_id, "business_state_invalid")
        elif row.get("business_valid") is True and not result_errors and type_valid:
            business["valid_completed"] += 1
        elif result_errors or not type_valid:
            business["invalid_completed"] += 1
        else:
            business["unaudited_completed"] += 1
        outcomes = [receipt_results[id(receipt)][0] for receipt in receipts]
        if any(outcome in ("transport_error", "missing_status", "unexpected_status") for outcome in outcomes):
            counts["completed_despite_http_failure"] += 1
        if "invalid_body" in outcomes:
            counts["completed_despite_invalid_http_body"] += 1
        if mode == "postgres_pool" and result is not None:
            for receipt in receipts:
                outcome, body = receipt_results[id(receipt)]
                if outcome == "success" and body != result:
                    issue(command_id, "http_and_persisted_result_mismatch")

    failed = bool(issues or http_failures or counts["failed"])
    incomplete = bool(
        counts["dropped"] or counts["pending"] or counts["missing"] or counts["unknown"]
        or unknown_outcomes or business["unaudited_completed"] or not counts["planned"]
    )
    status = "failed" if failed else "incomplete" if incomplete else "pass"
    notes = [
        "HTTP acceptance (including 202) is not proof of command completion.",
        "Pending, missing and unknown commands are unresolved at the shutdown cutoff; they are not proven lost.",
        "Database latency is completed_at - created_at on the database clock: registration through terminal write. With worker-side registration it excludes RabbitMQ queue wait and connection acquisition before registration. It precedes COMMIT and is neither exact commit latency nor client end-to-end latency. Older runs that registered before publication have a different timing scope.",
        "HTTP latency uses client durations only. Missing, invalid and negative durations are excluded; unresolved commands have no invented completion latency.",
        "Percentiles use linear interpolation at (sample_count - 1) * percentile and describe observed samples only.",
        "Completion throughput includes submission, HTTP drain, passive drain and shutdown in its denominator; measurement time is excluded.",
    ]
    if counts["dropped"]:
        notes.append("The generator did not send every planned request; achieved load differs from offered load.")
    if counts["completed_despite_http_failure"]:
        notes.append("Some commands completed despite an HTTP failure. Client failures and business failures are reported separately.")
    if any(db_exclusions.values()) or http_exclusions:
        notes.append("Timing coverage is incomplete; excluded samples do not by themselves indicate a business failure.")

    return {
        "schema_version": 1,
        "status": status,
        "mode": mode,
        "operation": execution.get("operation"),
        "counts": counts,
        "drop_reasons": dict(sorted(drop_reasons.items())),
        "http_statuses": dict(sorted(http_statuses.items())),
        "latency_ms": {
            "http": {**_statistics(http_values), "excluded": dict(http_exclusions)},
            "db_completed": {**_statistics(db_values["completed"]), "excluded": dict(db_exclusions["completed"])},
            "db_failed": {**_statistics(db_values["failed"]), "excluded": dict(db_exclusions["failed"])},
        },
        "throughput": {
            "http_requests_per_s": _rate(counts["sent"], window.get("actual_duration_s")),
            "http_window_s": window.get("actual_duration_s"),
            "completed_commands_per_s": _rate(counts["completed"], lifecycle.get("processing_duration_s")),
            "business_valid_completed_commands_per_s": _rate(business["valid_completed"], lifecycle.get("processing_duration_s")),
            "processing_window_s": lifecycle.get("processing_duration_s"),
        },
        "integrity": {**business, "issues": issues},
        "http_failures": http_failures,
        "cohorts": cohorts,
        "execution": dict(window),
        "lifecycle": dict(lifecycle),
        "notes": notes,
    }
