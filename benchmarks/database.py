"""Prepare isolated benchmark data and reconcile HTTP receipts with PostgreSQL."""

import argparse
import json
import os
import time
from collections import Counter
from copy import deepcopy
from datetime import datetime
from math import isfinite
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from psycopg.conninfo import conninfo_to_dict

BENCHMARK_DATABASE = "return_flow_benchmark"


def benchmark_connection() -> psycopg.Connection:
    """Refuse development databases before issuing any benchmark SQL."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise ValueError("DATABASE_URL must name the isolated benchmark database.")
    if conninfo_to_dict(url).get("dbname") != BENCHMARK_DATABASE:
        raise ValueError(f"Benchmark commands require database {BENCHMARK_DATABASE!r}.")
    connection = psycopg.connect(url, connect_timeout=5)
    if connection.info.dbname != BENCHMARK_DATABASE:
        connection.close()
        raise ValueError("Connected to an unexpected database; benchmark aborted.")
    return connection


def seed_id(index: int) -> str:
    return f"00000000-0000-4000-8000-{index:012d}"


def positive_int(value: str) -> int:
    parsed = int(value)
    if not 1 <= parsed <= 999_999_999_999:
        raise argparse.ArgumentTypeError("Expected a positive integer below 10^12.")
    return parsed


def positive_timeout(value: str) -> float:
    parsed = float(value)
    if not isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("Expected a finite timeout greater than zero.")
    return parsed


def wait_idle(timeout: float) -> dict[str, Any]:
    """Wait for stopped benchmark clients to disconnect before auditing or reseeding."""
    if not isfinite(timeout) or timeout <= 0:
        raise ValueError("Expected a finite timeout greater than zero.")
    started = time.monotonic()
    deadline = started + timeout
    with benchmark_connection() as connection:
        # Each poll needs a fresh pg_stat_activity snapshot, outside a long transaction.
        connection.autocommit = True
        while True:
            row = connection.execute(
                """
                SELECT count(*) FROM pg_stat_activity
                WHERE datname = current_database()
                  AND backend_type = 'client backend'
                  AND pid <> pg_backend_pid()
                """
            ).fetchone()
            assert row is not None
            remaining = row[0]
            now = time.monotonic()
            if remaining == 0:
                return {
                    "status": "passed",
                    "remaining_connections": 0,
                    "waited_seconds": round(now - started, 3),
                }
            if now >= deadline:
                raise TimeoutError(
                    f"Benchmark database still has {remaining} client connection(s) "
                    f"after {timeout:g} seconds; audit and reseeding are unsafe."
                )
            time.sleep(min(0.2, deadline - now))



def drain(timeout: float) -> dict[str, Any]:
    """Observe pending work after the generator finishes, without HTTP polling."""
    if not isfinite(timeout) or timeout <= 0:
        raise ValueError("Expected a finite timeout greater than zero.")
    started = time.monotonic()
    deadline = started + timeout
    with benchmark_connection() as connection:
        connection.autocommit = True
        while True:
            row = connection.execute(
                "SELECT count(*) FROM commands WHERE status = 'pending'"
            ).fetchone()
            assert row is not None
            pending = row[0]
            now = time.monotonic()
            if pending == 0 or now >= deadline:
                return {
                    "status": "passed" if pending == 0 else "timed_out",
                    "pending_commands": pending,
                    "waited_seconds": round(now - started, 3),
                }
            time.sleep(min(1.0, deadline - now))

def seed(size: int) -> dict[str, Any]:
    with benchmark_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE commands, returns, deliveries")
            cursor.executemany(
                "INSERT INTO deliveries (id, status) VALUES (%s, 'delivered')",
                ((seed_id(index),) for index in range(1, size + 1)),
            )
    return {"status": "passed", "seed_deliveries": size, "returns": 0}


def read_receipts(path: Path) -> list[dict[str, Any]]:
    receipts = []
    identifier_fields = {"flow_id", "delivery_id", "delivered_id", "return_id", "return_delivery_id"}
    list_fields = {"command_ids", "accepted_command_ids"}
    time_fields = {"started_at_ms", "finished_at_ms"}
    fields = identifier_fields | list_fields | time_fields | {"return_attempted"}
    for number, line in enumerate(path.read_text().splitlines(), 1):
        # k6's raw format is preferred. Accept JSON-formatted log records as well.
        if line.startswith("{"):
            try:
                record = json.loads(line)
                line = str(record.get("msg", record.get("message", line)))
            except (json.JSONDecodeError, AttributeError):
                pass
        marker = line.find("RECEIPT ")
        if marker < 0:
            continue
        try:
            receipt, _ = json.JSONDecoder().raw_decode(line[marker + 8 :])
            if not isinstance(receipt, dict) or not receipt or receipt.keys() - fields:
                raise ValueError("Unexpected receipt fields.")
            if ("return_id" in receipt) != ("return_delivery_id" in receipt):
                raise ValueError("A return receipt must include its delivery ID.")
            parsed: dict[str, Any] = {}
            for key, value in receipt.items():
                if key in identifier_fields:
                    parsed[key] = str(UUID(value))
                elif key in list_fields:
                    if not isinstance(value, list):
                        raise ValueError(f"{key} must be a list of UUIDs.")
                    parsed[key] = [str(UUID(identifier)) for identifier in value]
                elif key == "return_attempted":
                    if not isinstance(value, bool):
                        raise ValueError("return_attempted must be a boolean.")
                    parsed[key] = value
                else:
                    if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or value < 0:
                        raise ValueError(f"{key} must be a finite non-negative timestamp.")
                    parsed[key] = value
            if "flow_id" in parsed and not time_fields <= parsed.keys():
                raise ValueError("A flow receipt must include start and finish timestamps.")
            if not set(parsed.get("accepted_command_ids", [])) <= set(parsed.get("command_ids", [])):
                raise ValueError("Accepted commands must have submission receipts.")
            receipts.append(parsed)
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError(f"Invalid receipt on line {number}: {exc}") from exc
    return receipts


def metric_count(summary: dict[str, Any], name: str) -> int:
    try:
        value = summary["metrics"][name]["values"]["count"]
    except (KeyError, TypeError) as exc:
        raise ValueError(f"Missing counter {name!r} in k6 summary.") from exc
    if not isinstance(value, (int, float)) or value < 0 or int(value) != value:
        raise ValueError(f"Invalid counter {name!r} in k6 summary.")
    return int(value)


def reconcile(
    *,
    size: int,
    scenario: str,
    summary: dict[str, Any],
    receipts: list[dict[str, Any]],
    deliveries: dict[str, str],
    returns: dict[str, tuple[str, str]],
) -> dict[str, Any]:
    """Check identities, relationships and state, not just aggregate row counts."""
    errors: list[str] = []
    created = [receipt["delivery_id"] for receipt in receipts if "delivery_id" in receipt]
    delivered = [receipt["delivered_id"] for receipt in receipts if "delivered_id" in receipt]
    returned = [
        (receipt["return_id"], receipt["return_delivery_id"])
        for receipt in receipts
        if "return_id" in receipt
    ]
    counters = {
        name: metric_count(summary, name)
        for name in (
            "workflows_attempted", "workflows_completed", "deliveries_created",
            "deliveries_delivered", "returns_created", "unknown_writes"
        )
    }
    if counters["workflows_attempted"] == 0:
        errors.append("No workflows were attempted; this execution cannot validate the scenario.")
    if counters["workflows_completed"] > counters["workflows_attempted"]:
        errors.append("Completed workflows exceed attempted workflows.")
    unknown = counters["unknown_writes"]
    for name, identifiers in (
        ("deliveries_created", created),
        ("deliveries_delivered", delivered),
        ("returns_created", [item[0] for item in returned]),
    ):
        if len(identifiers) != counters[name]:
            errors.append(f"{name}: {counters[name]} confirmations but {len(identifiers)} receipts.")
        duplicates = [identifier for identifier, count in Counter(identifiers).items() if count > 1]
        if duplicates:
            errors.append(f"{name}: duplicate confirmed IDs: {duplicates[:5]}.")

    seeded = {seed_id(index) for index in range(1, size + 1)}
    created_ids, delivered_ids = set(created), set(delivered)
    return_ids = {item[0] for item in returned}
    if seeded & created_ids:
        errors.append("New deliveries reused seeded IDs.")
    if delivered_ids - created_ids:
        errors.append("A confirmed transition has no corresponding creation receipt.")
    if scenario == "read" and (created_ids or delivered_ids or return_ids or unknown):
        errors.append("The read scenario performed writes.")
    if scenario == "create" and (delivered_ids or return_ids):
        errors.append("The create scenario performed transitions or created returns.")

    for identifier in seeded | delivered_ids:
        if deliveries.get(identifier) != "delivered":
            errors.append(f"Delivery {identifier} should exist with status delivered.")
    for identifier in created_ids:
        actual = deliveries.get(identifier)
        if actual is None:
            errors.append(f"Confirmed delivery {identifier} is missing.")
        elif identifier not in delivered_ids and actual != "pending":
            # A lost response may hide a committed transition in the flow scenario.
            if not (scenario == "flow" and unknown and actual == "delivered"):
                errors.append(f"Delivery {identifier} changed without a confirmed transition.")
    for identifier, delivery_id in returned:
        if returns.get(identifier) != (delivery_id, "requested"):
            errors.append(f"Confirmed return {identifier} is missing or has incorrect data.")
        if delivery_id not in delivered_ids:
            errors.append(f"Return {identifier} has no corresponding confirmed delivery transition.")
    invalid_deliveries = sum(status not in {"pending", "delivered"} for status in deliveries.values())
    invalid_returns = sum(
        status != "requested" or deliveries.get(delivery_id) != "delivered"
        for delivery_id, status in returns.values()
    )
    if invalid_deliveries:
        errors.append(f"{invalid_deliveries} deliveries have invalid states.")
    if invalid_returns:
        errors.append(f"{invalid_returns} returns have invalid states, missing or pending deliveries.")

    extra_deliveries = set(deliveries) - seeded - created_ids
    extra_returns = set(returns) - return_ids
    extra_count = len(extra_deliveries) + len(extra_returns)
    if extra_count > unknown:
        errors.append(f"{extra_count} unconfirmed inserted rows exceed {unknown} ambiguous writes.")
    status = "failed" if errors else "inconclusive" if unknown else "passed"
    return {
        "status": status,
        "scenario": scenario,
        "errors": errors[:50],
        "error_count": len(errors),
        "counts": {
            "seed_deliveries": size,
            "stored_deliveries": len(deliveries),
            "stored_returns": len(returns),
            "extra_deliveries": len(extra_deliveries),
            "extra_returns": len(extra_returns),
            **counters,
        },
        "notes": ["Ambiguous writes require reconciliation; correctness is not confirmed."] if unknown else [],
    }


def reconcile_commands(
    audit: dict[str, Any], *, receipts: list[dict[str, Any]],
    summary: dict[str, Any], commands: dict[str, dict[str, Any]],
    deliveries: dict[str, str], returns: dict[str, tuple[str, str]], size: int,
    allow_unfinished: bool = False,
) -> dict[str, Any]:
    """Account for every command and match its committed outcome to business rows.

    Receipts include attempted command IDs even when publication or polling timed
    out. Such attempts remain inconclusive: a late database result must not turn
    an unsuccessful client workflow into a successful benchmark execution.
    """
    errors = list(audit["errors"])
    previous_visible_errors = len(errors)
    submitted = [identifier for receipt in receipts for identifier in receipt.get("command_ids", [])]
    expected = set(submitted)
    submitted_count = metric_count(summary, "commands_submitted")
    if len(submitted) != submitted_count:
        errors.append(f"{submitted_count} submitted commands but {len(submitted)} command ID receipts.")
    if audit["scenario"] == "read" and submitted:
        errors.append("The read scenario submitted write commands.")
    if set(commands) - expected:
        errors.append("Persisted commands have no corresponding submission receipt.")
    missing = expected - set(commands)
    if not allow_unfinished and len(missing) > audit["counts"]["unknown_writes"]:
        errors.append("Missing commands exceed the number of ambiguous writes.")

    states = Counter(command["status"] for command in commands.values())
    creations: dict[str, str] = {}
    returned: dict[str, str] = {}
    transitioned: set[str] = set()
    for identifier, command in commands.items():
        kind, state = command["type"], command["status"]
        payload, result, error = command["payload"], command["result"], command["error"]
        if not isinstance(payload, dict) or any((
            payload.get("command_id") != identifier,
            payload.get("type") != kind,
            payload.get("version") != 1,
        )):
            errors.append(f"Command {identifier} has an inconsistent payload identity.")
            continue
        if kind not in {"create_delivery", "ensure_delivery_delivered", "request_return"}:
            errors.append(f"Command {identifier} has an unexpected type.")
            continue
        if audit["scenario"] == "create" and kind != "create_delivery":
            errors.append("The create scenario submitted a transition or return command.")
        if state == "pending":
            if result is not None or error is not None:
                errors.append(f"Pending command {identifier} already has a terminal outcome.")
            continue
        if state == "failed":
            errors.append(f"Command {identifier} failed during a valid benchmark workflow: {error}.")
            if result is not None or not error:
                errors.append(f"Failed command {identifier} has an inconsistent outcome.")
            continue
        if state != "completed" or not isinstance(result, dict) or error is not None:
            errors.append(f"Command {identifier} has an invalid terminal state.")
            continue
        entity_id = result.get("id")
        if not isinstance(entity_id, str):
            errors.append(f"Completed command {identifier} has no business result ID.")
            continue
        if kind == "create_delivery":
            if entity_id in creations:
                errors.append(f"Distinct create commands share delivery {entity_id}.")
            creations[entity_id] = identifier
            if result != {"id": entity_id, "status": "pending"} or entity_id not in deliveries:
                errors.append(f"Create command {identifier} has an invalid or missing delivery result.")
        elif kind == "ensure_delivery_delivered":
            transitioned.add(entity_id)
            if (
                payload.get("delivery_id") != entity_id
                or result != {"id": entity_id, "status": "delivered"}
                or deliveries.get(entity_id) != "delivered"
            ):
                errors.append(f"Delivery command {identifier} does not match its committed transition.")
        else:
            if entity_id in returned:
                errors.append(f"Distinct return commands share return {entity_id}.")
            returned[entity_id] = identifier
            parent = payload.get("delivery_id")
            if (
                result != {"id": entity_id, "delivery_id": parent, "status": "requested"}
                or returns.get(entity_id) != (parent, "requested")
                or deliveries.get(parent) != "delivered"
            ):
                errors.append(f"Return command {identifier} does not match its committed return.")

    seeded = {seed_id(index) for index in range(1, size + 1)}
    if set(deliveries) != seeded | set(creations):
        errors.append("Delivery rows do not match seeded data and completed create commands.")
    if set(returns) != set(returned):
        errors.append("Return rows do not match completed return commands; possible duplicate or partial write.")
    for identifier in creations:
        expected_state = "delivered" if identifier in transitioned else "pending"
        if deliveries.get(identifier) != expected_state:
            errors.append(f"Delivery {identifier} changed without a completed transition command.")

    # A confirmed resource must belong to a completed command from that same
    # workflow, rather than merely matching a row created by another request.
    for receipt in receipts:
        workflow = [commands[item] for item in receipt.get("command_ids", []) if item in commands]
        for field, kind in (
            ("delivery_id", "create_delivery"),
            ("delivered_id", "ensure_delivery_delivered"),
            ("return_id", "request_return"),
        ):
            if field in receipt and not any(
                command["type"] == kind and command["status"] == "completed"
                and isinstance(command["result"], dict)
                and command["result"].get("id") == receipt[field]
                for command in workflow
            ):
                errors.append(f"Confirmed {field} has no matching completed command in its workflow.")

    unfinished = states["pending"] + len(missing)
    audit["error_count"] += len(errors) - previous_visible_errors
    audit["errors"] = errors[:50]
    audit["counts"].update({
        "commands_submitted": submitted_count,
        "stored_commands": len(commands),
        "commands_pending": states["pending"],
        "commands_completed": states["completed"],
        "commands_failed": states["failed"],
        "commands_missing": len(missing),
    })
    if unfinished:
        audit["notes"].append("Some submitted commands are pending or missing; completion is not confirmed.")
    audit["status"] = (
        "failed" if errors else "inconclusive"
        if unfinished or audit["counts"]["unknown_writes"] else "passed"
    )
    return audit


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower), 3)


def _finite_timestamp(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or value < 0:
        raise ValueError("Expected a finite non-negative timestamp.")
    return float(value)


def _command_timestamp(value: Any) -> float:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("Command timestamps must include a timezone.")
    return _finite_timestamp(value.timestamp() * 1000)


def reconcile_flows(
    *, size: int, scenario: str, variant: str, summary: dict[str, Any],
    receipts: list[dict[str, Any]], deliveries: dict[str, str],
    returns: dict[str, tuple[str, str]], commands: dict[str, dict[str, Any]],
    return_commit_times: dict[str, datetime | None],
) -> dict[str, Any]:
    """Measure complete flows from receipts and committed state after the load.

    Event submission is not completion. A final command can complete after its
    HTTP response (including an ambiguous response); its committed outcome is
    counted while client errors still make the execution unsuccessful.
    """
    if scenario != "flow":
        raise ValueError("E2E reconciliation requires the flow scenario.")
    benchmark = summary["benchmark"]
    started = _finite_timestamp(benchmark["started_at_ms"])
    duration = _finite_timestamp(benchmark["offered_duration_ms"])
    if duration <= 0:
        raise ValueError("The offered duration must be positive.")
    end = started + duration
    attempted = metric_count(summary, "workflows_attempted")
    errors: list[str] = []
    flow_ids = [receipt.get("flow_id") for receipt in receipts]
    if len(receipts) != attempted:
        errors.append(f"{attempted} attempted flows but {len(receipts)} flow receipts.")
    duplicated_flows = {identifier for identifier, count in Counter(flow_ids).items() if count > 1}
    if None in flow_ids or duplicated_flows:
        errors.append("Flow receipts must have distinct flow IDs.")
    submitted = [identifier for receipt in receipts for identifier in receipt.get("command_ids", [])]
    duplicate_commands = {identifier for identifier, count in Counter(submitted).items() if count > 1}
    if duplicate_commands:
        errors.append("Commands must belong to exactly one position in one flow.")

    # Verify the original HTTP accounting before deriving any database receipts.
    for metric, field in (
        ("deliveries_created", "delivery_id"),
        ("deliveries_delivered", "delivered_id"),
        ("returns_created", "return_id"),
    ):
        observed = sum(field in receipt for receipt in receipts)
        if metric_count(summary, metric) != observed:
            errors.append(f"{metric}: summary confirmations do not match flow receipts.")

    queued = variant == "event_driven"
    tracks_commands = queued or benchmark.get("version", 0) >= 5
    state_receipts = deepcopy(receipts)
    state_summary = deepcopy(summary)
    recovered: set[int] = set()
    if tracks_commands:
        # Late completions have no HTTP result receipt. Derive receipts strictly
        # from this flow's commands for the existing exhaustive row audit.
        for index, (original, derived) in enumerate(zip(receipts, state_receipts)):
            for identifier in original.get("command_ids", []):
                command = commands.get(identifier, {})
                result = command.get("result")
                if command.get("status") != "completed" or not isinstance(result, dict):
                    continue
                kind, entity_id = command.get("type"), result.get("id")
                if not isinstance(entity_id, str):
                    continue
                field = {
                    "create_delivery": "delivery_id", "ensure_delivery_delivered": "delivered_id",
                    "request_return": "return_id",
                }.get(kind)
                if field:
                    if field in original and original[field] != entity_id:
                        errors.append(f"Flow {original.get('flow_id')} has a conflicting {field} receipt.")
                    derived[field] = entity_id
                    if field == "return_id":
                        derived["return_delivery_id"] = result.get("delivery_id")
                        if not queued and field not in original:
                            recovered.add(index)
    else:
        returns_by_delivery: dict[str, list[str]] = {}
        for return_id, (delivery_id, _) in returns.items():
            returns_by_delivery.setdefault(delivery_id, []).append(return_id)
        candidates = []
        for index, receipt in enumerate(receipts):
            parent = receipt.get("delivery_id")
            if (
                "return_id" in receipt or not receipt.get("return_attempted")
                or parent is None or receipt.get("delivered_id") != parent
                or deliveries.get(parent) != "delivered"
            ):
                continue
            matching = returns_by_delivery.get(parent, [])
            if len(matching) > 1:
                errors.append(f"Flow {receipt.get('flow_id')} has multiple returns; its final outcome is ambiguous.")
            elif len(matching) == 1 and returns[matching[0]] == (parent, "requested"):
                candidates.append((index, matching[0]))
        if len(candidates) > metric_count(summary, "unknown_writes"):
            errors.append("Recovered return candidates exceed ambiguous write attempts.")
        else:
            for index, return_id in candidates:
                state_receipts[index].update(
                    return_id=return_id, return_delivery_id=receipts[index]["delivery_id"],
                )
                recovered.add(index)
    for metric, field in (
        ("deliveries_created", "delivery_id"),
        ("deliveries_delivered", "delivered_id"),
        ("returns_created", "return_id"),
    ):
        state_summary["metrics"][metric]["values"]["count"] = sum(
            field in receipt for receipt in state_receipts
        )
    audit = reconcile(
        size=size, scenario=scenario, summary=state_summary, receipts=state_receipts,
        deliveries=deliveries, returns=returns,
    )
    if tracks_commands:
        audit = reconcile_commands(
            audit, receipts=state_receipts, summary=state_summary, commands=commands,
            deliveries=deliveries, returns=returns, size=size, allow_unfinished=True,
        )
    audit["variant"] = variant
    # Never replace the generator's counters with derived database outcomes.
    for metric in ("deliveries_created", "deliveries_delivered", "returns_created"):
        audit["counts"][metric] = metric_count(summary, metric)

    outcomes: Counter[str] = Counter()
    records: list[dict[str, Any]] = []
    latencies: list[float] = []
    expected_types = ("create_delivery", "ensure_delivery_delivered", "request_return")
    for receipt in receipts:
        identifier = receipt.get("flow_id")
        derived = state_receipts[len(records)]
        flow_errors: list[str] = []
        command_records: list[dict[str, Any]] = []
        status = "incomplete"
        completed_at: float | None = None
        try:
            flow_started = _finite_timestamp(receipt["started_at_ms"])
            finished = _finite_timestamp(receipt["finished_at_ms"])
            if finished < flow_started or flow_started < started:
                raise ValueError("Receipt timestamps are out of order or precede the load window.")
            end = max(end, finished)
        except (KeyError, TypeError, ValueError) as error:
            flow_errors.append(str(error))
            flow_started = started
        if identifier is None or identifier in duplicated_flows:
            flow_errors.append("Missing or duplicate flow ID.")
        if tracks_commands:
            command_ids = receipt.get("command_ids", [])
            accepted_ids = receipt.get("accepted_command_ids", [])
            if len(command_ids) > 3 or any(item in duplicate_commands for item in command_ids):
                flow_errors.append("A flow must contain at most three distinct ordered commands.")
            if len(set(accepted_ids)) != len(accepted_ids) or not set(accepted_ids) <= set(command_ids):
                flow_errors.append("Accepted command IDs do not match submitted commands.")
            workflow = [commands.get(item) for item in command_ids]
            parent: str | None = None
            previous_completion = flow_started
            for position, command in enumerate(workflow):
                command_record: dict[str, Any] = {
                    "command_id": command_ids[position],
                    "type": command.get("type") if command is not None else None,
                    "status": command.get("status") if command is not None else "missing",
                    "accepted": command_ids[position] in accepted_ids,
                    "created_at_ms": None, "completed_at_ms": None, "duration_ms": None,
                }
                command_records.append(command_record)
                if command is None:
                    continue
                if position >= 3 or command.get("type") != expected_types[position]:
                    flow_errors.append("Command types do not follow create, deliver, return order.")
                result = command.get("result")
                payload = command.get("payload")
                if not isinstance(payload, dict) or (
                    payload.get("command_id") != command_ids[position]
                    or payload.get("type") != command.get("type") or payload.get("version") != 1
                ):
                    flow_errors.append("Command payload identity does not match its submission.")
                state = command.get("status")
                if state == "completed" and (not isinstance(result, dict) or command.get("error") is not None):
                    flow_errors.append("Completed command has an invalid terminal outcome.")
                elif state == "pending" and (result is not None or command.get("error") is not None):
                    flow_errors.append("Pending command already has a terminal outcome.")
                elif state not in {"pending", "completed", "failed"}:
                    flow_errors.append("Command has an invalid status.")
                if command.get("status") == "completed" and isinstance(result, dict):
                    if position == 0:
                        parent = result.get("id")
                    elif not isinstance(payload, dict) or payload.get("delivery_id") != parent:
                        flow_errors.append("A command refers to another flow's delivery.")
                    if position == 1 and result.get("id") != parent:
                        flow_errors.append("The delivered entity differs from the created entity.")
                    if position == 2 and result.get("delivery_id") != parent:
                        flow_errors.append("The return belongs to another flow's delivery.")
                    expected_result = (
                        {"id": parent, "status": "pending"} if position == 0 else
                        {"id": parent, "status": "delivered"} if position == 1 else
                        {"id": result.get("id"), "delivery_id": parent, "status": "requested"}
                    )
                    if not isinstance(result.get("id"), str) or result != expected_result:
                        flow_errors.append("Command result does not match its flow operation.")
                try:
                    created_at = _command_timestamp(command.get("created_at"))
                    command_record["created_at_ms"] = created_at
                    if created_at + 1 < previous_completion:
                        raise ValueError("Command creation precedes its flow or dependency completion.")
                    if command.get("status") in {"completed", "failed"}:
                        terminal_at = _command_timestamp(command.get("completed_at"))
                        command_record["completed_at_ms"] = terminal_at
                        if terminal_at < created_at:
                            raise ValueError("Command completion precedes its creation.")
                        command_record["duration_ms"] = round(terminal_at - created_at, 3)
                        previous_completion = terminal_at
                        end = max(end, terminal_at)
                        if position == 2 and command.get("status") == "completed":
                            completed_at = terminal_at
                    elif command.get("completed_at") is not None:
                        raise ValueError("A pending command already has a completion timestamp.")
                except (TypeError, ValueError) as error:
                    flow_errors.append(str(error))
            if any(command is not None and command.get("status") == "failed" for command in workflow):
                status = "failed"
            elif any(command is None for command in workflow):
                status = "missing"
            elif any(command.get("status") == "pending" for command in workflow):
                status = "pending"
            elif len(workflow) == 3 and all(command.get("status") == "completed" for command in workflow):
                status = "completed"
        else:
            if receipt.get("command_ids") or receipt.get("accepted_command_ids"):
                flow_errors.append("Legacy direct flows must not submit commands.")
            parent = derived.get("delivery_id")
            return_id = derived.get("return_id")
            if return_id:
                if (
                    parent != derived.get("delivered_id") or parent != derived.get("return_delivery_id")
                    or deliveries.get(parent) != "delivered"
                    or returns.get(return_id) != (parent, "requested")
                ):
                    flow_errors.append("The final return does not match the complete committed flow.")
                else:
                    status = "completed"
        # A valid command result alone is insufficient if its business row is
        # missing or inconsistent. Apply the same identity checks per flow.
        if status == "completed" and (
            deliveries.get(derived.get("delivery_id")) != "delivered"
            or derived.get("delivery_id") != derived.get("delivered_id")
            or derived.get("delivery_id") != derived.get("return_delivery_id")
            or returns.get(derived.get("return_id")) != (derived.get("delivery_id"), "requested")
        ):
            flow_errors.append("Completed flow outcomes do not match committed business rows.")
        if status == "completed":
            try:
                committed_at = _command_timestamp(return_commit_times.get(derived.get("return_id")))
                if committed_at < flow_started or (completed_at is not None and committed_at < completed_at):
                    raise ValueError("Return commit timestamp precedes flow creation or command completion.")
                completed_at = committed_at
                end = max(end, committed_at)
            except (TypeError, ValueError) as error:
                flow_errors.append(f"Missing or invalid return commit timestamp: {error}")
        if flow_errors:
            status = "failed"
            errors.extend(f"Flow {identifier}: {error}" for error in flow_errors)
        record: dict[str, Any] = {
            "flow_id": identifier, "status": status, "started_at_ms": flow_started,
            "recovered": len(records) in recovered,
        }
        if tracks_commands:
            record["commands"] = command_records
        if flow_errors:
            record["errors"] = flow_errors
        if status == "completed" and completed_at is not None:
            latency = completed_at - flow_started
            latencies.append(latency)
            record.update(completed_at_ms=completed_at, completion_ms=round(latency, 3))
        outcomes[status] += 1
        records.append(record)

    if not queued and metric_count(summary, "workflows_completed") != sum("return_id" in receipt for receipt in receipts):
        errors.append("Client-completed flows do not match final HTTP result receipts.")
    if queued and metric_count(summary, "workflows_completed") != 0:
        errors.append("Queued flows must be counted as completed by the post-run audit, not by HTTP acceptance.")
    if "workflows_submitted" in summary["metrics"]:
        confirmed_submissions = sum(
            len(receipt.get("command_ids", [])) == 3
            and receipt["command_ids"][-1] in receipt.get("accepted_command_ids", [])
            if queued else "return_id" in receipt
            for receipt in receipts
        )
        if metric_count(summary, "workflows_submitted") != confirmed_submissions:
            errors.append("Submitted workflow counter does not match final operation acceptance receipts.")
    if "technical_errors" in summary["metrics"] and metric_count(summary, "technical_errors"):
        errors.append("Client errors occurred; late completion does not make this load execution successful.")
    if outcomes["failed"] and not errors:
        errors.append("One or more flows failed during execution.")
    elapsed = (end - started) / 1000
    audit["flows"] = {
        "attempted": attempted,
        **{state: outcomes[state] for state in ("completed", "failed", "pending", "missing", "incomplete")},
        "receipt_count": len(receipts),
        "recovered": len(recovered),
        "unobserved": max(0, attempted - len(receipts)),
        "completion_p50_ms": _percentile(latencies, 0.50),
        "completion_p95_ms": _percentile(latencies, 0.95),
        "completion_p99_ms": _percentile(latencies, 0.99),
        "elapsed_seconds": round(elapsed, 6),
        "completed_per_s": outcomes["completed"] / elapsed,
        "started_at_ms": started,
        "finished_at_ms": end,
        "records": records,
    }
    audit["error_count"] += len(errors)
    audit["errors"] = (audit["errors"] + errors)[:50]
    if audit["error_count"]:
        audit["status"] = "failed"
    elif outcomes["completed"] != attempted or audit["counts"]["unknown_writes"]:
        audit["status"] = "inconclusive"
    audit["notes"].append(
        "Pending, missing and incomplete flows are unresolved observations, not proven message loss. "
        "Both variants finish at the return row's PostgreSQL transaction commit timestamp. "
        "Command completion timestamps are diagnostic samples taken just before commit. "
        "Recovered direct outcomes do not replace the original HTTP success counters."
    )
    return audit


def verify(args: argparse.Namespace) -> dict[str, Any]:
    summary = json.loads(args.summary.read_text())
    receipts = read_receipts(args.receipts)
    e2e = summary.get("benchmark", {}).get("version", 0) >= 4
    return_commit_times: dict[str, datetime | None] = {}
    with benchmark_connection() as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        if e2e:
            setting = connection.execute("SHOW track_commit_timestamp").fetchone()
            if setting is None or setting[0] != "on":
                raise ValueError("E2E benchmarks require PostgreSQL track_commit_timestamp=on before the load.")
        deliveries = {
            str(identifier): status
            for identifier, status in connection.execute("SELECT id, status FROM deliveries")
        }
        returns = {}
        commit_column = ", pg_xact_commit_timestamp(xmin)" if e2e else ""
        for row in connection.execute(f"SELECT id, delivery_id, status{commit_column} FROM returns"):
            identifier, delivery_id, status = row[:3]
            returns[str(identifier)] = (str(delivery_id), status)
            if e2e:
                return_commit_times[str(identifier)] = row[3]
        commands = {
            str(identifier): {"type": kind, "payload": payload, "status": status,
                              "result": result, "error": error,
                              "created_at": created_at, "completed_at": completed_at}
            for identifier, kind, payload, status, result, error, created_at, completed_at in connection.execute(
                "SELECT command_id, type, payload, status, result, error, created_at, completed_at FROM commands"
            )
        } if args.variant == "event_driven" or summary.get("benchmark", {}).get("version", 0) >= 5 else {}
    if e2e:
        return reconcile_flows(
            size=args.size, scenario=args.scenario, variant=args.variant, summary=summary,
            receipts=receipts, deliveries=deliveries, returns=returns, commands=commands,
            return_commit_times=return_commit_times,
        )
    audit = reconcile(
        size=args.size, scenario=args.scenario, summary=summary, receipts=receipts,
        deliveries=deliveries, returns=returns,
    )
    audit["variant"] = args.variant
    if args.variant == "event_driven":
        return reconcile_commands(
            audit, receipts=receipts, summary=summary, commands=commands,
            deliveries=deliveries, returns=returns, size=args.size,
        )
    return audit


def write_result(result: dict[str, Any], output: Path | None) -> None:
    encoded = json.dumps(result, indent=2) + "\n"
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(encoded)
    print(encoded, end="")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("seed")
    prepare.add_argument("--size", type=positive_int, default=1000)
    idle = commands.add_parser("wait-idle")
    idle.add_argument("--timeout", type=positive_timeout, default=30.0)
    idle.add_argument("--output", type=Path)
    drain_parser = commands.add_parser("drain")
    drain_parser.add_argument("--timeout", type=positive_timeout, default=30.0)
    drain_parser.add_argument("--output", type=Path)
    check = commands.add_parser("verify")
    check.add_argument("--size", type=positive_int, default=1000)
    check.add_argument("--scenario", choices=("read", "create", "flow"), required=True)
    check.add_argument("--variant", choices=("postgres_pool", "event_driven"), default="postgres_pool")
    check.add_argument("--summary", type=Path, required=True)
    check.add_argument("--receipts", type=Path, required=True)
    check.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "seed":
            result = seed(args.size)
        elif args.command == "wait-idle":
            result = wait_idle(args.timeout)
        elif args.command == "drain":
            result = drain(args.timeout)
        else:
            result = verify(args)
    except (OSError, ValueError, psycopg.Error) as exc:
        result = {"status": "failed", "errors": [str(exc)]}
    write_result(result, getattr(args, "output", None))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
