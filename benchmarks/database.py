"""Prepare isolated benchmark data and reconcile HTTP receipts with PostgreSQL."""

import argparse
import json
import os
import time
from collections import Counter
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


def seed(size: int) -> dict[str, Any]:
    with benchmark_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE returns, deliveries")
            cursor.executemany(
                "INSERT INTO deliveries (id, status) VALUES (%s, 'delivered')",
                ((seed_id(index),) for index in range(1, size + 1)),
            )
    return {"status": "passed", "seed_deliveries": size, "returns": 0}


def read_receipts(path: Path) -> list[dict[str, str]]:
    receipts = []
    fields = {"delivery_id", "delivered_id", "return_id", "return_delivery_id"}
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
            receipts.append({key: str(UUID(value)) for key, value in receipt.items()})
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
    receipts: list[dict[str, str]],
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


def verify(args: argparse.Namespace) -> dict[str, Any]:
    summary = json.loads(args.summary.read_text())
    receipts = read_receipts(args.receipts)
    with benchmark_connection() as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        deliveries = {
            str(identifier): status
            for identifier, status in connection.execute("SELECT id, status FROM deliveries")
        }
        returns = {
            str(identifier): (str(delivery_id), status)
            for identifier, delivery_id, status in connection.execute(
                "SELECT id, delivery_id, status FROM returns"
            )
        }
    return reconcile(
        size=args.size, scenario=args.scenario, summary=summary, receipts=receipts,
        deliveries=deliveries, returns=returns,
    )


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
    check = commands.add_parser("verify")
    check.add_argument("--size", type=positive_int, default=1000)
    check.add_argument("--scenario", choices=("read", "create", "flow"), required=True)
    check.add_argument("--summary", type=Path, required=True)
    check.add_argument("--receipts", type=Path, required=True)
    check.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "seed":
            result = seed(args.size)
        elif args.command == "wait-idle":
            result = wait_idle(args.timeout)
        else:
            result = verify(args)
    except (OSError, ValueError, psycopg.Error) as exc:
        result = {"status": "failed", "errors": [str(exc)]}
    write_result(result, getattr(args, "output", None))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
