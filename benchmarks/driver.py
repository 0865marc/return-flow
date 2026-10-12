"""Container entry point. Database access exists only in prepare/snapshot modes."""

import argparse
import asyncio
import json
import os
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen
from uuid import uuid4

from workload import execute

RESULTS = Path("/results")


def write_json(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, default=str) + "\n")
    temporary.replace(path)


def connect_when_ready(timeout=60):
    import psycopg

    deadline = time.monotonic() + timeout
    while True:
        try:
            return psycopg.connect(os.environ["DATABASE_URL"], connect_timeout=3)
        except psycopg.OperationalError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.5)


def prepare(plan, mode):
    # Readiness checks and fixture writes finish before the execution phase.
    from adapters.persistence.schema import SCHEMA_STATEMENTS

    with connect_when_ready() as connection:
        for statement in SCHEMA_STATEMENTS:
            connection.execute(statement)
        fixtures = [
            (item["delivery_id"], "pending" if item["operation"] == "deliver" else "delivered")
            for item in plan if item.get("delivery_id")
        ]
        if fixtures:
            with connection.cursor() as cursor:
                cursor.executemany("INSERT INTO deliveries (id, status) VALUES (%s, %s)", fixtures)
    if mode == "event_driven":
        asyncio.run(wait_for_broker())


async def wait_for_broker():
    import aio_pika

    deadline = time.monotonic() + 60
    while True:
        try:
            connection = await aio_pika.connect(
                "amqp://benchmark:benchmark@rabbitmq:5672/", timeout=3
            )
            await connection.close()
            return
        except (OSError, asyncio.TimeoutError, aio_pika.exceptions.AMQPException):
            if time.monotonic() >= deadline:
                raise
            await asyncio.sleep(0.5)


def ready(mode):
    # One warmup command proves the complete path, including the queued worker.
    # Polling here is preparation only; the load entry point never calls this.
    import psycopg

    deadline = time.monotonic() + 60
    while True:
        try:
            with urlopen("http://api:8000/health", timeout=2) as response:
                if response.status == 200:
                    break
        except (URLError, TimeoutError, OSError):
            if time.monotonic() >= deadline:
                raise
        time.sleep(0.25)
    key = str(uuid4())
    path = "/deliveries-async" if mode == "event_driven" else "/deliveries"
    request = Request("http://api:8000" + path, data=b"", method="POST", headers={"Idempotency-Key": key})
    with urlopen(request, timeout=10) as response:
        response.read()
    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as connection:
        while time.monotonic() < deadline:
            row = connection.execute("SELECT status FROM commands WHERE command_id = %s", (key,)).fetchone()
            if row and row[0] == "completed":
                return
            if row and row[0] == "failed":
                raise RuntimeError("The warmup command failed.")
            time.sleep(0.25)
    raise TimeoutError("The warmup command did not complete before execution.")


def business_is_valid(item, row, delivery, returned):
    """Validate actual rows, not just the command's claimed terminal status."""
    result = row.get("result") or {}
    if not isinstance(result, dict):
        return False
    if item["operation"] == "create":
        return bool(delivery and str(delivery["id"]) == result.get("id")
                    and delivery["status"] == result.get("status") == "pending")
    if item["operation"] == "deliver":
        return bool(delivery and str(delivery["id"]) == item["delivery_id"] == result.get("id")
                    and delivery["status"] == result.get("status") == "delivered")
    return bool(returned and delivery
                and str(returned["id"]) == result.get("id")
                and str(returned["delivery_id"]) == item["delivery_id"] == result.get("delivery_id")
                and returned["status"] == result.get("status") == "requested"
                and str(delivery["id"]) == item["delivery_id"]
                and delivery["status"] == "delivered")


def snapshot(plan):
    import psycopg
    from psycopg.rows import dict_row

    rows = []
    with psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row) as connection:
        # Runner has stopped API and worker before invoking this function.
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        for start in range(0, len(plan), 1000):
            batch = plan[start:start + 1000]
            commands = connection.execute(
                "SELECT command_id, type, payload, status, result, error, error_code, created_at, completed_at "
                "FROM commands WHERE command_id = ANY(%s::uuid[])",
                ([item["command_id"] for item in batch],),
            ).fetchall()
            by_id = {str(row["command_id"]): row for row in commands}
            # Cast the table ID to text so malformed result IDs are reported as
            # failed integrity, rather than aborting the audit with a UUID error.
            result_ids = [str(row["result"].get("id", "")) for row in commands if isinstance(row["result"], dict)]
            delivery_ids = result_ids + [item["delivery_id"] for item in batch if item.get("delivery_id")]
            deliveries = {str(row["id"]): row for row in connection.execute(
                "SELECT id, status FROM deliveries WHERE id::text = ANY(%s::text[])", (delivery_ids,)
            ).fetchall()}
            returns = {str(row["id"]): row for row in connection.execute(
                "SELECT id, delivery_id, status FROM returns WHERE id::text = ANY(%s::text[])", (result_ids,)
            ).fetchall()}
            for item in batch:
                row = by_id.get(item["command_id"])
                if row is None:
                    continue
                result = row.get("result") or {}
                result_id = str(result.get("id", "")) if isinstance(result, dict) else ""
                delivery_id = item.get("delivery_id", result_id)
                row["business_valid"] = business_is_valid(item, row, deliveries.get(delivery_id), returns.get(result_id))
                payload = row.pop("payload")
                row["business_valid"] = row["business_valid"] and (
                    payload.get("command_id") == item["command_id"]
                    and payload.get("delivery_id") == item.get("delivery_id")
                    and payload.get("type") == row["type"]
                )
                rows.append(row)
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["prepare", "ready", "execute", "snapshot"])
    args = parser.parse_args()
    config = json.loads((RESULTS / "config.json").read_text())
    plan = json.loads((RESULTS / "plan.json").read_text())
    if args.action == "prepare":
        prepare(plan, config["mode"])
    elif args.action == "ready":
        ready(config["mode"])
    elif args.action == "execute":
        data = execute("http://api:8000", config["mode"], plan, config["rate"],
                       config["duration"], config["concurrency"], config["timeout"])
        # No backend reads, and no analytics or disk writes during passive drain.
        started = time.monotonic()
        time.sleep(config["drain_seconds"])
        data["passive_drain_actual_s"] = time.monotonic() - started
        write_json(RESULTS / "execution.json", data)
    else:
        write_json(RESULTS / "snapshot.json", snapshot(plan))


if __name__ == "__main__":
    main()
