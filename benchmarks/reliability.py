"""Exercise concurrent requests and current duplicate-request semantics separately."""

import argparse
import json
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from math import isfinite
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

try:
    from .database import benchmark_connection, positive_int, write_result
except ImportError:  # Direct CLI execution inside the benchmark container.
    from database import benchmark_connection, positive_int, write_result


def request(
    base_url: str, path: str, timeout: float, body: dict | None = None,
    *, command_id: str | None = None, method: str = "POST",
) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else b""
    headers = {"Content-Type": "application/json"}
    if command_id:
        headers["Idempotency-Key"] = command_id
    message = Request(
        base_url.rstrip("/") + path,
        data=None if method == "GET" else data,
        headers=headers,
        method=method,
    )
    try:
        with urlopen(message, timeout=timeout) as response:
            return response.status, json.loads(response.read())
    except HTTPError as exc:
        return exc.code, {"error": exc.read().decode(errors="replace")}
    except (URLError, TimeoutError, OSError, ValueError) as exc:
        return 0, {"error": str(exc)}


def run(args: argparse.Namespace) -> dict:
    # The guard runs before any HTTP mutation. The runner targets this isolated API.
    with benchmark_connection():
        pass
    return run_commands(args)


def wait_command(args: argparse.Namespace, command_id: str) -> dict:
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        status, record = request(
            args.base_url, f"/commands/{command_id}",
            max(0.001, deadline - time.monotonic()), method="GET",
        )
        if status != 200 or record.get("command_id") != command_id:
            raise ValueError(f"Unable to inspect command {command_id}: HTTP {status}.")
        if record.get("status") in {"completed", "failed"}:
            return record
        if record.get("status") != "pending":
            raise ValueError(f"Command {command_id} returned an invalid state.")
        time.sleep(min(0.02, max(0, deadline - time.monotonic())))
    raise TimeoutError(f"Command {command_id} did not reach a terminal state within {args.timeout:g}s.")


def run_commands(args: argparse.Namespace) -> dict:
    """Check equal idempotency guarantees through each public execution path."""
    queued = args.variant == "event_driven"
    errors: list[str] = []
    outcomes: dict[str, dict] = {}
    direct_results: dict[str, list[dict]] = {}
    with benchmark_connection() as connection:
        existing_deliveries = {str(row[0]) for row in connection.execute("SELECT id FROM deliveries")}
        existing_returns = {str(row[0]) for row in connection.execute("SELECT id FROM returns")}
        existing_commands = {str(row[0]) for row in connection.execute("SELECT command_id FROM commands")}

    def route(path: str) -> str:
        return path if queued else path.removesuffix("-async")

    def submit(
        path: str, body: dict | None = None, *, key: str | None = None,
        failure_status: int | None = None,
    ) -> str:
        identifier = key or str(uuid4())
        status, response = request(
            args.base_url, route(path), args.timeout, body, command_id=identifier,
        )
        expected_status = 202 if queued else failure_status or (200 if "/deliver-async" in path else 201)
        if status != expected_status:
            raise ValueError(f"Expected HTTP {expected_status} for {path}: HTTP {status}, {response}.")
        if queued and response != {"command_id": identifier, "status": "accepted"}:
            raise ValueError(f"Expected command acceptance for {path}: {response}.")
        if not queued and failure_status is None:
            direct_results.setdefault(identifier, []).append(response)
        return identifier

    def completed(identifier: str, *, expected: str = "completed") -> dict:
        outcome = wait_command(args, identifier)
        if outcome["status"] != expected:
            raise ValueError(f"Command {identifier} should be {expected}: {outcome}.")
        if expected == "failed" and (not outcome.get("error") or outcome.get("result") is not None):
            raise ValueError("A failed command must preserve an error without a business result.")
        outcomes[identifier] = outcome
        if any(result != outcome.get("result") for result in direct_results.get(identifier, [])):
            errors.append("Direct retry responses do not match the stored command result.")
        return outcome.get("result") or {}

    def concurrent(path: str, body: dict | None = None, *, same_key: bool) -> list[str]:
        barrier = threading.Barrier(args.concurrency)
        shared = str(uuid4())

        def worker(_: int) -> str:
            barrier.wait(timeout=args.timeout)
            return submit(path, body, key=shared if same_key else None)

        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            return list(executor.map(worker, range(args.concurrency)))

    # Concurrent retries of a create command must yield the same delivery.
    creation_keys = concurrent("/deliveries-async", same_key=True)
    creation = completed(creation_keys[0])
    delivery_id = str(UUID(creation["id"]))
    if creation != {"id": delivery_id, "status": "pending"}:
        errors.append("Creating a delivery returned incorrect business data.")
    transition_path = f"/deliveries/{delivery_id}/deliver-async"
    transition_keys = concurrent(transition_path, same_key=True)
    transition = completed(transition_keys[0])
    if transition != {"id": delivery_id, "status": "delivered"}:
        errors.append("The delivery transition returned incorrect business data.")
    # Distinct ensure-delivered commands also succeed once delivery is delivered.
    for identifier in concurrent(transition_path, same_key=False):
        if completed(identifier) != transition:
            errors.append("An ensure-delivered command returned a different delivery.")

    body = {"delivery_id": delivery_id}
    repeated_keys = concurrent("/returns-async", body, same_key=True)
    first_return = completed(repeated_keys[0])
    returned_ids = [str(UUID(first_return["id"]))]
    if first_return != {"id": returned_ids[0], **body, "status": "requested"}:
        errors.append("The return command returned incorrect business data.")
    # Retrying after completion must preserve the exact result as well.
    submit("/returns-async", body, key=repeated_keys[0])
    if completed(repeated_keys[0]) != first_return:
        errors.append("Retrying a completed command changed its result.")
    for identifier in concurrent("/returns-async", body, same_key=False):
        returned = completed(identifier)
        return_id = str(UUID(returned["id"]))
        returned_ids.append(return_id)
        if returned != {"id": return_id, **body, "status": "requested"}:
            errors.append("A distinct return command returned incorrect business data.")
    if len(set(returned_ids)) != args.concurrency + 1:
        errors.append("Distinct command keys must create distinct returns.")

    pending_key = submit("/deliveries-async")
    pending_id = str(UUID(completed(pending_key)["id"]))
    missing_id = str(uuid4())
    for parent, failure_status in ((pending_id, 409), (missing_id, 404)):
        identifier = submit("/returns-async", {"delivery_id": parent}, failure_status=failure_status)
        completed(identifier, expected="failed")
        first_failure = outcomes[identifier].copy()
        submit("/returns-async", {"delivery_id": parent}, key=identifier, failure_status=failure_status)
        completed(identifier, expected="failed")
        if outcomes[identifier] != first_failure:
            errors.append("Retrying a failed command changed its terminal outcome.")

    conflicts = []
    for path, payload in (("/returns-async", {"delivery_id": pending_id}), ("/deliveries-async", None)):
        status, _ = request(
            args.base_url, route(path), args.timeout, payload, command_id=repeated_keys[0],
        )
        conflicts.append(status)
        if status != 409:
            errors.append(f"Reusing a key for a different payload or operation must return 409, received {status}.")

    with benchmark_connection() as connection:
        delivery_rows = {
            str(identifier): state for identifier, state in connection.execute(
                "SELECT id, status FROM deliveries"
            ) if str(identifier) not in existing_deliveries
        }
        return_rows = {
            str(identifier): (str(parent), state)
            for identifier, parent, state in connection.execute(
                "SELECT id, delivery_id, status FROM returns"
            ) if str(identifier) not in existing_returns
        }
        command_rows = {
            str(identifier): {"command_id": str(identifier), "type": kind,
                              "status": state, "result": result, "error": error}
            for identifier, kind, state, result, error in connection.execute(
                "SELECT command_id, type, status, result, error FROM commands"
            ) if str(identifier) not in existing_commands
        }
    if delivery_rows != {delivery_id: "delivered", pending_id: "pending"}:
        errors.append("Persisted delivery states do not match completed commands.")
    if return_rows != {identifier: (delivery_id, "requested") for identifier in returned_ids}:
        errors.append("Persisted returns contain missing, duplicate or partially committed business writes.")
    if command_rows != outcomes:
        errors.append("Persisted command outcomes do not match the terminal HTTP responses.")
    return {
        "status": "failed" if errors else "passed", "variant": args.variant,
        "errors": errors, "concurrency": args.concurrency,
        "stored_returns": len(return_rows), "stored_commands": len(command_rows),
        "command_states": dict(Counter(record["status"] for record in command_rows.values())),
        "key_conflict_statuses": conflicts,
        "notes": ["Retries with the same key reuse outcomes; distinct return keys create distinct returns."],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--concurrency", type=positive_int, default=10)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--variant", choices=("postgres_pool", "event_driven"), default="postgres_pool")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.concurrency < 2 or not isfinite(args.timeout) or args.timeout <= 0:
        parser.error("Use at least two concurrent requests and a positive timeout.")
    try:
        result = run(args)
    except Exception as exc:
        result = {"status": "failed", "errors": [str(exc)]}
    write_result(result, args.output)
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
