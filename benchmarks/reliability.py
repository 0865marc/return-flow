"""Exercise concurrent requests and current duplicate-request semantics separately."""

import argparse
import json
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from math import isfinite
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

from database import benchmark_connection, positive_int, write_result


def request(base_url: str, path: str, timeout: float, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else b""
    message = Request(
        base_url.rstrip("/") + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
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
    errors: list[str] = []

    def create_delivery() -> str:
        status, delivery = request(args.base_url, "/deliveries", args.timeout)
        if status != 201 or delivery.get("status") != "pending":
            raise ValueError(f"Unable to prepare a pending delivery: HTTP {status}.")
        return str(UUID(delivery["id"]))

    def concurrent(path: str, body: dict | None = None) -> list[tuple[int, dict]]:
        barrier = threading.Barrier(args.concurrency)

        def worker(_: int) -> tuple[int, dict]:
            barrier.wait(timeout=args.timeout)
            return request(args.base_url, path, args.timeout, body)

        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            return list(executor.map(worker, range(args.concurrency)))

    delivery_id = create_delivery()
    transitions = concurrent(f"/deliveries/{delivery_id}/deliver")
    transition_statuses = Counter(status for status, _ in transitions)
    expected = Counter({200: 1, 409: args.concurrency - 1})
    if transition_statuses != expected:
        errors.append(f"Expected one 200 and {args.concurrency - 1} conflicts: {dict(transition_statuses)}.")
    for status, response in transitions:
        if status == 200 and response != {"id": delivery_id, "status": "delivered"}:
            errors.append("The successful transition returned incorrect delivery data.")

    repeated = concurrent("/returns", {"delivery_id": delivery_id})
    repeat_statuses = Counter(status for status, _ in repeated)
    returned_ids = []
    for status, response in repeated:
        if status != 201:
            errors.append(f"Repeated return request should create a return: HTTP {status}.")
            continue
        if response.get("delivery_id") != delivery_id or response.get("status") != "requested":
            errors.append("A repeated return request returned incorrect data.")
        try:
            returned_ids.append(str(UUID(response["id"])))
        except (KeyError, TypeError, ValueError, AttributeError):
            errors.append("A repeated return request returned an invalid ID.")
    if len(set(returned_ids)) != args.concurrency:
        errors.append("Repeated requests must currently create distinct returns; idempotency is not implemented.")

    pending_id = create_delivery()
    missing_id = str(uuid4())
    pending_status, _ = request(args.base_url, "/returns", args.timeout, {"delivery_id": pending_id})
    missing_status, _ = request(args.base_url, "/returns", args.timeout, {"delivery_id": missing_id})
    if pending_status != 409:
        errors.append(f"Returning a pending delivery should fail with 409, received {pending_status}.")
    if missing_status != 404:
        errors.append(f"Returning a missing delivery should fail with 404, received {missing_status}.")

    with benchmark_connection() as connection:
        delivery_rows = {
            str(identifier): status for identifier, status in connection.execute(
                "SELECT id, status FROM deliveries WHERE id = ANY(%s::uuid[])",
                ([delivery_id, pending_id, missing_id],),
            )
        }
        return_rows = {
            str(identifier): (str(parent_id), status)
            for identifier, parent_id, status in connection.execute(
                "SELECT id, delivery_id, status FROM returns "
                "WHERE delivery_id = ANY(%s::uuid[]) OR id = ANY(%s::uuid[])",
                ([delivery_id, pending_id, missing_id], returned_ids),
            )
        }
    if delivery_rows != {delivery_id: "delivered", pending_id: "pending"}:
        errors.append("Persisted delivery states do not match the accepted/rejected operations.")
    expected_returns = {identifier: (delivery_id, "requested") for identifier in returned_ids}
    if len(return_rows) != args.concurrency or return_rows != expected_returns:
        errors.append("Persisted returns do not match every confirmed ID, state and delivery relation.")
    return {
        "status": "failed" if errors else "passed",
        "errors": errors,
        "concurrency": args.concurrency,
        "transition_statuses": dict(transition_statuses),
        "repeated_return_statuses": dict(repeat_statuses),
        "pending_return_status": pending_status,
        "missing_return_status": missing_status,
        "stored_returns": len(return_rows),
        "notes": ["Repeated valid requests currently create distinct returns; no idempotency guarantee."],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--concurrency", type=positive_int, default=10)
    parser.add_argument("--timeout", type=float, default=10)
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
