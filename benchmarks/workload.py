"""Generate write-only, open-loop load without backend observation.

Only request receipts are retained while load is running.  Validation of state,
JSON response interpretation and statistical analysis belong to measurement.
"""

from __future__ import annotations

from datetime import datetime, timezone
import http.client
import json
import math
import queue
import threading
import time
from urllib.parse import urlsplit
from uuid import UUID, uuid5


_OPERATIONS = {"create", "deliver", "return"}
_MODES = {"postgres_pool", "event_driven"}
_RESPONSE_LIMIT = 4096
# compose.yml configures Uvicorn's 5 s keep-alive timeout. Retire a client
# connection earlier, so a quiet worker cannot send into a server-closed socket.
_KEEPALIVE_IDLE_LIMIT_S = 4.0


def _expire_idle_connection(connection, last_finished_at: float | None, now: float) -> None:
    if last_finished_at is not None and now - last_finished_at >= _KEEPALIVE_IDLE_LIMIT_S:
        connection.close()


def make_plan(run_id: str, operation: str, count: int) -> list[dict]:
    """Prepare reproducible identities before starting the execution clock."""
    namespace = UUID(run_id)
    if operation not in _OPERATIONS:
        raise ValueError(f"Unsupported operation: {operation}")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ValueError("count must be a non-negative integer")
    records = []
    for index in range(count):
        record = {
            "index": index,
            "operation": operation,
            "command_id": str(uuid5(namespace, f"{operation}:command:{index}")),
        }
        if operation != "create":
            record["delivery_id"] = str(uuid5(namespace, f"delivery:{index}"))
        records.append(record)
    return records


def _positive_finite(value: float, name: str) -> None:
    if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and greater than zero")


def execute(
    base_url: str,
    mode: str,
    plan: list[dict],
    rate: float,
    duration: float,
    concurrency: int,
    timeout: float,
) -> dict:
    """Submit at a fixed offered rate, dropping load instead of queuing it.

    Each worker owns one persistent HTTP connection.  There are at most
    ``concurrency`` assigned requests, including requests not yet started by a
    worker.  Missed generator slots are dropped; submissions are never replayed
    to catch up.  The submission window is followed only by an HTTP drain, not
    by command polling or domain-state checks.  Socket timeout applies to socket
    operations, as in ``http.client``, rather than imposing a total deadline.
    """
    if mode not in _MODES:
        raise ValueError(f"Unsupported mode: {mode}")
    for name, value in (("rate", rate), ("duration", duration), ("timeout", timeout)):
        _positive_finite(value, name)
    if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
        raise ValueError("concurrency must be a positive integer")
    url = urlsplit(base_url)
    if (
        url.scheme not in {"http", "https"}
        or not url.hostname
        or url.username is not None
        or url.password is not None
        or url.query
        or url.fragment
    ):
        raise ValueError("base_url must be an HTTP(S) URL without credentials, query or fragment")
    connection_type = (
        http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
    )
    port = url.port
    prefix = url.path.rstrip("/")
    asynchronous = mode == "event_driven"

    # Materialize all payloads, paths, headers and receipt slots outside timing.
    records = []
    requests = []
    operations = {entry["operation"] for entry in plan}
    if len(operations) > 1:
        raise ValueError("A plan must contain one operation")
    for entry in plan:
        operation = entry["operation"]
        if operation not in _OPERATIONS:
            raise ValueError(f"Unsupported operation: {operation}")
        UUID(entry["command_id"])
        if operation != "create":
            UUID(entry["delivery_id"])
        if operation == "create":
            path = "/deliveries-async" if asynchronous else "/deliveries"
            body = b""
        elif operation == "deliver":
            suffix = "deliver-async" if asynchronous else "deliver"
            path = f"/deliveries/{entry['delivery_id']}/{suffix}"
            body = b""
        else:
            path = "/returns-async" if asynchronous else "/returns"
            body = json.dumps({"delivery_id": entry["delivery_id"]}).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            "Idempotency-Key": entry["command_id"],
        }
        record = dict(entry)
        record.update(
            outcome="dropped",
            drop_reason="duration_elapsed",
            expected_http_status=202 if asynchronous else (200 if operation == "deliver" else 201),
            started_offset_s=None,
            latency_ms=None,
            http_status=None,
            response_body=None,
            response_truncated=False,
            transport_error=None,
        )
        records.append(record)
        requests.append((record, prefix + path, body, headers))

    # Prefer a recently used connection instead of rotating through dormant
    # sockets when the offered rate only needs a small fraction of the workers.
    idle: queue.LifoQueue[int] = queue.LifoQueue(maxsize=concurrency)
    inboxes: list[queue.Queue] = [queue.Queue(maxsize=1) for _ in range(concurrency)]
    ready = threading.Barrier(concurrency + 1)
    started = 0.0
    deadline = 0.0

    def worker(worker_id: int) -> None:
        connection = connection_type(url.hostname, port=port, timeout=timeout)
        last_finished_at = None
        idle.put(worker_id)
        ready.wait()
        try:
            while True:
                request = inboxes[worker_id].get()
                if request is None:
                    return
                record, path, body, headers = request
                request_started = time.monotonic()
                if request_started >= deadline:
                    record["drop_reason"] = "generator_lag"
                    idle.put(worker_id)
                    continue
                record["outcome"] = "sent"
                record["drop_reason"] = None
                record["started_offset_s"] = request_started - started
                try:
                    # Local retirement and any reconnect are part of this
                    # request's HTTP latency, just like its initial connection.
                    _expire_idle_connection(connection, last_finished_at, request_started)
                    connection.request("POST", path, body=body, headers=headers)
                    response = connection.getresponse()
                    record["http_status"] = response.status
                    payload = response.read(_RESPONSE_LIMIT + 1)
                    record["response_body"] = payload[:_RESPONSE_LIMIT].decode("utf-8", errors="replace")
                    record["response_truncated"] = len(payload) > _RESPONSE_LIMIT
                    if record["response_truncated"]:
                        # Unread response bytes cannot be reused as another response.
                        connection.close()
                except (OSError, http.client.HTTPException) as error:
                    record["transport_error"] = f"{type(error).__name__}: {error}"
                    # A subsequent, distinct request may reconnect.  This request
                    # is never retried, even when the server may have accepted it.
                    connection.close()
                finally:
                    last_finished_at = time.monotonic()
                    record["latency_ms"] = (last_finished_at - request_started) * 1000
                    idle.put(worker_id)
        finally:
            connection.close()

    workers = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(concurrency)]
    for thread in workers:
        thread.start()
    ready.wait()
    started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    deadline = started + duration
    interval = 1.0 / rate
    index = 0
    last_dispatch = None
    try:
        while index < len(requests):
            due = started + index * interval
            if due >= deadline:
                break
            # Maintain minimum dispatch spacing after scheduler delays, avoiding
            # a burst at the next absolute slot after a late submission.
            wake_at = max(due, last_dispatch + interval) if last_dispatch is not None else due
            now = time.monotonic()
            if now < min(wake_at, deadline):
                time.sleep(min(wake_at, deadline) - now)
                now = time.monotonic()
            if now >= deadline:
                break
            current_slot = int((now - started) * rate)
            while index < min(current_slot, len(requests)):
                records[index]["drop_reason"] = "generator_lag"
                index += 1
            if index >= len(requests) or started + index * interval >= deadline:
                break
            try:
                worker_id = idle.get_nowait()
            except queue.Empty:
                records[index]["drop_reason"] = "capacity"
            else:
                inboxes[worker_id].put_nowait(requests[index])
                last_dispatch = time.monotonic()
            index += 1
        # Retain the configured observation-free window even for a short plan.
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        for unsubmitted in range(index, len(records)):
            if unsubmitted * interval < duration:
                records[unsubmitted]["drop_reason"] = "generator_lag"
    finally:
        submission_finished = time.monotonic()
        for inbox in inboxes:
            inbox.put(None)
        for thread in workers:
            thread.join()
    finished = time.monotonic()
    finished_at = datetime.now(timezone.utc).isoformat()
    return {
        "mode": mode,
        "operation": next(iter(operations), None),
        "records": records,
        "execution": {
            "started_at": started_at,
            "finished_at": finished_at,
            "submission_duration_s": submission_finished - started,
            "http_drain_duration_s": finished - submission_finished,
            "actual_duration_s": finished - started,
            "duration_s": duration,
            "rate": rate,
            "concurrency": concurrency,
        },
    }
