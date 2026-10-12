from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time
import unittest
from unittest.mock import Mock, patch
from uuid import UUID

from benchmarks.workload import _expire_idle_connection, execute, make_plan


RUN_ID = "a0c15d28-a28e-45d0-968e-ab9d156f8c09"


@contextmanager
def backend(*, status=202, delay=0.0, response=None):
    calls = []
    lock = threading.Lock()
    response = response if response is not None else b'{"status":"accepted"}'

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            with lock:
                calls.append({
                    "method": "POST", "path": self.path, "body": body,
                    "command_id": self.headers.get("Idempotency-Key"),
                    "client_port": self.client_address[1], "at": time.monotonic(),
                })
            if delay:
                time.sleep(delay)
            self.send_response(status)
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            try:
                self.wfile.write(response)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_GET(self):
            with lock:
                calls.append({"method": "GET", "path": self.path})
            self.send_response(500)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


class PlanTests(unittest.TestCase):
    def test_plan_has_reproducible_distinct_identities_without_backend_access(self):
        first = make_plan(RUN_ID, "deliver", 3)
        self.assertEqual(first, make_plan(RUN_ID, "deliver", 3))
        self.assertEqual([entry["index"] for entry in first], [0, 1, 2])
        self.assertEqual(len({entry["command_id"] for entry in first}), 3)
        self.assertEqual(len({entry["delivery_id"] for entry in first}), 3)
        self.assertTrue(all(UUID(entry["command_id"]).version == 5 for entry in first))
        self.assertNotEqual(first[0]["command_id"], make_plan(RUN_ID, "return", 1)[0]["command_id"])
        self.assertNotIn("delivery_id", make_plan(RUN_ID, "create", 1)[0])

    def test_invalid_configuration_fails_before_any_requests(self):
        with backend() as (url, calls):
            for kwargs in ({"rate": 0}, {"duration": float("nan")}, {"concurrency": 0}, {"timeout": -1}):
                options = dict(rate=10, duration=0.1, concurrency=1, timeout=0.2)
                options.update(kwargs)
                with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                    execute(url, "event_driven", make_plan(RUN_ID, "create", 1), **options)
            self.assertEqual(calls, [])


class ExecutionTests(unittest.TestCase):
    def test_idle_connection_expires_before_server_keepalive_without_retry(self):
        # Clock values are supplied directly: no real multi-second wait and no
        # request/retry is needed to discover that this connection is stale.
        connection = Mock(spec=["close", "request", "getresponse"])
        for last_finished_at, now, expected_closes in (
            (None, 100.0, 0),
            (100.0, 103.999, 0),
            (100.0, 104.0, 1),
            (100.0, 110.0, 1),
        ):
            with self.subTest(last_finished_at=last_finished_at, now=now):
                connection.reset_mock()
                _expire_idle_connection(connection, last_finished_at, now)
                self.assertEqual(connection.close.call_count, expected_closes)
                connection.request.assert_not_called()
                connection.getresponse.assert_not_called()

    def test_idle_workers_reuse_the_warm_connection(self):
        with backend() as (url, calls):
            result = execute(url, "event_driven", make_plan(RUN_ID, "create", 3),
                             rate=20, duration=0.15, concurrency=4, timeout=0.2)
        self.assertEqual(len(calls), 3)
        self.assertEqual(len({call["client_port"] for call in calls}), 1)
        self.assertTrue(all(entry["transport_error"] is None for entry in result["records"]))

    def test_async_acceptance_stays_write_only_and_reuses_connection(self):
        plan = make_plan(RUN_ID, "create", 3)
        with backend() as (url, calls):
            result = execute(url, "event_driven", plan, rate=20, duration=0.15, concurrency=1, timeout=0.2)
        self.assertEqual([call["method"] for call in calls], ["POST"] * 3)
        self.assertEqual([call["path"] for call in calls], ["/deliveries-async"] * 3)
        self.assertEqual(len({call["client_port"] for call in calls}), 1)
        self.assertEqual([call["command_id"] for call in calls], [entry["command_id"] for entry in plan])
        self.assertTrue(all(entry["outcome"] == "sent" for entry in result["records"]))
        self.assertTrue(all(entry["http_status"] == 202 for entry in result["records"]))
        self.assertTrue(all(entry["response_body"] == '{"status":"accepted"}' for entry in result["records"]))
        self.assertNotIn("outcome", plan[0])
        self.assertGreaterEqual(result["execution"]["submission_duration_s"], 0.15)
        self.assertEqual(result["mode"], "event_driven")
        self.assertEqual(result["operation"], "create")

    def test_every_operation_uses_the_correct_post_contract(self):
        for mode in ("postgres_pool", "event_driven"):
            for operation in ("create", "deliver", "return"):
                with self.subTest(mode=mode, operation=operation), backend() as (url, calls):
                    plan = make_plan(RUN_ID, operation, 1)
                    result = execute(url + "/api/", mode, plan, rate=10, duration=0.03, concurrency=1, timeout=0.2)
                    async_suffix = "-async" if mode == "event_driven" else ""
                    expected_path = {
                        "create": f"/api/deliveries{async_suffix}",
                        "deliver": f"/api/deliveries/{plan[0].get('delivery_id')}/deliver{async_suffix}",
                        "return": f"/api/returns{async_suffix}",
                    }[operation]
                    self.assertEqual(calls[0]["path"], expected_path)
                    self.assertEqual(calls[0]["method"], "POST")
                    if operation == "return":
                        self.assertEqual(json.loads(calls[0]["body"]), {"delivery_id": plan[0]["delivery_id"]})
                    expected_status = 202 if mode == "event_driven" else (200 if operation == "deliver" else 201)
                    self.assertEqual(result["records"][0]["expected_http_status"], expected_status)

    def test_busy_workers_drop_requests_without_an_unbounded_queue(self):
        with backend(delay=0.14) as (url, calls):
            result = execute(url, "event_driven", make_plan(RUN_ID, "create", 8),
                             rate=100, duration=0.08, concurrency=1, timeout=0.5)
        self.assertEqual(len(calls), 1)
        sent = [entry for entry in result["records"] if entry["outcome"] == "sent"]
        dropped = [entry for entry in result["records"] if entry["outcome"] == "dropped"]
        self.assertEqual(len(sent), 1)
        self.assertEqual(len(dropped), 7)
        self.assertTrue(any(entry["drop_reason"] == "capacity" for entry in dropped))
        self.assertTrue(all(entry["http_status"] is None for entry in dropped))
        self.assertGreater(result["execution"]["http_drain_duration_s"], 0.02)
        self.assertEqual(len({call["command_id"] for call in calls}), 1)

    def test_timeouts_never_retry_or_observe_the_backend(self):
        with backend(delay=0.10) as (url, calls):
            result = execute(url, "event_driven", make_plan(RUN_ID, "create", 1),
                             rate=10, duration=0.02, concurrency=1, timeout=0.03)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["method"], "POST")
        record = result["records"][0]
        self.assertEqual(record["outcome"], "sent")
        self.assertIn("TimeoutError", record["transport_error"])
        self.assertIsNone(record["http_status"])
        self.assertGreater(record["latency_ms"], 20)

    def test_http_failure_is_a_receipt_not_a_retry(self):
        with backend(status=503) as (url, calls):
            result = execute(url, "event_driven", make_plan(RUN_ID, "create", 1),
                             rate=10, duration=0.03, concurrency=1, timeout=0.2)
        self.assertEqual(len(calls), 1)
        self.assertEqual(result["records"][0]["http_status"], 503)
        self.assertIsNone(result["records"][0]["transport_error"])

    def test_delayed_generator_drops_missed_slots_without_catchup(self):
        original_sleep = time.sleep
        delayed_once = False

        def delayed_sleep(seconds):
            nonlocal delayed_once
            if not delayed_once:
                delayed_once = True
                original_sleep(seconds + 0.04)
            else:
                original_sleep(seconds)

        with backend() as (url, calls), patch("benchmarks.workload.time.sleep", side_effect=delayed_sleep):
            result = execute(url, "event_driven", make_plan(RUN_ID, "create", 9),
                             rate=100, duration=0.09, concurrency=4, timeout=0.2)
        self.assertTrue(any(entry["drop_reason"] == "generator_lag" for entry in result["records"]))
        self.assertLess(len(calls), 9)
        starts = sorted(entry["started_offset_s"] for entry in result["records"] if entry["outcome"] == "sent")
        self.assertTrue(all(later - earlier > 0.007 for earlier, later in zip(starts, starts[1:])))

    def test_response_capture_is_bounded_and_window_drops_excess_plan(self):
        with backend(response=b"x" * 6000) as (url, calls):
            result = execute(url, "event_driven", make_plan(RUN_ID, "create", 2),
                             rate=10, duration=0.03, concurrency=1, timeout=0.2)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(result["records"][0]["response_body"]), 4096)
        self.assertTrue(result["records"][0]["response_truncated"])
        self.assertEqual(result["records"][1]["drop_reason"], "duration_elapsed")


if __name__ == "__main__":
    unittest.main()
