from types import SimpleNamespace
from uuid import uuid4

import pytest

from benchmarks import reliability


@pytest.fixture
def command_args():
    return SimpleNamespace(base_url="http://benchmark-api", timeout=0.1, variant="event_driven", concurrency=2)


@pytest.fixture
def polling_clock(monkeypatch):
    clock = SimpleNamespace(elapsed=0.0)
    monkeypatch.setattr(reliability.time, "monotonic", lambda: clock.elapsed)

    def sleep(seconds):
        clock.elapsed += seconds

    monkeypatch.setattr(reliability.time, "sleep", sleep)
    return clock


def test_pending_commands_do_not_count_as_reliable_completion(monkeypatch, command_args, polling_clock):
    identifier = str(uuid4())
    monkeypatch.setattr(reliability, "request", lambda *args, **kwargs: (
        200, {"command_id": identifier, "status": "pending"},
    ))

    with pytest.raises(TimeoutError, match="did not reach a terminal state"):
        reliability.wait_command(command_args, identifier)

    assert polling_clock.elapsed == pytest.approx(command_args.timeout)


@pytest.mark.parametrize("state", ["completed", "failed"])
def test_polling_preserves_terminal_outcomes(monkeypatch, command_args, polling_clock, state):
    identifier = str(uuid4())
    record = {"command_id": identifier, "status": state, "result": None, "error": "failed"}
    replies = iter([(200, {"command_id": identifier, "status": "pending"}), (200, record)])
    monkeypatch.setattr(reliability, "request", lambda *args, **kwargs: next(replies))

    assert reliability.wait_command(command_args, identifier) == record
    assert polling_clock.elapsed > 0


def test_polling_rejects_another_commands_result(monkeypatch, command_args, polling_clock):
    monkeypatch.setattr(reliability, "request", lambda *args, **kwargs: (
        200, {"command_id": str(uuid4()), "status": "completed"},
    ))

    with pytest.raises(ValueError, match="Unable to inspect"):
        reliability.wait_command(command_args, str(uuid4()))


def test_event_reliability_rejects_development_database_before_http(monkeypatch, command_args):
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/return_flow")

    def unexpected_http(*args, **kwargs):
        pytest.fail("No HTTP mutation is allowed before checking the isolated database.")

    monkeypatch.setattr(reliability, "request", unexpected_http)
    with pytest.raises(ValueError, match="require database"):
        reliability.run(command_args)


def test_request_sends_idempotency_header_and_get_has_no_body(monkeypatch):
    identifier = str(uuid4())
    captured = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def read(self):
            return b"{}"

    def urlopen(request, **kwargs):
        captured.append(request)
        return Response()

    monkeypatch.setattr(reliability, "urlopen", urlopen)

    reliability.request("http://benchmark-api", "/deliveries-async", 1, command_id=identifier)
    reliability.request("http://benchmark-api", f"/commands/{identifier}", 1, method="GET")

    assert captured[0].get_method() == "POST"
    assert captured[0].get_header("Idempotency-key") == identifier
    assert captured[1].get_method() == "GET"
    assert captured[1].data is None


@pytest.mark.parametrize("variant, path, expected_status", [
    ("postgres_pool", "/deliveries", 201),
    ("event_driven", "/deliveries-async", 202),
])
def test_preflight_retries_one_key_through_selected_public_route(
    monkeypatch, command_args, variant, path, expected_status,
):
    from contextlib import nullcontext

    command_args.variant = variant
    calls = []
    delivery = {"id": str(uuid4()), "status": "pending"}
    monkeypatch.setattr(reliability, "benchmark_connection", lambda: nullcontext(
        SimpleNamespace(execute=lambda query: []),
    ))

    def request(base_url, requested_path, timeout, body, *, command_id):
        calls.append((requested_path, command_id))
        response = {"command_id": command_id, "status": "accepted"} if variant == "event_driven" else delivery
        return expected_status, response

    def stop_after_creation(*args, **kwargs):
        raise RuntimeError("creation submissions checked")

    monkeypatch.setattr(reliability, "request", request)
    monkeypatch.setattr(reliability, "wait_command", stop_after_creation)

    with pytest.raises(RuntimeError, match="creation submissions checked"):
        reliability.run(command_args)

    assert len(calls) == command_args.concurrency
    assert {called_path for called_path, _ in calls} == {path}
    assert len({key for _, key in calls}) == 1
    assert all(key for _, key in calls)
