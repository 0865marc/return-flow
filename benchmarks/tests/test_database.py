import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from benchmarks.database import (
    benchmark_connection, drain, main, read_receipts, reconcile, reconcile_commands,
    reconcile_flows, seed_id, verify, wait_idle,
)


@pytest.fixture
def valid_flow():
    delivery_id, return_id = str(uuid4()), str(uuid4())
    counts = {
        "workflows_attempted": 1,
        "workflows_completed": 1,
        "deliveries_created": 1,
        "deliveries_delivered": 1,
        "returns_created": 1,
        "unknown_writes": 0,
    }
    return {
        "size": 1,
        "scenario": "flow",
        "summary": {"metrics": {key: {"values": {"count": value}} for key, value in counts.items()}},
        "receipts": [
            {"delivery_id": delivery_id},
            {"delivered_id": delivery_id},
            {"return_id": return_id, "return_delivery_id": delivery_id},
        ],
        "deliveries": {seed_id(1): "delivered", delivery_id: "delivered"},
        "returns": {return_id: (delivery_id, "requested")},
    }


def test_confirmed_flow_is_verified_by_id_and_relationship(valid_flow):
    assert reconcile(**valid_flow)["status"] == "passed"


def test_matching_counts_do_not_hide_a_lost_confirmed_delivery(valid_flow):
    confirmed_id = valid_flow["receipts"][0]["delivery_id"]
    valid_flow["deliveries"].pop(confirmed_id)
    valid_flow["deliveries"][str(uuid4())] = "delivered"

    result = reconcile(**valid_flow)

    assert result["status"] == "failed"
    assert any("missing" in error for error in result["errors"])


def test_return_cannot_point_to_a_different_delivered_entity(valid_flow):
    return_id = valid_flow["receipts"][2]["return_id"]
    valid_flow["returns"][return_id] = (seed_id(1), "requested")

    result = reconcile(**valid_flow)

    assert result["status"] == "failed"
    assert any("incorrect data" in error for error in result["errors"])


def test_lost_receipts_cannot_make_confirmed_operations_disappear(valid_flow):
    valid_flow["receipts"].pop()

    result = reconcile(**valid_flow)

    assert result["status"] == "failed"
    assert any("confirmations" in error for error in result["errors"])


def test_reused_id_is_detected_even_if_confirmation_counts_match(valid_flow):
    valid_flow["receipts"].append(valid_flow["receipts"][0])
    valid_flow["summary"]["metrics"]["deliveries_created"]["values"]["count"] = 2

    result = reconcile(**valid_flow)

    assert result["status"] == "failed"
    assert any("duplicate confirmed IDs" in error for error in result["errors"])


@pytest.mark.parametrize("extra_rows", [0, 1])
def test_unknown_write_is_inconclusive_even_without_extra_rows(valid_flow, extra_rows):
    valid_flow["summary"]["metrics"]["unknown_writes"]["values"]["count"] = 1
    if extra_rows:
        valid_flow["deliveries"][str(uuid4())] = "pending"

    assert reconcile(**valid_flow)["status"] == "inconclusive"


def test_extra_rows_without_ambiguous_writes_are_a_failure(valid_flow):
    valid_flow["deliveries"][str(uuid4())] = "pending"

    assert reconcile(**valid_flow)["status"] == "failed"


def test_unknown_write_does_not_excuse_missing_confirmed_data(valid_flow):
    valid_flow["summary"]["metrics"]["unknown_writes"]["values"]["count"] = 10
    valid_flow["returns"].clear()

    assert reconcile(**valid_flow)["status"] == "failed"


def test_pending_delivery_cannot_have_a_return(valid_flow):
    valid_flow["deliveries"][valid_flow["receipts"][0]["delivery_id"]] = "pending"

    result = reconcile(**valid_flow)

    assert result["status"] == "failed"
    assert any("pending deliveries" in error for error in result["errors"])


def test_empty_load_is_not_considered_a_pass(valid_flow):
    for metric in valid_flow["summary"]["metrics"].values():
        metric["values"]["count"] = 0
    valid_flow["receipts"] = []
    valid_flow["returns"] = {}
    valid_flow["deliveries"] = {seed_id(1): "delivered"}

    assert reconcile(**valid_flow)["status"] == "failed"


def test_missing_summary_counters_are_not_silently_zero(valid_flow):
    valid_flow["summary"] = {"metrics": {}}

    with pytest.raises(ValueError, match="Missing counter"):
        reconcile(**valid_flow)


@pytest.mark.parametrize("log_format", ["raw", "json"])
def test_receipts_are_read_from_k6_logs(tmp_path, log_format):
    receipt = {"delivery_id": str(uuid4())}
    message = "RECEIPT " + json.dumps(receipt)
    line = message if log_format == "raw" else json.dumps({"msg": message, "level": "info"})
    path = tmp_path / "receipts.log"
    path.write_text("An unrelated k6 log line\n" + line + "\n")

    assert read_receipts(path) == [receipt]


def test_truncated_receipt_fails_instead_of_skipping_confirmation(tmp_path):
    path = tmp_path / "receipts.log"
    path.write_text('RECEIPT {"delivery_id":')

    with pytest.raises(ValueError, match="Invalid receipt"):
        read_receipts(path)


@pytest.mark.parametrize("url", ["postgresql://localhost/return_flow", "dbname=postgres", "host=localhost"])
def test_development_databases_are_rejected_before_connecting(monkeypatch, url):
    monkeypatch.setenv("DATABASE_URL", url)

    def unsafe_connection(*args, **kwargs):
        pytest.fail("The database safety guard must run before connecting.")

    monkeypatch.setattr("benchmarks.database.psycopg.connect", unsafe_connection)
    with pytest.raises(ValueError, match="require database"):
        benchmark_connection()


class ActivityConnection:
    def __init__(self, counts):
        self.counts = iter(counts)
        self.remaining = None
        self.autocommit = False
        self.queries = []
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def execute(self, query):
        assert self.autocommit, "Polling must not reuse a transaction's activity snapshot."
        self.queries.append(query)
        self.remaining = next(self.counts, self.remaining)
        return SimpleNamespace(fetchone=lambda: (self.remaining,))


@pytest.fixture
def idle_clock(monkeypatch):
    clock = SimpleNamespace(elapsed=0.0, sleeps=[])

    def sleep(seconds):
        clock.sleeps.append(seconds)
        clock.elapsed += seconds

    monkeypatch.setattr("benchmarks.database.time.monotonic", lambda: clock.elapsed)
    monkeypatch.setattr("benchmarks.database.time.sleep", sleep)
    return clock


def test_wait_idle_waits_for_all_other_database_clients(monkeypatch, idle_clock):
    connection = ActivityConnection([2, 1, 0])
    monkeypatch.setattr("benchmarks.database.benchmark_connection", lambda: connection)

    result = wait_idle(1)

    assert result == {"status": "passed", "remaining_connections": 0, "waited_seconds": 0.4}
    assert idle_clock.sleeps == [0.2, 0.2]
    assert connection.closed
    assert len(connection.queries) == 3
    for query in connection.queries:
        assert query.strip().startswith("SELECT count(*) FROM pg_stat_activity")
        assert "datname = current_database()" in query
        assert "backend_type = 'client backend'" in query
        assert "pid <> pg_backend_pid()" in query
        assert "state =" not in query  # Idle sessions must also disappear before reseeding.


def test_wait_idle_returns_immediately_when_no_clients_remain(monkeypatch, idle_clock):
    connection = ActivityConnection([0])
    monkeypatch.setattr("benchmarks.database.benchmark_connection", lambda: connection)

    assert wait_idle(1)["waited_seconds"] == 0
    assert idle_clock.sleeps == []
    assert connection.closed


def test_wait_idle_stops_at_deadline_without_terminating_clients(monkeypatch, idle_clock):
    connection = ActivityConnection([2])
    monkeypatch.setattr("benchmarks.database.benchmark_connection", lambda: connection)

    with pytest.raises(TimeoutError, match=r"2 client connection\(s\).*unsafe"):
        wait_idle(0.5)

    assert idle_clock.elapsed == pytest.approx(0.5)
    assert idle_clock.sleeps == pytest.approx([0.2, 0.2, 0.1])
    assert connection.closed
    assert all(query.strip().startswith("SELECT count(*)") for query in connection.queries)


def test_wait_idle_cli_records_timeout_as_failure(monkeypatch, idle_clock, tmp_path, capsys):
    connection = ActivityConnection([1])
    output = tmp_path / "idle.json"
    monkeypatch.setattr("benchmarks.database.benchmark_connection", lambda: connection)
    monkeypatch.setattr("sys.argv", ["database.py", "wait-idle", "--timeout", "0.1", "--output", str(output)])

    assert main() == 1
    result = json.loads(output.read_text())
    assert result["status"] == "failed"
    assert "1 client connection(s)" in result["errors"][0]
    assert json.loads(capsys.readouterr().out) == result


@pytest.mark.parametrize("timeout", ["0", "-1", "nan", "inf"])
def test_wait_idle_cli_rejects_non_positive_or_non_finite_timeout(monkeypatch, timeout):
    monkeypatch.setattr("sys.argv", ["database.py", "wait-idle", "--timeout", timeout])

    with pytest.raises(SystemExit) as error:
        main()

    assert error.value.code == 2


def test_wait_idle_rejects_development_database_before_connecting(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/return_flow")

    def unsafe_connection(*args, **kwargs):
        pytest.fail("The database safety guard must run before polling activity.")

    monkeypatch.setattr("benchmarks.database.psycopg.connect", unsafe_connection)

    with pytest.raises(ValueError, match="require database"):
        wait_idle(1)


@pytest.fixture
def event_flow(valid_flow):
    command_ids = [str(uuid4()) for _ in range(3)]
    delivery_id = valid_flow["receipts"][0]["delivery_id"]
    return_id = valid_flow["receipts"][2]["return_id"]
    valid_flow["receipts"] = [{
        "command_ids": command_ids, "delivery_id": delivery_id,
        "delivered_id": delivery_id, "return_id": return_id, "return_delivery_id": delivery_id,
    }]
    valid_flow["summary"]["metrics"]["commands_submitted"] = {"values": {"count": 3}}
    commands = {}
    for identifier, kind, result in zip(command_ids, (
        "create_delivery", "ensure_delivery_delivered", "request_return",
    ), (
        {"id": delivery_id, "status": "pending"},
        {"id": delivery_id, "status": "delivered"},
        {"id": return_id, "delivery_id": delivery_id, "status": "requested"},
    )):
        payload = {"command_id": identifier, "type": kind, "version": 1}
        if kind != "create_delivery":
            payload["delivery_id"] = delivery_id
        commands[identifier] = {
            "type": kind, "payload": payload, "status": "completed", "result": result, "error": None,
        }
    return valid_flow, commands


def audit_events(flow, commands):
    audit = reconcile(**flow)
    return reconcile_commands(audit, commands=commands, **{
        key: flow[key] for key in ("size", "receipts", "summary", "deliveries", "returns")
    })


def test_async_completed_commands_match_each_business_write(event_flow):
    flow, commands = event_flow

    result = audit_events(flow, commands)

    assert result["status"] == "passed"
    assert result["counts"]["commands_completed"] == 3
    assert result["counts"]["commands_pending"] == 0
    assert result["counts"]["commands_failed"] == 0


def test_async_database_completion_cannot_hide_a_client_timeout(event_flow):
    flow, commands = event_flow
    flow["summary"]["metrics"]["unknown_writes"]["values"]["count"] = 1

    assert audit_events(flow, commands)["status"] == "inconclusive"


@pytest.mark.parametrize("persisted", [False, True])
def test_async_unconfirmed_command_is_not_a_success(event_flow, persisted):
    flow, commands = event_flow
    identifier = str(uuid4())
    flow["receipts"].append({"command_ids": [identifier]})
    flow["summary"]["metrics"]["commands_submitted"]["values"]["count"] += 1
    flow["summary"]["metrics"]["unknown_writes"]["values"]["count"] = 1
    if persisted:
        commands[identifier] = {
            "type": "create_delivery", "status": "pending", "result": None, "error": None,
            "payload": {"command_id": identifier, "type": "create_delivery", "version": 1},
        }

    result = audit_events(flow, commands)

    assert result["status"] == "inconclusive"
    assert result["counts"]["commands_pending"] == int(persisted)
    assert result["counts"]["commands_missing"] == int(not persisted)


def test_async_unrecorded_business_write_fails_even_with_ambiguous_requests(event_flow):
    flow, commands = event_flow
    flow["summary"]["metrics"]["unknown_writes"]["values"]["count"] = 10
    flow["returns"][str(uuid4())] = (flow["receipts"][0]["delivery_id"], "requested")

    result = audit_events(flow, commands)

    assert result["status"] == "failed"
    assert any("partial write" in error for error in result["errors"])


def test_async_completed_result_must_match_its_payload(event_flow):
    flow, commands = event_flow
    commands[flow["receipts"][0]["command_ids"][2]]["payload"]["delivery_id"] = seed_id(1)

    result = audit_events(flow, commands)

    assert result["status"] == "failed"
    assert any("committed return" in error for error in result["errors"])


def test_async_command_identity_must_match_the_serialized_payload(event_flow):
    flow, commands = event_flow
    next(iter(commands.values()))["payload"]["command_id"] = str(uuid4())

    result = audit_events(flow, commands)

    assert result["status"] == "failed"
    assert any("payload identity" in error for error in result["errors"])


def test_async_failed_terminal_outcome_is_not_a_success(event_flow):
    flow, commands = event_flow
    identifier = str(uuid4())
    flow["receipts"].append({"command_ids": [identifier]})
    flow["summary"]["metrics"]["commands_submitted"]["values"]["count"] += 1
    commands[identifier] = {
        "type": "create_delivery", "status": "failed", "result": None, "error": "execution failed",
        "payload": {"command_id": identifier, "type": "create_delivery", "version": 1},
    }

    result = audit_events(flow, commands)

    assert result["status"] == "failed"
    assert result["counts"]["commands_failed"] == 1


def test_async_missing_submission_receipts_cannot_hide_incomplete_work(event_flow):
    flow, commands = event_flow
    flow["summary"]["metrics"]["commands_submitted"]["values"]["count"] += 1

    result = audit_events(flow, commands)

    assert result["status"] == "failed"
    assert any("submission" in error or "command ID receipts" in error for error in result["errors"])


def test_async_confirmation_must_belong_to_its_workflow(event_flow):
    flow, commands = event_flow
    identifiers = flow["receipts"][0].pop("command_ids")
    flow["receipts"].append({"command_ids": identifiers})

    result = audit_events(flow, commands)

    assert result["status"] == "failed"
    assert any("in its workflow" in error for error in result["errors"])


def test_receipts_preserve_all_submitted_command_ids(tmp_path):
    receipt = {"command_ids": [str(uuid4()), str(uuid4())], "delivery_id": str(uuid4())}
    path = tmp_path / "receipts.log"
    path.write_text("RECEIPT " + json.dumps(receipt))

    assert read_receipts(path) == [receipt]


@pytest.mark.parametrize("value", [str(uuid4()), ["invalid"], {"unexpected": str(uuid4())}])
def test_invalid_command_receipts_are_rejected(tmp_path, value):
    path = tmp_path / "receipts.log"
    path.write_text("RECEIPT " + json.dumps({"command_ids": value}))

    with pytest.raises(ValueError, match="Invalid receipt"):
        read_receipts(path)


@pytest.fixture
def measured_event_flow(event_flow):
    flow, commands = event_flow
    receipt = flow["receipts"][0]
    receipt.update(
        flow_id=str(uuid4()), started_at_ms=1_000_100, finished_at_ms=1_000_500,
        accepted_command_ids=list(receipt["command_ids"]),
    )
    receipt.pop("return_id")
    receipt.pop("return_delivery_id")
    metrics = flow["summary"]["metrics"]
    metrics["workflows_completed"]["values"]["count"] = 0
    metrics["returns_created"]["values"]["count"] = 0
    metrics["technical_errors"] = {"values": {"count": 0}}
    flow["summary"]["benchmark"] = {
        "version": 4, "scenario": "flow", "started_at_ms": 1_000_000,
        "offered_duration_ms": 1_000,
    }
    for command, (created, completed) in zip(commands.values(), ((110, 200), (220, 400), (420, 2500))):
        command["created_at"] = datetime.fromtimestamp((1_000_000 + created) / 1000, timezone.utc)
        command["completed_at"] = datetime.fromtimestamp((1_000_000 + completed) / 1000, timezone.utc)
    return {
        **flow, "commands": commands, "variant": "event_driven",
        "return_commit_times": {
            identifier: datetime.fromtimestamp(1002.5, timezone.utc) for identifier in flow["returns"]
        },
    }


def test_e2e_counts_late_committed_return_without_final_polling(measured_event_flow):
    result = reconcile_flows(**measured_event_flow)

    assert result["status"] == "passed"
    assert result["counts"]["returns_created"] == 0  # Preserve generator observations.
    assert result["counts"]["workflows_completed"] == 0
    assert result["counts"]["commands_completed"] == 3
    assert result["flows"]["completed"] == 1
    assert result["flows"]["completion_p95_ms"] == 2400
    assert result["flows"]["elapsed_seconds"] == 2.5
    assert result["flows"]["completed_per_s"] == 0.4
    assert result["flows"]["records"][0]["commands"][-1] == {
        "command_id": measured_event_flow["receipts"][0]["command_ids"][-1],
        "type": "request_return", "status": "completed", "accepted": True,
        "created_at_ms": 1_000_420, "completed_at_ms": 1_002_500, "duration_ms": 2080,
    }


def test_e2e_late_completion_after_503_is_counted_but_run_still_fails(measured_event_flow):
    measured_event_flow["receipts"][0]["accepted_command_ids"].pop()
    metrics = measured_event_flow["summary"]["metrics"]
    metrics["technical_errors"]["values"]["count"] = 1
    metrics["unknown_writes"]["values"]["count"] = 1

    result = reconcile_flows(**measured_event_flow)

    assert result["flows"]["completed"] == 1
    assert result["status"] == "failed"
    assert any("Client errors" in error for error in result["errors"])


@pytest.mark.parametrize("outcome", ["pending", "missing", "failed"])
def test_e2e_distinguishes_unresolved_commands_from_terminal_failure(measured_event_flow, outcome):
    command_id = measured_event_flow["receipts"][0]["command_ids"][-1]
    measured_event_flow["returns"] = {}
    if outcome == "missing":
        measured_event_flow["commands"].pop(command_id)
    else:
        command = measured_event_flow["commands"][command_id]
        command.update(status=outcome, result=None, error="execution failed" if outcome == "failed" else None)
        if outcome == "pending":
            command["completed_at"] = None

    result = reconcile_flows(**measured_event_flow)

    assert result["flows"]["completed"] == 0
    assert result["flows"][outcome] == 1
    assert result["flows"]["completion_p95_ms"] is None
    assert result["status"] == ("failed" if outcome == "failed" else "inconclusive")
    assert any("not proven message loss" in note for note in result["notes"])
    command_record = result["flows"]["records"][0]["commands"][-1]
    assert command_record["command_id"] == command_id
    assert command_record["status"] == outcome
    if outcome != "failed":
        assert command_record["duration_ms"] is None
        assert command_record["completed_at_ms"] is None
    if outcome == "missing":
        assert command_record["created_at_ms"] is None


def test_e2e_partial_flow_is_separate_from_pending_or_missing(measured_event_flow):
    receipt = measured_event_flow["receipts"][0]
    final_id = receipt["command_ids"].pop()
    receipt["accepted_command_ids"].pop()
    measured_event_flow["commands"].pop(final_id)
    measured_event_flow["returns"] = {}
    measured_event_flow["summary"]["metrics"]["commands_submitted"]["values"]["count"] = 2

    result = reconcile_flows(**measured_event_flow)

    assert result["flows"]["incomplete"] == 1
    assert result["flows"]["pending"] == result["flows"]["missing"] == 0
    assert result["status"] == "inconclusive"


def test_e2e_checks_entire_chain_even_when_every_command_matches_its_own_row(measured_event_flow):
    command_id = measured_event_flow["receipts"][0]["command_ids"][-1]
    command = measured_event_flow["commands"][command_id]
    command["payload"]["delivery_id"] = seed_id(1)
    command["result"]["delivery_id"] = seed_id(1)
    measured_event_flow["returns"][command["result"]["id"]] = (seed_id(1), "requested")

    result = reconcile_flows(**measured_event_flow)

    assert result["status"] == "failed"
    assert result["flows"]["completed"] == 0
    assert result["flows"]["failed"] == 1
    assert any("another flow" in error for error in result["errors"])


def test_e2e_receipt_counter_mismatch_cannot_be_hidden_by_database_completion(measured_event_flow):
    measured_event_flow["summary"]["metrics"]["workflows_attempted"]["values"]["count"] = 2

    result = reconcile_flows(**measured_event_flow)

    assert result["status"] == "failed"
    assert result["flows"]["attempted"] == 2
    assert result["flows"]["completed"] == 1
    assert result["flows"]["unobserved"] == 1
    assert any("attempted flows" in error for error in result["errors"])


def test_e2e_preserves_real_confirmation_counter_validation(measured_event_flow):
    measured_event_flow["summary"]["metrics"]["returns_created"]["values"]["count"] = 1

    result = reconcile_flows(**measured_event_flow)

    assert result["status"] == "failed"
    assert any("confirmations do not match" in error for error in result["errors"])


@pytest.mark.parametrize("timestamp", [None, "not a date", datetime(2026, 1, 1)])
def test_e2e_missing_or_invalid_completion_time_is_not_a_measured_success(measured_event_flow, timestamp):
    command_id = measured_event_flow["receipts"][0]["command_ids"][-1]
    measured_event_flow["commands"][command_id]["completed_at"] = timestamp

    result = reconcile_flows(**measured_event_flow)

    assert result["status"] == "failed"
    assert result["flows"]["completed"] == 0
    assert result["flows"]["completion_p95_ms"] is None


def test_e2e_invalid_business_row_is_not_counted_as_completed(measured_event_flow):
    measured_event_flow["returns"] = {}

    result = reconcile_flows(**measured_event_flow)

    assert result["status"] == "failed"
    assert result["flows"]["completed"] == 0


def test_e2e_delayed_http_response_extends_window_without_coordinator_time(measured_event_flow):
    measured_event_flow["receipts"][0]["finished_at_ms"] = 1_004_000

    result = reconcile_flows(**measured_event_flow)

    assert result["flows"]["elapsed_seconds"] == 4
    assert result["flows"]["completed_per_s"] == 0.25
    assert result["flows"]["completion_p95_ms"] == 2400


def test_e2e_direct_flow_uses_return_commit_time_and_verifies_committed_rows(valid_flow):
    receipt = {key: value for part in valid_flow["receipts"] for key, value in part.items()}
    receipt.update(flow_id=str(uuid4()), started_at_ms=100_000, finished_at_ms=100_400)
    valid_flow["receipts"] = [receipt]
    valid_flow["summary"]["benchmark"] = {
        "version": 4, "started_at_ms": 100_000, "offered_duration_ms": 1000,
    }

    result = reconcile_flows(
        **valid_flow, variant="postgres_pool", commands={},
        return_commit_times={receipt["return_id"]: datetime.fromtimestamp(100.3, timezone.utc)},
    )

    assert result["status"] == "passed"
    assert result["flows"]["completed"] == 1
    assert result["flows"]["completion_p95_ms"] == 300
    assert result["flows"]["elapsed_seconds"] == 1


@pytest.fixture
def ambiguous_direct_flow(valid_flow):
    receipt = {key: value for part in valid_flow["receipts"] for key, value in part.items()}
    return_id = receipt.pop("return_id")
    receipt.pop("return_delivery_id")
    receipt.update(
        flow_id=str(uuid4()), started_at_ms=100_000, finished_at_ms=101_000,
        return_attempted=True,
    )
    valid_flow["receipts"] = [receipt]
    metrics = valid_flow["summary"]["metrics"]
    metrics["workflows_completed"]["values"]["count"] = 0
    metrics["returns_created"]["values"]["count"] = 0
    metrics["unknown_writes"]["values"]["count"] = 1
    metrics["technical_errors"] = {"values": {"count": 1}}
    valid_flow["summary"]["benchmark"] = {
        "version": 4, "started_at_ms": 100_000, "offered_duration_ms": 1000,
    }
    return {
        **valid_flow, "variant": "postgres_pool", "commands": {},
        "return_commit_times": {return_id: datetime.fromtimestamp(100.3, timezone.utc)},
    }


def test_e2e_direct_ambiguous_final_post_can_complete_without_hiding_http_error(ambiguous_direct_flow):
    result = reconcile_flows(**ambiguous_direct_flow)

    assert result["flows"]["completed"] == 1
    assert result["flows"]["recovered"] == 1
    assert result["flows"]["records"][0]["recovered"] is True
    assert result["flows"]["completion_p95_ms"] == 300
    assert result["counts"]["workflows_completed"] == 0
    assert result["counts"]["returns_created"] == 0
    assert result["status"] == "failed"
    assert any("Client errors" in error for error in result["errors"])


def test_e2e_direct_duplicate_returns_are_not_a_recovered_success(ambiguous_direct_flow):
    delivery_id = ambiguous_direct_flow["receipts"][0]["delivery_id"]
    ambiguous_direct_flow["returns"][str(uuid4())] = (delivery_id, "requested")

    result = reconcile_flows(**ambiguous_direct_flow)

    assert result["flows"]["completed"] == 0
    assert result["flows"]["recovered"] == 0
    assert result["status"] == "failed"
    assert any("multiple returns" in error for error in result["errors"])


@pytest.mark.parametrize("missing_evidence", ["return_attempted", "delivered_id", "unknown_writes"])
def test_e2e_direct_recovery_requires_final_attempt_and_ambiguous_write_evidence(ambiguous_direct_flow, missing_evidence):
    if missing_evidence == "unknown_writes":
        ambiguous_direct_flow["summary"]["metrics"]["unknown_writes"]["values"]["count"] = 0
    else:
        ambiguous_direct_flow["receipts"][0].pop(missing_evidence)

    result = reconcile_flows(**ambiguous_direct_flow)

    assert result["flows"]["completed"] == 0
    assert result["flows"]["recovered"] == 0


@pytest.mark.parametrize("variant_fixture", ["measured_event_flow", "ambiguous_direct_flow"])
def test_e2e_commit_timestamp_is_required_for_both_variants(request, variant_fixture):
    flow = request.getfixturevalue(variant_fixture)
    flow["return_commit_times"] = {}

    result = reconcile_flows(**flow)

    assert result["flows"]["completed"] == 0
    assert result["flows"]["completion_p95_ms"] is None
    assert result["status"] == "failed"
    assert any("return commit timestamp" in error for error in result["errors"])


def test_e2e_queued_finish_uses_commit_not_precommit_command_sample(measured_event_flow):
    return_id = next(iter(measured_event_flow["returns"]))
    measured_event_flow["return_commit_times"][return_id] = datetime.fromtimestamp(1002.75, timezone.utc)

    result = reconcile_flows(**measured_event_flow)

    assert result["status"] == "passed"
    assert result["flows"]["completion_p95_ms"] == 2650
    assert result["flows"]["elapsed_seconds"] == 2.75
    record = result["flows"]["records"][0]
    assert record["completed_at_ms"] == 1_002_750
    assert record["commands"][-1]["completed_at_ms"] == 1_002_500


@pytest.mark.parametrize("tracking", ["on", "off"])
def test_verify_requires_tracking_and_reads_return_transaction_commits(
    ambiguous_direct_flow, monkeypatch, tmp_path, tracking,
):
    flow = ambiguous_direct_flow
    queries = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, query):
            queries.append(query)
            if query.startswith("SET TRANSACTION"):
                return []
            if query == "SHOW track_commit_timestamp":
                return SimpleNamespace(fetchone=lambda: (tracking,))
            if query == "SELECT id, status FROM deliveries":
                return list(flow["deliveries"].items())
            assert query == "SELECT id, delivery_id, status, pg_xact_commit_timestamp(xmin) FROM returns"
            return [
                (identifier, delivery_id, status, flow["return_commit_times"][identifier])
                for identifier, (delivery_id, status) in flow["returns"].items()
            ]

    monkeypatch.setattr("benchmarks.database.benchmark_connection", Connection)
    summary_path, receipts_path = tmp_path / "summary.json", tmp_path / "receipts.log"
    summary_path.write_text(json.dumps(flow["summary"]))
    receipts_path.write_text("RECEIPT " + json.dumps(flow["receipts"][0]))
    args = SimpleNamespace(
        summary=summary_path, receipts=receipts_path, size=1, scenario="flow", variant="postgres_pool",
    )
    if tracking == "off":
        with pytest.raises(ValueError, match="track_commit_timestamp=on"):
            verify(args)
        assert len(queries) == 2
    else:
        result = verify(args)
        assert result["flows"]["completed"] == 1
        assert result["flows"]["completion_p95_ms"] == 300


def test_receipts_preserve_flow_clock_and_accepted_command_ids(tmp_path):
    command_id = str(uuid4())
    receipt = {
        "flow_id": str(uuid4()), "started_at_ms": 1000, "finished_at_ms": 1200,
        "command_ids": [command_id], "accepted_command_ids": [command_id],
    }
    path = tmp_path / "receipts.log"
    path.write_text("RECEIPT " + json.dumps(receipt))

    assert read_receipts(path) == [receipt]


@pytest.mark.parametrize("value", [True, False, 0, 1, "true", None])
def test_return_attempt_receipt_requires_a_boolean(tmp_path, value):
    receipt = {
        "flow_id": str(uuid4()), "started_at_ms": 1000, "finished_at_ms": 1200,
        "return_attempted": value,
    }
    path = tmp_path / "receipts.log"
    path.write_text("RECEIPT " + json.dumps(receipt))
    if isinstance(value, bool):
        assert read_receipts(path) == [receipt]
    else:
        with pytest.raises(ValueError, match="return_attempted must be a boolean"):
            read_receipts(path)


@pytest.mark.parametrize("timestamp", [None, True, "123", -1, float("nan"), float("inf")])
def test_receipt_clock_rejects_invalid_timestamps(tmp_path, timestamp):
    path = tmp_path / "receipts.log"
    path.write_text("RECEIPT " + json.dumps({
        "flow_id": str(uuid4()), "started_at_ms": timestamp, "finished_at_ms": 1000,
    }))

    with pytest.raises(ValueError, match="Invalid receipt"):
        read_receipts(path)


def test_drain_polls_pending_work_only_after_generator_ends(monkeypatch, idle_clock):
    connection = ActivityConnection([3, 2, 0])
    monkeypatch.setattr("benchmarks.database.benchmark_connection", lambda: connection)

    assert drain(5) == {"status": "passed", "pending_commands": 0, "waited_seconds": 2.0}
    assert idle_clock.sleeps == [1, 1]
    assert connection.closed
    assert all(query == "SELECT count(*) FROM commands WHERE status = 'pending'" for query in connection.queries)


def test_drain_timeout_records_unresolved_work_without_raising(monkeypatch, idle_clock):
    connection = ActivityConnection([2])
    monkeypatch.setattr("benchmarks.database.benchmark_connection", lambda: connection)

    assert drain(1.5) == {"status": "timed_out", "pending_commands": 2, "waited_seconds": 1.5}
    assert idle_clock.sleeps == [1, 0.5]


def test_drain_cli_saves_timeout_result_for_audit(monkeypatch, idle_clock, tmp_path, capsys):
    connection = ActivityConnection([1])
    output = tmp_path / "drain.json"
    monkeypatch.setattr("benchmarks.database.benchmark_connection", lambda: connection)
    monkeypatch.setattr("sys.argv", ["database.py", "drain", "--timeout", "0.1", "--output", str(output)])

    assert main() == 1
    result = json.loads(output.read_text())
    assert result == {"status": "timed_out", "pending_commands": 1, "waited_seconds": 0.1}
    assert json.loads(capsys.readouterr().out) == result


@pytest.fixture
def measured_direct_command_flow(measured_event_flow):
    flow = measured_event_flow
    flow["variant"] = "postgres_pool"
    flow["summary"]["benchmark"]["version"] = 5
    receipt = flow["receipts"][0]
    last = flow["commands"][receipt["command_ids"][-1]]["result"]
    receipt.update(return_id=last["id"], return_delivery_id=last["delivery_id"], finished_at_ms=1_002_600)
    metrics = flow["summary"]["metrics"]
    for name in ("workflows_completed", "workflows_submitted", "returns_created"):
        metrics[name] = {"values": {"count": 1}}
    return flow


def test_v5_direct_audits_same_commands_and_return_commit_as_queued(measured_direct_command_flow):
    result = reconcile_flows(**measured_direct_command_flow)

    assert result["status"] == "passed"
    assert result["variant"] == "postgres_pool"
    assert result["counts"]["stored_commands"] == 3
    assert result["flows"]["completed"] == 1
    assert result["flows"]["completion_p95_ms"] == 2400
    assert len(result["flows"]["records"][0]["commands"]) == 3


def test_v5_direct_lost_final_response_recovers_by_command_without_hiding_error(measured_direct_command_flow):
    flow = measured_direct_command_flow
    receipt = flow["receipts"][0]
    receipt.pop("return_id")
    receipt.pop("return_delivery_id")
    receipt["accepted_command_ids"].pop()
    metrics = flow["summary"]["metrics"]
    for name, count in (("workflows_completed", 0), ("workflows_submitted", 0),
                        ("returns_created", 0), ("unknown_writes", 1), ("technical_errors", 1)):
        metrics[name] = {"values": {"count": count}}

    result = reconcile_flows(**flow)

    assert result["flows"]["completed"] == 1
    assert result["flows"]["recovered"] == 1
    assert result["flows"]["records"][0]["commands"][-1]["accepted"] is False
    assert result["counts"]["returns_created"] == 0
    assert result["status"] == "failed"
    assert any("Client errors" in error for error in result["errors"])


def test_v5_direct_lost_creation_response_is_attributed_but_not_a_complete_flow(measured_direct_command_flow):
    flow = measured_direct_command_flow
    receipt = flow["receipts"][0]
    first_id = receipt["command_ids"][0]
    delivery_id = receipt.pop("delivery_id")
    for field in ("delivered_id", "return_id", "return_delivery_id"):
        receipt.pop(field)
    receipt.update(command_ids=[first_id], accepted_command_ids=[])
    flow["commands"] = {first_id: flow["commands"][first_id]}
    flow["deliveries"][delivery_id] = "pending"
    flow["returns"] = {}
    metrics = flow["summary"]["metrics"]
    for name in ("workflows_completed", "workflows_submitted", "deliveries_created",
                 "deliveries_delivered", "returns_created", "commands_completed"):
        metrics[name] = {"values": {"count": 0}}
    for name in ("unknown_writes", "technical_errors", "commands_submitted"):
        metrics[name] = {"values": {"count": 1}}

    result = reconcile_flows(**flow)

    assert result["flows"]["completed"] == 0
    assert result["flows"]["incomplete"] == 1
    assert result["counts"]["extra_deliveries"] == 0
    assert result["counts"]["commands_completed"] == 1
    assert result["flows"]["records"][0]["commands"][0]["command_id"] == first_id


def test_v5_direct_success_without_persisted_commands_fails_audit(measured_direct_command_flow):
    measured_direct_command_flow["commands"] = {}

    result = reconcile_flows(**measured_direct_command_flow)

    assert result["status"] == "failed"
    assert result["flows"]["completed"] == 0
    assert result["counts"]["commands_missing"] == 3


def test_v5_direct_verify_reads_commands_and_preserves_snapshot_audit(
    measured_direct_command_flow, monkeypatch, tmp_path,
):
    flow = measured_direct_command_flow
    queries = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, query):
            queries.append(query)
            if query.startswith("SET TRANSACTION"):
                return []
            if query == "SHOW track_commit_timestamp":
                return SimpleNamespace(fetchone=lambda: ("on",))
            if query == "SELECT id, status FROM deliveries":
                return list(flow["deliveries"].items())
            if "FROM returns" in query:
                return [
                    (identifier, parent, status, flow["return_commit_times"][identifier])
                    for identifier, (parent, status) in flow["returns"].items()
                ]
            assert query.endswith("FROM commands")
            return [
                (identifier, *(command[field] for field in (
                    "type", "payload", "status", "result", "error", "created_at", "completed_at",
                )))
                for identifier, command in flow["commands"].items()
            ]

    monkeypatch.setattr("benchmarks.database.benchmark_connection", Connection)
    summary_path, receipts_path = tmp_path / "summary.json", tmp_path / "receipts.log"
    summary_path.write_text(json.dumps(flow["summary"]))
    receipts_path.write_text("RECEIPT " + json.dumps(flow["receipts"][0]))

    result = verify(SimpleNamespace(
        summary=summary_path, receipts=receipts_path, size=flow["size"],
        scenario="flow", variant="postgres_pool",
    ))

    assert result["status"] == "passed"
    assert result["counts"]["stored_commands"] == 3
    assert queries[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert queries[-1].endswith("FROM commands")
