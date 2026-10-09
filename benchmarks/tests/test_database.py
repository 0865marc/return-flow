import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from benchmarks.database import benchmark_connection, main, read_receipts, reconcile, seed_id, wait_idle


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
