import json
from uuid import uuid4

import pytest

from benchmarks.database import benchmark_connection, read_receipts, reconcile, seed_id


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
