from copy import deepcopy
import json
import math
import unittest
from uuid import UUID

from benchmarks.analytics import build_report


COMMAND_ID = str(UUID(int=1))
DELIVERY_ID = str(UUID(int=2))
RETURN_ID = str(UUID(int=3))


def receipt(**changes):
    record = {
        "index": 0,
        "operation": "create",
        "command_id": COMMAND_ID,
        "outcome": "sent",
        "expected_http_status": 202,
        "http_status": 202,
        "response_body": json.dumps({"command_id": COMMAND_ID, "status": "accepted"}),
        "latency_ms": 12,
        "transport_error": None,
    }
    record.update(changes)
    return record


def persisted(**changes):
    row = {
        "command_id": COMMAND_ID,
        "type": "create_delivery",
        "status": "completed",
        "result": {"id": DELIVERY_ID, "status": "pending"},
        "error": None,
        "error_code": None,
        "created_at": "2026-10-12T12:00:00+00:00",
        "completed_at": "2026-10-12T12:00:00.100000+00:00",
        "business_valid": True,
    }
    row.update(changes)
    return row


def report(records=None, rows=None, mode="event_driven", **lifecycle):
    return build_report(
        {
            "mode": mode,
            "operation": "create",
            "records": [receipt()] if records is None else records,
            "execution": {"actual_duration_s": 2},
        },
        [persisted()] if rows is None else rows,
        {"processing_duration_s": 10, **lifecycle},
    )


class AnalyticsTests(unittest.TestCase):
    def test_202_is_not_completion(self):
        result = report(rows=[persisted(status="pending", result=None, completed_at=None)])
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["counts"]["http_success"], 1)
        self.assertEqual(result["counts"]["pending"], 1)
        self.assertEqual(result["counts"]["completed"], 0)
        self.assertEqual(result["latency_ms"]["db_completed"]["count"], 0)
        self.assertIsNone(result["latency_ms"]["db_completed"]["p95"])

    def test_timeout_can_still_complete(self):
        result = report(records=[receipt(http_status=None, response_body=None, transport_error="TimeoutError")])
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["counts"]["http_transport_errors"], 1)
        self.assertEqual(result["counts"]["completed"], 1)
        self.assertEqual(result["counts"]["completed_despite_http_failure"], 1)
        self.assertEqual(result["counts"]["failed"], 0)
        self.assertEqual(result["integrity"]["valid_completed"], 1)

    def test_failed_command_not_successful_acceptance(self):
        result = report(rows=[persisted(status="failed", result=None, error="Not found", error_code="not_found")])
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["counts"]["http_success"], 1)
        self.assertEqual(result["counts"]["failed"], 1)
        self.assertEqual(result["latency_ms"]["db_failed"]["mean"], 100)
        self.assertEqual(result["latency_ms"]["db_completed"]["count"], 0)
        self.assertEqual(result["cohorts"]["failed_commands"][0]["error_code"], "not_found")

    def test_missing_and_unknown_are_unresolved_not_lost(self):
        for rows, field in (([], "missing"), ([persisted(status="future_status")], "unknown")):
            with self.subTest(field=field):
                result = report(rows=rows)
                self.assertEqual(result["status"], "incomplete")
                self.assertEqual(result["counts"][field], 1)
                self.assertEqual(result["cohorts"][field + "_command_ids"], [COMMAND_ID])
                self.assertNotIn("lost", result["counts"])

    def test_dropped_are_not_missing_commands(self):
        result = report(records=[receipt(), receipt(index=1, command_id=str(UUID(int=4)), outcome="dropped", drop_reason="capacity")])
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["counts"]["planned"], 2)
        self.assertEqual(result["counts"]["sent"], 1)
        self.assertEqual(result["counts"]["dropped"], 1)
        self.assertEqual(result["counts"]["missing"], 0)
        self.assertEqual(result["latency_ms"]["http"]["count"], 1)
        self.assertEqual(result["drop_reasons"], {"capacity": 1})

    def test_completed_business_audit_must_be_valid(self):
        invalid = report(rows=[persisted(business_valid=False)])
        self.assertEqual(invalid["status"], "failed")
        self.assertEqual(invalid["integrity"]["invalid_completed"], 1)
        self.assertIn("business_state_invalid", [issue["code"] for issue in invalid["integrity"]["issues"]])
        unknown = report(rows=[persisted(business_valid=None)])
        self.assertEqual(unknown["status"], "incomplete")
        self.assertEqual(unknown["integrity"]["unaudited_completed"], 1)

    def test_wrong_command_type_is_integrity_failure(self):
        result = report(rows=[persisted(type="request_return")])
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["integrity"]["issues"][0]["code"], "command_type_mismatch")
        self.assertEqual(result["integrity"]["valid_completed"], 0)
        self.assertEqual(result["integrity"]["invalid_completed"], 1)

    def test_acceptance_validates_command_id_and_status(self):
        for body in ({"command_id": RETURN_ID, "status": "accepted"}, {"command_id": COMMAND_ID, "status": "completed"}, [], "broken json"):
            with self.subTest(body=body):
                result = report(records=[receipt(response_body=json.dumps(body))])
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["counts"]["http_expected_status"], 1)
                self.assertEqual(result["counts"]["http_invalid_body"], 1)
                self.assertEqual(result["counts"]["http_success"], 0)
                self.assertEqual(result["counts"]["completed_despite_invalid_http_body"], 1)

    def test_direct_response_must_match_persisted_entity(self):
        result = report(
            records=[receipt(expected_http_status=201, http_status=201, response_body=json.dumps({"id": RETURN_ID, "status": "pending"}))],
            mode="postgres_pool",
        )
        self.assertEqual(result["status"], "failed")
        self.assertIn("http_and_persisted_result_mismatch", [issue["code"] for issue in result["integrity"]["issues"]])

    def test_delivery_and_return_result_correlation(self):
        scenarios = [
            ("deliver", "ensure_delivery_delivered", 200, {"id": DELIVERY_ID, "status": "delivered"}),
            ("return", "request_return", 201, {"id": RETURN_ID, "delivery_id": DELIVERY_ID, "status": "requested"}),
        ]
        for operation, command_type, http_status, result_body in scenarios:
            with self.subTest(operation=operation):
                record = receipt(operation=operation, delivery_id=DELIVERY_ID, expected_http_status=http_status, http_status=http_status, response_body=json.dumps(result_body))
                row = persisted(type=command_type, result=result_body)
                self.assertEqual(report(records=[record], rows=[row], mode="postgres_pool")["status"], "pass")
                record["delivery_id"] = COMMAND_ID
                result = report(records=[record], rows=[row], mode="postgres_pool")
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["counts"]["http_invalid_body"], 1)
                self.assertIn("persisted_delivery_id_mismatch", [issue["code"] for issue in result["integrity"]["issues"]])

    def test_unexpected_http_is_separate_from_persisted_completion(self):
        result = report(records=[receipt(http_status=503, response_body='{"detail":"unavailable"}')])
        self.assertEqual(result["counts"]["http_unexpected_status"], 1)
        self.assertEqual(result["counts"]["http_invalid_body"], 0)
        self.assertEqual(result["counts"]["completed_despite_http_failure"], 1)
        self.assertEqual(result["http_statuses"], {"503": 1})

    def test_timestamp_exclusions_keep_completion_known(self):
        scenarios = [
            ({"created_at": None}, "missing_timestamps"),
            ({"completed_at": "not a date"}, "invalid_timestamps"),
            ({"created_at": "2026-10-12T12:00:00"}, "invalid_timestamps"),
            ({"completed_at": "2026-10-12T11:59:59Z"}, "negative_duration"),
        ]
        for changes, exclusion in scenarios:
            with self.subTest(exclusion=exclusion):
                result = report(rows=[persisted(**changes)])
                self.assertEqual(result["status"], "pass")
                self.assertEqual(result["counts"]["completed"], 1)
                timing = result["latency_ms"]["db_completed"]
                self.assertEqual(timing["count"], 0)
                self.assertIsNone(timing["mean"])
                self.assertEqual(timing["excluded"], {exclusion: 1})

    def test_percentiles_and_throughput_use_correct_cohorts_and_windows(self):
        records = []
        rows = []
        for index, latency in enumerate((10, 20, 30, 40)):
            command_id = str(UUID(int=index + 100))
            records.append(receipt(index=index, command_id=command_id, latency_ms=latency, response_body=json.dumps({"command_id": command_id, "status": "accepted"})))
            rows.append(persisted(command_id=command_id))
        result = report(records=records, rows=rows, measurement_duration_s=500)
        self.assertEqual(result["status"], "pass")
        http = result["latency_ms"]["http"]
        self.assertEqual(http["mean"], 25)
        self.assertEqual(http["p50"], 25)
        self.assertAlmostEqual(http["p95"], 38.5)
        self.assertAlmostEqual(http["p99"], 39.7)
        self.assertEqual(http["max"], 40)
        self.assertEqual(result["throughput"]["http_requests_per_s"], 2)
        self.assertEqual(result["throughput"]["completed_commands_per_s"], 0.4)
        self.assertEqual(result["latency_ms"]["db_completed"]["mean"], 100)

    def test_http_invalid_timing_and_nonpositive_windows(self):
        for latency in (None, -1, math.nan, math.inf):
            with self.subTest(latency=latency):
                result = report(records=[receipt(latency_ms=latency)], processing_duration_s=0)
                self.assertEqual(result["latency_ms"]["http"]["count"], 0)
                self.assertIsNone(result["throughput"]["completed_commands_per_s"])
                json.dumps(result, allow_nan=False)

    def test_duplicate_commands_cannot_inflate_completion_throughput(self):
        result = report(records=[receipt(), receipt(index=1)], rows=[persisted(), persisted()])
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["counts"]["sent"], 2)
        self.assertEqual(result["counts"]["unique_sent_commands"], 1)
        self.assertEqual(result["counts"]["completed"], 1)
        self.assertEqual(result["throughput"]["completed_commands_per_s"], 0.1)

    def test_empty_workload_is_incomplete(self):
        result = report(records=[], rows=[])
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["counts"]["planned"], 0)

    def test_report_does_not_modify_inputs(self):
        execution = {"mode": "event_driven", "operation": "create", "records": [receipt()], "execution": {"actual_duration_s": 2}}
        snapshot = [persisted()]
        lifecycle = {"processing_duration_s": 10}
        original = deepcopy((execution, snapshot, lifecycle))
        build_report(execution, snapshot, lifecycle)
        self.assertEqual((execution, snapshot, lifecycle), original)


if __name__ == "__main__":
    unittest.main()
