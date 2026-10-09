import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from run import arguments, make_plan, result_row
from resources import sample_coverage


def summary(*, attempted=100, completed=100, dropped=0, unknown=0, elapsed=12000):
    return {
        "state": {"testRunDurationMs": elapsed},
        "metrics": {
            "workflows_attempted": {"values": {"count": attempted}},
            "workflows_completed": {"values": {"count": completed}},
            "dropped_iterations": {"values": {"count": dropped}},
            "unknown_writes": {"values": {"count": unknown}},
        },
    }


def test_each_pair_has_the_same_load_and_alternates_execution_order():
    plan = list(make_plan(arguments(["--repetitions", "2", "--rates", "10", "50"])))
    assert [row["adapter"] for row in plan[:4]] == ["postgres", "postgres_pool"] * 2
    assert [row["adapter"] for row in plan[4:]] == ["postgres_pool", "postgres"] * 2
    for left, right in zip(plan[::2], plan[1::2]):
        assert {key: val for key, val in left.items() if key != "adapter"} == {
            key: val for key, val in right.items() if key != "adapter"
        }


def test_throughput_includes_time_spent_draining_in_flight_work():
    result = result_row({}, summary(elapsed=20000), {"status": "passed"}, 0, 10)
    assert result["status"] == "passed"
    assert result["completed_per_s"] == 5


@pytest.mark.parametrize("metrics", [
    {}, summary(attempted=0, completed=0), summary(completed=99),
    summary(dropped=1), summary(unknown=1), summary(elapsed=0),
])
def test_incomplete_or_untrustworthy_measurements_cannot_pass(metrics):
    assert result_row({}, metrics, {"status": "passed"}, 0, 10)["status"] == "failed"


@pytest.mark.parametrize("audit", ["failed", "inconclusive"])
def test_successful_http_is_insufficient_when_database_audit_does_not_pass(audit):
    assert result_row({}, summary(), {"status": audit}, 0, 10)["status"] == "failed"


def test_k6_threshold_failure_is_preserved():
    assert result_row({}, summary(), {"status": "passed"}, 99, 10)["status"] == "failed"


def test_missing_resource_samples_are_explicit_instead_of_implying_zero_usage(tmp_path):
    coverage = sample_coverage(tmp_path / "resources.jsonl")
    assert coverage["samples_by_service"] == {"api": 0, "postgres": 0, "k6": 0}
    assert len(coverage["warnings"]) == 3


@pytest.mark.parametrize("options", [
    ["--rates", "0"], ["--pool-timeout", "nan"], ["--request-timeout", "inf"],
    ["--preallocated-vus", "30", "--max-vus", "10"], ["--rates", "10", "10"],
])
def test_invalid_configuration_is_rejected_before_creating_containers(options):
    with pytest.raises(SystemExit):
        arguments(options)
