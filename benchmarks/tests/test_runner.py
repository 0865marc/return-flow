import sys
import json
from contextlib import asynccontextmanager, nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import APIRouter, FastAPI

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import run as runner
from run import Experiment, arguments, make_plan, requires_quiescence, result_row
from resources import sample_coverage


def summary(*, attempted=100, completed=100, dropped=0, unknown=0, errors=0, elapsed=12000):
    return {
        "state": {"testRunDurationMs": elapsed},
        "metrics": {
            "workflows_attempted": {"values": {"count": attempted}},
            "workflows_completed": {"values": {"count": completed}},
            "dropped_iterations": {"values": {"count": dropped}},
            "unknown_writes": {"values": {"count": unknown}},
            "technical_errors": {"values": {"count": errors}},
            **{key: {"values": {"count": value}} for key, value in {
                "http_requests_total": 4 * attempted,
                "http_requests_succeeded": 4 * attempted - errors,
                "http_requests_failed": errors,
                "http_responses_4xx": 0,
                "http_responses_5xx": errors,
                "http_transport_errors": 0,
                "http_timeouts": 0,
                "http_unexpected_responses": 0,
            }.items()},
        },
    }


def test_each_pair_has_the_same_load_and_alternates_execution_order():
    plan = list(make_plan(arguments([
        "--adapters", "postgres", "postgres_pool",
        "--repetitions", "2", "--rates", "10", "50",
    ])))
    assert [row["adapter"] for row in plan[:4]] == ["postgres", "postgres_pool"] * 2
    assert [row["adapter"] for row in plan[4:]] == ["postgres_pool", "postgres"] * 2
    for left, right in zip(plan[::2], plan[1::2]):
        assert {key: val for key, val in left.items() if key != "adapter"} == {
            key: val for key, val in right.items() if key != "adapter"
        }


def test_smoke_covers_each_adapter_and_scenario_once():
    plan = list(make_plan(arguments(["--smoke"])))
    assert len(plan) == 6
    assert all(run["execution_model"] == "async" for run in plan)
    assert {(run["adapter"], run["scenario"]) for run in plan} == {
        (adapter, scenario)
        for adapter in ("postgres", "postgres_pool")
        for scenario in ("read", "create", "flow")
    }


def test_throughput_includes_time_spent_draining_in_flight_work():
    result = result_row({"execution_model": "async"}, summary(elapsed=20000), {"status": "passed"}, 0, 10)
    assert result["status"] == "passed"
    assert result["completed_per_s"] == 5
    assert result["execution_model"] == "async"


@pytest.mark.parametrize("async_http, async_repository", [(True, True), (False, True), (True, False)])
def test_image_preflight_rejects_old_synchronous_routes_or_repositories(monkeypatch, async_http, async_repository):
    async def asynchronous():
        pass

    def synchronous():
        pass

    @asynccontextmanager
    async def lifespan(application):
        application.state.delivery_repository = repository
        application.state.return_repository = repository
        yield

    endpoint = asynchronous if async_http else synchronous
    operation = asynchronous if async_repository else synchronous
    repository = SimpleNamespace(add=operation, get=operation, save=operation)
    router = APIRouter()
    router.add_api_route("/deliveries", endpoint)
    app = FastAPI(lifespan=lifespan)
    app.include_router(router)
    app.add_api_route("/health", asynchronous)
    http_module = SimpleNamespace(router=router)
    monkeypatch.setitem(sys.modules, "adapters", SimpleNamespace(http=http_module))
    monkeypatch.setitem(sys.modules, "adapters.http", http_module)
    monkeypatch.setitem(sys.modules, "main", SimpleNamespace(app=app))
    experiment = Experiment.__new__(Experiment)
    experiment.compose_command = lambda *args: exec(args[-1], {})

    if async_http and async_repository:
        experiment.verify_execution_model()
    else:
        with pytest.raises(RuntimeError, match="Rebuild without --no-build"):
            experiment.verify_execution_model()


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


def test_failed_http_percentage_uses_requests_not_workflows():
    data = summary(attempted=10, completed=8, errors=2, unknown=2)
    data["metrics"]["http_requests_total"]["values"]["count"] = 36
    data["metrics"]["http_requests_succeeded"]["values"]["count"] = 34
    data["metrics"]["http_responses_5xx"]["values"]["count"] = 1
    data["metrics"]["http_transport_errors"]["values"]["count"] = 1
    data["metrics"]["http_timeouts"]["values"]["count"] = 1
    row = result_row({}, data, {"status": "inconclusive"}, 99, 10)
    assert row["http_error_percent"] == pytest.approx(100 * 2 / 36)
    assert row["failed_workflows"] == 2
    assert row["workflow_error_percent"] == 20
    assert row["http_requests_unfinished"] == 0
    assert row["unknown_writes"] == 2
    assert row["status"] == "failed"


def test_interrupted_requests_are_not_reported_as_successful_or_classified_failures():
    data = summary(completed=99)
    data["metrics"]["http_requests_succeeded"]["values"]["count"] -= 1
    row = result_row({}, data, {"status": "passed"}, 99, 10)
    assert row["http_requests_unfinished"] == 1
    assert row["http_requests_failed"] == 0
    assert row["failed_workflows"] == 1
    assert requires_quiescence(data)


def test_skipped_measurements_have_no_fabricated_zero_counters():
    row = result_row({}, None, {"status": "not_run"}, 1, 30)
    assert row["status"] == "not_run"
    for key in ("attempted", "completed", "completed_per_s", "failed_workflows",
                "http_requests_total", "http_requests_failed", "http_error_percent",
                "technical_errors", "unknown_writes", "dropped_iterations", "measured_seconds"):
        assert row[key] is None


def test_dropped_iterations_alone_do_not_require_restarting_a_quiescent_api():
    assert not requires_quiescence(summary(dropped=5))
    assert requires_quiescence(summary(completed=99, errors=1, unknown=1))
    assert requires_quiescence({})


@pytest.mark.parametrize("with_errors, drain_failure", [(False, False), (True, False), (True, True)])
def test_failed_warmup_is_audited_and_followed_by_measurement(tmp_path, monkeypatch, with_errors, drain_failure):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "ResourceSampler", lambda *args: nullcontext())
    experiment = Experiment(arguments(["--adapters", "postgres", "--rates", "50", "--repetitions", "1"]))
    events = []
    warmup = summary(dropped=5, completed=99 if with_errors else 100,
                     errors=int(with_errors), unknown=int(with_errors))

    def compose(*args, **kwargs):
        if args[0] == "stop":
            events.append("stop")
        if args[0] == "up":
            events.append("start")
        return SimpleNamespace(returncode=0, stdout="{}")

    def script(name, *args, **kwargs):
        if name == "reliability.py":
            destination = args[args.index("--output") + 1].removeprefix("/results/")
            (experiment.output / destination).write_text('{"status": "passed"}')
        elif args[0] == "seed":
            events.append("seed")
        elif args[0] == "wait-idle":
            assert kwargs["standalone"] is True
            events.append("drain")
            if drain_failure:
                raise RuntimeError("Database clients did not disconnect")

    def load(run, folder, duration):
        phase = "warmup" if folder.name == "warmup" else "measurement"
        events.append(f"load:{phase}")
        (folder / "summary.json").write_text(json.dumps(warmup if phase == "warmup" else summary()))
        return 99 if phase == "warmup" else 0

    def verify(run, folder, *, standalone=False):
        phase = "warmup" if folder.name == "warmup" else "measurement"
        assert standalone == (with_errors and phase == "warmup")
        events.append(f"verify:{phase}")
        return {"status": "inconclusive" if standalone else "passed"}

    experiment.capture_metadata = lambda: None
    experiment.command = lambda *args, **kwargs: SimpleNamespace(stdout="{}")
    experiment.compose_command = compose
    experiment.api_script = script
    experiment.load = load
    experiment.verify = verify

    code = experiment.execute()
    if drain_failure:
        assert code == 1
        assert not experiment.rows
        assert "load:measurement" not in events
        assert events[-1] == "drain"
        assert experiment.manifest["status"] == "error"
        assert "Database clients did not disconnect" in experiment.manifest["error"]
        return
    assert code == 0
    row = experiment.rows[0]
    assert row["warmup_status"] == "failed"
    assert row["warmup_dropped_iterations"] == 5
    assert row["status"] == "passed"
    assert row["warmup_recovery"] == ("api_restarted" if with_errors else "none")
    assert events.index("verify:warmup") < events.index("load:measurement")
    if with_errors:
        index = events.index("load:warmup")
        assert events[index:index + 7] == [
            "load:warmup", "stop", "drain", "verify:warmup", "start", "seed", "load:measurement",
        ]
    else:
        assert "stop" not in events
    report = (experiment.output / "report.md").read_text()
    assert "Warmup (excluded from measurements)" in report
    assert "failed" in report
    assert "api_restarted" in report


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
