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
        "--adapters", "postgres_pool", "event_driven",
        "--repetitions", "2", "--rates", "10", "50",
    ])))
    assert [row["adapter"] for row in plan[:4]] == ["postgres_pool", "event_driven"] * 2
    assert [row["adapter"] for row in plan[4:]] == ["event_driven", "postgres_pool"] * 2
    for left, right in zip(plan[::2], plan[1::2]):
        for key in ("scenario", "rate", "repetition", "application_cpus", "persistence_adapter", "idempotency"):
            assert left[key] == right[key]


def test_smoke_covers_each_adapter_and_scenario_once():
    plan = list(make_plan(arguments(["--smoke"])))
    assert len(plan) == 2
    assert all(run["execution_model"] == "async" for run in plan)
    assert {(run["adapter"], run["scenario"]) for run in plan} == {
        (adapter, scenario)
        for adapter in ("postgres_pool", "event_driven")
        for scenario in ("flow",)
    }


def test_throughput_includes_time_spent_draining_in_flight_work():
    result = result_row({"execution_model": "async"}, summary(elapsed=20000), {"status": "passed"}, 0, 10)
    assert result["status"] == "passed"
    assert result["completed_per_s"] == 5
    assert result["execution_model"] == "async"


def test_event_variant_splits_the_same_application_cpu_and_memory_budget():
    plan = list(make_plan(arguments(["--rates", "10", "--repetitions", "1", "--app-cpus", "2"])))
    for run in plan:
        assert run["api_cpus"] + run["worker_cpus"] == 2
        assert run["api_memory_mib"] + run["worker_memory_mib"] == 1024
        assert run["postgres_cpus"] == run["k6_cpus"] == 1
    event = next(run for run in plan if run["adapter"] == "event_driven")
    assert event["api_cpus"] == event["worker_cpus"] == 1
    assert event["api_workers"] == event["worker_processes"] == 1
    assert event["persistence_adapter"] == "postgres_pool"
    assert event["rabbitmq_cpus"] == 1


def test_worker_concurrency_is_recorded_and_passed_to_compose(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    experiment = Experiment(arguments(["--worker-concurrency", "3", "--repetitions", "1"]))
    assert experiment.env["BENCH_WORKER_CONCURRENCY"] == "3"
    for case in experiment.manifest["plan"]:
        expected = 3 if case["adapter"] == "event_driven" else 0
        assert case["worker_concurrency"] == case["worker_prefetch"] == expected
        assert case["api_cpus"] + case["worker_cpus"] == 2


def test_event_reset_purges_messages_and_resets_database_while_consumers_are_stopped(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    experiment = Experiment(arguments(["--adapters", "event_driven"]))
    experiment.current_variant = "event_driven"
    events = []
    experiment.quiesce = lambda folder: events.append("quiesce")

    def script(name, *args, **kwargs):
        assert kwargs["standalone"] is True
        events.append((name, args[0]))

    experiment.api_script = script
    experiment.start_api = lambda: events.append("start")
    experiment.seed()
    assert events == ["quiesce", ("messaging.py", "reset"), ("database.py", "seed"), "start"]


def test_event_quiescence_stops_both_processes_before_waiting_for_database(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    experiment = Experiment(arguments(["--adapters", "event_driven"]))
    experiment.current_variant = "event_driven"
    commands = []
    experiment.compose_command = lambda *args, **kwargs: commands.append(args)
    experiment.api_script = lambda *args, **kwargs: commands.append(args)
    experiment.quiesce(experiment.output / "phase")
    assert commands[0][-2:] == ("api", "worker")
    assert commands[1][:2] == ("database.py", "wait-idle")


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
    experiment = Experiment(arguments(["--adapters", "postgres_pool", "--rates", "50", "--repetitions", "1"]))
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
    ["--app-cpus", "0"], ["--command-timeout", "nan"], ["--poll-interval", "0"],
    ["--poll-interval", "2", "--poll-max-interval", "1"], ["--drain-timeout", "nan"],
    ["--publish-concurrency", "0"], ["--worker-concurrency", "0"],
    ["--worker-concurrency", "-1"], ["--scenarios", "read"],
])
def test_invalid_configuration_is_rejected_before_creating_containers(options):
    with pytest.raises(SystemExit):
        arguments(options)


def test_queued_results_use_committed_outcomes_not_acceptance_or_k6_completed():
    data = summary(attempted=10, completed=0)
    data["benchmark"] = {"version": 4}
    data["metrics"]["workflows_submitted"] = {"values": {"count": 10}}
    data["metrics"]["commands_completed"] = {"values": {"count": 20}}
    audit = {"status": "passed", "counts": {"commands_completed": 30, "commands_failed": 0},
             "flows": {"attempted": 10, "completed": 10, "elapsed_seconds": 15,
                       "completion_p50_ms": 500, "completion_p95_ms": 1500,
                       "completion_p99_ms": 2000, "failed": 0, "pending": 0,
                       "missing": 0, "incomplete": 0}}
    row = result_row({"adapter": "event_driven"}, data, audit, 0, 10)
    assert row["status"] == "passed"
    assert row["completed"] == 10
    assert row["completed_per_s"] == 10 / 15
    assert row["measured_seconds"] == 15
    assert row["workflow_p95_ms"] == 1500
    assert row["commands_completed"] == 30
    assert row["failed_workflows"] == 0


def test_v4_without_flow_audit_cannot_pass_even_with_successful_http():
    data = summary()
    data["benchmark"] = {"version": 4}
    assert result_row({}, data, {"status": "passed"}, 0, 10)["status"] == "failed"


def test_late_completed_flow_keeps_its_transport_error_visible():
    data = summary(attempted=1, completed=0, errors=1, unknown=1)
    data["benchmark"] = {"version": 4}
    audit = {"status": "passed", "flows": {"attempted": 1, "completed": 1, "elapsed_seconds": 12}}
    row = result_row({}, data, audit, 99, 10)
    assert row["completed"] == 1
    assert row["failed_workflows"] == 0
    assert row["http_requests_failed"] == row["unknown_writes"] == 1
    assert row["status"] == "failed"


@pytest.mark.parametrize("drain_status", ["passed", "timed_out"])
def test_worker_drains_after_load_before_stop_and_offline_audit(tmp_path, monkeypatch, drain_status):
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "ResourceSampler", lambda *args: nullcontext())
    experiment = Experiment(arguments(["--adapters", "event_driven"]))
    experiment.current_variant = "event_driven"
    folder = experiment.output / "measurement"
    events = []

    def load(run, destination, duration):
        events.append("load")
        (destination / "summary.json").write_text(json.dumps(summary()))
        return 0

    def script(name, *args, **kwargs):
        output = experiment.output / args[args.index("--output") + 1].removeprefix("/results/")
        if name == "database.py":
            assert args[0] == "drain"
            assert kwargs["allowed"] == (0, 1)
            assert kwargs["standalone"] is True
            events.append("drain")
            output.write_text(json.dumps({"status": drain_status, "waited_seconds": 2}))
        else:
            events.append("inspect")
            output.write_text(json.dumps({"queues": {"pending": {"ready": 0}, "failed": {"ready": 0}}}))

    def verify(run, destination, *, standalone=False):
        assert standalone
        events.append("audit")
        return {"status": "passed"}

    experiment.load = load
    experiment.api_script = script
    experiment.quiesce = lambda destination: events.append("stop")
    experiment.verify = verify
    def compose(*args, **kwargs):
        if args[0] == "stop":
            assert args[-1] == "api"
            events.append("stop-api")
    experiment.compose_command = compose
    row = experiment.run_phase({"adapter": "event_driven"}, folder, 10)
    assert events == ["load", "stop-api", "drain", "stop", "audit", "inspect"]
    assert row["command_drain_status"] == drain_status
    assert row["status"] == ("passed" if drain_status == "passed" else "failed")


def test_removed_connection_per_operation_variant_is_rejected():
    with pytest.raises(SystemExit):
        arguments(["--adapters", "postgres"])
