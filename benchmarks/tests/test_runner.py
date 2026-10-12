from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


# The runner is also a standalone CLI, with sibling imports outside a package.
with patch.object(sys, "path", [str(Path(__file__).resolve().parents[1]), *sys.path]):
    from benchmarks import driver, run


class FakeCompose:
    """Model process shutdown and artifact handoff without starting Docker."""

    def __init__(self, project, env, *, stop_error=False, still_running=False,
                 ps_error=False, execute_error=False):
        self.project = project
        self.env = env
        self.output = Path(env["BENCH_OUTPUT"])
        self.events = []
        self.stopped = False
        self.confirmed_stopped = False
        self.stop_error = stop_error
        self.still_running = still_running
        self.ps_error = ps_error
        self.execute_error = execute_error

    def __call__(self, *args, capture=False, check=True):
        self.events.append((args, {"capture": capture, "check": check}))
        if args[0] == "stop":
            if self.stop_error:
                raise subprocess.CalledProcessError(1, ["docker", "compose", *args])
            self.stopped = True
        if args[0] == "ps":
            if self.ps_error:
                raise subprocess.CalledProcessError(1, ["docker", "compose", *args])
            if self.still_running:
                return SimpleNamespace(stdout="a-running-worker\n")
            self.confirmed_stopped = self.stopped
            return SimpleNamespace(stdout="")
        return SimpleNamespace(stdout="fixture\n")

    def driver(self, action):
        self.events.append((("driver", action), {}))
        if action == "execute":
            if self.execute_error:
                raise subprocess.CalledProcessError(1, ["driver", "execute"])
            (self.output / "execution.json").write_text(json.dumps({
                "records": [], "passive_drain_actual_s": 0.1,
            }))
        if action == "snapshot":
            if not self.confirmed_stopped:
                raise AssertionError("Snapshot ran before shutdown was confirmed")
            (self.output / "snapshot.json").write_text("[]")


class RunnerLifecycleTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.args = run.arguments(["--duration", "0.1", "--drain-seconds", "0.1"])
        self.instances = []

    def execute_case(self, mode="event_driven", **faults):
        def compose_factory(project, env):
            instance = FakeCompose(project, env, **faults)
            self.instances.append(instance)
            return instance

        with patch.object(run, "Compose", side_effect=compose_factory), \
                patch.object(run, "build_report", return_value={"status": "pass"}) as report, \
                redirect_stdout(io.StringIO()):
            result = run.run_case(self.args, mode, "create", 10, 1, self.root)
        return result, report

    def test_measurement_waits_for_execute_stop_and_confirmation_in_both_modes(self):
        for mode in run.MODES:
            with self.subTest(mode=mode):
                result, report = self.execute_case(mode)
                compose = self.instances[-1]
                events = [args for args, _ in compose.events]
                services = ("api", "worker") if mode == "event_driven" else ("api",)
                stop = ("stop", "--timeout", "10", *services)
                confirm = ("ps", "--status", "running", "-q", *services)
                self.assertLess(events.index(("driver", "execute")), events.index(stop))
                self.assertLess(events.index(stop), events.index(confirm))
                self.assertLess(events.index(confirm), events.index(("driver", "snapshot")))
                self.assertEqual([event for event in events if event[0] == "driver"], [
                    ("driver", "prepare"), ("driver", "ready"),
                    ("driver", "execute"), ("driver", "snapshot"),
                ])
                self.assertEqual(result["status"], "pass")
                self.assertTrue(compose.confirmed_stopped)
                report.assert_called_once()
                saved = json.loads(Path(result["report"]).read_text())
                lifecycle = saved["lifecycle"]
                self.assertLessEqual(
                    datetime.fromisoformat(lifecycle["execution_finished_at"]),
                    datetime.fromisoformat(lifecycle["measurement_started_at"]),
                )
                self.assertEqual(lifecycle["passive_drain_s"], 0.1)
                self.assertEqual(saved["config"]["project"], compose.project)
                self.assertEqual(compose.config_path, compose.output / "compose.resolved.yml")
                self.assert_scoped_cleanup(compose)

    def assert_scoped_cleanup(self, compose):
        self.assertRegex(compose.project, r"^rfbench-[0-9a-f]{16}$")
        down_calls = [(args, kwargs) for args, kwargs in compose.events if args[0] == "down"]
        self.assertEqual(down_calls, [(
            ("down", "--volumes", "--remove-orphans", "--timeout", "10"),
            {"capture": False, "check": False},
        )])
        self.assertEqual(compose.events[-1], down_calls[0])

    def assert_no_measurement(self, fault, exception):
        with self.assertRaises(exception):
            self.execute_case(**{fault: True})
        compose = self.instances[-1]
        self.assertNotIn(("driver", "snapshot"), [args for args, _ in compose.events])
        self.assertFalse((compose.output / "report.json").exists())
        self.assertFalse((compose.output / "snapshot.json").exists())
        error = json.loads((compose.output / "error.json").read_text())
        self.assertNotIn("measurement_started_at", error["lifecycle"])
        self.assertEqual(error["type"], exception.__name__)
        self.assert_scoped_cleanup(compose)

    def test_stop_failure_prevents_snapshot_and_still_cleans_up(self):
        self.assert_no_measurement("stop_error", subprocess.CalledProcessError)

    def test_running_process_after_stop_prevents_snapshot_and_still_cleans_up(self):
        self.assert_no_measurement("still_running", RuntimeError)

    def test_failure_to_confirm_shutdown_prevents_snapshot_and_still_cleans_up(self):
        self.assert_no_measurement("ps_error", subprocess.CalledProcessError)

    def test_execution_failure_prevents_snapshot_and_still_cleans_up(self):
        self.assert_no_measurement("execute_error", subprocess.CalledProcessError)

    def test_separate_cases_get_unique_projects_despite_inherited_compose_name(self):
        with patch.dict(os.environ, {"COMPOSE_PROJECT_NAME": "return-flow"}):
            self.execute_case("postgres_pool")
            self.execute_case("event_driven")
        first, second = self.instances
        self.assertNotEqual(first.project, second.project)
        for compose in self.instances:
            self.assert_scoped_cleanup(compose)
            self.assertNotEqual(compose.project, "return-flow")


class ComposeIsolationTests(unittest.TestCase):
    def test_frozen_configuration_is_used_for_shutdown_and_cleanup(self):
        compose = run.Compose("rfbench-0123456789abcdef", {})
        compose.config_path = Path("/tmp/case/compose.resolved.yml")
        with patch.object(run.subprocess, "run") as docker:
            compose("down", "--volumes", check=False)
        self.assertEqual(docker.call_args.args[0][5], "/tmp/case/compose.resolved.yml")

    def test_non_benchmark_projects_are_rejected_before_any_docker_command(self):
        with patch.object(run.subprocess, "run") as docker:
            for project in ("return-flow", "", "production", "rfbench", "other-rfbench-123"):
                with self.subTest(project=project), self.assertRaises(ValueError):
                    run.Compose(project, {})
            docker.assert_not_called()

    def test_cleanup_command_explicitly_targets_generated_project_and_benchmark_file(self):
        project = "rfbench-0123456789abcdef"
        env = {"COMPOSE_PROJECT_NAME": "return-flow"}
        with patch.object(run.subprocess, "run") as docker:
            run.Compose(project, env)("down", "--volumes", "--remove-orphans", check=False)
        docker.assert_called_once_with(
            ["docker", "compose", "-p", project, "-f", str(run.HERE / "compose.yml"),
             "down", "--volumes", "--remove-orphans"],
            env=env, check=False, text=True, stdout=None,
        )


class ArgumentTests(unittest.TestCase):
    def test_invalid_cli_values_fail_before_docker_or_output_creation(self):
        cases = [
            ["--rates", "0"], ["--rates", "nan"], ["--rates", "inf"],
            ["--duration", "-1"], ["--duration", "nan"],
            ["--timeout", "0"], ["--drain-seconds", "-1"], ["--drain-seconds", "inf"],
            ["--concurrency", "0"], ["--concurrency", "1.5"],
            ["--pool-size", "-1"], ["--worker-concurrency", "0"],
            ["--repetitions", "0"], ["--stop-timeout", "0"],
            ["--app-cpus", "nan"], ["--db-cpus", "0"],
            ["--broker-cpus", "-1"], ["--client-cpus", "inf"],
            ["--operations", "flow"], ["--adapters", "production"],
        ]
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "should-not-exist"
            with patch.object(run, "Compose") as compose, redirect_stderr(io.StringIO()):
                for argv in cases:
                    with self.subTest(argv=argv), self.assertRaises(SystemExit) as error:
                        run.main([*argv, "--output", str(output)])
                    self.assertEqual(error.exception.code, 2)
                    self.assertFalse(output.exists())
                compose.assert_not_called()

    def test_zero_drain_and_fractional_resource_limits_are_valid(self):
        args = run.arguments(["--drain-seconds", "0", "--app-cpus", "0.5", "--rates", "0.1"])
        self.assertEqual(args.drain_seconds, 0)
        self.assertEqual(args.app_cpus, 0.5)
        self.assertEqual(args.rates, [0.1])

    def test_smoke_has_bounded_work_but_preserves_explicit_scenario_selection(self):
        args = run.arguments(["--smoke", "--operations", "return", "--adapters", "event_driven"])
        self.assertEqual((args.duration, args.rates, args.repetitions, args.drain_seconds), (2, [5], 1, 1))
        self.assertEqual(args.operations, ["return"])
        self.assertEqual(args.adapters, ["event_driven"])


class BusinessIntegrityTests(unittest.TestCase):
    def test_completed_create_requires_the_matching_persisted_delivery(self):
        item = {"operation": "create"}
        command = {"status": "completed", "result": {"id": "delivery-1", "status": "pending"}}
        self.assertTrue(driver.business_is_valid(item, command, {"id": "delivery-1", "status": "pending"}, None))
        for delivery in (None, {"id": "another", "status": "pending"},
                         {"id": "delivery-1", "status": "delivered"}):
            with self.subTest(delivery=delivery):
                self.assertFalse(driver.business_is_valid(item, command, delivery, None))

    def test_return_requires_matching_parent_and_real_delivered_delivery(self):
        item = {"operation": "return", "delivery_id": "delivery-1"}
        returned = {"id": "return-1", "delivery_id": "delivery-1", "status": "requested"}
        command = {"result": dict(returned)}
        delivery = {"id": "delivery-1", "status": "delivered"}
        self.assertTrue(driver.business_is_valid(item, command, delivery, returned))
        invalid = [
            (None, returned), (delivery, None),
            ({**delivery, "status": "pending"}, returned),
            (delivery, {**returned, "delivery_id": "another"}),
            (delivery, {**returned, "id": "another"}),
        ]
        for parent, child in invalid:
            with self.subTest(parent=parent, child=child):
                self.assertFalse(driver.business_is_valid(item, command, parent, child))

    def test_deliver_cannot_validate_a_different_delivery_or_pending_status(self):
        item = {"operation": "deliver", "delivery_id": "delivery-1"}
        command = {"result": {"id": "delivery-1", "status": "delivered"}}
        self.assertTrue(driver.business_is_valid(item, command, {"id": "delivery-1", "status": "delivered"}, None))
        for delivery in ({"id": "another", "status": "delivered"},
                         {"id": "delivery-1", "status": "pending"}, None):
            with self.subTest(delivery=delivery):
                self.assertFalse(driver.business_is_valid(item, command, delivery, None))

    def test_malformed_command_result_cannot_pass_integrity(self):
        for result in (None, [], "invalid", 42, {}):
            with self.subTest(result=result):
                self.assertFalse(driver.business_is_valid({"operation": "create"}, {"result": result}, None, None))


class DriverPhaseTests(unittest.TestCase):
    def test_execute_finishes_passive_drain_before_writing_receipts_and_never_reads_backend(self):
        events = []
        config = {
            "mode": "event_driven", "rate": 10, "duration": 0.1,
            "concurrency": 2, "timeout": 1, "drain_seconds": 3,
        }
        plan = [{"operation": "create", "command_id": "known-id"}]

        def execute(*args):
            events.append("execute")
            self.assertEqual(args, ("http://api:8000", "event_driven", plan, 10, 0.1, 2, 1))
            return {"records": [{"command_id": "known-id"}]}

        def sleep(seconds):
            events.append("drain")
            self.assertEqual(seconds, 3)

        original_write = driver.write_json

        def write_json(path, value):
            events.append("write")
            self.assertEqual(events, ["execute", "drain", "write"])
            original_write(path, value)

        with tempfile.TemporaryDirectory() as temporary:
            results = Path(temporary)
            (results / "config.json").write_text(json.dumps(config))
            (results / "plan.json").write_text(json.dumps(plan))
            with patch.object(driver, "RESULTS", results), \
                    patch.object(sys, "argv", ["driver.py", "execute"]), \
                    patch.object(driver, "execute", side_effect=execute), \
                    patch.object(driver.time, "sleep", side_effect=sleep), \
                    patch.object(driver, "write_json", side_effect=write_json), \
                    patch.object(driver, "connect_when_ready") as database, \
                    patch.object(driver, "prepare") as prepare, \
                    patch.object(driver, "ready") as ready, \
                    patch.object(driver, "snapshot") as snapshot:
                driver.main()
            for backend_read in (database, prepare, ready, snapshot):
                backend_read.assert_not_called()
            self.assertEqual(events, ["execute", "drain", "write"])
            receipts = json.loads((results / "execution.json").read_text())
            self.assertEqual(receipts["records"], [{"command_id": "known-id"}])
            self.assertGreaterEqual(receipts["passive_drain_actual_s"], 0)


if __name__ == "__main__":
    unittest.main()
