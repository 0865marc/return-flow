#!/usr/bin/env python3
"""Run isolated, two-phase benchmarks. Host requirements: Python and Docker."""

import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import uuid4

from analytics import build_report
from workload import make_plan

HERE = Path(__file__).resolve().parent
MODES = ("postgres_pool", "event_driven")
OPERATIONS = ("create", "deliver", "return")


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def positive(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be positive and finite")
    return number


def nonnegative(value):
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("must be nonnegative and finite")
    return number


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adapters", nargs="+", choices=MODES, default=list(MODES))
    parser.add_argument("--operations", nargs="+", choices=OPERATIONS, default=list(OPERATIONS))
    parser.add_argument("--rates", nargs="+", type=positive, default=[10.0, 50.0, 200.0])
    parser.add_argument("--duration", type=positive, default=15.0, help="POST submission window, seconds")
    parser.add_argument("--repetitions", type=positive_int, default=3)
    parser.add_argument("--concurrency", type=positive_int, default=64)
    parser.add_argument("--timeout", type=positive, default=10.0, help="HTTP socket timeout, seconds")
    parser.add_argument("--drain-seconds", type=nonnegative, default=15.0, help="passive processing after HTTP, no reads")
    parser.add_argument("--stop-timeout", type=positive_int, default=10)
    parser.add_argument("--app-cpus", type=positive, default=2.0)
    parser.add_argument("--db-cpus", type=positive, default=1.0)
    parser.add_argument("--broker-cpus", type=positive, default=1.0)
    parser.add_argument("--client-cpus", type=positive, default=1.0)
    parser.add_argument("--pool-size", type=positive_int, default=10)
    parser.add_argument("--worker-concurrency", type=positive_int, default=8)
    parser.add_argument("--output", type=Path, default=HERE / "results")
    parser.add_argument("--no-build", action="store_true", help="use already built return-flow-benchmark-v2:local")
    parser.add_argument("--smoke", action="store_true", help="2 s, 5 requests/s, 1 repeat, all chosen scenarios")
    args = parser.parse_args(argv)
    if args.smoke:
        args.duration, args.rates, args.repetitions, args.drain_seconds = 2.0, [5.0], 1, 1.0
    return args


def save(path, value):
    path.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n")


class Compose:
    def __init__(self, project, env):
        # All destructive cleanup is scoped to this newly allocated project.
        if not project.startswith("rfbench-"):
            raise ValueError("Refusing a non-benchmark Docker project")
        self.project, self.env = project, env
        self.config_path = HERE / "compose.yml"

    def __call__(self, *args, capture=False, check=True):
        return subprocess.run(
            ["docker", "compose", "-p", self.project, "-f", str(self.config_path), *args],
            env=self.env, check=check, text=True,
            stdout=subprocess.PIPE if capture else None,
        )

    def driver(self, action):
        self("run", "--rm", "--no-deps", "-T", "bench", action)


def environment(args, mode, output):
    queued = mode == "event_driven"
    api_cpus = args.app_cpus / 2 if queued else args.app_cpus
    env = dict(os.environ)
    # Do not inherit benchmark tuning implicitly from the shell.
    env.update({
        "BENCH_OUTPUT": str(output.resolve()),
        "BENCH_UID": str(os.getuid()), "BENCH_GID": str(os.getgid()),
        "BENCH_API_CPUS": str(api_cpus),
        "BENCH_WORKER_CPUS": str(args.app_cpus / 2),
        "BENCH_API_WORKERS": str(max(1, math.ceil(api_cpus))),
        "BENCH_DB_CPUS": str(args.db_cpus),
        "BENCH_BROKER_CPUS": str(args.broker_cpus),
        "BENCH_CLIENT_CPUS": str(args.client_cpus),
        "BENCH_POOL_SIZE": str(args.pool_size),
        "BENCH_POOL_TIMEOUT": "5",
        "BENCH_WORKER_CONCURRENCY": str(args.worker_concurrency),
        "BENCH_RABBITMQ_URL": "amqp://benchmark:benchmark@rabbitmq:5672/" if queued else "",
    })
    return env


def run_case(args, mode, operation, rate, repetition, root, build=False):
    run_id = str(uuid4())
    output = root / f"{mode}-{operation}-{rate:g}rps-{repetition}"
    output.mkdir()
    env = environment(args, mode, output)
    project = "rfbench-" + uuid4().hex[:16]
    compose = Compose(project, env)
    services = ["api", "worker"] if mode == "event_driven" else ["api"]
    plan = make_plan(run_id, operation, math.ceil(rate * args.duration))
    config = {
        "format_version": 1, "methodology": "two_phase_no_observation",
        "run_id": run_id, "project": project, "mode": mode, "operation": operation,
        "rate": rate, "duration": args.duration, "repetition": repetition,
        "concurrency": args.concurrency, "timeout": args.timeout,
        "drain_seconds": args.drain_seconds, "stop_timeout": args.stop_timeout,
        "resources": {key: value for key, value in env.items()
                      if key.startswith("BENCH_") and key not in {"BENCH_OUTPUT", "BENCH_RABBITMQ_URL", "BENCH_UID", "BENCH_GID"}},
    }
    save(output / "config.json", config)
    save(output / "plan.json", plan)
    lifecycle = {"preparation_started_at": utc_now()}
    try:
        if build:
            compose("build", "api")
        # Preserve effective resource limits and immutable image IDs for review.
        resolved = compose("config", capture=True).stdout
        frozen_config = output / "compose.resolved.yml"
        frozen_config.write_text(resolved)
        # All later commands, including cleanup, use this case's resolved copy.
        # Editing the source Compose during a run must not change its resources.
        compose.config_path = frozen_config
        infrastructure = ["postgres", "rabbitmq"] if mode == "event_driven" else ["postgres"]
        compose("up", "-d", "--no-deps", *infrastructure)
        compose.driver("prepare")
        compose("up", "-d", "--no-deps", *services)
        compose.driver("ready")
        (output / "images.txt").write_text(compose("images", capture=True).stdout)
        lifecycle["execution_started_at"] = utc_now()
        processing_started = time.monotonic()
        print(f"  Ejecución: {mode}, {operation}, {rate:g} req/s, repetición {repetition}", flush=True)
        compose.driver("execute")
        shutdown_started = time.monotonic()
        lifecycle["shutdown_started_at"] = utc_now()
        # Never query commands while application processes can still do work.
        compose("stop", "--timeout", str(args.stop_timeout), *services)
        lifecycle["shutdown_s"] = time.monotonic() - shutdown_started
        if compose("ps", "--status", "running", "-q", *services, capture=True).stdout.strip():
            raise RuntimeError("API/worker still running; refusing to start measurement")
        lifecycle["processing_duration_s"] = time.monotonic() - processing_started
        lifecycle["execution_finished_at"] = utc_now()
        lifecycle["measurement_started_at"] = utc_now()
        measurement_started = time.monotonic()
        print("  Medición: snapshot de PostgreSQL y analíticas locales", flush=True)
        compose.driver("snapshot")
        execution = json.loads((output / "execution.json").read_text())
        snapshot = json.loads((output / "snapshot.json").read_text())
        lifecycle["passive_drain_s"] = execution["passive_drain_actual_s"]
        report = build_report(execution, snapshot, lifecycle)
        lifecycle["measurement_duration_s"] = time.monotonic() - measurement_started
        lifecycle["measurement_finished_at"] = utc_now()
        report["lifecycle"] = lifecycle
        report["config"] = config
        save(output / "lifecycle.json", lifecycle)
        save(output / "report.json", report)
        print(f"  Resultado: {report['status']} — {output / 'report.json'}", flush=True)
        return {"case": output.name, "status": report["status"], "report": str(output / "report.json")}
    except BaseException as error:
        save(output / "error.json", {"error": str(error), "type": type(error).__name__, "lifecycle": lifecycle})
        raise
    finally:
        compose("down", "--volumes", "--remove-orphans", "--timeout", str(args.stop_timeout), check=False)


def main(argv=None):
    args = arguments(argv)
    root = args.output.resolve() / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8])
    root.mkdir(parents=True)
    print(f"Resultados: {root}", flush=True)
    summary = []
    first = True
    try:
        # Alternate mode ordering across repetitions to reduce ordering bias.
        for repetition in range(1, args.repetitions + 1):
            for operation in args.operations:
                for rate in args.rates:
                    modes = args.adapters if repetition % 2 else list(reversed(args.adapters))
                    for mode in modes:
                        result = run_case(args, mode, operation, rate, repetition, root, build=first and not args.no_build)
                        first = False
                        summary.append(result)
                        save(root / "summary.json", summary)
    except (OSError, subprocess.CalledProcessError, RuntimeError, ValueError) as error:
        print(f"Benchmark interrumpido: {error}. Artefactos: {root}", file=sys.stderr)
        return 2
    return 0 if all(case["status"] == "pass" for case in summary) else 1


if __name__ == "__main__":
    raise SystemExit(main())
