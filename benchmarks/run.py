#!/usr/bin/env python3
"""Run isolated, repeatable persistence comparisons using Docker and stdlib Python."""

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from resources import ResourceSampler, sample_coverage

ROOT = Path(__file__).resolve().parent
REPOSITORY = ROOT.parent
ADAPTERS = ("postgres", "postgres_pool")
EXECUTION_MODEL = "async"
RESULT_FORMAT_VERSION = 2
SCENARIOS = ("read", "create", "flow")
HTTP_COUNTERS = (
    "http_requests_total", "http_requests_succeeded", "http_requests_failed",
    "http_responses_4xx", "http_responses_5xx", "http_transport_errors",
    "http_timeouts", "http_unexpected_responses",
)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def positive_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return parsed


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adapters", nargs="+", choices=ADAPTERS, default=list(ADAPTERS))
    parser.add_argument("--scenarios", nargs="+", choices=SCENARIOS, default=["flow"])
    parser.add_argument("--rates", nargs="+", type=positive_int, default=[10, 50, 400], help="iterations per second")
    parser.add_argument("--duration", type=positive_int, default=30, help="measurement seconds")
    parser.add_argument("--warmup", type=positive_int, default=5, help="warmup seconds, recorded separately")
    parser.add_argument("--repetitions", type=positive_int, default=3)
    parser.add_argument("--seed-size", type=positive_int, default=1000)
    parser.add_argument("--pool-size", type=positive_int, default=10)
    parser.add_argument("--pool-timeout", type=positive_float, default=5)
    parser.add_argument("--preallocated-vus", type=positive_int, default=50)
    parser.add_argument("--max-vus", type=positive_int, default=200)
    parser.add_argument("--request-timeout", type=positive_float, default=5)
    parser.add_argument("--smoke", action="store_true", help="short check of all adapters and scenarios")
    parser.add_argument("--no-build", action="store_true", help="reuse an existing benchmark API image")
    args = parser.parse_args(argv)
    if args.smoke:
        args.adapters, args.scenarios, args.rates = list(ADAPTERS), list(SCENARIOS), [2]
        args.duration, args.warmup, args.repetitions = 3, 1, 1
        args.preallocated_vus = args.max_vus = 10
    if args.max_vus < args.preallocated_vus:
        parser.error("--max-vus must be >= --preallocated-vus")
    if args.seed_size > 999999999999:
        parser.error("--seed-size exceeds the deterministic UUID range")
    for key in ("adapters", "scenarios", "rates"):
        if len(getattr(args, key)) != len(set(getattr(args, key))):
            parser.error(f"--{key} must not contain duplicates")
    return args


def make_plan(args):
    for repetition in range(1, args.repetitions + 1):
        order = args.adapters if repetition % 2 else list(reversed(args.adapters))
        for scenario in args.scenarios:
            for rate in args.rates:
                for adapter in order:
                    yield {"adapter": adapter, "execution_model": EXECUTION_MODEL,
                           "scenario": scenario, "rate": rate, "repetition": repetition}


def write_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, indent=2) + "\n")


class Experiment:
    def __init__(self, args) -> None:
        self.args = args
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.identifier = f"{stamp}-{uuid.uuid4().hex[:8]}"
        self.project = f"return-flow-bench-{self.identifier.lower()}"
        self.output = ROOT / "results" / self.identifier
        self.output.mkdir(parents=True)
        self.env = dict(os.environ, BENCH_OUTPUT_DIR=str(self.output),
                        BENCH_UID=str(os.getuid()), BENCH_GID=str(os.getgid()),
                        BENCH_POOL_SIZE=str(args.pool_size), BENCH_POOL_TIMEOUT=str(args.pool_timeout))
        self.compose = ["docker", "compose", "--project-name", self.project, "--file", str(ROOT / "compose.yml")]
        self.rows = []
        self.manifest = {"id": self.identifier, "project": self.project, "configuration": vars(args),
                         "execution_model": EXECUTION_MODEL,
                         "result_format_version": RESULT_FORMAT_VERSION,
                         "host": {"platform": platform.platform(), "cpu_count": os.cpu_count()},
                         "plan": list(make_plan(args)), "status": "running"}

    def command(self, command, *, log: Path | None = None, allowed=(0,), timeout=180):
        result = subprocess.run(command, cwd=REPOSITORY, env=self.env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        with (self.output / "orchestrator.log").open("a") as stream:
            stream.write(f"$ {' '.join(command)}\n{result.stdout}\n")
        if log:
            log.write_text(result.stdout)
        if result.returncode not in allowed:
            raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(command)}\n{result.stdout[-3000:]}")
        return result

    def compose_command(self, *args, **kwargs):
        return self.command([*self.compose, *args], **kwargs)

    def api_script(self, script, *args, standalone=False, **kwargs):
        prefix = ("run", "--rm", "--no-deps", "-T") if standalone else ("exec", "-T")
        return self.compose_command(*prefix, "api", "python", f"/benchmarks/{script}", *args, **kwargs)

    def seed(self):
        self.api_script("database.py", "seed", "--size", str(self.args.seed_size))

    def verify_execution_model(self):
        # --no-build may point to an image from the previous synchronous implementation.
        self.compose_command("exec", "-T", "api", "python", "-c", """
import asyncio
import inspect
from fastapi.routing import APIRoute
from adapters.http import router
from main import app

async def verify():
    async with app.router.lifespan_context(app):
        # Included routers may remain deferred in app.routes.
        operations = [route.endpoint for route in (*router.routes, *app.routes) if isinstance(route, APIRoute)]
        operations.extend(getattr(app.state.delivery_repository, name) for name in ("add", "get", "save"))
        operations.extend(getattr(app.state.return_repository, name) for name in ("add", "get"))
        if not all(inspect.iscoroutinefunction(operation) for operation in operations):
            raise RuntimeError("Benchmark requires async HTTP and repositories. Rebuild without --no-build.")

asyncio.run(verify())
""")

    def load(self, run, folder: Path, duration: int):
        relative = folder.relative_to(self.output).as_posix()
        folder.mkdir(parents=True, exist_ok=True)
        settings = {"SCENARIO": run["scenario"], "RATE": run["rate"], "DURATION": f"{duration}s",
                    "PREALLOCATED_VUS": self.args.preallocated_vus, "MAX_VUS": self.args.max_vus,
                    "REQUEST_TIMEOUT": f"{self.args.request_timeout}s", "SEED_SIZE": self.args.seed_size,
                    "SUMMARY_PATH": f"/results/{relative}/summary.json"}
        params = [argument for key, value in settings.items() for argument in ("-e", f"{key}={value}")]
        result = self.compose_command(
            "run", "--rm", "--no-deps", "-T", "--name", f"{self.project}-load", *params,
            "k6", "run", "--quiet", "--log-format", "raw", "--console-output",
            f"/results/{relative}/receipts.log", "/scenarios/load.js",
            log=folder / "k6.log", allowed=(0, 99),
            timeout=duration + math.ceil(4 * self.args.request_timeout) + 90,
        )
        if not (folder / "summary.json").is_file():
            raise RuntimeError(f"k6 did not produce a summary in {folder}")
        return result.returncode

    def verify(self, run, folder, *, standalone=False):
        relative = folder.relative_to(self.output).as_posix()
        self.api_script("database.py", "verify", "--size", str(self.args.seed_size),
                        "--scenario", run["scenario"], "--summary", f"/results/{relative}/summary.json",
                        "--receipts", f"/results/{relative}/receipts.log",
                        "--output", f"/results/{relative}/verification.json", allowed=(0, 1),
                        standalone=standalone)
        return json.loads((folder / "verification.json").read_text())

    def start_api(self):
        self.compose_command("up", "-d", "--no-build", "--wait", "--wait-timeout", "60", "api")

    def quiesce(self, folder):
        # Stop request handling before auditing ambiguous writes or clearing data.
        timeout = math.ceil(4 * self.args.request_timeout + 10)
        self.compose_command("stop", "--timeout", str(timeout), "api", timeout=timeout + 30)
        relative = folder.relative_to(self.output).as_posix()
        self.api_script("database.py", "wait-idle", "--timeout", str(timeout),
                        "--output", f"/results/{relative}/drain.json", standalone=True,
                        timeout=timeout + 30)

    def run_phase(self, run, folder, duration):
        folder.mkdir(parents=True, exist_ok=True)
        with ResourceSampler(self.project, folder / "resources.jsonl"):
            code = self.load(run, folder, duration)
        summary = json.loads((folder / "summary.json").read_text())
        stopped = requires_quiescence(summary)
        if stopped:
            self.quiesce(folder)
        verification = self.verify(run, folder, standalone=stopped)
        row = result_row(run, summary, verification, code, duration)
        row["api_stopped"] = stopped
        coverage = sample_coverage(folder / "resources.jsonl")
        write_json(folder / "resource-coverage.json", coverage)
        row["resource_warnings"] = len(coverage["warnings"])
        self.compose_command("logs", "--no-color", "--tail", "200", log=folder / "services.log")
        write_json(folder / "result.json", row)
        return row

    def execute(self):
        print(f"Results: {self.output}", flush=True)
        try:
            self.capture_metadata()
            write_json(self.output / "manifest.json", self.manifest)
            if not self.args.no_build:
                print("Building benchmark API image…", flush=True)
                self.compose_command("build", "api", timeout=600)
            self.compose_command("pull", "postgres", "k6", timeout=600)
            images = self.command([
                "docker", "image", "inspect", "return-flow-benchmark-api:local", "postgres:17-alpine", "grafana/k6:1.6.1",
                "--format", "{{json .}}",
            ]).stdout
            self.manifest["images"] = [json.loads(line) for line in images.splitlines() if line]
            write_json(self.output / "manifest.json", self.manifest)
            reliability = {}
            for position, run in enumerate(self.manifest["plan"], start=1):
                name = f"{position:03d}-{run['adapter']}-{run['scenario']}-{run['rate']}rps-rep{run['repetition']}"
                folder = self.output / name
                folder.mkdir()
                write_json(folder / "configuration.json", dict(run, duration=self.args.duration))
                print(f"[{position}/{len(self.manifest['plan'])}] {name}", flush=True)
                self.env["BENCH_ADAPTER"] = run["adapter"]
                # Recreate only this isolated stack so each run starts with a fresh database.
                self.compose_command("down", "--volumes", "--remove-orphans")
                self.start_api()
                if run["adapter"] not in reliability:
                    self.verify_execution_model()
                    self.seed()
                    destination = f"reliability-{run['adapter']}.json"
                    self.api_script("reliability.py", "--output", f"/results/{destination}", allowed=(0, 1))
                    reliability[run["adapter"]] = json.loads((self.output / destination).read_text())["status"]
                if reliability[run["adapter"]] != "passed":
                    # Functional preflight failures are not load measurements.
                    verification = {"status": "not_run", "errors": ["reliability_failed; measurement skipped."]}
                    write_json(folder / "verification.json", verification)
                    row = result_row(run, None, verification, 1, self.args.duration)
                    row.update(status="reliability_failed", api_stopped=None, resource_warnings=None)
                    warmup = result_row(run, None, verification, 1, self.args.warmup)
                    warmup["api_stopped"] = None
                else:
                    self.seed()
                    warmup = self.run_phase(run, folder / "warmup", self.args.warmup)
                    if warmup["api_stopped"]:
                        self.start_api()
                    # All warmup requests completed, or the old API has stopped and
                    # its database sessions have drained. Its failures stay recorded.
                    self.seed()
                    row = self.run_phase(run, folder, self.args.duration)
                for key in ("status", "verification", "attempted", "completed", "failed_workflows",
                            "dropped_iterations", "http_requests_total", "http_requests_failed",
                            "http_error_percent", "http_timeouts", "unknown_writes"):
                    row[f"warmup_{key}"] = warmup[key]
                row["warmup_recovery"] = (
                    "not_run" if warmup["api_stopped"] is None else
                    "api_restarted" if warmup["api_stopped"] else "none"
                )
                row["reliability"] = reliability[run["adapter"]]
                row["run"] = name
                self.rows.append(row)
                self.save_report()
                print(f"  {row['status']}: {display(row['completed_per_s'], 2)} completed/s; "
                      f"HTTP failed={display(row['http_requests_failed'])}/{display(row['http_requests_total'])}; "
                      f"audit={row['verification']}; warmup={row['warmup_status']}", flush=True)
                if row["resource_warnings"]:
                    print("  Resource sampling incomplete; see resource-coverage.json", flush=True)
            self.manifest["status"] = "passed" if all(
                row["status"] == "passed" and row["reliability"] == "passed" for row in self.rows
            ) else "failed"
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError, KeyboardInterrupt) as error:
            self.manifest["status"] = "interrupted" if isinstance(error, KeyboardInterrupt) else "error"
            self.manifest["error"] = str(error)
            print(f"Evaluation stopped: {error}", file=sys.stderr, flush=True)
            try:
                self.compose_command("logs", "--no-color", "--tail", "200", log=self.output / "failure-services.log")
            except (OSError, RuntimeError, subprocess.SubprocessError):
                pass
        finally:
            try:
                self.compose_command("down", "--volumes", "--remove-orphans", timeout=90)
            except (OSError, RuntimeError, subprocess.SubprocessError) as error:
                self.manifest["cleanup_error"] = str(error)
                self.manifest["status"] = "error"
                print(f"Cleanup failed for {self.project}: {error}", file=sys.stderr)
            self.manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
            write_json(self.output / "manifest.json", self.manifest)
        print(f"Results ({self.manifest['status']}): {self.output}", flush=True)
        return 0 if self.manifest["status"] == "passed" else 1

    def capture_metadata(self):
        self.command(["docker", "info", "--format", "{{json .}}"], log=self.output / "docker-info.json")
        self.manifest["compose_version"] = self.command(["docker", "compose", "version", "--short"]).stdout.strip()
        self.manifest["git_commit"] = self.command(["git", "rev-parse", "HEAD"]).stdout.strip()
        self.manifest["git_status"] = self.command(["git", "status", "--porcelain"]).stdout
        paths = self.command(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", "api", "benchmarks"]).stdout
        self.manifest["source_sha256"] = {
            path: hashlib.sha256((REPOSITORY / path).read_bytes()).hexdigest()
            for path in paths.split("\0") if path and (REPOSITORY / path).is_file()
        }
        self.compose_command("config", log=self.output / "compose.resolved.yml")

    def save_report(self):
        with (self.output / "comparison.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(self.rows[0]))
            writer.writeheader()
            writer.writerows(self.rows)
        lines = ["# Persistence comparison", "", "Execution model: `async` (HTTP, use cases and PostgreSQL).",
                 "Both adapters use the same application code; only connection management changes.",
                 "Each row is one run; warmup is reported separately and does not gate measurement.",
                 "Throughput uses k6 measurement time including draining in-flight requests.",
                 "Percentiles describe individual runs, not an average across repetitions.", "",
                 "## Measurements", "",
                 "| Adapter | Scenario | Offered iterations/s | Rep | Completed/s | Workflow p95 ms | Failed HTTP / started | HTTP failure % | Incomplete flows | Not started | Audit | Result |",
                 "|---|---|---:|---:|---:|---:|---|---:|---:|---:|---|---|"]
        for row in self.rows:
            lines.append(f"| {row['adapter']} | {row['scenario']} | {row['rate']} | {row['repetition']} | "
                         f"{display(row['completed_per_s'], 2)} | {display(row['workflow_p95_ms'], 2)} | "
                         f"{display(row['http_requests_failed'])} / {display(row['http_requests_total'])} | "
                         f"{display(row['http_error_percent'], 2)} | {display(row['failed_workflows'])} | "
                         f"{display(row['dropped_iterations'])} | {row['verification']} | {row['status']} |")
        lines.extend(["", "## HTTP failure breakdown (measurement)", "",
                      "| Adapter | Scenario | Rate | Rep | 4xx | 5xx | Transport | Timeouts (in transport) | Unexpected response | Unfinished HTTP | Ambiguous writes |",
                      "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"])
        for row in self.rows:
            counts = [display(row[key]) for key in (
                "http_responses_4xx", "http_responses_5xx", "http_transport_errors", "http_timeouts",
                "http_unexpected_responses", "http_requests_unfinished", "unknown_writes",
            )]
            lines.append(f"| {row['adapter']} | {row['scenario']} | {row['rate']} | {row['repetition']} | "
                         + " | ".join(counts) + " |")
        lines.extend(["", "## Warmup (excluded from measurements)", "",
                      "| Adapter | Scenario | Rate | Rep | Completed / attempted | Failed HTTP / started | HTTP failure % | Not started | Ambiguous writes | Audit | Result | Recovery |",
                      "|---|---|---:|---:|---|---|---:|---:|---:|---|---|---|"])
        for row in self.rows:
            lines.append(f"| {row['adapter']} | {row['scenario']} | {row['rate']} | {row['repetition']} | "
                         f"{display(row['warmup_completed'])} / {display(row['warmup_attempted'])} | "
                         f"{display(row['warmup_http_requests_failed'])} / {display(row['warmup_http_requests_total'])} | "
                         f"{display(row['warmup_http_error_percent'], 2)} | {display(row['warmup_dropped_iterations'])} | "
                         f"{display(row['warmup_unknown_writes'])} | {row['warmup_verification']} | "
                         f"{row['warmup_status']} | {row['warmup_recovery']} |")
        lines.extend(["", "N/A means not measured or unavailable; it is not zero.",
                      "HTTP failure % = classified failed requests / requests started. Unfinished HTTP is reported separately.",
                      "Incomplete flows = attempted - completed. Not started = k6 dropped iterations, not server rejections.",
                      "4xx, 5xx, transport and unexpected responses partition classified failures; timeouts are a subset of transport.",
                      "Ambiguous writes overlap failed requests and do not prove data loss.",
                      "`api_restarted` means recovery required restarting the API after warmup; its process/pool is no longer warm.",
                      "`passed` means no correctness/load-generation errors in the measurement; no latency SLA was set.",
                      "A failed warmup stays visible but does not by itself fail a successful measurement.",
                      "Expected 409 conflicts are checked separately in reliability-*.json.",
                      "Resource samples, receipts, raw summaries and logs are saved per run.",
                      "All containers share the host. Receipt logging and sampling have measurement overhead.",
                      "A smoke run checks the harness and does not establish performance capacity.", ""])
        (self.output / "report.md").write_text("\n".join(lines))


def display(value, decimals=None):
    if value is None or value == "":
        return "N/A"
    return f"{value:.{decimals}f}" if decimals is not None else str(value)


def requires_quiescence(summary):
    """Successful completed workflows have no requests left; errors may hide writes."""
    metrics = summary.get("metrics", {})

    def count(name):
        return metrics.get(name, {}).get("values", {}).get("count")

    attempted = count("workflows_attempted")
    total = count("http_requests_total")
    return not (
        attempted is not None and attempted > 0 and count("workflows_completed") == attempted
        and count("technical_errors") == 0 and count("unknown_writes") == 0
        and total is not None and total > 0 and count("http_requests_succeeded") == total
        and count("http_requests_failed") == 0
    )


def result_row(run, summary, verification, returncode, duration):
    measured = summary is not None
    metrics = (summary or {}).get("metrics", {})

    def value(name, key, default=0):
        if not measured:
            return None
        return metrics.get(name, {}).get("values", {}).get(key, default)

    thresholds_ok = all(
        threshold.get("ok", False)
        for metric in metrics.values() for threshold in metric.get("thresholds", {}).values()
    )
    elapsed = (summary or {}).get("state", {}).get("testRunDurationMs", 0) / 1000 if measured else None
    attempted = value("workflows_attempted", "count")
    completed = value("workflows_completed", "count")
    http = {name: value(name, "count", None) for name in HTTP_COUNTERS}
    counts_present = all(isinstance(n, (int, float)) and math.isfinite(n) and n >= 0 and int(n) == n
                         for n in http.values())
    total, failed = http["http_requests_total"], http["http_requests_failed"]
    unfinished = total - http["http_requests_succeeded"] - failed if counts_present else None
    classified = sum(http[name] for name in (
        "http_responses_4xx", "http_responses_5xx", "http_transport_errors", "http_unexpected_responses"
    )) if counts_present else None
    valid = (measured and returncode == 0 and thresholds_ok and verification["status"] == "passed"
             and elapsed > 0 and completed > 0 and attempted == completed
             and value("technical_errors", "count") == 0
             and value("unknown_writes", "count") == 0
             and value("dropped_iterations", "count") == 0
             and counts_present and total > 0 and unfinished == 0 and failed == classified == 0
             and http["http_timeouts"] <= http["http_transport_errors"])
    return dict(run, status="not_run" if not measured else "passed" if valid else "failed",
                verification=verification["status"], attempted=attempted,
                completed=completed, completed_per_s=completed / elapsed if elapsed else None,
                failed_workflows=attempted - completed if measured else None,
                workflow_error_percent=100 * (attempted - completed) / attempted if attempted else None,
                **http, http_requests_unfinished=unfinished,
                http_error_percent=100 * failed / total if counts_present and total else None,
                measured_seconds=elapsed, configured_seconds=duration,
                actual_http_per_s=value("http_reqs", "rate"), http_p50_ms=value("http_req_duration", "med", ""),
                http_p95_ms=value("http_req_duration", "p(95)", ""), http_p99_ms=value("http_req_duration", "p(99)", ""),
                workflow_p50_ms=value("workflow_duration", "med", ""), workflow_p95_ms=value("workflow_duration", "p(95)", ""),
                workflow_p99_ms=value("workflow_duration", "p(99)", ""),
                technical_errors=value("technical_errors", "count"), unknown_writes=value("unknown_writes", "count"),
                dropped_iterations=value("dropped_iterations", "count"))


def main(argv=None):
    return Experiment(arguments(argv)).execute()


if __name__ == "__main__":
    sys.exit(main())
