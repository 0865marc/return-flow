"""Sample only this experiment's containers without a monitoring service."""

import json
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path


class ResourceSampler:
    def __init__(self, project: str, output: Path) -> None:
        self.project = project
        self.output = output
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._sample, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop_event.set()
        self.thread.join(timeout=15)

    def _sample(self) -> None:
        with self.output.open("w") as stream:
            while not self.stop_event.is_set():
                record = {"timestamp": datetime.now(timezone.utc).isoformat()}
                try:
                    containers = subprocess.check_output(
                        ["docker", "ps", "--format", "{{.ID}} {{.Names}}", "--filter",
                         f"label=com.docker.compose.project={self.project}"],
                        text=True, stderr=subprocess.PIPE, timeout=5,
                    ).splitlines()
                    # Wait for the generator so short runs do not sample only startup.
                    if not any(line.endswith(f"{self.project}-load") for line in containers):
                        self.stop_event.wait(0.5)
                        continue
                    ids = [line.split()[0] for line in containers]
                    if ids:
                        result = subprocess.run(
                            ["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids],
                            capture_output=True, text=True, timeout=8,
                        )
                        record["containers"] = [
                            json.loads(line) for line in result.stdout.splitlines() if line
                        ]
                        if result.returncode:
                            record["warning"] = result.stderr.strip()
                    else:
                        record["containers"] = []
                except (OSError, subprocess.SubprocessError, ValueError) as error:
                    record["warning"] = str(error)
                stream.write(json.dumps(record) + "\n")
                stream.flush()
                self.stop_event.wait(2)


def sample_coverage(path: Path) -> dict:
    coverage = {"api": 0, "postgres": 0, "k6": 0}
    warnings = []
    if path.exists():
        for line in path.read_text().splitlines():
            sample = json.loads(line)
            if sample.get("warning"):
                warnings.append(sample["warning"])
            for container in sample.get("containers", []):
                name = container.get("Name", "")
                for service, suffix in (("api", "-api-1"), ("postgres", "-postgres-1"), ("k6", "-load")):
                    if name.endswith(suffix):
                        coverage[service] += 1
    for service, count in coverage.items():
        if not count:
            warnings.append(f"No resource samples for {service}.")
    return {"samples_by_service": coverage, "warnings": warnings}
