"""Inspect or reset only the isolated benchmark's RabbitMQ queues."""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import aio_pika
from aio_pika.abc import AbstractQueue
from psycopg.conninfo import conninfo_to_dict

# The same helper runs from the repository and as /benchmarks/messaging.py
# beside the application's /api directory in its container.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "api"))

from adapters.messaging.settings import MessagingSettings
from adapters.messaging.topology import declare_topology


BENCHMARK_DATABASE = "return_flow_benchmark"
BENCHMARK_QUEUE = "return_flow.benchmark.commands"


def benchmark_settings() -> MessagingSettings:
    """Reject development defaults before connecting to or mutating RabbitMQ."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url or conninfo_to_dict(database_url).get("dbname") != BENCHMARK_DATABASE:
        raise ValueError(f"Messaging helpers require database {BENCHMARK_DATABASE!r}.")
    if os.environ.get("RABBITMQ_QUEUE") != BENCHMARK_QUEUE:
        raise ValueError(f"Messaging helpers require queue {BENCHMARK_QUEUE!r}.")
    return MessagingSettings.from_env()


def queue_counts(queue: AbstractQueue) -> dict[str, int]:
    result = queue.declaration_result
    ready, consumers = result.message_count, result.consumer_count
    if not isinstance(ready, int) or not isinstance(consumers, int) or min(ready, consumers) < 0:
        raise ValueError(f"RabbitMQ did not report valid counts for queue {queue.name!r}.")
    return {"ready": ready, "consumers": consumers}


async def inspect_queues(*, reset: bool = False) -> dict:
    """Return a queue snapshot; reset additionally records counts actually purged.

    The runner must stop both API and worker first for a reset. Consumer counts
    are an additional check, not a lock against a process reconnecting later.
    """
    settings = benchmark_settings()
    connection = await aio_pika.connect(settings.url, timeout=settings.connect_timeout)
    async with connection:
        channel = await connection.channel()
        pending = await declare_topology(channel, settings.queue_name)
        failed = await channel.declare_queue(f"{settings.queue_name}.failed", durable=True)
        queues = {"pending": pending, "failed": failed}
        counts = {name: queue_counts(queue) for name, queue in queues.items()}
        result = {"status": "passed", "queues": counts}
        if reset:
            # Check BOTH queues before purging either, so a DLQ consumer also
            # prevents clearing the work queue.
            if any(values["consumers"] for values in counts.values()):
                raise RuntimeError("Cannot reset benchmark queues while consumers are connected.")
            result["purged"] = {
                name: (await queue.purge(timeout=settings.publish_timeout)).message_count
                for name, queue in queues.items()
            }
        return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect", "reset"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        result = asyncio.run(inspect_queues(reset=args.command == "reset"))
    except Exception as error:
        result = {"status": "failed", "errors": [str(error)]}
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded)
    print(encoded, end="")
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
