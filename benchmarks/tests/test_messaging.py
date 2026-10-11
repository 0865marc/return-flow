import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from benchmarks import messaging


@pytest.fixture
def isolated_environment(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://benchmark:benchmark@postgres/return_flow_benchmark")
    monkeypatch.setenv("RABBITMQ_QUEUE", "return_flow.benchmark.commands")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://benchmark:benchmark@rabbitmq/")


@pytest.mark.parametrize("variable,value", [
    ("DATABASE_URL", None),
    ("DATABASE_URL", "postgresql://localhost/return_flow"),
    ("RABBITMQ_QUEUE", None),
    ("RABBITMQ_QUEUE", "return_flow.deliveries.commands"),
])
def test_helper_rejects_non_benchmark_configuration_before_connecting(
    monkeypatch, isolated_environment, variable, value,
):
    if value is None:
        monkeypatch.delenv(variable)
    else:
        monkeypatch.setenv(variable, value)
    connect = AsyncMock()
    monkeypatch.setattr(messaging.aio_pika, "connect", connect)

    with pytest.raises(ValueError, match="require"):
        asyncio.run(messaging.inspect_queues(reset=True))

    connect.assert_not_awaited()


@pytest.fixture
def broker(monkeypatch, isolated_environment):
    def queue(name, ready):
        return SimpleNamespace(
            name=name,
            declaration_result=SimpleNamespace(message_count=ready, consumer_count=0),
            purge=AsyncMock(return_value=SimpleNamespace(message_count=ready)),
        )

    pending = queue(messaging.BENCHMARK_QUEUE, 3)
    failed = queue(messaging.BENCHMARK_QUEUE + ".failed", 2)
    channel = SimpleNamespace(declare_queue=AsyncMock(return_value=failed))
    connection = AsyncMock()
    connection.channel.return_value = channel
    monkeypatch.setattr(messaging.aio_pika, "connect", AsyncMock(return_value=connection))
    monkeypatch.setattr(messaging, "declare_topology", AsyncMock(return_value=pending))
    return {"pending": pending, "failed": failed}


def test_inspection_reports_both_queues_without_purging(broker):
    result = asyncio.run(messaging.inspect_queues())

    assert result == {"status": "passed", "queues": {
        "pending": {"ready": 3, "consumers": 0},
        "failed": {"ready": 2, "consumers": 0},
    }}
    for queue in broker.values():
        queue.purge.assert_not_awaited()


@pytest.mark.parametrize("queue_name", ["pending", "failed"])
def test_any_connected_consumer_prevents_both_purges(broker, queue_name):
    broker[queue_name].declaration_result.consumer_count = 1

    with pytest.raises(RuntimeError, match="consumers"):
        asyncio.run(messaging.inspect_queues(reset=True))

    for queue in broker.values():
        queue.purge.assert_not_awaited()


def test_reset_purges_work_and_dead_letter_queues(broker):
    result = asyncio.run(messaging.inspect_queues(reset=True))

    assert result["purged"] == {"pending": 3, "failed": 2}
    for queue in broker.values():
        queue.purge.assert_awaited_once()


def test_missing_consumer_count_does_not_authorize_reset(broker):
    broker["pending"].declaration_result.consumer_count = None

    with pytest.raises(ValueError, match="valid counts"):
        asyncio.run(messaging.inspect_queues(reset=True))

    for queue in broker.values():
        queue.purge.assert_not_awaited()


def test_cli_writes_failed_inspection_to_output(monkeypatch, tmp_path):
    monkeypatch.setattr(messaging, "inspect_queues", AsyncMock(side_effect=RuntimeError("Unavailable")))
    output = tmp_path / "phase" / "queues.json"

    assert messaging.main(["inspect", "--output", str(output)]) == 1
    assert json.loads(output.read_text()) == {"status": "failed", "errors": ["Unavailable"]}
