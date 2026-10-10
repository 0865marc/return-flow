import asyncio
import signal
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import worker
from adapters.messaging.settings import MessagingSettings

pytestmark = pytest.mark.anyio


@pytest.fixture
def worker_resources(monkeypatch):
    events = []
    repository = object()
    factory = object()
    messages = [object(), object()]
    handler = AsyncMock()

    @asynccontextmanager
    async def persistence(database_url, *, factory):
        events.append("persistence opened")
        try:
            yield SimpleNamespace(commands=repository)
        finally:
            events.append("persistence closed")

    @asynccontextmanager
    async def iterator():
        events.append("iterator opened")

        async def iterate():
            for message in messages:
                yield message

        try:
            yield iterate()
        finally:
            events.append("iterator closed")

    class Connection:
        channel = AsyncMock()

        async def __aenter__(self):
            events.append("connection opened")
            return self

        async def __aexit__(self, *exc_info):
            events.append("connection closed")

    channel = SimpleNamespace(set_qos=AsyncMock())
    connection = Connection()
    connection.channel.return_value = channel
    queue = SimpleNamespace(iterator=Mock(side_effect=iterator))
    connect = AsyncMock(return_value=connection)
    topology = AsyncMock(return_value=queue)
    get_factory = Mock(return_value=factory)
    open_persistence = Mock(side_effect=persistence)
    make_consumer = Mock(return_value=SimpleNamespace(handle=handler))
    monkeypatch.setattr(worker, "get_persistence_factory", get_factory)
    monkeypatch.setattr(worker, "open_persistence", open_persistence)
    monkeypatch.setattr(worker, "connect_robust", connect)
    monkeypatch.setattr(worker, "declare_topology", topology)
    monkeypatch.setattr(worker, "CommandConsumer", make_consumer)
    return SimpleNamespace(
        events=events,
        repository=repository,
        factory=factory,
        get_factory=get_factory,
        open_persistence=open_persistence,
        messages=messages,
        handler=handler,
        connection=connection,
        channel=channel,
        connect=connect,
        topology=topology,
        make_consumer=make_consumer,
    )


async def test_worker_wires_resources_and_closes_them_in_reverse_order(worker_resources):
    resources = worker_resources
    settings = MessagingSettings(
        url="amqp://localhost", queue_name="test.commands", connect_timeout=3
    )

    await worker.run_worker(
        settings, "postgresql://localhost/test", persistence_adapter="postgres"
    )

    resources.get_factory.assert_called_once_with("postgres")
    resources.open_persistence.assert_called_once_with(
        "postgresql://localhost/test", factory=resources.factory
    )
    resources.connect.assert_awaited_once_with(settings.url, timeout=3)
    resources.channel.set_qos.assert_awaited_once_with(prefetch_count=1)
    resources.topology.assert_awaited_once_with(resources.channel, "test.commands")
    resources.make_consumer.assert_called_once_with(resources.repository)
    assert [
        call.args[0] for call in resources.handler.await_args_list
    ] == resources.messages
    assert resources.events == [
        "persistence opened",
        "connection opened",
        "iterator opened",
        "iterator closed",
        "connection closed",
        "persistence closed",
    ]


@pytest.mark.parametrize("failure_stage", ["connect", "channel", "topology", "handle"])
async def test_worker_closes_acquired_resources_when_startup_or_processing_fails(
    worker_resources, failure_stage
):
    resources = worker_resources
    failing_call = {
        "connect": resources.connect,
        "channel": resources.connection.channel,
        "topology": resources.topology,
        "handle": resources.handler,
    }[failure_stage]
    failing_call.side_effect = ConnectionError("Connection lost")

    with pytest.raises(ConnectionError, match="Connection lost"):
        await worker.run_worker(MessagingSettings(url="amqp://localhost"))

    assert resources.events[-1] == "persistence closed"
    if failure_stage != "connect":
        assert resources.events[-2] == "connection closed"
    if failure_stage == "handle":
        assert resources.events[-3] == "iterator closed"


async def test_worker_cancellation_unwinds_in_flight_processing_and_resources(
    worker_resources,
):
    resources = worker_resources
    processing_started = asyncio.Event()

    async def handle(message):
        processing_started.set()
        await asyncio.Future()

    resources.handler.side_effect = handle
    task = asyncio.create_task(
        worker.run_worker(MessagingSettings(url="amqp://localhost"))
    )
    await asyncio.wait_for(processing_started.wait(), timeout=1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert resources.handler.await_count == 1
    assert resources.events[-3:] == [
        "iterator closed",
        "connection closed",
        "persistence closed",
    ]


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
async def test_main_handles_shutdown_signal_and_removes_signal_handlers(
    monkeypatch, signum
):
    registered = {}
    removed = []
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def run_worker():
        started.set()
        try:
            await asyncio.Future()
        finally:
            stopped.set()

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(
        loop,
        "add_signal_handler",
        lambda signal, callback: registered.setdefault(signal, callback),
    )
    monkeypatch.setattr(loop, "remove_signal_handler", removed.append)
    monkeypatch.setattr(worker, "run_worker", run_worker)
    task = asyncio.create_task(worker.main())
    await asyncio.wait_for(started.wait(), timeout=1)
    registered[signum]()
    await asyncio.wait_for(task, timeout=1)

    assert stopped.is_set()
    assert set(registered) == {signal.SIGTERM, signal.SIGINT}
    assert set(removed) == set(registered)


async def test_main_propagates_failure_and_removes_signal_handlers(monkeypatch):
    registered = Mock()
    removed = Mock()
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", registered)
    monkeypatch.setattr(loop, "remove_signal_handler", removed)
    monkeypatch.setattr(
        worker, "run_worker", AsyncMock(side_effect=ConnectionError("Broker down"))
    )

    with pytest.raises(ConnectionError, match="Broker down"):
        await worker.main()

    expected_signals = [signal.SIGTERM, signal.SIGINT]
    assert [call.args[0] for call in registered.call_args_list] == expected_signals
    assert [call.args[0] for call in removed.call_args_list] == expected_signals
