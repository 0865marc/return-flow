import asyncio
import signal
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

import worker
from adapters.commands.contracts import CreateDeliveryCommand
from adapters.messaging.consumer import CommandConsumer
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
    open_persistence = Mock(side_effect=persistence)
    make_consumer = Mock(return_value=SimpleNamespace(handle=handler))
    monkeypatch.setattr(worker, "open_persistence", open_persistence)
    monkeypatch.setattr(worker, "connect_robust", connect)
    monkeypatch.setattr(worker, "declare_topology", topology)
    monkeypatch.setattr(worker, "CommandConsumer", make_consumer)
    return SimpleNamespace(
        events=events,
        repository=repository,
        factory=factory,
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
        settings, "postgresql://localhost/test", persistence_factory=resources.factory
    )

    resources.open_persistence.assert_called_once_with(
        "postgresql://localhost/test", factory=resources.factory
    )
    resources.connect.assert_awaited_once_with(settings.url, timeout=3)
    resources.channel.set_qos.assert_awaited_once_with(prefetch_count=8)
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


@pytest.mark.parametrize("failure_stage", ["connect", "channel", "topology"])
async def test_worker_closes_acquired_resources_when_startup_or_processing_fails(
    worker_resources, failure_stage
):
    resources = worker_resources
    failing_call = {
        "connect": resources.connect,
        "channel": resources.connection.channel,
        "topology": resources.topology,
    }[failure_stage]
    failing_call.side_effect = ConnectionError("Connection lost")

    with pytest.raises(ConnectionError, match="Connection lost"):
        await worker.run_worker(MessagingSettings(url="amqp://localhost"))

    assert resources.events[-1] == "persistence closed"
    if failure_stage != "connect":
        assert resources.events[-2] == "connection closed"


@pytest.mark.parametrize("concurrency", [1, 3, 8])
async def test_worker_overlaps_processing_and_bounds_tasks(worker_resources, concurrency):
    resources = worker_resources
    resources.messages[:] = [object() for _ in range(concurrency * 3)]
    capacity_reached = asyncio.Event()
    release = asyncio.Event()
    active = 0
    maximum_active = 0
    finished = []

    async def handle(message):
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(active, maximum_active)
        if active == concurrency:
            capacity_reached.set()
        try:
            await release.wait()
            finished.append(message)
        finally:
            active -= 1

    resources.handler.side_effect = handle
    existing_tasks = asyncio.all_tasks()
    task = asyncio.create_task(
        worker.run_worker(
            MessagingSettings(url="amqp://localhost", worker_concurrency=concurrency)
        )
    )
    try:
        await asyncio.wait_for(capacity_reached.wait(), timeout=1)
        assert resources.handler.await_count == concurrency
        # A waiting task must not be created for every queued message.
        assert len(asyncio.all_tasks() - existing_tasks) == concurrency + 1
        resources.channel.set_qos.assert_awaited_once_with(prefetch_count=concurrency)
        assert resources.events[-1] == "iterator opened"
        release.set()
        await asyncio.wait_for(task, timeout=1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert maximum_active == concurrency
    assert set(finished) == set(resources.messages)
    assert active == 0
    assert resources.events[-3:] == [
        "iterator closed",
        "connection closed",
        "persistence closed",
    ]


@pytest.mark.parametrize("stop_reason", ["cancellation", "handler_failure"])
async def test_worker_cancels_and_awaits_all_handlers_before_closing_resources(
    worker_resources, stop_reason
):
    resources = worker_resources
    commands = [CreateDeliveryCommand() for _ in range(3)]
    resources.messages[:] = [
        SimpleNamespace(
            body=command.model_dump_json().encode(),
            message_id=str(command.command_id),
            ack=AsyncMock(),
            reject=AsyncMock(),
        )
        for command in commands
    ]
    all_started = asyncio.Event()
    trigger_failure = asyncio.Event()
    started = []
    stopped = []

    async def execute(command, handler):
        started.append(command.command_id)
        if len(started) == len(commands):
            all_started.set()
        try:
            if command == commands[0] and stop_reason == "handler_failure":
                await trigger_failure.wait()
                raise ConnectionError("Database connection lost")
            await asyncio.Future()
        finally:
            # Simulate asynchronous transaction rollback while resources are open.
            await asyncio.sleep(0)
            assert resources.events[-1] == "iterator opened"
            stopped.append(command.command_id)

    store = SimpleNamespace(execute=AsyncMock(side_effect=execute))
    resources.make_consumer.return_value = CommandConsumer(store)
    task = asyncio.create_task(
        worker.run_worker(
            MessagingSettings(url="amqp://localhost", worker_concurrency=3)
        )
    )
    try:
        await asyncio.wait_for(all_started.wait(), timeout=1)
        for message in resources.messages:
            message.ack.assert_not_awaited()
            message.reject.assert_not_awaited()
        if stop_reason == "cancellation":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=1)
        else:
            trigger_failure.set()
            with pytest.raises(ExceptionGroup) as raised:
                await asyncio.wait_for(task, timeout=1)
            assert len(raised.value.exceptions) == 1
            error = raised.value.exceptions[0]
            assert isinstance(error, ConnectionError)
            assert str(error) == "Database connection lost"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert set(started) == set(stopped) == {command.command_id for command in commands}
    for message in resources.messages:
        message.ack.assert_not_awaited()
        message.reject.assert_not_awaited()
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
