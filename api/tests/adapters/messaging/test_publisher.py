import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from aio_pika import DeliveryMode
from aio_pika.exceptions import ChannelInvalidStateError, DeliveryError
from pamqp.commands import Basic

from adapters.messaging import publisher as publisher_module
from adapters.commands.contracts import EnsureDeliveryDelivered
from adapters.messaging.publisher import MessagingUnavailableError, RabbitMQPublisher
from adapters.messaging.settings import MessagingSettings

pytestmark = pytest.mark.anyio


def make_transport():
    underlay = SimpleNamespace(basic_publish=AsyncMock(return_value=Basic.Ack()))
    channel = SimpleNamespace(
        is_closed=False,
        get_underlay_channel=AsyncMock(return_value=underlay),
    )
    connection = SimpleNamespace(
        is_closed=False, channel=AsyncMock(return_value=channel), close=AsyncMock()
    )
    closed = asyncio.Event()

    async def close():
        closed.set()

    connection.close.side_effect = close
    return SimpleNamespace(
        channel=channel, underlay=underlay, connection=connection, closed=closed
    )


@pytest.fixture
def transport(monkeypatch):
    transport = make_transport()
    connection, channel = transport.connection, transport.channel
    connect = AsyncMock(return_value=connection)
    topology = AsyncMock()
    monkeypatch.setattr(publisher_module.aio_pika, "connect", connect)
    monkeypatch.setattr(publisher_module, "declare_topology", topology)
    return SimpleNamespace(
        channel=channel,
        underlay=transport.underlay,
        connection=connection,
        connect=connect,
        topology=topology,
        closed=transport.closed,
    )


async def test_publisher_waits_for_confirmation_and_reuses_connection(transport):
    publisher = RabbitMQPublisher(MessagingSettings(url="amqp://unused/"))
    command = EnsureDeliveryDelivered(delivery_id=uuid4())
    started = asyncio.Event()
    confirmed = asyncio.Event()

    async def publish(*args, **kwargs):
        started.set()
        await confirmed.wait()
        return Basic.Ack()

    transport.underlay.basic_publish.side_effect = publish
    task = asyncio.create_task(publisher.publish(command))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        assert not task.done()
        confirmed.set()
        await task
        await publisher.publish(command)
        transport.connect.assert_awaited_once()
        transport.connection.channel.assert_awaited_once_with(
            publisher_confirms=True, on_return_raises=True
        )
        transport.topology.assert_awaited_once_with(
            transport.channel, publisher.settings.queue_name
        )
        call = transport.underlay.basic_publish.call_args
        properties = call.kwargs["properties"]
        assert EnsureDeliveryDelivered.model_validate_json(call.kwargs["body"]) == command
        assert properties.delivery_mode == DeliveryMode.PERSISTENT
        assert properties.content_type == "application/json"
        assert properties.content_encoding == "utf-8"
        assert properties.message_type == command.type
        assert UUID(properties.message_id)
        assert properties.correlation_id == str(command.command_id)
        assert call.kwargs["exchange"] == ""
        assert call.kwargs["routing_key"] == publisher.settings.queue_name
        assert call.kwargs["mandatory"] is True
        assert call.kwargs["wait"] is False
        assert call.kwargs["timeout"] == publisher.settings.publish_timeout
    finally:
        confirmed.set()
        await task
        await publisher.close()
    transport.connection.close.assert_awaited_once()


@pytest.mark.parametrize(
    "error",
    [
        OSError("Connection refused"),
        TimeoutError(),
        ChannelInvalidStateError("Channel closed"),
        DeliveryError(None, Basic.Nack()),
    ],
)
async def test_failed_publish_is_not_retried_and_next_request_reconnects(transport, error):
    publisher = RabbitMQPublisher(MessagingSettings(url="amqp://unused/"))
    transport.underlay.basic_publish.side_effect = error
    command = EnsureDeliveryDelivered(delivery_id=uuid4())

    with pytest.raises(MessagingUnavailableError):
        await publisher.publish(command)

    transport.underlay.basic_publish.assert_awaited_once()
    await asyncio.wait_for(transport.closed.wait(), timeout=1)
    transport.connection.close.assert_awaited_once()
    transport.underlay.basic_publish.side_effect = None
    await publisher.publish(command)
    assert transport.connect.await_count == 2
    await publisher.close()


async def test_unavailable_broker_is_reported_without_publishing(transport):
    publisher = RabbitMQPublisher(MessagingSettings(url="amqp://unused/"))
    transport.connect.side_effect = ConnectionRefusedError()
    with pytest.raises(MessagingUnavailableError):
        await publisher.publish(EnsureDeliveryDelivered(delivery_id=uuid4()))
    transport.underlay.basic_publish.assert_not_awaited()


async def test_publication_has_a_deadline_even_when_confirmation_never_arrives(transport):
    publisher = RabbitMQPublisher(
        MessagingSettings(url="amqp://unused/", publish_timeout=0.02)
    )

    async def never_confirm(*args, **kwargs):
        await asyncio.Event().wait()

    transport.underlay.basic_publish.side_effect = never_confirm
    with pytest.raises(MessagingUnavailableError):
        await asyncio.wait_for(
            publisher.publish(EnsureDeliveryDelivered(delivery_id=uuid4())), timeout=1
        )
    await publisher.close()
    transport.connection.close.assert_awaited_once()


@pytest.mark.parametrize("confirmation", [None, Basic.Nack(), Basic.Reject()])
async def test_missing_or_negative_confirmation_is_not_accepted(transport, confirmation):
    publisher = RabbitMQPublisher(MessagingSettings(url="amqp://unused/"))
    transport.underlay.basic_publish.return_value = confirmation
    with pytest.raises(MessagingUnavailableError):
        await publisher.publish(EnsureDeliveryDelivered(delivery_id=uuid4()))
    await publisher.close()


async def test_cancelled_publish_closes_connection_and_propagates(transport):
    publisher = RabbitMQPublisher(MessagingSettings(url="amqp://unused/"))
    transport.underlay.basic_publish.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await publisher.publish(EnsureDeliveryDelivered(delivery_id=uuid4()))
    await publisher.close()
    transport.connection.close.assert_awaited_once()


async def test_publications_overlap_with_a_limit_and_individual_confirmations(transport):
    publisher = RabbitMQPublisher(
        MessagingSettings(url="amqp://unused/", publish_concurrency=2)
    )
    started = asyncio.Queue()
    confirmations = [asyncio.Event() for _ in range(3)]
    messages = []

    async def publish(*, body, properties, **kwargs):
        index = len(messages)
        messages.append(SimpleNamespace(body=body, properties=properties))
        await started.put(index)
        await confirmations[index].wait()
        return Basic.Ack()

    transport.underlay.basic_publish.side_effect = publish
    # Duplicate commands may be published concurrently, with independently
    # correlated transport confirmations and the same business idempotency key.
    command = EnsureDeliveryDelivered(delivery_id=uuid4())
    tasks = [asyncio.create_task(publisher.publish(command)) for _ in range(3)]
    try:
        assert await asyncio.wait_for(started.get(), timeout=1) == 0
        assert await asyncio.wait_for(started.get(), timeout=1) == 1
        assert len(messages) == 2
        assert not any(task.done() for task in tasks)
        transport.connect.assert_awaited_once()
        transport.topology.assert_awaited_once()

        confirmations[1].set()
        await asyncio.wait_for(tasks[1], timeout=1)
        assert not tasks[0].done()
        assert await asyncio.wait_for(started.get(), timeout=1) == 2
        assert not tasks[2].done()
        assert len({message.properties.message_id for message in messages}) == 3
        assert {message.properties.correlation_id for message in messages} == {
            str(command.command_id)
        }
        assert all(
            EnsureDeliveryDelivered.model_validate_json(message.body) == command
            for message in messages
        )
    finally:
        for confirmation in confirmations:
            confirmation.set()
        await asyncio.gather(*tasks)
        await publisher.close()


async def test_connection_setup_is_shared_and_cancelling_a_waiter_does_not_close_it(
    transport,
):
    publisher = RabbitMQPublisher(MessagingSettings(url="amqp://unused/"))
    started = asyncio.Event()
    ready = asyncio.Event()

    async def topology(*args, **kwargs):
        started.set()
        await ready.wait()

    transport.topology.side_effect = topology
    command = EnsureDeliveryDelivered(delivery_id=uuid4())
    tasks = [asyncio.create_task(publisher.publish(command)) for _ in range(3)]
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        tasks[1].cancel()
        with pytest.raises(asyncio.CancelledError):
            await tasks[1]
        transport.connect.assert_awaited_once()
        transport.connection.close.assert_not_awaited()
        ready.set()
        await asyncio.gather(tasks[0], tasks[2])
        transport.connect.assert_awaited_once()
        assert transport.underlay.basic_publish.await_count == 2
    finally:
        ready.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await publisher.close()


@pytest.mark.parametrize("sibling_fails", [False, True])
async def test_cancellation_preserves_sibling_and_late_failure_preserves_replacement(
    transport, sibling_fails
):
    publisher = RabbitMQPublisher(MessagingSettings(url="amqp://unused/"))
    replacement = make_transport()
    transport.connect.side_effect = [transport.connection, replacement.connection]
    started = asyncio.Queue()
    confirmations = [asyncio.Event(), asyncio.Event()]
    calls = 0

    async def publish(*args, **kwargs):
        nonlocal calls
        index = calls
        calls += 1
        await started.put(index)
        await confirmations[index].wait()
        if sibling_fails:
            raise OSError("Old connection failed")
        return Basic.Ack()

    transport.underlay.basic_publish.side_effect = publish
    command = EnsureDeliveryDelivered(delivery_id=uuid4())
    tasks = [asyncio.create_task(publisher.publish(command)) for _ in range(2)]
    try:
        await asyncio.wait_for(started.get(), timeout=1)
        await asyncio.wait_for(started.get(), timeout=1)
        tasks[0].cancel()
        with pytest.raises(asyncio.CancelledError):
            await tasks[0]
        assert not tasks[1].done()
        transport.connection.close.assert_not_awaited()

        await publisher.publish(command)
        assert transport.connect.await_count == 2
        confirmations[1].set()
        if sibling_fails:
            with pytest.raises(MessagingUnavailableError):
                await tasks[1]
        else:
            await tasks[1]
        await asyncio.wait_for(transport.closed.wait(), timeout=1)
        transport.connection.close.assert_awaited_once()
        replacement.connection.close.assert_not_awaited()

        await publisher.publish(command)
        assert transport.connect.await_count == 2
        assert replacement.underlay.basic_publish.await_count == 2
    finally:
        for confirmation in confirmations:
            confirmation.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await publisher.close()
    replacement.connection.close.assert_awaited_once()


async def test_deadline_includes_waiting_for_capacity_without_touching_connection(transport):
    publisher = RabbitMQPublisher(
        MessagingSettings(url="amqp://unused/", publish_concurrency=1, publish_timeout=0.02)
    )
    command = EnsureDeliveryDelivered(delivery_id=uuid4())
    await publisher.publish(command)
    async with publisher._slots:
        with pytest.raises(MessagingUnavailableError):
            await asyncio.wait_for(publisher.publish(command), timeout=1)
    transport.connection.close.assert_not_awaited()
    transport.underlay.basic_publish.assert_awaited_once()
    await publisher.publish(command)
    transport.connect.assert_awaited_once()
    await publisher.close()


async def test_deadline_does_not_wait_for_connection_cleanup(transport):
    publisher = RabbitMQPublisher(
        MessagingSettings(url="amqp://unused/", publish_timeout=0.02)
    )
    cleanup = asyncio.Event()

    async def close():
        await cleanup.wait()

    async def publish(*args, **kwargs):
        await asyncio.Event().wait()

    transport.connection.close.side_effect = close
    transport.underlay.basic_publish.side_effect = publish
    try:
        with pytest.raises(MessagingUnavailableError):
            await asyncio.wait_for(
                publisher.publish(EnsureDeliveryDelivered(delivery_id=uuid4())), timeout=1
            )
    finally:
        cleanup.set()
        await publisher.close()


@pytest.mark.parametrize("cancelled", [False, True])
async def test_incomplete_setup_closes_connection_and_can_reconnect(transport, cancelled):
    publisher = RabbitMQPublisher(MessagingSettings(url="amqp://unused/"))
    transport.topology.side_effect = asyncio.CancelledError() if cancelled else OSError()
    expected_error = asyncio.CancelledError if cancelled else MessagingUnavailableError
    command = EnsureDeliveryDelivered(delivery_id=uuid4())
    with pytest.raises(expected_error):
        await publisher.publish(command)
    transport.underlay.basic_publish.assert_not_awaited()
    await asyncio.wait_for(transport.closed.wait(), timeout=1)
    transport.topology.side_effect = None
    await publisher.publish(command)
    assert transport.connect.await_count == 2
    await publisher.close()


async def test_shutdown_waits_for_confirmations_and_rejects_new_publications(transport):
    publisher = RabbitMQPublisher(MessagingSettings(url="amqp://unused/"))
    started = asyncio.Event()
    confirmed = asyncio.Event()

    async def publish(*args, **kwargs):
        started.set()
        await confirmed.wait()
        return Basic.Ack()

    transport.underlay.basic_publish.side_effect = publish
    command = EnsureDeliveryDelivered(delivery_id=uuid4())
    task = asyncio.create_task(publisher.publish(command))
    await asyncio.wait_for(started.wait(), timeout=1)
    closing = asyncio.create_task(publisher.close())
    try:
        # Yield until shutdown marks the publisher closed, without releasing
        # the in-flight broker confirmation.
        await asyncio.sleep(0)
        assert not closing.done()
        transport.connection.close.assert_not_awaited()
        with pytest.raises(MessagingUnavailableError):
            await publisher.publish(command)
    finally:
        confirmed.set()
        await task
        await asyncio.wait_for(closing, timeout=1)
    transport.connection.close.assert_awaited_once()
