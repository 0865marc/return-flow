import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from aio_pika import DeliveryMode
from aio_pika.exceptions import ChannelInvalidStateError, DeliveryError
from pamqp.commands import Basic

from adapters.messaging import publisher as publisher_module
from adapters.messaging.contracts import EnsureDeliveryDelivered
from adapters.messaging.publisher import MessagingUnavailableError, RabbitMQPublisher
from adapters.messaging.settings import MessagingSettings

pytestmark = pytest.mark.anyio


@pytest.fixture
def transport(monkeypatch):
    channel = SimpleNamespace(
        is_closed=False,
        default_exchange=SimpleNamespace(publish=AsyncMock(return_value=Basic.Ack())),
    )
    connection = SimpleNamespace(
        is_closed=False, channel=AsyncMock(return_value=channel), close=AsyncMock()
    )
    connect = AsyncMock(return_value=connection)
    topology = AsyncMock()
    monkeypatch.setattr(publisher_module.aio_pika, "connect", connect)
    monkeypatch.setattr(publisher_module, "declare_topology", topology)
    return SimpleNamespace(
        channel=channel, connection=connection, connect=connect, topology=topology
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

    transport.channel.default_exchange.publish.side_effect = publish
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
        call = transport.channel.default_exchange.publish.call_args
        message = call.args[0]
        assert EnsureDeliveryDelivered.model_validate_json(message.body) == command
        assert message.delivery_mode == DeliveryMode.PERSISTENT
        assert message.message_id == str(command.command_id)
        assert call.kwargs["mandatory"] is True
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
    transport.channel.default_exchange.publish.side_effect = error
    command = EnsureDeliveryDelivered(delivery_id=uuid4())

    with pytest.raises(MessagingUnavailableError):
        await publisher.publish(command)

    transport.channel.default_exchange.publish.assert_awaited_once()
    transport.connection.close.assert_awaited_once()
    transport.channel.default_exchange.publish.side_effect = None
    await publisher.publish(command)
    assert transport.connect.await_count == 2
    await publisher.close()


async def test_unavailable_broker_is_reported_without_publishing(transport):
    publisher = RabbitMQPublisher(MessagingSettings(url="amqp://unused/"))
    transport.connect.side_effect = ConnectionRefusedError()
    with pytest.raises(MessagingUnavailableError):
        await publisher.publish(EnsureDeliveryDelivered(delivery_id=uuid4()))
    transport.channel.default_exchange.publish.assert_not_awaited()


async def test_publication_has_a_deadline_even_when_confirmation_never_arrives(transport):
    publisher = RabbitMQPublisher(
        MessagingSettings(url="amqp://unused/", publish_timeout=0.02)
    )

    async def never_confirm(*args, **kwargs):
        await asyncio.Event().wait()

    transport.channel.default_exchange.publish.side_effect = never_confirm
    with pytest.raises(MessagingUnavailableError):
        await asyncio.wait_for(
            publisher.publish(EnsureDeliveryDelivered(delivery_id=uuid4())), timeout=1
        )
    transport.connection.close.assert_awaited_once()


async def test_missing_confirmation_is_not_accepted(transport):
    publisher = RabbitMQPublisher(MessagingSettings(url="amqp://unused/"))
    transport.channel.default_exchange.publish.return_value = None
    with pytest.raises(MessagingUnavailableError):
        await publisher.publish(EnsureDeliveryDelivered(delivery_id=uuid4()))


async def test_cancelled_publish_closes_connection_and_propagates(transport):
    publisher = RabbitMQPublisher(MessagingSettings(url="amqp://unused/"))
    transport.channel.default_exchange.publish.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await publisher.publish(EnsureDeliveryDelivered(delivery_id=uuid4()))
    transport.connection.close.assert_awaited_once()
