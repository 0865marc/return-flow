import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pydantic import ValidationError

from adapters.messaging.consumer import CommandConsumer
from adapters.commands.executor import execute_command
from adapters.commands.contracts import (
    CommandRecord,
    CreateDeliveryCommand,
    EnsureDeliveryDelivered,
    IdempotencyConflictError,
    RequestReturnCommand,
)
from adapters.persistence import Repositories
from domain import Delivery, DeliveryStatus
from tests.application.fakes import InMemoryDeliveryRepository, InMemoryReturnRepository

pytestmark = pytest.mark.anyio


def incoming_message(command=None):
    command = command or CreateDeliveryCommand()
    return SimpleNamespace(
        body=command.model_dump_json().encode(),
        message_id=str(command.command_id),
        ack=AsyncMock(),
        reject=AsyncMock(),
    )


def completed(command):
    return CommandRecord(
        command_id=command.command_id, type=command.type,
        status="completed", result={"id": str(uuid4()), "status": "pending"},
    )


async def test_ack_waits_for_store_to_commit():
    command = CreateDeliveryCommand()
    message = incoming_message(command)
    executing = asyncio.Event()
    committed = asyncio.Event()

    async def execute(received, handler):
        assert received == command
        executing.set()
        await committed.wait()
        return completed(command)

    store = SimpleNamespace(execute=AsyncMock(side_effect=execute))
    task = asyncio.create_task(CommandConsumer(store).handle(message))
    try:
        await asyncio.wait_for(executing.wait(), 1)
        message.ack.assert_not_awaited()
        committed.set()
        await task
    finally:
        committed.set()
        await task
    message.ack.assert_awaited_once_with()
    message.reject.assert_not_awaited()


@pytest.mark.parametrize(
    "operation", ["create_delivery", "ensure_delivery_delivered", "request_return"]
)
async def test_dispatches_all_commands_to_existing_use_cases(operation):
    deliveries = InMemoryDeliveryRepository()
    returns = InMemoryReturnRepository()
    repositories = Repositories(deliveries, returns)
    delivery = Delivery(
        status=DeliveryStatus.DELIVERED
        if operation == "request_return" else DeliveryStatus.PENDING
    )
    await deliveries.add(delivery)
    command = {
        "create_delivery": CreateDeliveryCommand(),
        "ensure_delivery_delivered": EnsureDeliveryDelivered(delivery_id=delivery.id),
        "request_return": RequestReturnCommand(delivery_id=delivery.id),
    }[operation]
    results = []

    async def execute(received, handler):
        result = await handler(repositories)
        results.append(result)
        return CommandRecord(
            command_id=received.command_id, type=received.type,
            status="completed", result=result,
        )

    message = incoming_message(command)
    await CommandConsumer(SimpleNamespace(execute=execute)).handle(message)
    result = results[0]
    assert all(isinstance(value, str) for value in result.values())
    if operation == "create_delivery":
        assert result["status"] == "pending"
        assert len(deliveries.deliveries) == 2
    elif operation == "ensure_delivery_delivered":
        assert result == {"id": str(delivery.id), "status": "delivered"}
        assert deliveries.save_count == 1
    else:
        assert result["delivery_id"] == str(delivery.id)
        assert result["status"] == "requested"
        assert len(returns.returns) == 1
    message.ack.assert_awaited_once_with()


@pytest.mark.parametrize("concurrent_read", [None, 1, 2])
async def test_ensure_delivered_reuses_state_after_concurrent_completion(concurrent_read):
    class ConcurrentRepository(InMemoryDeliveryRepository):
        reads = 0

        async def get(self, delivery_id):
            delivery = await super().get(delivery_id)
            self.reads += 1
            if delivery is not None and self.reads == concurrent_read:
                concurrent = delivery.model_copy(deep=True)
                concurrent.mark_delivered()
                await self.save(concurrent, expected_status=delivery.status)
            return delivery

    repository = ConcurrentRepository()
    delivery = Delivery(
        status=DeliveryStatus.DELIVERED
        if concurrent_read is None else DeliveryStatus.PENDING
    )
    await repository.add(delivery)
    command = EnsureDeliveryDelivered(delivery_id=delivery.id)
    result = await execute_command(
        command, Repositories(repository, InMemoryReturnRepository())
    )
    assert result == {"id": str(delivery.id), "status": "delivered"}
    assert repository.save_count == (0 if concurrent_read is None else 1)


async def test_failed_record_is_dead_lettered_after_failure_is_saved(caplog):
    command = CreateDeliveryCommand()
    record = CommandRecord(
        command_id=command.command_id, type=command.type,
        status="failed", error="Operation failed",
    )
    store = SimpleNamespace(execute=AsyncMock(return_value=record))
    message = incoming_message(command)
    await CommandConsumer(store).handle(message)
    message.ack.assert_not_awaited()
    message.reject.assert_awaited_once_with(requeue=False)
    assert str(command.command_id) in caplog.text


@pytest.mark.parametrize(
    "error", [asyncio.CancelledError(), ConnectionError("Database unavailable")]
)
async def test_uncommitted_or_unknown_execution_leaves_message_unacknowledged(error):
    store = SimpleNamespace(execute=AsyncMock(side_effect=error))
    message = incoming_message()
    with pytest.raises(type(error)):
        await CommandConsumer(store).handle(message)
    message.ack.assert_not_awaited()
    message.reject.assert_not_awaited()


async def test_ack_failure_propagates_without_dead_lettering_committed_work():
    command = CreateDeliveryCommand()
    store = SimpleNamespace(execute=AsyncMock(return_value=completed(command)))
    message = incoming_message(command)
    message.ack.side_effect = ConnectionError("Channel closed after commit")
    with pytest.raises(ConnectionError, match="after commit"):
        await CommandConsumer(store).handle(message)
    message.reject.assert_not_awaited()


async def test_store_validation_failure_is_not_mistaken_for_an_invalid_message():
    command = CreateDeliveryCommand()
    with pytest.raises(ValidationError) as invalid_result:
        CommandRecord(command_id=command.command_id, type=command.type, result={"id": {}})
    store = SimpleNamespace(execute=AsyncMock(side_effect=invalid_result.value))
    message = incoming_message(command)
    with pytest.raises(ValidationError):
        await CommandConsumer(store).handle(message)
    message.ack.assert_not_awaited()
    message.reject.assert_not_awaited()


@pytest.mark.parametrize("body", [
    b"not-json",
    b'{"type":"create_delivery"}',
    json.dumps({"command_id": str(uuid4()), "version": 2, "type": "create_delivery"}).encode(),
    json.dumps({"command_id": str(uuid4()), "version": 1, "type": "unknown"}).encode(),
])
async def test_invalid_commands_are_rejected_before_accessing_store(body):
    store = SimpleNamespace(execute=AsyncMock())
    message = incoming_message()
    message.body = body
    await CommandConsumer(store).handle(message)
    store.execute.assert_not_awaited()
    message.ack.assert_not_awaited()
    message.reject.assert_awaited_once_with(requeue=False)


async def test_same_id_with_different_payload_is_dead_lettered():
    store = SimpleNamespace(
        execute=AsyncMock(side_effect=IdempotencyConflictError("Different payload"))
    )
    message = incoming_message()
    await CommandConsumer(store).handle(message)
    message.ack.assert_not_awaited()
    message.reject.assert_awaited_once_with(requeue=False)


async def test_failed_rejection_propagates_for_connection_cleanup():
    message = incoming_message()
    message.body = b"invalid"
    message.reject.side_effect = ConnectionError("Cannot reject")
    with pytest.raises(ConnectionError, match="Cannot reject"):
        await CommandConsumer(None).handle(message)
    message.ack.assert_not_awaited()
