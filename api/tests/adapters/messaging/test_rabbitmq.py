import asyncio
import os
from collections.abc import AsyncGenerator
from uuid import UUID, uuid4

import aio_pika
import psycopg
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractIncomingMessage
from aio_pika.exceptions import ChannelNotFoundEntity
from fastapi.testclient import TestClient

from adapters.messaging.consumer import CommandConsumer
from adapters.commands.contracts import CreateDeliveryCommand, EnsureDeliveryDelivered, RequestReturnCommand
from adapters.messaging.publisher import MessagingUnavailableError, RabbitMQPublisher
from adapters.messaging.settings import MessagingSettings
from adapters.messaging.topology import declare_topology
from composition import open_persistence
from domain.delivery import Delivery, DeliveryStatus
from main import create_app
from worker import run_worker

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


@pytest.fixture
async def rabbitmq() -> AsyncGenerator[tuple[MessagingSettings, AbstractConnection]]:
    url = os.environ.get("TEST_RABBITMQ_URL")
    if not url:
        pytest.skip("Set TEST_RABBITMQ_URL to run RabbitMQ integration tests.")

    settings = MessagingSettings(
        url=url, queue_name=f"test_return_flow_{uuid4().hex}"
    )
    connection = await aio_pika.connect(url, timeout=5)
    try:
        yield settings, connection
    finally:
        # Each test owns only these queues; never purge an application queue.
        for name in (settings.queue_name, f"{settings.queue_name}.failed"):
            channel = await connection.channel()
            try:
                await channel.queue_delete(name, timeout=5)
            except ChannelNotFoundEntity:
                pass
            finally:
                if not channel.is_closed:
                    await channel.close()
        await connection.close()


@pytest.fixture
async def publisher(rabbitmq) -> AsyncGenerator[RabbitMQPublisher]:
    settings, _ = rabbitmq
    publisher = RabbitMQPublisher(settings)
    try:
        yield publisher
    finally:
        await publisher.close()


@pytest.fixture
async def repositories(postgres_url: str):
    async with open_persistence(postgres_url) as repositories:
        yield repositories


async def next_message(queue) -> AbstractIncomingMessage:
    # Dead-lettering and requeueing after channel closure are asynchronous.
    async with asyncio.timeout(5):
        while (message := await queue.get(fail=False, timeout=5)) is None:
            await asyncio.sleep(0.02)
    return message


async def test_command_waits_for_a_worker_and_is_acknowledged_after_commit(
    repositories, rabbitmq, publisher
) -> None:
    settings, connection = rabbitmq
    delivery = Delivery()
    await repositories.deliveries.add(delivery)
    command = EnsureDeliveryDelivered(delivery_id=delivery.id)
    await repositories.commands.register(command)

    # No consumer is registered, and closing the producer leaves work queued.
    await publisher.publish(command)
    await publisher.close()
    assert (await repositories.deliveries.get(delivery.id)).status == DeliveryStatus.PENDING

    channel = await connection.channel()
    queue = await channel.get_queue(settings.queue_name)
    message = await next_message(queue)
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert EnsureDeliveryDelivered.model_validate_json(message.body) == command

    await CommandConsumer(repositories.commands).handle(message)

    assert (await repositories.deliveries.get(delivery.id)).status == DeliveryStatus.DELIVERED
    record = await repositories.commands.get(command.command_id)
    assert record.status == "completed"
    assert record.result == {"id": str(delivery.id), "status": "delivered"}
    assert await queue.get(fail=False, timeout=5) is None
    failed = await channel.get_queue(f"{settings.queue_name}.failed")
    assert await failed.get(fail=False, timeout=5) is None


@pytest.mark.parametrize("first_transport", ["direct", "queued"])
async def test_http_and_worker_share_one_result_for_the_same_command(
    repositories, postgres_url, rabbitmq, publisher, monkeypatch, first_transport
) -> None:
    settings, connection = rabbitmq
    monkeypatch.setenv("RABBITMQ_URL", settings.url)
    monkeypatch.setenv("RABBITMQ_QUEUE", settings.queue_name)
    command = CreateDeliveryCommand()
    headers = {"Idempotency-Key": str(command.command_id)}
    with TestClient(create_app(postgres_url)) as client:
        if first_transport == "queued":
            assert client.post("/deliveries-async", headers=headers).status_code == 202
            assert client.get(f"/commands/{command.command_id}").json()["status"] == "pending"
        direct = client.post("/deliveries", headers=headers)
        assert direct.status_code == 201
        if first_transport == "direct":
            assert client.post("/deliveries-async", headers=headers).status_code == 202
            # Simulate a duplicate publication of the already completed command.
            await publisher.publish(command)

        channel = await connection.channel()
        queue = await channel.get_queue(settings.queue_name)
        message = await next_message(queue)
        await CommandConsumer(repositories.commands).handle(message)
        assert await queue.get(fail=False, timeout=5) is None
        assert client.post("/deliveries", headers=headers).json() == direct.json()
        record = client.get(f"/commands/{command.command_id}").json()
        assert record["status"] == "completed"
        assert record["result"] == direct.json()
    with psycopg.connect(postgres_url) as database:
        assert database.execute("SELECT COUNT(*) FROM deliveries").fetchone() == (1,)
        assert database.execute("SELECT COUNT(*) FROM commands").fetchone() == (1,)


class SimulatedWorkerCrash(RuntimeError):
    pass


class CloseChannelBeforeAcknowledgement:
    def __init__(
        self, message: AbstractIncomingMessage, channel: AbstractChannel
    ) -> None:
        self.message = message
        self.channel = channel

    def __getattr__(self, name):
        return getattr(self.message, name)

    async def ack(self) -> None:
        await self.channel.close()
        raise SimulatedWorkerCrash("Worker stopped after commit and before ACK.")


async def test_redelivery_after_commit_before_ack_is_safe(
    repositories, postgres_url, rabbitmq, publisher
) -> None:
    settings, connection = rabbitmq
    delivery = Delivery(status=DeliveryStatus.DELIVERED)
    await repositories.deliveries.add(delivery)
    command = RequestReturnCommand(delivery_id=delivery.id)
    await repositories.commands.register(command)
    await publisher.publish(command)
    first_channel = await connection.channel()
    first_queue = await first_channel.get_queue(settings.queue_name)
    original = await next_message(first_queue)
    consumer = CommandConsumer(repositories.commands)

    with pytest.raises(SimulatedWorkerCrash):
        await consumer.handle(CloseChannelBeforeAcknowledgement(original, first_channel))

    completed = await repositories.commands.get(command.command_id)
    assert completed.status == "completed"
    assert completed.result["delivery_id"] == str(delivery.id)
    assert completed.result["status"] == "requested"
    second_channel = await connection.channel()
    second_queue = await second_channel.get_queue(settings.queue_name)
    redelivered = await next_message(second_queue)
    assert redelivered.redelivered is True
    assert redelivered.body == original.body

    await consumer.handle(redelivered)

    assert await repositories.commands.get(command.command_id) == completed
    with psycopg.connect(postgres_url) as database:
        rows = database.execute("SELECT id FROM returns").fetchall()
    assert rows == [(UUID(completed.result["id"]),)]
    assert await second_queue.get(fail=False, timeout=5) is None
    failed = await second_channel.get_queue(f"{settings.queue_name}.failed")
    assert await failed.get(fail=False, timeout=5) is None


@pytest.mark.parametrize(
    "body", [b"not json", b'{"version": 2}', b'{"type": "create_delivery"}']
)
async def test_invalid_command_is_dead_lettered(rabbitmq, body) -> None:
    settings, connection = rabbitmq
    channel = await connection.channel()
    queue = await declare_topology(channel, settings.queue_name)
    await channel.default_exchange.publish(
        aio_pika.Message(body=body), routing_key=settings.queue_name, mandatory=True
    )
    message = await next_message(queue)

    # Validation must reject the message before accessing a repository.
    await CommandConsumer(None).handle(message)

    failed = await channel.get_queue(f"{settings.queue_name}.failed")
    rejected = await next_message(failed)
    assert rejected.body == body
    assert rejected.headers["x-death"][0]["reason"] == "rejected"
    await rejected.ack()
    assert await queue.get(fail=False, timeout=5) is None


async def test_unroutable_publish_is_not_reported_as_accepted(
    rabbitmq, publisher
) -> None:
    settings, connection = rabbitmq
    command = EnsureDeliveryDelivered(delivery_id=uuid4())
    await publisher.publish(command)
    channel = await connection.channel()
    # Remove this test's queue after the publisher has established its channel.
    await channel.queue_delete(settings.queue_name, timeout=5)

    with pytest.raises(MessagingUnavailableError):
        await publisher.publish(command)


async def test_concurrent_publications_of_one_command_are_confirmed_individually(
    rabbitmq, publisher
) -> None:
    settings, connection = rabbitmq
    command = EnsureDeliveryDelivered(delivery_id=uuid4())
    await asyncio.gather(*(publisher.publish(command) for _ in range(16)))

    channel = await connection.channel()
    queue = await channel.get_queue(settings.queue_name)
    message_ids = set()
    for _ in range(16):
        message = await next_message(queue)
        assert EnsureDeliveryDelivered.model_validate_json(message.body) == command
        assert message.correlation_id == str(command.command_id)
        message_ids.add(message.message_id)
        await message.ack()
    assert len(message_ids) == 16
    assert await queue.get(fail=False, timeout=5) is None


async def test_concurrent_unroutable_retries_are_all_reported_as_unconfirmed(
    rabbitmq, publisher
) -> None:
    settings, connection = rabbitmq
    command = EnsureDeliveryDelivered(delivery_id=uuid4())
    await publisher.publish(command)
    channel = await connection.channel()
    await channel.queue_delete(settings.queue_name, timeout=5)

    results = await asyncio.gather(
        *(publisher.publish(command) for _ in range(16)), return_exceptions=True
    )
    assert all(isinstance(result, MessagingUnavailableError) for result in results)


async def wait_for_command(client, command_id, task, *, status="completed"):
    async with asyncio.timeout(10):
        while True:
            if task.done():
                await task
                pytest.fail("Worker stopped before processing the command")
            response = client.get(f"/commands/{command_id}")
            assert response.status_code == 200
            command = response.json()
            if command["status"] != "pending":
                assert command["status"] == status, command
                return command
            # TestClient runs the API in another thread; let this loop run the worker.
            await asyncio.sleep(0.02)


@pytest.mark.e2e
async def test_complete_async_return_flow_and_idempotent_http_retries(
    rabbitmq, postgres_url, monkeypatch
) -> None:
    settings, _ = rabbitmq
    monkeypatch.setenv("RABBITMQ_URL", settings.url)
    monkeypatch.setenv("RABBITMQ_QUEUE", settings.queue_name)

    with TestClient(
        create_app(postgres_url)
    ) as client:
        create_id = str(uuid4())
        create_headers = {"Idempotency-Key": create_id}
        accepted = client.post("/deliveries-async", headers=create_headers)
        assert accepted.status_code == 202
        assert accepted.json()["status"] == "accepted"
        assert accepted.json()["command_id"] == create_id

        # Duplicate publications before a worker starts must still create one delivery.
        repeated = client.post("/deliveries-async", headers=create_headers)
        assert repeated.status_code == 202
        assert repeated.json()["command_id"] == create_id
        pending = client.get(f"/commands/{create_id}")
        assert pending.status_code == 200
        assert pending.json()["status"] == "pending"
        assert pending.json()["result"] is None
        with psycopg.connect(postgres_url) as database:
            assert database.execute("SELECT COUNT(*) FROM deliveries").fetchone() == (0,)

        task = asyncio.create_task(
            run_worker(settings, postgres_url)
        )
        try:
            created = await wait_for_command(client, create_id, task)
            delivery = created["result"]
            delivery_id = delivery["id"]
            assert delivery["status"] == "pending"
            assert client.get(f"/deliveries/{delivery_id}").json() == delivery

            # Completed retries return the same command and its stored resource ID.
            assert client.post("/deliveries-async", headers=create_headers).status_code == 202
            assert client.get(f"/commands/{create_id}").json()["result"] == delivery

            deliver_id = str(uuid4())
            response = client.post(
                f"/deliveries/{delivery_id}/deliver-async",
                headers={"Idempotency-Key": deliver_id},
            )
            assert response.status_code == 202
            delivered = await wait_for_command(client, deliver_id, task)
            assert delivered["result"] == {"id": delivery_id, "status": "delivered"}
            assert client.get(f"/deliveries/{delivery_id}").json() == delivered["result"]

            return_id = str(uuid4())
            return_headers = {"Idempotency-Key": return_id}
            body = {"delivery_id": delivery_id}
            response = client.post("/returns-async", headers=return_headers, json=body)
            assert response.status_code == 202
            assert response.json()["command_id"] == return_id
            completed = await wait_for_command(client, return_id, task)
            returned = completed["result"]
            assert returned["delivery_id"] == delivery_id
            assert returned["status"] == "requested"
            assert client.get(f"/returns/{returned['id']}").json() == returned

            response = client.post("/returns-async", headers=return_headers, json=body)
            assert response.status_code == 202
            assert response.json()["command_id"] == return_id
            assert client.get(f"/commands/{return_id}").json()["result"] == returned

            conflicting = client.post(
                "/returns-async",
                headers=return_headers,
                json={"delivery_id": str(uuid4())},
            )
            assert conflicting.status_code == 409
            with psycopg.connect(postgres_url) as database:
                assert database.execute("SELECT COUNT(*) FROM deliveries").fetchone() == (1,)
                assert database.execute("SELECT COUNT(*) FROM returns").fetchone() == (1,)

            # A distinct command retains the existing ability to request another return.
            another_id = str(uuid4())
            response = client.post(
                "/returns-async", headers={"Idempotency-Key": another_id}, json=body
            )
            assert response.status_code == 202
            another = await wait_for_command(client, another_id, task)
            assert another["result"]["id"] != returned["id"]
            assert another["result"]["delivery_id"] == delivery_id

            # The original route still accepts requests without an idempotency header.
            response = client.post(f"/deliveries/{delivery_id}/deliver-async")
            assert response.status_code == 202
            await wait_for_command(client, response.json()["command_id"], task)
            assert client.post(f"/deliveries/{delivery_id}/deliver").status_code == 200
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task


@pytest.mark.e2e
async def test_business_failure_is_queryable_and_dead_lettered(
    rabbitmq, postgres_url, monkeypatch
) -> None:
    settings, connection = rabbitmq
    monkeypatch.setenv("RABBITMQ_URL", settings.url)
    monkeypatch.setenv("RABBITMQ_QUEUE", settings.queue_name)

    with TestClient(
        create_app(postgres_url)
    ) as client:
        delivery = client.post("/deliveries", headers={"Idempotency-Key": str(uuid4())}).json()
        command_id = str(uuid4())
        response = client.post(
            "/returns-async",
            headers={"Idempotency-Key": command_id},
            json={"delivery_id": delivery["id"]},
        )
        assert response.status_code == 202
        task = asyncio.create_task(
            run_worker(settings, postgres_url)
        )
        try:
            failed = await wait_for_command(client, command_id, task, status="failed")
            assert failed["result"] is None
            assert isinstance(failed["error"], str) and failed["error"]
            assert client.get(f"/deliveries/{delivery['id']}").json() == delivery
            with psycopg.connect(postgres_url) as database:
                assert database.execute("SELECT COUNT(*) FROM returns").fetchone() == (0,)

            channel = await connection.channel()
            dead_letters = await channel.get_queue(f"{settings.queue_name}.failed")
            rejected = await next_message(dead_letters)
            command = RequestReturnCommand.model_validate_json(rejected.body)
            assert str(command.command_id) == command_id
            assert rejected.headers["x-death"][0]["reason"] == "rejected"
            await rejected.ack()
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
