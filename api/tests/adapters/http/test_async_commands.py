from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import composition
from adapters.http import register_exception_handlers, router
from adapters.messaging.contracts import (
    Command,
    CommandRecord,
    IdempotencyConflictError,
)
from adapters.messaging.publisher import MessagingUnavailableError
from adapters.persistence import Repositories
from main import create_app
from tests.application.fakes import InMemoryDeliveryRepository, InMemoryReturnRepository


class FakeCommandStore:
    def __init__(self):
        self.commands = {}
        self.records = {}

    async def register(self, command: Command) -> CommandRecord:
        previous = self.commands.get(command.command_id)
        if previous is not None and previous != command:
            raise IdempotencyConflictError(
                "Idempotency-Key already identifies a different command."
            )
        if previous is None:
            self.commands[command.command_id] = command
            self.records[command.command_id] = CommandRecord(
                command_id=command.command_id, type=command.type
            )
        return self.records[command.command_id]

    async def get(self, command_id: UUID) -> CommandRecord | None:
        return self.records.get(command_id)


@pytest.fixture
def client():
    app = FastAPI()
    app.state.delivery_repository = InMemoryDeliveryRepository()
    app.state.return_repository = InMemoryReturnRepository()
    app.state.command_store = FakeCommandStore()
    app.state.command_publisher = SimpleNamespace(publish=AsyncMock())
    app.include_router(router)
    register_exception_handlers(app)
    with TestClient(app) as client:
        yield client


def test_async_endpoint_publishes_without_mutating_the_delivery(client):
    delivery = client.post("/deliveries").json()
    response = client.post(f"/deliveries/{delivery['id']}/deliver-async")

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    command_id = UUID(response.json()["command_id"])
    assert response.headers["Location"] == f"/commands/{command_id}"
    publish = client.app.state.command_publisher.publish
    publish.assert_awaited_once()
    command = publish.call_args.args[0]
    assert command.command_id == command_id
    assert command.delivery_id == UUID(delivery["id"])
    assert command.version == 1
    assert command.type == "ensure_delivery_delivered"
    assert client.get(f"/deliveries/{delivery['id']}").json() == delivery


@pytest.mark.parametrize(
    ("path", "body", "command_type"),
    [
        ("/deliveries-async", None, "create_delivery"),
        ("/returns-async", {"delivery_id": str(uuid4())}, "request_return"),
        (f"/deliveries/{uuid4()}/deliver-async", None, "ensure_delivery_delivered"),
    ],
)
def test_registers_before_publication_and_retries_with_the_same_identity(
    client, path, body, command_type
):
    command_id = uuid4()
    headers = {"Idempotency-Key": str(command_id)}
    store = client.app.state.command_store

    async def publish(command):
        assert store.records[command.command_id].status == "pending"

    publisher = client.app.state.command_publisher
    publisher.publish.side_effect = publish
    first = client.post(path, headers=headers, json=body)
    second = client.post(path, headers=headers, json=body)

    assert first.status_code == second.status_code == 202
    assert first.json() == second.json() == {
        "command_id": str(command_id),
        "status": "accepted",
    }
    assert (
        first.headers["Location"] == second.headers["Location"]
        == f"/commands/{command_id}"
    )
    assert len(store.records) == 1
    assert publisher.publish.await_count == 2
    command = publisher.publish.call_args.args[0]
    assert command.command_id == command_id
    assert command.type == command_type
    if body is not None:
        assert command.delivery_id == UUID(body["delivery_id"])
    assert client.get(first.headers["Location"]).json() == {
        "command_id": str(command_id),
        "type": command_type,
        "status": "pending",
        "result": None,
        "error": None,
    }
    assert client.app.state.return_repository.returns == {}


@pytest.mark.parametrize(
    ("path", "body"),
    [
        ("/deliveries-async", None),
        ("/returns-async", {"delivery_id": str(uuid4())}),
    ],
)
def test_creation_requires_an_idempotency_key(client, path, body):
    response = client.post(path, json=body)
    assert response.status_code == 422
    assert client.app.state.command_store.records == {}
    client.app.state.command_publisher.publish.assert_not_awaited()


@pytest.mark.parametrize(
    ("path", "body"),
    [
        ("/deliveries-async", None),
        ("/returns-async", {"delivery_id": str(uuid4())}),
        (f"/deliveries/{uuid4()}/deliver-async", None),
    ],
)
def test_idempotency_key_must_be_a_uuid(client, path, body):
    response = client.post(
        path, json=body, headers={"Idempotency-Key": "not-a-uuid"}
    )
    assert response.status_code == 422
    assert client.app.state.command_store.records == {}
    client.app.state.command_publisher.publish.assert_not_awaited()


@pytest.mark.parametrize(
    "body",
    [{}, {"delivery_id": "not-a-uuid"}, {"delivery_id": str(uuid4()), "extra": True}],
)
def test_invalid_return_body_is_not_registered_or_published(client, body):
    response = client.post(
        "/returns-async", json=body, headers={"Idempotency-Key": str(uuid4())}
    )
    assert response.status_code == 422
    assert client.app.state.command_store.records == {}
    client.app.state.command_publisher.publish.assert_not_awaited()


def test_same_key_cannot_be_reused_for_a_different_command_type(client):
    headers = {"Idempotency-Key": str(uuid4())}
    assert client.post("/deliveries-async", headers=headers).status_code == 202
    response = client.post(
        "/returns-async", headers=headers, json={"delivery_id": str(uuid4())}
    )
    assert response.status_code == 409
    client.app.state.command_publisher.publish.assert_awaited_once()


@pytest.mark.parametrize("operation", ["return", "deliver"])
def test_same_key_cannot_be_reused_for_a_different_payload(client, operation):
    headers = {"Idempotency-Key": str(uuid4())}

    def enqueue(delivery_id):
        if operation == "return":
            return client.post(
                "/returns-async", headers=headers, json={"delivery_id": str(delivery_id)}
            )
        return client.post(f"/deliveries/{delivery_id}/deliver-async", headers=headers)

    assert enqueue(uuid4()).status_code == 202
    assert enqueue(uuid4()).status_code == 409
    client.app.state.command_publisher.publish.assert_awaited_once()


@pytest.mark.parametrize("command_status", ["completed", "failed"])
def test_terminal_command_can_be_queried_and_is_not_republished(client, command_status):
    command_id = uuid4()
    headers = {"Idempotency-Key": str(command_id)}
    response = client.post("/deliveries-async", headers=headers)
    store = client.app.state.command_store
    result = {"delivery_id": str(uuid4())} if command_status == "completed" else None
    error = (
        "The operation could not be completed." if command_status == "failed" else None
    )
    store.records[command_id] = store.records[command_id].model_copy(
        update={"status": command_status, "result": result, "error": error}
    )
    publisher = client.app.state.command_publisher
    client.app.state.command_publisher = None

    lookup = client.get(response.headers["Location"])
    assert lookup.status_code == 200
    assert lookup.json() == {
        "command_id": str(command_id),
        "type": "create_delivery",
        "status": command_status,
        "result": result,
        "error": error,
    }
    retry = client.post("/deliveries-async", headers=headers)
    assert retry.status_code == 202
    assert retry.json() == response.json()
    publisher.publish.assert_awaited_once()


def test_unknown_command_returns_404(client):
    assert client.get(f"/commands/{uuid4()}").status_code == 404


def test_invalid_command_id_returns_422(client):
    assert client.get("/commands/not-a-uuid").status_code == 422


def test_entity_existence_is_checked_by_worker_not_enqueue_endpoint(client):
    response = client.post(f"/deliveries/{uuid4()}/deliver-async")
    assert response.status_code == 202
    client.app.state.command_publisher.publish.assert_awaited_once()


def test_invalid_id_is_not_published(client):
    response = client.post("/deliveries/not-a-uuid/deliver-async")
    assert response.status_code == 422
    client.app.state.command_publisher.publish.assert_not_awaited()


@pytest.mark.parametrize("operation", ["create", "return", "deliver"])
def test_unconfirmed_publication_returns_id_and_can_be_retried(client, operation):
    publisher = client.app.state.command_publisher
    publisher.publish.side_effect = MessagingUnavailableError("Cannot confirm")
    command_id = uuid4()
    headers = {"Idempotency-Key": str(command_id)} if operation != "deliver" else {}
    body = None
    if operation == "create":
        path = "/deliveries-async"
    elif operation == "return":
        path = "/returns-async"
        body = {"delivery_id": str(uuid4())}
    else:
        path = f"/deliveries/{uuid4()}/deliver-async"
    response = client.post(path, headers=headers, json=body)
    assert response.status_code == 503
    command_id = UUID(response.json()["command_id"])
    assert "could not be confirmed" in response.json()["detail"]
    assert f"Idempotency-Key: {command_id}" in response.json()["detail"]
    assert client.get(response.headers["Location"]).json()["status"] == "pending"
    original_command = publisher.publish.call_args.args[0]

    publisher.publish.side_effect = None
    retry = client.post(path, headers={"Idempotency-Key": str(command_id)}, json=body)
    assert retry.status_code == 202
    assert retry.json()["command_id"] == str(command_id)
    assert publisher.publish.call_args.args[0] == original_command
    assert len(client.app.state.command_store.records) == 1


def test_unconfigured_messaging_returns_recoverable_503_while_sync_routes_work(client):
    client.app.state.command_publisher = None
    delivery = client.post("/deliveries").json()
    response = client.post(f"/deliveries/{delivery['id']}/deliver-async")
    assert response.status_code == 503
    assert UUID(response.json()["command_id"])
    assert client.get(response.headers["Location"]).json()["status"] == "pending"
    assert client.post(f"/deliveries/{delivery['id']}/deliver").status_code == 200


def test_app_lifecycle_creates_lazy_publisher_and_closes_it(monkeypatch):
    store = FakeCommandStore()

    @asynccontextmanager
    async def persistence_factory(url):
        yield Repositories(
            InMemoryDeliveryRepository(), InMemoryReturnRepository(), commands=store
        )

    publisher = SimpleNamespace(publish=AsyncMock(), close=AsyncMock())
    monkeypatch.setitem(composition.PERSISTENCE_FACTORIES, "test", persistence_factory)
    monkeypatch.setenv("RABBITMQ_URL", "amqp://unused/")
    monkeypatch.setattr(composition, "RabbitMQPublisher", lambda settings: publisher)

    with TestClient(create_app("unused", persistence_adapter="test")) as client:
        assert client.app.state.command_store is store
        assert client.get("/health").status_code == 200
        publisher.publish.assert_not_awaited()
        publisher.close.assert_not_awaited()
        assert client.post(f"/deliveries/{uuid4()}/deliver-async").status_code == 202

    publisher.close.assert_awaited_once()
