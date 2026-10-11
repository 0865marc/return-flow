from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from adapters.persistence import Repositories
from adapters.commands.contracts import CommandRecord, RequestReturnCommand
from adapters.http import register_exception_handlers, router
from tests.adapters.http.fakes import FakeCommandStore
from tests.application.fakes import InMemoryDeliveryRepository, InMemoryReturnRepository


@pytest.fixture
def client():
    app = FastAPI()
    app.state.delivery_repository = InMemoryDeliveryRepository()
    app.state.return_repository = InMemoryReturnRepository()
    app.state.command_store = FakeCommandStore(Repositories(
        app.state.delivery_repository, app.state.return_repository
    ))
    app.include_router(router)
    register_exception_handlers(app)
    with TestClient(app) as client:
        yield client


def test_create_deliver_and_request_return(client):
    response = client.post("/deliveries", headers={"Idempotency-Key": str(uuid4())})
    assert response.status_code == 201
    delivery = response.json()
    delivery_id = delivery["id"]
    assert UUID(delivery_id)
    assert delivery["status"] == "pending"

    response = client.get(f"/deliveries/{delivery_id}")
    assert response.status_code == 200
    assert response.json() == delivery

    response = client.post(f"/deliveries/{delivery_id}/deliver")
    assert response.status_code == 200
    assert response.json() == {"id": delivery_id, "status": "delivered"}
    assert client.get(f"/deliveries/{delivery_id}").json()["status"] == "delivered"

    response = client.post("/returns", json={"delivery_id": delivery_id}, headers={"Idempotency-Key": str(uuid4())})
    assert response.status_code == 201
    returned = response.json()
    assert UUID(returned["id"])
    assert returned["delivery_id"] == delivery_id
    assert returned["status"] == "requested"

    response = client.get(f"/returns/{returned['id']}")
    assert response.status_code == 200
    assert response.json() == returned


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("get", "/deliveries/{id}"),
        ("post", "/deliveries/{id}/deliver"),
        ("get", "/returns/{id}"),
    ],
)
def test_unknown_entity_returns_404(client, method, path):
    response = client.request(method, path.format(id=uuid4()))
    assert response.status_code == 404
    assert "was not found" in response.json()["detail"]


def test_request_return_for_unknown_delivery_returns_404(client):
    response = client.post("/returns", json={"delivery_id": str(uuid4())}, headers={"Idempotency-Key": str(uuid4())})
    assert response.status_code == 404
    assert "was not found" in response.json()["detail"]


def test_request_return_for_pending_delivery_returns_409(client):
    delivery = client.post("/deliveries", headers={"Idempotency-Key": str(uuid4())}).json()
    response = client.post("/returns", json={"delivery_id": delivery["id"]}, headers={"Idempotency-Key": str(uuid4())})
    assert response.status_code == 409
    assert response.json() == {
        "detail": "A return can only be requested for a delivered delivery."
    }
    assert client.app.state.return_repository.returns == {}


def test_deliver_delivered_delivery_returns_current_result(client):
    delivery_id = client.post("/deliveries", headers={"Idempotency-Key": str(uuid4())}).json()["id"]
    assert client.post(f"/deliveries/{delivery_id}/deliver").status_code == 200

    response = client.post(f"/deliveries/{delivery_id}/deliver")
    assert response.status_code == 200
    assert response.json() == {"id": delivery_id, "status": "delivered"}
    assert client.get(f"/deliveries/{delivery_id}").json()["status"] == "delivered"


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("get", "/deliveries/not-a-uuid"),
        ("post", "/deliveries/not-a-uuid/deliver"),
        ("get", "/returns/not-a-uuid"),
    ],
)
def test_invalid_path_uuid_returns_422(client, method, path):
    response = client.request(method, path)
    assert response.status_code == 422


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"delivery_id": "not-a-uuid"},
        {"delivery_id": str(uuid4()), "status": "completed"},
    ],
)
def test_invalid_return_body_returns_422(client, body):
    response = client.post("/returns", json=body, headers={"Idempotency-Key": str(uuid4())})
    assert response.status_code == 422


@pytest.mark.parametrize("path,body", [
    ("/deliveries", None), ("/returns", {"delivery_id": str(uuid4())}),
    ("/integrations/deliveries", None),
    ("/integrations/returns", {"id_entrega": str(uuid4())}),
])
@pytest.mark.parametrize("idempotency_key", [None, "invalid"])
def test_direct_creations_require_valid_idempotency_key(client, path, body, idempotency_key):
    headers = {"X-Carrier": "correos"}
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    response = client.post(path, headers=headers, json=body)
    assert response.status_code == 422
    assert client.app.state.command_store.records == {}


def test_same_key_cannot_identify_another_direct_operation(client):
    headers = {"Idempotency-Key": str(uuid4())}
    created = client.post("/deliveries", headers=headers)
    assert created.status_code == 201
    assert client.post("/deliveries", headers=headers).json() == created.json()
    conflict = client.post(f"/deliveries/{created.json()['id']}/deliver", headers=headers)
    assert conflict.status_code == 409
    assert client.get(f"/deliveries/{created.json()['id']}").json()["status"] == "pending"


def test_return_key_cannot_be_reused_with_another_delivery(client):
    first = client.post("/deliveries", headers={"Idempotency-Key": str(uuid4())}).json()
    second = client.post("/deliveries", headers={"Idempotency-Key": str(uuid4())}).json()
    client.post(f"/deliveries/{first['id']}/deliver")
    client.post(f"/deliveries/{second['id']}/deliver")
    headers = {"Idempotency-Key": str(uuid4())}
    body = {"delivery_id": first["id"]}
    response = client.post("/returns", headers=headers, json=body)
    assert response.status_code == 201
    assert client.post("/returns", headers=headers, json=body).json() == response.json()
    conflict = client.post("/returns", headers=headers, json={"delivery_id": second["id"]})
    assert conflict.status_code == 409
    assert len(client.app.state.return_repository.returns) == 1


@pytest.mark.parametrize("error_code,expected_status", [
    ("not_found", 404), ("business_rule", 409), ("execution_failed", 500), (None, 500),
])
def test_replayed_failure_preserves_its_classification(client, error_code, expected_status):
    command = RequestReturnCommand(delivery_id=uuid4())
    store = client.app.state.command_store
    store.commands[command.command_id] = command
    store.records[command.command_id] = CommandRecord(
        command_id=command.command_id, type=command.type, status="failed",
        error="Stored failure", error_code=error_code,
    )
    for _ in range(2):
        response = client.post(
            "/returns", headers={"Idempotency-Key": str(command.command_id)},
            json={"delivery_id": str(command.delivery_id)},
        )
        assert response.status_code == expected_status
        assert response.json() == {"detail": "Stored failure"}
    assert client.app.state.return_repository.returns == {}
