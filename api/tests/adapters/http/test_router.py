from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from adapters.http import register_exception_handlers, router
from tests.application.fakes import InMemoryDeliveryRepository, InMemoryReturnRepository


@pytest.fixture
def client():
    app = FastAPI()
    app.state.delivery_repository = InMemoryDeliveryRepository()
    app.state.return_repository = InMemoryReturnRepository()
    app.include_router(router)
    register_exception_handlers(app)
    with TestClient(app) as client:
        yield client


def test_create_deliver_and_request_return(client):
    response = client.post("/deliveries")
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

    response = client.post("/returns", json={"delivery_id": delivery_id})
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
    response = client.post("/returns", json={"delivery_id": str(uuid4())})
    assert response.status_code == 404
    assert "was not found" in response.json()["detail"]


def test_request_return_for_pending_delivery_returns_409(client):
    delivery = client.post("/deliveries").json()
    response = client.post("/returns", json={"delivery_id": delivery["id"]})
    assert response.status_code == 409
    assert response.json() == {
        "detail": "A return can only be requested for a delivered delivery."
    }
    assert client.app.state.return_repository.returns == {}


def test_deliver_delivered_delivery_returns_409(client):
    delivery_id = client.post("/deliveries").json()["id"]
    assert client.post(f"/deliveries/{delivery_id}/deliver").status_code == 200

    response = client.post(f"/deliveries/{delivery_id}/deliver")
    assert response.status_code == 409
    assert response.json() == {
        "detail": "Only pending deliveries can be marked as delivered."
    }
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
    response = client.post("/returns", json=body)
    assert response.status_code == 422
