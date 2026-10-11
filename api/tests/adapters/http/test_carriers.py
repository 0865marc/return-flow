from collections.abc import Iterator
from dataclasses import dataclass
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from adapters.persistence import Repositories
from adapters.http import register_exception_handlers, router
from adapters.http.dependencies import get_delivery_flow, get_delivery_update_flow
from composition import CARRIER_ADAPTERS
from domain import Delivery, DeliveryStatus, Return
from tests.adapters.http.fakes import FakeCommandStore
from tests.application.fakes import InMemoryDeliveryRepository, InMemoryReturnRepository

CREATE_PATH = "/integrations/deliveries"
DELIVER_PATH = "/integrations/deliveries/{delivery_id}/deliver"
RETURN_PATH = "/integrations/returns"


@dataclass(frozen=True)
class CarrierContract:
    name: str
    delivery_id_field: str
    return_id_field: str
    status_field: str
    pending: str
    delivered: str
    requested: str

    @property
    def headers(self) -> dict[str, str]:
        return {"X-Carrier": self.name, "Idempotency-Key": str(uuid4())}


@pytest.fixture(
    params=[
        CarrierContract(
            "correos",
            "id_entrega",
            "id_devolucion",
            "estado",
            "pendiente",
            "entregada",
            "solicitada",
        ),
        CarrierContract(
            "dhl",
            "shipmentId",
            "returnId",
            "status",
            "PENDING",
            "DELIVERED",
            "REQUESTED",
        ),
    ],
    ids=["correos", "dhl"],
)
def carrier(request: pytest.FixtureRequest) -> CarrierContract:
    return request.param


@pytest.fixture
def client() -> Iterator[TestClient]:
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


def test_carrier_flow_uses_shared_routes_and_business_state(client, carrier):
    response = client.post(CREATE_PATH, headers=carrier.headers)
    assert response.status_code == 201
    delivery_id = response.json()[carrier.delivery_id_field]
    assert UUID(delivery_id)
    assert response.json() == {
        carrier.delivery_id_field: delivery_id,
        carrier.status_field: carrier.pending,
    }
    assert client.get(f"/deliveries/{delivery_id}").json() == {
        "id": delivery_id,
        "status": "pending",
    }

    response = client.post(
        DELIVER_PATH.format(delivery_id=delivery_id), headers=carrier.headers
    )
    assert response.status_code == 200
    assert response.json() == {
        carrier.delivery_id_field: delivery_id,
        carrier.status_field: carrier.delivered,
    }
    assert client.get(f"/deliveries/{delivery_id}").json() == {
        "id": delivery_id,
        "status": "delivered",
    }

    response = client.post(
        RETURN_PATH,
        headers=carrier.headers,
        json={carrier.delivery_id_field: delivery_id},
    )
    assert response.status_code == 201
    return_id = response.json()[carrier.return_id_field]
    assert UUID(return_id)
    assert response.json() == {
        carrier.return_id_field: return_id,
        carrier.delivery_id_field: delivery_id,
        carrier.status_field: carrier.requested,
    }
    assert client.get(f"/returns/{return_id}").json() == {
        "id": return_id,
        "delivery_id": delivery_id,
        "status": "requested",
    }


@pytest.mark.parametrize("operation", ["deliver", "return"])
def test_unknown_delivery_returns_404(client, carrier, operation):
    delivery_id = str(uuid4())
    if operation == "deliver":
        response = client.post(
            DELIVER_PATH.format(delivery_id=delivery_id), headers=carrier.headers
        )
    else:
        response = client.post(
            RETURN_PATH,
            headers=carrier.headers,
            json={carrier.delivery_id_field: delivery_id},
        )

    assert response.status_code == 404
    assert "was not found" in response.json()["detail"]
    assert client.app.state.return_repository.returns == {}


def test_carriers_preserve_business_conflicts(client, carrier):
    delivery_id = client.post(CREATE_PATH, headers=carrier.headers).json()[
        carrier.delivery_id_field
    ]

    response = client.post(
        RETURN_PATH,
        headers=carrier.headers,
        json={carrier.delivery_id_field: delivery_id},
    )
    assert response.status_code == 409
    assert client.app.state.return_repository.returns == {}

    path = DELIVER_PATH.format(delivery_id=delivery_id)
    assert client.post(path, headers=carrier.headers).status_code == 200
    assert client.post(path, headers=carrier.headers).status_code == 200
    assert client.get(f"/deliveries/{delivery_id}").json()["status"] == "delivered"


def test_invalid_delivery_id_returns_422(client, carrier):
    response = client.post(
        DELIVER_PATH.format(delivery_id="not-a-uuid"), headers=carrier.headers
    )
    assert response.status_code == 422


@pytest.mark.parametrize(
    "invalid_body", ["missing", "invalid_uuid", "extra", "generic", "other_carrier"]
)
def test_invalid_return_body_is_rejected_before_business_logic(
    client, carrier, invalid_body
):
    other_field = "shipmentId" if carrier.name == "correos" else "id_entrega"
    bodies = {
        "missing": {},
        "invalid_uuid": {carrier.delivery_id_field: "not-a-uuid"},
        "extra": {carrier.delivery_id_field: str(uuid4()), "unexpected": True},
        "generic": {"delivery_id": str(uuid4())},
        "other_carrier": {other_field: str(uuid4())},
    }
    flow = AsyncMock()
    client.app.dependency_overrides[get_delivery_flow] = lambda: flow

    response = client.post(
        RETURN_PATH, headers=carrier.headers, json=bodies[invalid_body]
    )

    assert response.status_code == 422
    assert flow.mock_calls == []


@pytest.mark.parametrize("content", ["{", "[]", "null"])
def test_invalid_json_return_body_is_rejected_before_business_logic(
    client, carrier, content
):
    flow = AsyncMock()
    client.app.dependency_overrides[get_delivery_flow] = lambda: flow

    response = client.post(
        RETURN_PATH,
        headers={**carrier.headers, "Content-Type": "application/json"},
        content=content,
    )

    assert response.status_code == 422
    assert flow.mock_calls == []


@pytest.mark.parametrize("headers", [{}, {"X-Carrier": "unknown"}])
@pytest.mark.parametrize("path", [CREATE_PATH, DELIVER_PATH, RETURN_PATH])
def test_missing_or_unknown_carrier_is_rejected_before_business_logic(
    client, headers, path
):
    flow = AsyncMock()
    client.app.dependency_overrides[get_delivery_flow] = lambda: flow
    delivery_id = str(uuid4())

    response = client.post(
        path.format(delivery_id=delivery_id),
        headers=headers,
        json={"id_entrega": delivery_id},
    )

    assert response.status_code == 422
    assert flow.mock_calls == []


def test_both_adapters_can_use_the_same_port_without_repositories(carrier):
    delivery = Delivery()
    returned = Return(delivery_id=delivery.id)
    calls: list[tuple[str, UUID | None]] = []

    class RecordingDeliveryFlow:
        async def create_delivery(self) -> Delivery:
            calls.append(("create", None))
            return delivery

        async def mark_delivered(self, delivery_id: UUID) -> Delivery:
            calls.append(("deliver", delivery_id))
            return Delivery(id=delivery_id, status=DeliveryStatus.DELIVERED)

        async def request_return(self, delivery_id: UUID) -> Return:
            calls.append(("return", delivery_id))
            return returned

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_delivery_flow] = lambda: RecordingDeliveryFlow()
    app.dependency_overrides[get_delivery_update_flow] = lambda: RecordingDeliveryFlow()

    with TestClient(app) as client:
        assert client.post(CREATE_PATH, headers=carrier.headers).status_code == 201
        assert client.post(
            DELIVER_PATH.format(delivery_id=delivery.id), headers=carrier.headers
        ).status_code == 200
        response = client.post(
            RETURN_PATH,
            headers=carrier.headers,
            json={carrier.delivery_id_field: str(delivery.id)},
        )
        assert response.status_code == 201
        assert response.json()[carrier.return_id_field] == str(returned.id)

    assert calls == [("create", None), ("deliver", delivery.id), ("return", delivery.id)]


def test_registering_another_adapter_reuses_the_existing_routes(client, monkeypatch):
    monkeypatch.setitem(CARRIER_ADAPTERS, "another-carrier", CARRIER_ADAPTERS["correos"])
    headers = {"X-Carrier": "another-carrier", "Idempotency-Key": str(uuid4())}

    response = client.post(CREATE_PATH, headers=headers)
    assert response.status_code == 201
    delivery_id = response.json()["id_entrega"]
    headers["Idempotency-Key"] = str(uuid4())
    response = client.post(
        DELIVER_PATH.format(delivery_id=delivery_id), headers=headers
    )
    assert response.status_code == 200
    assert response.json()["estado"] == "entregada"
    headers["Idempotency-Key"] = str(uuid4())
    response = client.post(
        RETURN_PATH, headers=headers, json={"id_entrega": delivery_id}
    )
    assert response.status_code == 201
    assert response.json()["id_entrega"] == delivery_id
    assert response.json()["estado"] == "solicitada"


def test_openapi_documents_shared_routes_and_both_contracts(client):
    schema = client.get("/openapi.json").json()
    paths = schema["paths"]
    assert {path for path in paths if path.startswith("/integrations/")} == {
        CREATE_PATH,
        DELIVER_PATH,
        RETURN_PATH,
    }
    operation = paths[RETURN_PATH]["post"]
    body_schema = operation["requestBody"]["content"]["application/json"]["schema"]
    body_contracts = {item["title"]: item for item in body_schema["oneOf"]}
    assert set(body_contracts) == {"CorreosReturnBody", "DHLReturnBody"}
    for title, delivery_id_field in [
        ("CorreosReturnBody", "id_entrega"),
        ("DHLReturnBody", "shipmentId"),
    ]:
        contract = body_contracts[title]
        assert contract["type"] == "object"
        assert contract["required"] == [delivery_id_field]
        assert contract["additionalProperties"] is False
        assert set(contract["properties"]) == {delivery_id_field}
        identifier = contract["properties"][delivery_id_field]
        assert identifier["type"] == "string"
        assert identifier["format"] == "uuid"

    for path, status_code, model_suffix in [
        (CREATE_PATH, "201", "DeliveryResponse"),
        (DELIVER_PATH, "200", "DeliveryResponse"),
        (RETURN_PATH, "201", "ReturnResponse"),
    ]:
        operation = paths[path]["post"]
        carrier_header = next(
            parameter
            for parameter in operation["parameters"]
            if parameter["name"].lower() == "x-carrier"
        )
        assert carrier_header["in"] == "header"
        assert carrier_header["required"] is True
        response_schema = operation["responses"][status_code]["content"][
            "application/json"
        ]["schema"]
        alternatives = response_schema.get("oneOf", response_schema.get("anyOf", []))
        assert {item["$ref"] for item in alternatives} == {
            f"#/components/schemas/Correos{model_suffix}",
            f"#/components/schemas/DHL{model_suffix}",
        }
        for item in alternatives:
            assert item["$ref"].split("/")[-1] in schema["components"]["schemas"]
