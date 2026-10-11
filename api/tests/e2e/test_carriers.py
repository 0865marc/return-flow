from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from main import create_app

pytestmark = [pytest.mark.integration, pytest.mark.e2e]


@pytest.mark.parametrize(
    ("carrier", "delivery_id_field", "return_id_field"),
    [
        ("correos", "id_entrega", "id_devolucion"),
        ("dhl", "shipmentId", "returnId"),
    ],
    ids=["correos", "dhl"],
)
def test_carrier_flow_persists_after_app_restart(
    postgres_url,
    carrier,
    delivery_id_field,
    return_id_field,
):
    headers = {"X-Carrier": carrier}
    with TestClient(create_app(postgres_url)) as client:
        response = client.post("/integrations/deliveries", headers={**headers, "Idempotency-Key": str(uuid4())})
        assert response.status_code == 201
        delivery_id = response.json()[delivery_id_field]

        response = client.post(
            f"/integrations/deliveries/{delivery_id}/deliver", headers={**headers, "Idempotency-Key": str(uuid4())}
        )
        assert response.status_code == 200

        response = client.post(
            "/integrations/returns",
            headers={**headers, "Idempotency-Key": str(uuid4())},
            json={delivery_id_field: delivery_id},
        )
        assert response.status_code == 201
        return_id = response.json()[return_id_field]

    with TestClient(create_app(postgres_url)) as restarted_client:
        response = restarted_client.get(f"/deliveries/{delivery_id}")
        assert response.status_code == 200
        assert response.json() == {"id": delivery_id, "status": "delivered"}

        response = restarted_client.get(f"/returns/{return_id}")
        assert response.status_code == 200
        assert response.json() == {
            "id": return_id,
            "delivery_id": delivery_id,
            "status": "requested",
        }
