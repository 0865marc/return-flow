from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient

from main import create_app

pytestmark = [pytest.mark.integration, pytest.mark.e2e]


def test_return_request_persists_after_app_restart(postgres_url):
    with TestClient(create_app(postgres_url)) as client:
        response = client.post("/deliveries")
        assert response.status_code == 201
        delivery = response.json()
        assert delivery["status"] == "pending"

        response = client.post(f"/deliveries/{delivery['id']}/deliver")
        assert response.status_code == 200
        delivered = response.json()
        assert delivered == {"id": delivery["id"], "status": "delivered"}

        response = client.post("/returns", json={"delivery_id": delivery["id"]})
        assert response.status_code == 201
        returned = response.json()
        assert returned["delivery_id"] == delivery["id"]
        assert returned["status"] == "requested"

    with TestClient(create_app(postgres_url)) as restarted_client:
        response = restarted_client.get(f"/deliveries/{delivery['id']}")
        assert response.status_code == 200
        assert response.json() == delivered
        response = restarted_client.get(f"/returns/{returned['id']}")
        assert response.status_code == 200
        assert response.json() == returned


def test_pending_delivery_does_not_create_a_return(postgres_url):
    with TestClient(create_app(postgres_url)) as client:
        delivery = client.post("/deliveries").json()

        response = client.post("/returns", json={"delivery_id": delivery["id"]})

        assert response.status_code == 409
        assert client.get(f"/deliveries/{delivery['id']}").json() == delivery

    with psycopg.connect(postgres_url) as connection:
        assert connection.execute("SELECT COUNT(*) FROM returns").fetchone() == (0,)


def test_unknown_delivery_does_not_create_a_return(postgres_url):
    with TestClient(create_app(postgres_url)) as client:
        response = client.post("/returns", json={"delivery_id": str(uuid4())})

        assert response.status_code == 404

    with psycopg.connect(postgres_url) as connection:
        assert connection.execute("SELECT COUNT(*) FROM returns").fetchone() == (0,)
