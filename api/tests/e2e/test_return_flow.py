from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg_pool import AsyncConnectionPool

from main import create_app

pytestmark = [pytest.mark.integration, pytest.mark.e2e]


@pytest.fixture(params=["postgres", "postgres_pool"])
def persistence_adapter(request: pytest.FixtureRequest) -> str:
    return request.param


def test_return_request_persists_after_app_restart(postgres_url, persistence_adapter):
    with TestClient(
        create_app(postgres_url, persistence_adapter=persistence_adapter)
    ) as client:
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

    with TestClient(
        create_app(postgres_url, persistence_adapter=persistence_adapter)
    ) as restarted_client:
        response = restarted_client.get(f"/deliveries/{delivery['id']}")
        assert response.status_code == 200
        assert response.json() == delivered
        response = restarted_client.get(f"/returns/{returned['id']}")
        assert response.status_code == 200
        assert response.json() == returned


def test_pending_delivery_does_not_create_a_return(postgres_url, persistence_adapter):
    with TestClient(
        create_app(postgres_url, persistence_adapter=persistence_adapter)
    ) as client:
        delivery = client.post("/deliveries").json()

        response = client.post("/returns", json={"delivery_id": delivery["id"]})

        assert response.status_code == 409
        assert client.get(f"/deliveries/{delivery['id']}").json() == delivery

    with psycopg.connect(postgres_url) as connection:
        assert connection.execute("SELECT COUNT(*) FROM returns").fetchone() == (0,)


def test_unknown_delivery_does_not_create_a_return(postgres_url, persistence_adapter):
    with TestClient(
        create_app(postgres_url, persistence_adapter=persistence_adapter)
    ) as client:
        response = client.post("/returns", json={"delivery_id": str(uuid4())})

        assert response.status_code == 404

    with psycopg.connect(postgres_url) as connection:
        assert connection.execute("SELECT COUNT(*) FROM returns").fetchone() == (0,)


def test_app_shutdown_closes_pool_and_its_connections(postgres_url):
    application = create_app(postgres_url, persistence_adapter="postgres_pool")

    with TestClient(application) as client:
        pool = application.state.delivery_repository.pool
        assert not pool.closed
        assert client.portal is not None

        async def borrow_connection():
            async with pool.connection() as connection:
                assert not connection.closed
                return connection

        connection = client.portal.call(borrow_connection)

    assert pool.closed
    assert connection.closed


def test_exhausted_pool_returns_503_and_recovers(postgres_url, monkeypatch):
    monkeypatch.setenv("DATABASE_POOL_MAX_SIZE", "1")
    monkeypatch.setenv("DATABASE_POOL_TIMEOUT", "0.05")
    application = create_app(postgres_url, persistence_adapter="postgres_pool")

    with TestClient(application) as client:
        assert client.portal is not None
        context = application.state.delivery_repository.pool.connection()
        client.portal.call(context.__aenter__)
        try:
            response = client.post("/deliveries")
            assert response.status_code == 503
        finally:
            client.portal.call(context.__aexit__, None, None, None)

        response = client.post("/deliveries")
        assert response.status_code == 201
        delivery = response.json()
        assert client.get(f"/deliveries/{delivery['id']}").json() == delivery

    with psycopg.connect(postgres_url) as connection:
        assert connection.execute("SELECT COUNT(*) FROM deliveries").fetchone() == (1,)


def test_environment_selects_persistence_at_app_creation(postgres_url, monkeypatch):
    monkeypatch.setenv("PERSISTENCE_ADAPTER", "postgres_pool")
    application = create_app(postgres_url)
    # Later environment changes do not alter an already configured application.
    monkeypatch.setenv("PERSISTENCE_ADAPTER", "postgres")

    with TestClient(application) as client:
        assert isinstance(application.state.delivery_repository.pool, AsyncConnectionPool)
        delivery = client.post("/deliveries")
        assert delivery.status_code == 201
        assert client.get(f"/deliveries/{delivery.json()['id']}").json() == delivery.json()
