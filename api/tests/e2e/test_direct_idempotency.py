from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import OperationalError

from main import create_app

pytestmark = [pytest.mark.integration, pytest.mark.e2e]


def key():
    return {"Idempotency-Key": str(uuid4())}


@pytest.mark.parametrize("operation", ["delivery", "return"])
def test_concurrent_http_retries_across_api_instances_create_once(postgres_url, operation):
    headers = key()
    with TestClient(create_app(postgres_url)) as first, TestClient(
        create_app(postgres_url)
    ) as second:
        body = None
        path = "/deliveries"
        if operation == "return":
            delivery_id = first.post("/deliveries", headers=key()).json()["id"]
            assert first.post(f"/deliveries/{delivery_id}/deliver").status_code == 200
            path = "/returns"
            body = {"delivery_id": delivery_id}

        def submit(index):
            client = first if index % 2 == 0 else second
            return client.post(path, headers=headers, json=body)

        with ThreadPoolExecutor(max_workers=8) as threads:
            responses = list(threads.map(submit, range(8)))

        assert all(response.status_code == 201 for response in responses)
        assert all(response.json() == responses[0].json() for response in responses)
        record = first.get(f"/commands/{headers['Idempotency-Key']}").json()
        assert record["status"] == "completed"
        assert record["result"] == responses[0].json()

    # Replaying after restart must not rely on the previous process's memory.
    with TestClient(create_app(postgres_url)) as restarted:
        replay = restarted.post(path, headers=headers, json=body)
        assert replay.status_code == 201
        assert replay.json() == responses[0].json()

    with psycopg.connect(postgres_url) as connection:
        assert connection.execute("SELECT COUNT(*) FROM deliveries").fetchone() == (1,)
        assert connection.execute("SELECT COUNT(*) FROM returns").fetchone() == (
            1 if operation == "return" else 0,
        )


@pytest.mark.parametrize("stage", ["before_commit", "after_commit"])
def test_retry_after_interrupted_execution_or_response_does_not_duplicate(
    postgres_url, monkeypatch, stage
):
    headers = key()
    with TestClient(create_app(postgres_url)) as client:
        store = client.app.state.command_store
        execute = store.execute

        async def interrupt(command, handler):
            async def interrupted_handler(repositories):
                await handler(repositories)
                raise OperationalError("Injected connection failure before commit")

            if stage == "before_commit":
                return await execute(command, interrupted_handler)
            await execute(command, handler)
            raise OperationalError("Injected failure after commit, before HTTP response")

        monkeypatch.setattr(store, "execute", interrupt)
        assert client.post("/deliveries", headers=headers).status_code == 503
        with psycopg.connect(postgres_url) as connection:
            expected = (0,) if stage == "before_commit" else (1,)
            assert connection.execute("SELECT COUNT(*) FROM deliveries").fetchone() == expected
            assert connection.execute("SELECT COUNT(*) FROM commands").fetchone() == expected

        monkeypatch.setattr(store, "execute", execute)
        response = client.post("/deliveries", headers=headers)
        assert response.status_code == 201
        assert client.post("/deliveries", headers=headers).json() == response.json()

    with psycopg.connect(postgres_url) as connection:
        assert connection.execute("SELECT COUNT(*) FROM deliveries").fetchone() == (1,)
        assert connection.execute("SELECT COUNT(*) FROM commands").fetchone() == (1,)


def test_business_failure_remains_the_same_after_state_changes_and_restart(postgres_url):
    headers = key()
    with TestClient(create_app(postgres_url)) as client:
        delivery_id = client.post("/deliveries", headers=key()).json()["id"]
        body = {"delivery_id": delivery_id}
        rejected = client.post("/returns", headers=headers, json=body)
        assert rejected.status_code == 409
        assert client.post(f"/deliveries/{delivery_id}/deliver").status_code == 200

    with TestClient(create_app(postgres_url)) as client:
        replay = client.post("/returns", headers=headers, json=body)
        assert replay.status_code == 409
        assert replay.json() == rejected.json()
        assert client.get(f"/commands/{headers['Idempotency-Key']}").json()["status"] == "failed"
        assert client.post("/returns", headers=key(), json=body).status_code == 201

    with psycopg.connect(postgres_url) as connection:
        assert connection.execute("SELECT COUNT(*) FROM returns").fetchone() == (1,)
