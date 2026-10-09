import os
from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def postgres_url() -> Iterator[str]:
    base_url = os.environ.get("TEST_DATABASE_URL")
    if not base_url:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests.")

    schema = f"test_return_flow_{uuid4().hex}"
    with psycopg.connect(base_url, connect_timeout=5) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    isolated_url = make_conninfo(base_url, options=f"-csearch_path={schema}")
    try:
        yield isolated_url
    finally:
        with psycopg.connect(base_url, connect_timeout=5) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
