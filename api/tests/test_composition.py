from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

import composition
import main
from adapters.persistence import Repositories
from tests.application.fakes import InMemoryDeliveryRepository, InMemoryReturnRepository


@pytest.fixture
def repositories() -> Repositories:
    return Repositories(InMemoryDeliveryRepository(), InMemoryReturnRepository())


@pytest.mark.anyio
@pytest.mark.parametrize("autocommit", [False, True])
async def test_persistence_configures_shared_pool_and_closes_it(
    monkeypatch: pytest.MonkeyPatch, repositories: Repositories, autocommit: bool,
) -> None:
    closed = False

    @asynccontextmanager
    async def persistence(
        database_url: str, *, autocommit: bool = False,
    ) -> AsyncGenerator[Repositories]:
        nonlocal closed
        try:
            yield repositories
        finally:
            closed = True

    factory = Mock(side_effect=persistence)
    monkeypatch.setattr(composition.postgres, "open_repositories", factory)
    # Omitting the option preserves the worker's transactional pool default.
    context = (
        composition.open_persistence("postgresql://test", autocommit=True)
        if autocommit
        else composition.open_persistence("postgresql://test")
    )
    async with context as result:
        assert result is repositories
        assert not closed
    factory.assert_called_once_with("postgresql://test", autocommit=autocommit)
    assert closed


@pytest.mark.anyio
@pytest.mark.parametrize("autocommit", [False, True])
async def test_injected_persistence_factory_keeps_its_single_url_argument(
    repositories: Repositories, autocommit: bool,
) -> None:
    @asynccontextmanager
    async def persistence(database_url: str) -> AsyncGenerator[Repositories]:
        assert database_url == "postgresql://test"
        yield repositories

    async with composition.open_persistence(
        "postgresql://test", factory=persistence, autocommit=autocommit
    ) as result:
        assert result is repositories


def test_api_lifespan_enables_autocommit_reads(
    monkeypatch: pytest.MonkeyPatch, repositories: Repositories,
) -> None:
    @asynccontextmanager
    async def persistence(
        database_url: str | None = None,
        *,
        factory: composition.PersistenceFactory | None = None,
        autocommit: bool = False,
    ) -> AsyncGenerator[Repositories]:
        assert autocommit
        yield repositories

    monkeypatch.setattr(main, "open_persistence", persistence)
    monkeypatch.delenv("RABBITMQ_URL", raising=False)
    with TestClient(main.create_app("postgresql://test")) as client:
        assert client.get("/health").json() == {"status": "ok"}
        assert client.app.state.delivery_repository is repositories.deliveries
