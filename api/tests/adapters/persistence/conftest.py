import pytest

from adapters.persistence.postgres import initialize_database


@pytest.fixture
def database_url(postgres_url: str) -> str:
    initialize_database(postgres_url)
    return postgres_url
