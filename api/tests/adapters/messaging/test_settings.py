import pytest

from adapters.messaging.settings import MessagingSettings


def test_publish_concurrency_defaults_to_64_and_can_be_configured(monkeypatch):
    monkeypatch.setenv("RABBITMQ_URL", "amqp://unused/")
    monkeypatch.delenv("RABBITMQ_PUBLISH_CONCURRENCY", raising=False)
    assert MessagingSettings.from_env().publish_concurrency == 64
    monkeypatch.setenv("RABBITMQ_PUBLISH_CONCURRENCY", "8")
    assert MessagingSettings.from_env().publish_concurrency == 8


@pytest.mark.parametrize("value", [0, -1, 1.5, True])
def test_publish_concurrency_requires_a_positive_integer(value):
    with pytest.raises(ValueError, match="RABBITMQ_PUBLISH_CONCURRENCY"):
        MessagingSettings(url="amqp://unused/", publish_concurrency=value)


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "many"])
def test_invalid_publish_concurrency_environment_is_rejected(monkeypatch, value):
    monkeypatch.setenv("RABBITMQ_URL", "amqp://unused/")
    monkeypatch.setenv("RABBITMQ_PUBLISH_CONCURRENCY", value)
    with pytest.raises(ValueError):
        MessagingSettings.from_env()
