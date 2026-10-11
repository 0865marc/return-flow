import os
from dataclasses import dataclass, field
from math import isfinite


@dataclass(frozen=True)
class MessagingSettings:
    url: str = field(repr=False)
    queue_name: str = "return_flow.deliveries.commands"
    connect_timeout: float = 5
    publish_timeout: float = 5
    publish_concurrency: int = 64
    worker_concurrency: int = 8

    def __post_init__(self) -> None:
        if not self.url:
            raise ValueError("RABBITMQ_URL must not be empty.")
        if not self.queue_name or self.queue_name.startswith("amq."):
            raise ValueError("RABBITMQ_QUEUE must be nonempty and not start with 'amq.'.")
        # Leave room for the dead-letter queue's '.failed' suffix (AMQP shortstr).
        if len((self.queue_name + ".failed").encode()) > 255:
            raise ValueError("RABBITMQ_QUEUE is too long.")
        for name, value in (
            ("RABBITMQ_CONNECT_TIMEOUT", self.connect_timeout),
            ("RABBITMQ_PUBLISH_TIMEOUT", self.publish_timeout),
        ):
            if not isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be a positive finite number.")
        for name, value in (
            ("RABBITMQ_PUBLISH_CONCURRENCY", self.publish_concurrency),
            ("RABBITMQ_WORKER_CONCURRENCY", self.worker_concurrency),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer.")

    @classmethod
    def from_env(cls) -> "MessagingSettings":
        url = os.environ.get("RABBITMQ_URL")
        if not url:
            raise RuntimeError("RABBITMQ_URL must be configured to use messaging.")
        return cls(
            url=url,
            queue_name=os.environ.get("RABBITMQ_QUEUE", "return_flow.deliveries.commands"),
            connect_timeout=float(os.environ.get("RABBITMQ_CONNECT_TIMEOUT", "5")),
            publish_timeout=float(os.environ.get("RABBITMQ_PUBLISH_TIMEOUT", "5")),
            publish_concurrency=int(os.environ.get("RABBITMQ_PUBLISH_CONCURRENCY", "64")),
            worker_concurrency=int(os.environ.get("RABBITMQ_WORKER_CONCURRENCY", "8")),
        )
