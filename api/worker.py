import asyncio
import logging
import signal

from aio_pika import connect_robust

from adapters.messaging.consumer import CommandConsumer
from adapters.messaging.settings import MessagingSettings
from adapters.messaging.topology import declare_topology
from composition import get_persistence_factory, open_persistence

logger = logging.getLogger(__name__)


async def run_worker(
    settings: MessagingSettings | None = None,
    database_url: str | None = None,
    *,
    persistence_adapter: str | None = None,
) -> None:
    settings = settings or MessagingSettings.from_env()
    factory = get_persistence_factory(persistence_adapter)
    async with open_persistence(database_url, factory=factory) as repositories:
        if repositories.commands is None:
            raise RuntimeError("The worker requires a command store.")
        connection = await connect_robust(
            settings.url, timeout=settings.connect_timeout
        )
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)
            queue = await declare_topology(channel, settings.queue_name)
            consumer = CommandConsumer(repositories.commands)
            logger.info("Command worker consuming queue=%s", settings.queue_name)
            async with queue.iterator() as messages:
                async for message in messages:
                    await consumer.handle(message)


async def main() -> None:
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    assert task is not None
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, task.cancel)
    try:
        await run_worker()
    except asyncio.CancelledError:
        # Cancelling the iterator stops the subscription; closing the connection
        # returns any in-flight, unacknowledged command to RabbitMQ.
        logger.info("Command worker stopped")
    finally:
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(signum)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
