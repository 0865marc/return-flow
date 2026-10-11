import asyncio
import logging
import signal

from aio_pika import connect_robust
from aio_pika.abc import AbstractIncomingMessage

from adapters.messaging.consumer import CommandConsumer
from adapters.messaging.settings import MessagingSettings
from adapters.messaging.topology import declare_topology
from composition import PersistenceFactory, open_persistence

logger = logging.getLogger(__name__)


async def run_worker(
    settings: MessagingSettings | None = None,
    database_url: str | None = None,
    *,
    persistence_factory: PersistenceFactory | None = None,
) -> None:
    settings = settings or MessagingSettings.from_env()
    async with open_persistence(database_url, factory=persistence_factory) as repositories:
        if repositories.commands is None:
            raise RuntimeError("The worker requires a command store.")
        connection = await connect_robust(
            settings.url, timeout=settings.connect_timeout
        )
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=settings.worker_concurrency)
            queue = await declare_topology(channel, settings.queue_name)
            consumer = CommandConsumer(repositories.commands)
            slots = asyncio.Semaphore(settings.worker_concurrency)

            async def handle(message: AbstractIncomingMessage) -> None:
                try:
                    await consumer.handle(message)
                finally:
                    slots.release()

            logger.info(
                "Command worker consuming queue=%s concurrency=%s",
                settings.queue_name,
                settings.worker_concurrency,
            )
            async with queue.iterator() as messages:
                # Cancel and await handlers before closing their messaging and
                # database resources. A handler failure stops its siblings too;
                # unacknowledged messages remain available for redelivery.
                async with asyncio.TaskGroup() as tasks:
                    async for message in messages:
                        # Acquire before spawning to bound tasks as well as work.
                        await slots.acquire()
                        tasks.create_task(handle(message))


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
