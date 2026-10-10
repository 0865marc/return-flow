from aio_pika.abc import AbstractChannel, AbstractQueue


async def declare_topology(channel: AbstractChannel, queue_name: str) -> AbstractQueue:
    """Declare both destinations before accepting work, even without a worker."""
    failed_queue_name = f"{queue_name}.failed"
    await channel.declare_queue(failed_queue_name, durable=True)
    return await channel.declare_queue(
        queue_name,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": failed_queue_name,
        },
    )
