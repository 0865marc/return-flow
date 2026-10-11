import asyncio
import logging

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractConnection
from aio_pika.exceptions import AMQPException, ChannelInvalidStateError
from pamqp.commands import Basic

from adapters.commands.contracts import Command
from .settings import MessagingSettings
from .topology import declare_topology

logger = logging.getLogger(__name__)


class MessagingUnavailableError(RuntimeError):
    """Publication could not be confirmed; the command may have reached the broker."""


class RabbitMQPublisher:
    def __init__(self, settings: MessagingSettings) -> None:
        self.settings = settings
        self._connection: AbstractConnection | None = None
        self._channel: AbstractChannel | None = None
        self._lock = asyncio.Lock()

    async def _get_channel(self) -> AbstractChannel:
        if (
            self._connection is None
            or self._connection.is_closed
            or self._channel is None
            or self._channel.is_closed
        ):
            await self._disconnect()
            self._connection = await aio_pika.connect(
                self.settings.url, timeout=self.settings.connect_timeout
            )
            self._channel = await self._connection.channel(
                publisher_confirms=True, on_return_raises=True
            )
            await declare_topology(self._channel, self.settings.queue_name)
        return self._channel

    async def publish(self, command: Command) -> None:
        try:
            # Bound the whole operation, including connection setup and lock waits.
            async with asyncio.timeout(self.settings.publish_timeout):
                async with self._lock:
                    try:
                        channel = await self._get_channel()
                        confirmation = await channel.default_exchange.publish(
                            aio_pika.Message(
                                body=command.model_dump_json().encode(),
                                content_type="application/json",
                                content_encoding="utf-8",
                                message_id=str(command.command_id),
                                type=command.type,
                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            ),
                            routing_key=self.settings.queue_name,
                            mandatory=True,
                            timeout=self.settings.publish_timeout,
                        )
                        if not isinstance(confirmation, Basic.Ack):
                            raise MessagingUnavailableError(
                                "Missing publisher confirmation."
                            )
                    except BaseException:
                        # A subsequent request reconnects; never replay an uncertain publish.
                        await self._disconnect()
                        raise
        except (
            AMQPException,
            ChannelInvalidStateError,
            OSError,
            TimeoutError,
            MessagingUnavailableError,
        ) as error:
            logger.warning(
                "Command publication unconfirmed command_id=%s error=%s",
                command.command_id,
                type(error).__name__,
            )
            raise MessagingUnavailableError(
                "Command publication could not be confirmed."
            ) from error
        logger.info("Command accepted command_id=%s", command.command_id)

    async def _disconnect(self) -> None:
        connection, self._connection = self._connection, None
        self._channel = None
        if connection is not None:
            try:
                async with asyncio.timeout(self.settings.connect_timeout):
                    await connection.close()
            except (AMQPException, ChannelInvalidStateError, OSError, TimeoutError):
                logger.warning("Failed to close RabbitMQ publisher connection.")

    async def close(self) -> None:
        async with self._lock:
            await self._disconnect()
