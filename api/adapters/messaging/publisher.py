import asyncio
import logging
from dataclasses import dataclass
from typing import cast
from uuid import uuid4

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractConnection
from aio_pika.exceptions import AMQPException, ChannelInvalidStateError
from aiormq import Channel
from pamqp.commands import Basic

from adapters.commands.contracts import Command
from .settings import MessagingSettings
from .topology import declare_topology

logger = logging.getLogger(__name__)


class MessagingUnavailableError(RuntimeError):
    """Publication could not be confirmed; the command may have reached the broker."""


@dataclass
class _PublisherSession:
    connection: AbstractConnection
    channel: AbstractChannel
    in_flight: int = 0
    retired: bool = False
    closing: bool = False


class RabbitMQPublisher:
    def __init__(self, settings: MessagingSettings) -> None:
        self.settings = settings
        self._session: _PublisherSession | None = None
        self._connection_lock = asyncio.Lock()
        self._slots = asyncio.Semaphore(settings.publish_concurrency)
        self._close_tasks: set[asyncio.Task[None]] = set()
        self._in_flight = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self._closed = False

    async def _acquire_session(self) -> _PublisherSession:
        # Serialize setup only: each publication waits for its own confirmation.
        async with self._connection_lock:
            if self._closed:
                raise MessagingUnavailableError("Publisher is closed.")
            session = self._session
            if session is not None and (
                session.connection.is_closed or session.channel.is_closed
            ):
                self._retire(session)
                session = None
            if session is None:
                connection: AbstractConnection | None = None
                try:
                    connection = await aio_pika.connect(
                        self.settings.url, timeout=self.settings.connect_timeout
                    )
                    channel = await connection.channel(
                        publisher_confirms=True, on_return_raises=True
                    )
                    await declare_topology(channel, self.settings.queue_name)
                except BaseException:
                    if connection is not None:
                        self._schedule_close(connection)
                    raise
                session = _PublisherSession(connection, channel)
                self._session = session
            session.in_flight += 1
            self._in_flight += 1
            self._idle.clear()
            return session

    async def publish(self, command: Command) -> None:
        try:
            # Bound the whole operation, including setup and the concurrency limit.
            async with asyncio.timeout(self.settings.publish_timeout):
                async with self._slots:
                    session = await self._acquire_session()
                    confirmed = False
                    try:
                        message = aio_pika.Message(
                            body=command.model_dump_json().encode(),
                            content_type="application/json",
                            content_encoding="utf-8",
                            # aiormq correlates mandatory returns by message_id;
                            # concurrent retries of one command need distinct IDs.
                            message_id=str(uuid4()),
                            correlation_id=str(command.command_id),
                            type=command.type,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        )
                        # aio-pika creates an aiormq.Channel, whose concrete API
                        # exposes `wait`; its AbstractChannel interface does not.
                        channel = cast(
                            Channel, await session.channel.get_underlay_channel()
                        )
                        confirmation = await channel.basic_publish(
                            body=message.body,
                            properties=message.properties,
                            exchange="",
                            routing_key=self.settings.queue_name,
                            mandatory=True,
                            timeout=self.settings.publish_timeout,
                            # Skip the local drain under the channel lock. The
                            # broker confirmation below remains mandatory.
                            wait=False,
                        )
                        if not isinstance(confirmation, Basic.Ack):
                            raise MessagingUnavailableError(
                                "Missing publisher confirmation."
                            )
                        confirmed = True
                    finally:
                        self._release_session(session, failed=not confirmed)
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

    def _retire(self, session: _PublisherSession) -> None:
        # Retire only this generation. A late failure must not detach its replacement.
        if self._session is session:
            self._session = None
        session.retired = True
        if session.in_flight == 0 and not session.closing:
            session.closing = True
            self._schedule_close(session.connection)

    def _release_session(self, session: _PublisherSession, *, failed: bool) -> None:
        session.in_flight -= 1
        if failed or session.retired:
            # Never replay an uncertain publish or cancel its siblings' confirmations.
            self._retire(session)
        self._in_flight -= 1
        if self._in_flight == 0:
            self._idle.set()

    def _schedule_close(self, connection: AbstractConnection) -> None:
        # Cleanup must not extend a request's publication deadline.
        task = asyncio.create_task(self._close_connection(connection))
        self._close_tasks.add(task)
        task.add_done_callback(self._close_tasks.discard)

    async def _close_connection(self, connection: AbstractConnection) -> None:
        try:
            async with asyncio.timeout(self.settings.connect_timeout):
                await connection.close()
        except (AMQPException, ChannelInvalidStateError, OSError, TimeoutError):
            logger.warning("Failed to close RabbitMQ publisher connection.")

    async def close(self) -> None:
        async with self._connection_lock:
            self._closed = True
            if self._session is not None:
                self._retire(self._session)
        await self._idle.wait()
        if self._close_tasks:
            await asyncio.gather(*self._close_tasks)
