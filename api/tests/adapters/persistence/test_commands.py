import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime
from uuid import UUID, uuid4

import pytest
from psycopg import AsyncConnection, OperationalError
from psycopg.pq import TransactionStatus

from adapters.commands.contracts import (
    CreateDeliveryCommand,
    IdempotencyConflictError,
    RequestReturnCommand,
)
from adapters.persistence import Repositories, postgres
from adapters.persistence.commands import PostgresCommandStore
from adapters.persistence.postgres import initialize_database
from application.errors import BusinessRuleViolationError, EntityNotFoundError
from application.use_cases.deliveries import CreateDelivery
from application.use_cases.returns import RequestReturn
from domain.delivery import Delivery, DeliveryStatus
from domain.returns import Return

pytestmark = [pytest.mark.integration, pytest.mark.anyio]


@pytest.fixture
async def command_repositories(
    database_url: str, postgres_autocommit: bool,
) -> AsyncGenerator[Repositories]:
    async with postgres.open_repositories(
        database_url, autocommit=postgres_autocommit
    ) as repositories:
        yield repositories


async def create_delivery(repositories: Repositories) -> dict[str, str]:
    delivery = await CreateDelivery(repositories.deliveries).execute()
    return {"delivery_id": str(delivery.id)}


async def test_register_and_lookup_are_persistent_and_idempotent(
    command_repositories: Repositories,
) -> None:
    store = command_repositories.commands
    assert store is not None
    command = CreateDeliveryCommand()

    first = await store.register(command)

    assert first.status == "pending"
    assert first.result is None and first.error is None
    assert first.created_at is not None and first.created_at.tzinfo is not None
    assert first.completed_at is None
    assert await store.register(command) == first
    assert await store.get(command.command_id) == first
    assert await store.get(uuid4()) is None


async def test_command_lookup_only_opens_implicit_transaction_when_autocommit_is_disabled(
    command_repositories: Repositories, postgres_autocommit: bool,
) -> None:
    store = command_repositories.commands
    assert isinstance(store, PostgresCommandStore)
    record = await store.register(CreateDeliveryCommand())
    statuses: list[TransactionStatus] = []

    @asynccontextmanager
    async def observe_connection() -> AsyncGenerator[AsyncConnection]:
        async with store.connection_factory() as connection:
            yield connection
            # Inspect before the pool's context manager can commit an implicit
            # transaction. IDLE afterward would also pass with an extra COMMIT.
            statuses.append(connection.info.transaction_status)

    observed_store = PostgresCommandStore(observe_connection)
    assert await observed_store.get(record.command_id) == record
    assert await observed_store.get(uuid4()) is None
    expected = TransactionStatus.IDLE if postgres_autocommit else TransactionStatus.INTRANS
    assert statuses == [expected, expected]


@pytest.mark.parametrize("changed", ["type", "payload"])
async def test_reused_id_with_different_command_is_rejected(
    command_repositories: Repositories, changed: str
) -> None:
    store = command_repositories.commands
    assert store is not None
    command = RequestReturnCommand(delivery_id=uuid4())
    original = await store.register(command)
    different = (
        CreateDeliveryCommand(command_id=command.command_id)
        if changed == "type"
        else RequestReturnCommand(command_id=command.command_id, delivery_id=uuid4())
    )

    with pytest.raises(IdempotencyConflictError):
        await store.register(different)
    with pytest.raises(IdempotencyConflictError):
        await store.execute(different, create_delivery)

    assert await store.get(command.command_id) == original


async def test_completed_command_reuses_persisted_result(
    command_repositories: Repositories, database_url: str
) -> None:
    store = command_repositories.commands
    assert store is not None
    command = CreateDeliveryCommand()
    first = await store.execute(command, create_delivery)

    async def must_not_execute(repositories: Repositories) -> dict[str, str]:
        pytest.fail("A completed command must not execute again")

    assert first.status == "completed"
    assert first.result is not None
    assert first.created_at is not None and first.completed_at is not None
    assert first.completed_at >= first.created_at
    assert await store.get(command.command_id) == first
    assert await store.register(command) == first
    assert await store.execute(command, must_not_execute) == first
    assert await command_repositories.deliveries.get(UUID(first.result["delivery_id"]))
    async with await AsyncConnection.connect(database_url) as connection:
        cursor = await connection.execute("SELECT COUNT(*) FROM deliveries")
        assert await cursor.fetchone() == (1,)


@pytest.mark.parametrize("registered", [False, True])
async def test_concurrent_copies_commit_only_one_business_operation(
    command_repositories: Repositories, database_url: str, registered: bool
) -> None:
    store = command_repositories.commands
    assert store is not None
    command = CreateDeliveryCommand()
    if registered:
        await store.register(command)
    business_written = asyncio.Event()
    allow_commit = asyncio.Event()
    deliveries: list[Delivery] = []

    async def handler(repositories: Repositories) -> dict[str, str]:
        delivery = await CreateDelivery(repositories.deliveries).execute()
        deliveries.append(delivery)
        business_written.set()
        await allow_commit.wait()
        return {"delivery_id": str(delivery.id)}

    first = asyncio.create_task(store.execute(command, handler))
    second = None
    try:
        await asyncio.wait_for(business_written.wait(), timeout=5)
        second = asyncio.create_task(store.execute(command, handler))
        await asyncio.sleep(0.05)
        assert not second.done()
        # Neither business writes nor completion are visible before COMMIT.
        assert await command_repositories.deliveries.get(deliveries[0].id) is None
        current = await store.get(command.command_id)
        if registered:
            assert current is not None and current.status == "pending"
            assert current.completed_at is None
        else:
            assert current is None
    finally:
        allow_commit.set()
        results = await asyncio.gather(
            first, *([second] if second is not None else [])
        )

    assert len(deliveries) == 1
    assert len(results) == 2 and results[0] == results[1]
    assert results[0].status == "completed"
    assert await store.get(command.command_id) == results[0]
    async with await AsyncConnection.connect(database_url) as connection:
        cursor = await connection.execute("SELECT COUNT(*) FROM deliveries")
        assert await cursor.fetchone() == (1,)


@pytest.mark.parametrize("registered", [False, True])
async def test_cancellation_rolls_back_and_redelivery_can_execute(
    command_repositories: Repositories, registered: bool
) -> None:
    store = command_repositories.commands
    assert store is not None
    command = CreateDeliveryCommand()
    if registered:
        await store.register(command)
    business_written = asyncio.Event()
    delivery = Delivery()

    async def interrupted(repositories: Repositories) -> dict[str, str]:
        await repositories.deliveries.add(delivery)
        business_written.set()
        await asyncio.Event().wait()
        raise AssertionError("Unreachable")

    task = asyncio.create_task(store.execute(command, interrupted))
    try:
        await asyncio.wait_for(business_written.wait(), timeout=5)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert await command_repositories.deliveries.get(delivery.id) is None
    current = await store.get(command.command_id)
    if registered:
        assert current is not None and current.status == "pending"
        assert current.completed_at is None
    else:
        assert current is None
    result = await store.execute(command, create_delivery)
    assert result.status == "completed"


@pytest.mark.parametrize(
    ("error", "expected_error", "expected_code"),
    [
        (BusinessRuleViolationError("Delivery is pending."), "Delivery is pending.", "business_rule"),
        (EntityNotFoundError("Delivery not found."), "Delivery not found.", "not_found"),
        (RuntimeError("private database details"), "Command execution failed.", "execution_failed"),
    ],
)
async def test_failure_rolls_back_partial_writes_and_replays_the_error(
    command_repositories: Repositories, error: Exception, expected_error: str,
    expected_code: str,
) -> None:
    store = command_repositories.commands
    assert store is not None
    command = CreateDeliveryCommand()
    delivery = Delivery()

    async def failing(repositories: Repositories) -> dict[str, str]:
        await repositories.deliveries.add(delivery)
        raise error

    record = await store.execute(command, failing)

    assert record.status == "failed"
    assert record.result is None
    assert record.error == expected_error
    assert record.error_code == expected_code
    assert record.created_at is not None and record.completed_at is not None
    assert record.completed_at >= record.created_at
    assert await command_repositories.deliveries.get(delivery.id) is None
    assert await store.get(command.command_id) == record
    # Terminal failures also remain idempotent; a new command can retry the operation.
    assert await store.execute(command, create_delivery) == record


async def test_database_error_rolls_back_savepoint_and_persists_failure(
    command_repositories: Repositories,
) -> None:
    store = command_repositories.commands
    assert store is not None
    command = CreateDeliveryCommand()
    delivery = Delivery()

    async def failing(repositories: Repositories) -> dict[str, str]:
        await repositories.deliveries.add(delivery)
        await repositories.returns.add(Return(delivery_id=uuid4()))
        raise AssertionError("Foreign key should reject the return")

    record = await store.execute(command, failing)

    assert record.status == "failed"
    assert record.error == "Command execution failed."
    assert await command_repositories.deliveries.get(delivery.id) is None
    assert await store.get(command.command_id) == record


async def test_lost_connection_before_commit_does_not_report_success(
    command_repositories: Repositories, database_url: str
) -> None:
    store = command_repositories.commands
    assert store is not None
    command = CreateDeliveryCommand()
    await store.register(command)
    delivery = Delivery()

    async def interrupted(repositories: Repositories) -> dict[str, str]:
        await repositories.deliveries.add(delivery)
        backend_pid = repositories.deliveries.connection.info.backend_pid
        async with await AsyncConnection.connect(database_url) as connection:
            await connection.execute("SELECT pg_terminate_backend(%s)", (backend_pid,))
        return {"delivery_id": str(delivery.id)}

    with pytest.raises(OperationalError):
        await store.execute(command, interrupted)

    assert await command_repositories.deliveries.get(delivery.id) is None
    record = await store.get(command.command_id)
    assert record is not None and record.status == "pending"
    assert record.completed_at is None
    assert (await store.execute(command, create_delivery)).status == "completed"


async def test_completion_timestamp_uses_clock_after_business_operation(
    command_repositories: Repositories, database_url: str
) -> None:
    store = command_repositories.commands
    assert store is not None
    command = CreateDeliveryCommand()
    registered = await store.register(command)
    business_finished_at: datetime | None = None

    async def handler(repositories: Repositories) -> dict[str, str]:
        nonlocal business_finished_at
        result = await create_delivery(repositories)
        async with await AsyncConnection.connect(database_url) as connection:
            cursor = await connection.execute("SELECT clock_timestamp()")
            row = await cursor.fetchone()
            assert row is not None
            business_finished_at = row[0]
        return result

    completed = await store.execute(command, handler)

    assert completed.created_at == registered.created_at
    assert completed.created_at is not None
    assert completed.completed_at is not None and business_finished_at is not None
    assert completed.created_at <= business_finished_at <= completed.completed_at
    assert await store.execute(command, handler) == completed


async def test_schema_upgrade_preserves_unknown_legacy_timestamps(
    postgres_url: str,
) -> None:
    legacy_id = uuid4()
    async with await AsyncConnection.connect(postgres_url) as connection:
        await connection.execute(
            "CREATE TABLE commands (command_id UUID PRIMARY KEY, type TEXT NOT NULL, "
            "payload JSONB NOT NULL, status TEXT NOT NULL, result JSONB, error TEXT)"
        )
        await connection.execute(
            "INSERT INTO commands (command_id, type, payload, status, result) "
            "VALUES (%s, 'create_delivery', '{}'::jsonb, 'completed', "
            "'{\"delivery_id\": \"legacy\"}'::jsonb)",
            (legacy_id,),
        )

    # Repeated startup upgrades must be safe and must not fabricate timing for
    # commands that completed before instrumentation existed.
    await initialize_database(postgres_url)
    await initialize_database(postgres_url)

    async with postgres.open_repositories(postgres_url) as repositories:
        assert repositories.commands is not None
        legacy = await repositories.commands.get(legacy_id)
        assert legacy is not None and legacy.status == "completed"
        assert legacy.created_at is None and legacy.completed_at is None
        assert legacy.error_code is None
        current = await repositories.commands.register(CreateDeliveryCommand())
        assert current.created_at is not None and current.completed_at is None


async def test_distinct_commands_keep_allowing_multiple_returns_for_one_delivery(
    command_repositories: Repositories, database_url: str
) -> None:
    store = command_repositories.commands
    assert store is not None
    delivery = Delivery(status=DeliveryStatus.DELIVERED)
    await command_repositories.deliveries.add(delivery)

    async def request_return(repositories: Repositories) -> dict[str, str]:
        returned = await RequestReturn(
            repositories.deliveries, repositories.returns
        ).execute(delivery.id)
        return {"return_id": str(returned.id)}

    first = await store.execute(
        RequestReturnCommand(delivery_id=delivery.id), request_return
    )
    second = await store.execute(
        RequestReturnCommand(delivery_id=delivery.id), request_return
    )

    assert first.status == second.status == "completed"
    assert first.result != second.result
    async with await AsyncConnection.connect(database_url) as connection:
        cursor = await connection.execute("SELECT COUNT(*) FROM returns")
        assert await cursor.fetchone() == (2,)
