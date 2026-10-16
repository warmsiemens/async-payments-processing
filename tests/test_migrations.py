import os
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import create_async_engine

from app.db.base import Base
from app.db.session import Database

pytestmark = pytest.mark.integration


async def test_migrations_upgrade_downgrade_and_preserve_payments():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_DATABASE_URL to run migration integration tests")
    schema = f"test_migrations_{uuid4().hex}"
    admin = create_async_engine(url)
    async with admin.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    database = Database(url, connect_args={"server_settings": {"search_path": schema}})
    payment_id = uuid4()

    def migrate(connection, operation, revision):
        config = Config("alembic.ini")
        config.attributes["connection"] = connection
        operation(config, revision)

    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(migrate, command.upgrade, "0001")
            await connection.execute(
                text(
                    "INSERT INTO payments (id, amount, currency, description, metadata, "
                    "idempotency_key, request_hash, webhook_url) "
                    "VALUES (:id, 1.00, 'RUB', 'Migration test', '{}', 'migration', :hash, "
                    "'http://client.test/webhook')"
                ),
                {"id": payment_id, "hash": "a" * 64},
            )
            await connection.run_sync(migrate, command.upgrade, "head")

            def assert_columns(sync_connection):
                inspector = inspect(sync_connection)
                for name in ("payments", "outbox"):
                    assert {column["name"] for column in inspector.get_columns(name)} == {
                        column.name for column in Base.metadata.tables[name].columns
                    }

            await connection.run_sync(assert_columns)
            assert (
                await connection.scalar(
                    text("SELECT webhook_attempts FROM payments WHERE id=:id"), {"id": payment_id}
                )
                == 0
            )
            await connection.run_sync(migrate, command.downgrade, "0001")
            assert (
                await connection.scalar(
                    text("SELECT count(*) FROM payments WHERE id=:id"), {"id": payment_id}
                )
                == 1
            )
            await connection.run_sync(migrate, command.upgrade, "head")
            assert await connection.scalar(text("SELECT version_num FROM alembic_version")) == "0002"
            await connection.run_sync(assert_columns)
    finally:
        await database.close()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()
