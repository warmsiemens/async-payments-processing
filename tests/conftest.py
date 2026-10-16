import os
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.application import create_app
from app.core.config import Settings
from app.db.base import Base
from app.db.session import Database

TEST_KEY = "test-api-key-at-least-16-chars"


@pytest.fixture
def settings():
    return Settings(_env_file=None, api_key=TEST_KEY, retry_base_delay=0.02)


@pytest.fixture
async def unit_client(settings):
    database = Database(settings.database_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(settings, database)),
        base_url="http://test",
        headers={"X-API-Key": TEST_KEY},
    ) as client:
        yield client
    await database.close()


@pytest.fixture
async def database():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests")
    # Each test uses its own schema; no truncation or deletion of application data.
    schema = f"test_{uuid4().hex}"
    admin = create_async_engine(url)
    async with admin.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    database = Database(url, connect_args={"server_settings": {"search_path": schema}})
    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        yield database
    finally:
        await database.close()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()


@pytest.fixture
async def api_client(settings, database):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(settings, database)),
        base_url="http://test",
        headers={"X-API-Key": TEST_KEY},
    ) as client:
        yield client


@pytest.fixture
def payment_body():
    return {
        "amount": "120.50",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": {"order_id": 42},
        "webhook_url": "http://client.test/webhook",
    }
