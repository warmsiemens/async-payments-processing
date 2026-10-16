import asyncio
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select

from app.models import Outbox, Payment
from app.schemas.payment import PaymentCreate
from app.services.payments import create_payment


@pytest.mark.parametrize(
    "path", ["/api/v1/payments", "/health", "/docs", "/redoc", "/openapi.json", "/missing"]
)
@pytest.mark.parametrize("key", [None, "wrong-key"])
async def test_every_endpoint_requires_api_key(unit_client, path, key):
    unit_client.headers.pop("X-API-Key")
    response = await unit_client.get(path, headers={"X-API-Key": key} if key else {})
    assert response.status_code == 401


async def test_unicode_api_key_is_rejected(unit_client):
    response = await unit_client.get("/health", headers={b"X-API-Key": b"\xff"})
    assert response.status_code == 401


@pytest.mark.parametrize(
    "changes",
    [
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"amount": "NaN"},
        {"amount": "10000000000000000"},
        {"currency": "GBP"},
        {"webhook_url": "ftp://test/file"},
        {"webhook_url": "http://user:password@client.test"},
        {"metadata": []},
        {"unexpected": 1},
    ],
)
async def test_invalid_payment_is_rejected(unit_client, payment_body, changes):
    response = await unit_client.post(
        "/api/v1/payments", json=payment_body | changes, headers={"Idempotency-Key": "test"}
    )
    assert response.status_code == 422


@pytest.mark.parametrize("key", [None, "", "contains spaces", "x" * 256])
async def test_invalid_idempotency_key(unit_client, payment_body, key):
    response = await unit_client.post(
        "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": key} if key is not None else {}
    )
    assert response.status_code == 422


async def test_invalid_uuid(unit_client):
    assert (await unit_client.get("/api/v1/payments/not-a-uuid")).status_code == 422


async def test_openapi_describes_security(unit_client):
    response = await unit_client.get("/openapi.json")
    assert response.status_code == 200
    assert response.json()["components"]["securitySchemes"]["APIKeyHeader"]["name"] == "X-API-Key"


@pytest.mark.integration
async def test_create_get_and_idempotent_replay(api_client, database, payment_body):
    headers = {"Idempotency-Key": "order-42"}
    first = await api_client.post("/api/v1/payments", json=payment_body, headers=headers)
    assert first.status_code == 202
    assert first.json()["status"] == "pending"
    replay = await api_client.post("/api/v1/payments", json=payment_body, headers=headers)
    assert replay.json() == first.json()
    detail = await api_client.get(f"/api/v1/payments/{first.json()['payment_id']}")
    assert detail.status_code == 200
    assert detail.json()["amount"] == "120.50"
    assert detail.json()["metadata"] == {"order_id": 42}
    assert detail.json()["processed_at"] is None
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        event = (await session.execute(select(Outbox))).scalar_one()
        assert event.payment_id == UUID(first.json()["payment_id"])
        assert event.routing_key == "payments.new"


@pytest.mark.integration
async def test_changed_body_returns_conflict(api_client, payment_body):
    headers = {"Idempotency-Key": "order-42"}
    await api_client.post("/api/v1/payments", json=payment_body, headers=headers)
    response = await api_client.post(
        "/api/v1/payments", json=payment_body | {"amount": "99.00"}, headers=headers
    )
    assert response.status_code == 409


@pytest.mark.integration
async def test_decimal_representation_does_not_change_idempotency(api_client, payment_body):
    headers = {"Idempotency-Key": "order-42"}
    first = await api_client.post(
        "/api/v1/payments", json=payment_body | {"amount": "120.5"}, headers=headers
    )
    second = await api_client.post("/api/v1/payments", json=payment_body, headers=headers)
    assert first.json() == second.json()


@pytest.mark.integration
async def test_concurrent_requests_create_one_payment(api_client, database, payment_body):
    responses = await asyncio.gather(
        *[
            api_client.post("/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "concurrent"})
            for _ in range(10)
        ]
    )
    assert all(response.status_code == 202 for response in responses)
    assert len({response.json()["payment_id"] for response in responses}) == 1
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


@pytest.mark.integration
async def test_payment_and_outbox_roll_back_together(database, payment_body):
    with pytest.raises(RuntimeError):
        async with database.sessions() as session, session.begin():
            await create_payment(session, PaymentCreate(**payment_body), "rollback")
            raise RuntimeError("Transaction interrupted")
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


@pytest.mark.integration
async def test_unknown_payment(api_client):
    assert (await api_client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
