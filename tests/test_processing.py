import asyncio
import json
import logging
from datetime import timedelta
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from app.core.enums import PaymentStatus
from app.core.time import utcnow
from app.messaging.constants import DLQ_QUEUE, PAYMENTS_QUEUE
from app.models import Outbox, Payment
from app.schemas.events import PaymentEvent
from app.schemas.payment import PaymentCreate
from app.services.outbox import publish_one
from app.services.payments import create_payment
from app.services.processing import PaymentProcessor

pytestmark = pytest.mark.integration


async def seed(database, body):
    async with database.sessions() as session, session.begin():
        payment = await create_payment(session, PaymentCreate(**body), "processing")
    async with database.sessions() as session:
        record = (await session.execute(select(Outbox))).scalar_one()
        return payment.id, PaymentEvent.model_validate(record.payload)


@pytest.mark.parametrize("result", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_gateway_result_persisted_before_callback_and_duplicates_skipped(
    database,
    payment_body,
    settings,
    monkeypatch,
    result,
):
    payment_id, event = await seed(database, payment_body)
    gateway = AsyncMock(return_value=result)
    monkeypatch.setattr("app.services.processing.emulate_gateway", gateway)
    callbacks = []

    async def receive(request):
        async with database.sessions() as session:
            payment = await session.get(Payment, payment_id)
            assert payment.status == result
            assert payment.processed_at is not None
        callbacks.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        processor = PaymentProcessor(database, client, settings)
        await asyncio.gather(processor.process(event), processor.process(event))
        await processor.process(event)
    gateway.assert_awaited_once()
    assert len(callbacks) == 1
    async with database.sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.webhook_delivered_at is not None
        assert (await session.get(Outbox, event.event_id)).consumed_at is not None


async def test_three_attempts_then_dlq_without_reprocessing_gateway(
    database,
    payment_body,
    settings,
    monkeypatch,
):
    payment_id, event = await seed(database, payment_body)
    gateway = AsyncMock(return_value=PaymentStatus.SUCCEEDED)
    monkeypatch.setattr("app.services.processing.emulate_gateway", gateway)
    keys = []

    def receive(request):
        keys.append(request.headers["Idempotency-Key"])
        return httpx.Response(503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        processor = PaymentProcessor(database, client, settings)
        for attempt in range(1, 4):
            await processor.process(event)
            await processor.process(event)  # Redelivery must not create an extra retry.
            async with database.sessions() as session:
                if attempt < 3:
                    retry = (
                        await session.execute(
                            select(Outbox).where(
                                Outbox.routing_key == PAYMENTS_QUEUE,
                                Outbox.consumed_at.is_(None),
                            )
                        )
                    ).scalar_one()
                    assert retry.payload["attempt"] == attempt + 1
                    original = await session.get(Outbox, event.event_id)
                    delay = (retry.available_at - original.consumed_at).total_seconds()
                    assert delay >= settings.retry_base_delay * 2 ** (attempt - 1)
                    event = PaymentEvent.model_validate(retry.payload)
    assert len(keys) == 3
    assert len(set(keys)) == 1
    gateway.assert_awaited_once()
    async with database.sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == PaymentStatus.SUCCEEDED
        assert payment.processing_attempts == 1
        assert payment.webhook_attempts == 3
        assert payment.webhook_delivered_at is None
        dlq = (await session.execute(select(Outbox).where(Outbox.routing_key == DLQ_QUEUE))).scalar_one()
        assert dlq.payload["attempts"] == 3
        assert dlq.published_at is None
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 4


async def test_successful_webhook_retry_keeps_gateway_result(database, payment_body, settings, monkeypatch):
    payment_id, event = await seed(database, payment_body)
    gateway = AsyncMock(return_value=PaymentStatus.FAILED)
    monkeypatch.setattr("app.services.processing.emulate_gateway", gateway)
    replies = iter([500, 204])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(next(replies)))
    ) as client:
        processor = PaymentProcessor(database, client, settings)
        await processor.process(event)
        async with database.sessions() as session:
            retry = (await session.execute(select(Outbox).where(Outbox.consumed_at.is_(None)))).scalar_one()
        await processor.process(PaymentEvent.model_validate(retry.payload))
    gateway.assert_awaited_once()
    async with database.sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == PaymentStatus.FAILED
        assert payment.webhook_delivered_at is not None
        assert payment.last_error is None


async def test_publication_failure_is_retained_and_later_confirmed(database, payment_body, settings):
    _, event = await seed(database, payment_body)
    broker = AsyncMock()
    broker.publish.side_effect = ConnectionError("broker unavailable")
    assert await publish_one(database, broker, settings)
    async with database.sessions() as session, session.begin():
        record = await session.get(Outbox, event.event_id)
        assert record.published_at is None
        assert record.publish_attempts == 1
        # Make the persisted retry due without making this test depend on wall-clock timing.
        record.available_at = record.created_at
    broker.publish.side_effect = None
    assert await publish_one(database, broker, settings)
    async with database.sessions() as session:
        record = await session.get(Outbox, event.event_id)
        assert record.published_at is not None
        assert record.publish_attempts == 2
    assert broker.publish.call_args.kwargs["persist"] is True
    assert broker.publish.call_args.kwargs["mandatory"] is True


async def test_gateway_exception_also_exhausts_attempts(database, payment_body, settings, monkeypatch):
    payment_id, event = await seed(database, payment_body)
    gateway = AsyncMock(side_effect=TimeoutError())
    monkeypatch.setattr("app.services.processing.emulate_gateway", gateway)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200))
    ) as client:
        processor = PaymentProcessor(database, client, settings)
        for attempt in range(1, 4):
            await processor.process(event)
            if attempt < 3:
                async with database.sessions() as session:
                    record = (
                        await session.execute(select(Outbox).where(Outbox.consumed_at.is_(None)))
                    ).scalar_one()
                    event = PaymentEvent.model_validate(record.payload)
    assert gateway.await_count == 3
    async with database.sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == PaymentStatus.FAILED
        assert payment.processed_at is not None
        callback = (
            await session.execute(
                select(Outbox).where(Outbox.routing_key == PAYMENTS_QUEUE, Outbox.consumed_at.is_(None))
            )
        ).scalar_one()
        assert callback.payload["stage"] == "webhook"
        assert callback.payload["attempt"] == 1


async def next_event(database):
    async with database.sessions() as session:
        record = (
            await session.execute(
                select(Outbox).where(Outbox.routing_key == PAYMENTS_QUEUE, Outbox.consumed_at.is_(None))
            )
        ).scalar_one()
        return PaymentEvent.model_validate(record.payload)


async def test_gateway_and_webhook_do_not_hold_database_connections(
    database, payment_body, settings, monkeypatch
):
    payment_id, event = await seed(database, payment_body)

    async def assert_unlocked():
        assert database.engine.pool.checkedout() == 0
        async with database.sessions() as session, session.begin():
            await session.get(Payment, payment_id, with_for_update={"nowait": True})
            await session.get(Outbox, event.event_id, with_for_update={"nowait": True})

    async def gateway():
        await assert_unlocked()
        return PaymentStatus.SUCCEEDED

    async def receive(request):
        await assert_unlocked()
        return httpx.Response(200)

    monkeypatch.setattr("app.services.processing.emulate_gateway", gateway)
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        assert await PaymentProcessor(database, client, settings).process(event)
    async with database.sessions() as session:
        assert (await session.get(Payment, payment_id)).webhook_delivered_at is not None


@pytest.mark.parametrize("last_status", [200, 503])
async def test_terminal_gateway_error_is_notified_with_its_own_retry_budget(
    database, payment_body, settings, monkeypatch, last_status
):
    payment_id, event = await seed(database, payment_body)
    gateway = AsyncMock(side_effect=TimeoutError())
    monkeypatch.setattr("app.services.processing.emulate_gateway", gateway)
    callbacks = []
    replies = iter([503, 503, last_status])

    def receive(request):
        callbacks.append(request)
        assert json.loads(request.content)["status"] == "failed"
        return httpx.Response(next(replies))

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        processor = PaymentProcessor(database, client, settings)
        for _ in range(3):
            assert await processor.process(event)
            assert await processor.process(event)  # Duplicate does not create another callback intent.
            event = await next_event(database)
        assert event.stage == "webhook"
        assert event.attempt == 1
        for attempt in range(1, 4):
            assert await processor.process(event)
            assert await processor.process(event)
            if attempt < 3:
                event = await next_event(database)
                assert event.stage == "webhook"
                assert event.attempt == attempt + 1
    assert gateway.await_count == 3
    assert len(callbacks) == 3
    assert len({request.headers["Idempotency-Key"] for request in callbacks}) == 1
    async with database.sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == PaymentStatus.FAILED
        assert (payment.webhook_delivered_at is not None) == (last_status == 200)
        assert payment.processing_attempts == payment.webhook_attempts == 3
        dlq = (await session.execute(select(Outbox).where(Outbox.routing_key == DLQ_QUEUE))).scalars().all()
        assert {record.payload["stage"] for record in dlq} == (
            {"processing"} if last_status == 200 else {"processing", "webhook"}
        )


async def test_programming_error_is_logged_with_traceback_and_not_retried(
    database, payment_body, settings, monkeypatch, caplog
):
    _, event = await seed(database, payment_body)
    gateway = AsyncMock(side_effect=TypeError("Broken gateway implementation"))
    monkeypatch.setattr("app.services.processing.emulate_gateway", gateway)
    caplog.set_level(logging.ERROR, logger="app.services.processing")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200))
    ) as client:
        processor = PaymentProcessor(database, client, settings)
        assert await processor.process(event)
        callback = await next_event(database)
        assert callback.stage == "webhook"
        assert await processor.process(callback)
    gateway.assert_awaited_once()
    assert any(record.exc_info and record.levelno == logging.ERROR for record in caplog.records)
    async with database.sessions() as session:
        dlq = (await session.execute(select(Outbox).where(Outbox.routing_key == DLQ_QUEUE))).scalar_one()
        assert dlq.payload["attempts"] == 1
        assert dlq.payload["error"] == "TypeError"


async def test_expired_payment_lease_is_recovered_and_stale_worker_cannot_commit(
    database, payment_body, settings, monkeypatch
):
    payment_id, event = await seed(database, payment_body)
    entered = asyncio.Event()
    resume = asyncio.Event()
    calls = 0

    async def gateway():
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            await resume.wait()
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED

    monkeypatch.setattr("app.services.processing.emulate_gateway", gateway)
    callbacks = []

    def receive(request):
        callbacks.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        processor = PaymentProcessor(database, client, settings)
        async with asyncio.TaskGroup() as tasks:
            stale = tasks.create_task(processor.process(event))
            await asyncio.wait_for(entered.wait(), timeout=5)
            assert await processor.process(event) is False
            async with database.sessions() as session, session.begin():
                payment = await session.get(Payment, payment_id, with_for_update={"nowait": True})
                payment.worker_lease_until = utcnow() - timedelta(seconds=1)
            assert await processor.process(event)
            resume.set()
        assert stale.result() is False
    assert len(callbacks) == 1
    assert json.loads(callbacks[0].content)["status"] == "failed"
    async with database.sessions() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == PaymentStatus.FAILED
        assert payment.worker_token is None


async def test_publication_is_outside_transaction_and_expired_claim_is_fenced(
    database, payment_body, settings
):
    _, event = await seed(database, payment_body)
    entered = asyncio.Event()
    resume = asyncio.Event()
    first = AsyncMock()
    second = AsyncMock()

    async def publish(*args, **kwargs):
        assert database.engine.pool.checkedout() == 0
        entered.set()
        await resume.wait()
        raise ConnectionError("Late failure from the old publisher")

    first.publish.side_effect = publish
    async with asyncio.TaskGroup() as tasks:
        stale = tasks.create_task(publish_one(database, first, settings))
        await asyncio.wait_for(entered.wait(), timeout=5)
        assert await publish_one(database, second, settings) is False
        async with database.sessions() as session, session.begin():
            record = await session.get(Outbox, event.event_id, with_for_update={"nowait": True})
            record.publish_lease_until = utcnow() - timedelta(seconds=1)
        assert await publish_one(database, second, settings)
        resume.set()
    assert stale.result()
    async with database.sessions() as session:
        record = await session.get(Outbox, event.event_id)
        assert record.published_at is not None
        assert record.last_error is None
        assert record.publish_token is None
        assert record.publish_attempts == 2


async def test_failed_retry_commit_preserves_original_event_and_gateway_result(
    database, payment_body, settings, monkeypatch
):
    payment_id, event = await seed(database, payment_body)
    gateway = AsyncMock(return_value=PaymentStatus.SUCCEEDED)
    monkeypatch.setattr("app.services.processing.emulate_gateway", gateway)
    replies = iter([503, 200])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(next(replies)))
    ) as client:
        processor = PaymentProcessor(database, client, settings)
        with monkeypatch.context() as patches:
            patches.setattr(processor, "_record_failure", AsyncMock(side_effect=SQLAlchemyError()))
            with pytest.raises(SQLAlchemyError):
                await processor.process(event)
        async with database.sessions() as session:
            assert (await session.get(Outbox, event.event_id)).consumed_at is None
            assert await session.scalar(select(func.count()).select_from(Outbox)) == 1
            assert (await session.get(Payment, payment_id)).status == PaymentStatus.SUCCEEDED
        assert await processor.process(event)
    gateway.assert_awaited_once()


async def test_legacy_webhook_retry_keeps_its_attempt_budget(database, payment_body, settings, monkeypatch):
    payment_id, event = await seed(database, payment_body)
    async with database.sessions() as session, session.begin():
        payment = await session.get(Payment, payment_id)
        payment.status = PaymentStatus.SUCCEEDED
        payment.processed_at = utcnow()
        record = await session.get(Outbox, event.event_id)
        record.payload = {"event_id": str(event.event_id), "payment_id": str(payment_id), "attempt": 3}
        event = PaymentEvent.model_validate(record.payload)
    gateway = AsyncMock()
    monkeypatch.setattr("app.services.processing.emulate_gateway", gateway)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(503))
    ) as client:
        assert await PaymentProcessor(database, client, settings).process(event)
    gateway.assert_not_awaited()
    async with database.sessions() as session:
        dlq = (await session.execute(select(Outbox).where(Outbox.routing_key == DLQ_QUEUE))).scalar_one()
        assert dlq.payload["attempts"] == 3
        assert (await session.get(Payment, payment_id)).webhook_attempts == 3
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 2
