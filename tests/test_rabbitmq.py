import asyncio
import json
import os
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from faststream import AckPolicy
from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue
from sqlalchemy import select

from app.messaging.broker import make_broker
from app.messaging.constants import DLQ_QUEUE, PAYMENTS_QUEUE
from app.messaging.topology import payments_queue
from app.models import Outbox
from app.schemas.events import PaymentEvent
from app.schemas.payment import PaymentCreate
from app.services.outbox import publish_one
from app.services.payments import create_payment
from app.workers.consumer import PaymentConsumer, raw_body

pytestmark = pytest.mark.integration


@pytest.fixture
async def rabbit(settings):
    url = os.environ.get("TEST_RABBITMQ_URL")
    if not url:
        pytest.skip("Set TEST_RABBITMQ_URL to run real broker integration tests")
    settings.rabbitmq_url = url
    broker = make_broker(settings)
    prefix = f"test_{uuid4().hex}"
    exchange = RabbitExchange(prefix, type=ExchangeType.DIRECT, durable=True)
    dead_exchange = RabbitExchange(f"{prefix}.dead", type=ExchangeType.DIRECT, durable=True)
    dead_queue = RabbitQueue(f"{prefix}.dlq", durable=True, arguments={"x-queue-type": "quorum"})
    queue = RabbitQueue(
        prefix,
        durable=True,
        arguments={**payments_queue.arguments, "x-dead-letter-exchange": f"{prefix}.dead"},
    )
    await broker.connect()
    raw_exchange = await broker.declare_exchange(exchange)
    raw_dead_exchange = await broker.declare_exchange(dead_exchange)
    raw_dead_queue = await broker.declare_queue(dead_queue)
    await raw_dead_queue.bind(raw_dead_exchange, routing_key=DLQ_QUEUE)
    raw_queue = await broker.declare_queue(queue)
    await raw_queue.bind(raw_exchange, routing_key=PAYMENTS_QUEUE)
    try:
        yield broker, exchange, raw_queue, raw_dead_queue
    finally:
        await raw_queue.delete(if_unused=False, if_empty=False)
        await raw_dead_queue.delete(if_unused=False, if_empty=False)
        await raw_exchange.delete(if_unused=False)
        await raw_dead_exchange.delete(if_unused=False)
        await broker.stop()


async def test_outbox_publishes_persistent_confirmed_event(
    database, payment_body, settings, rabbit, monkeypatch
):
    broker, exchange, queue, _ = rabbit
    monkeypatch.setattr("app.services.outbox.payments_exchange", exchange)
    async with database.sessions() as session, session.begin():
        payment = await create_payment(session, PaymentCreate(**payment_body), "real-rabbit")
    assert await publish_one(database, broker, settings)
    message = await queue.get(timeout=5)
    assert message is not None
    assert json.loads(message.body)["payment_id"] == str(payment.id)
    assert message.delivery_mode.value == 2
    await message.ack()
    async with database.sessions() as session:
        record = (await session.execute(select(Outbox))).scalar_one()
        assert record.published_at is not None
        assert message.message_id == str(record.id)


async def test_unroutable_message_is_not_marked_published(
    database, payment_body, settings, rabbit, monkeypatch
):
    broker, exchange, queue, _ = rabbit
    monkeypatch.setattr("app.services.outbox.payments_exchange", exchange)
    async with database.sessions() as session, session.begin():
        await create_payment(session, PaymentCreate(**payment_body), "unroutable")
        await session.flush()
        record = (await session.execute(select(Outbox))).scalar_one()
        record.routing_key = "no-such-binding"
    assert await publish_one(database, broker, settings)
    async with database.sessions() as session:
        record = (await session.execute(select(Outbox))).scalar_one()
        assert record.published_at is None
        assert record.publish_attempts == 1


async def test_transport_redeliveries_do_not_exhaust_application_retry_budget(rabbit):
    broker, exchange, queue, _ = rabbit
    await broker.publish({"test": "redelivery"}, exchange=exchange, routing_key=PAYMENTS_QUEUE, persist=True)
    for _ in range(25):
        message = await queue.get(timeout=5)
        assert message is not None
        await message.nack(requeue=True)
    message = await queue.get(timeout=5)
    assert message is not None
    assert json.loads(message.body) == {"test": "redelivery"}
    await message.ack()


@pytest.mark.parametrize("body", [b"{broken json", b"\xff", b"{}"])
async def test_invalid_wire_message_reaches_dlq_and_next_payment_is_processed(rabbit, settings, body):
    broker, exchange, queue, dlq = rabbit
    processor = AsyncMock()
    processor.process.return_value = True
    processed = asyncio.Event()

    async def process(event):
        processed.set()
        return True

    processor.process.side_effect = process
    dead_letters = []
    dead_lettered = asyncio.Event()

    async def receive_dead_letter(message):
        dead_letters.append(message.body)
        await message.ack()
        dead_lettered.set()

    await dlq.consume(receive_dead_letter)
    consumer = PaymentConsumer(settings, processor)
    descriptor = RabbitQueue(queue.name, durable=True, arguments=queue.arguments)
    broker.subscriber(descriptor, exchange, ack_policy=AckPolicy.MANUAL, decoder=raw_body)(
        consumer.handle_payment
    )
    await broker.start()
    await broker.publish(
        body, content_type="application/json", exchange=exchange, routing_key=PAYMENTS_QUEUE, persist=True
    )
    event = PaymentEvent(event_id=uuid4(), payment_id=uuid4())
    await broker.publish(event.model_dump(mode="json"), exchange=exchange, routing_key=PAYMENTS_QUEUE)
    await asyncio.wait_for(processed.wait(), timeout=5)
    processor.process.assert_awaited_once_with(event)

    await asyncio.wait_for(dead_lettered.wait(), timeout=10)
    assert dead_letters == [body]
