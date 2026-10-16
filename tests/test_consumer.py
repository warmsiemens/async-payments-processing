from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.core.exceptions import InvalidEvent
from app.schemas.events import PaymentEvent
from app.workers.consumer import PaymentConsumer


@pytest.fixture
async def consumer(settings):
    return PaymentConsumer(settings, AsyncMock())


async def test_ack_only_after_outcome_is_saved(consumer):
    consumer.processor.process.return_value = True
    message = AsyncMock()
    event = PaymentEvent(event_id=uuid4(), payment_id=uuid4())
    await consumer.handle_payment(event.model_dump_json().encode(), message)
    consumer.processor.process.assert_awaited_once_with(event)
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()


@pytest.mark.parametrize("body", [b"{}", b'{"payment_id":"invalid"}', b'"wrong-body"', b"{broken", b"\xff"])
async def test_invalid_event_is_rejected_to_dead_letter_exchange(consumer, body):
    message = AsyncMock()
    await consumer.handle_payment(body, message)
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()


async def test_unknown_event_is_rejected(consumer):
    consumer.processor.process.side_effect = InvalidEvent()
    message = AsyncMock()
    event = PaymentEvent(event_id=uuid4(), payment_id=uuid4())
    await consumer.handle_payment(event.model_dump_json().encode(), message)
    message.reject.assert_awaited_once_with(requeue=False)


async def test_failed_database_commit_retains_original_message(consumer, monkeypatch):
    consumer.processor.process.side_effect = ConnectionError()
    monkeypatch.setattr("app.workers.consumer.asyncio.sleep", AsyncMock())
    message = AsyncMock()
    event = PaymentEvent(event_id=uuid4(), payment_id=uuid4())
    await consumer.handle_payment(event.model_dump_json().encode(), message)
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()


async def test_busy_payment_is_requeued_instead_of_acknowledged(consumer, monkeypatch):
    consumer.processor.process.return_value = False
    monkeypatch.setattr("app.workers.consumer.asyncio.sleep", AsyncMock())
    message = AsyncMock()
    event = PaymentEvent(event_id=uuid4(), payment_id=uuid4())
    await consumer.handle_payment(event.model_dump_json().encode(), message)
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()
