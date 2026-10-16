import asyncio
import logging
import random
from datetime import timedelta
from typing import Literal
from uuid import UUID, uuid4

import httpx
from sqlalchemy import update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.constants import MAX_ATTEMPTS
from app.core.enums import PaymentStatus
from app.core.exceptions import InvalidEvent, LeaseLost
from app.core.time import utcnow
from app.db.session import Database
from app.messaging.constants import DLQ_QUEUE, PAYMENTS_QUEUE
from app.models.outbox import Outbox
from app.models.payment import Payment
from app.schemas.events import PaymentEvent
from app.services.events import new_event

logger = logging.getLogger(__name__)


async def emulate_gateway() -> PaymentStatus:
    await asyncio.sleep(random.uniform(2, 5))
    return PaymentStatus.SUCCEEDED if random.random() < 0.9 else PaymentStatus.FAILED


class PaymentProcessor:
    def __init__(self, database: Database, client: httpx.AsyncClient, settings: Settings) -> None:
        self.database = database
        self.client = client
        self.settings = settings

    async def _lock(self, session: AsyncSession, event: PaymentEvent) -> tuple[Outbox, Payment]:
        record = await session.get(Outbox, event.event_id, with_for_update=True)
        if (
            record is None
            or record.payment_id != event.payment_id
            or record.routing_key != PAYMENTS_QUEUE
            # Normalize legacy events without a stage field as well.
            or PaymentEvent.model_validate(record.payload) != event
        ):
            raise InvalidEvent("Unknown or inconsistent outbox event")
        payment = await session.get(Payment, event.payment_id, with_for_update=True)
        if payment is None:
            raise InvalidEvent("Payment does not exist")
        return record, payment

    async def _owned(self, session: AsyncSession, event: PaymentEvent, token: UUID) -> tuple[Outbox, Payment]:
        record, payment = await self._lock(session, event)
        if (
            payment.worker_token != token
            or payment.worker_lease_until is None
            or payment.worker_lease_until <= utcnow()
        ):
            raise LeaseLost("Payment processing lease expired or was replaced")
        return record, payment

    async def _claim(self, event: PaymentEvent, token: UUID) -> bool:
        async with self.database.sessions() as session, session.begin():
            record, payment = await self._lock(session, event)
            if record.consumed_at is not None:
                return False
            if payment.worker_lease_until is not None and payment.worker_lease_until > utcnow():
                raise LeaseLost("Payment is being processed by another worker")
            payment.worker_token = token
            payment.worker_lease_until = utcnow() + timedelta(seconds=self.settings.worker_lease_seconds)
        return True

    async def _release(self, payment_id: UUID, token: UUID) -> None:
        async with self.database.sessions() as session, session.begin():
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id, Payment.worker_token == token)
                .values(worker_token=None, worker_lease_until=None)
            )

    async def process(self, event: PaymentEvent) -> bool:
        token = uuid4()
        try:
            claimed = await self._claim(event, token)
        except LeaseLost:
            return False
        if not claimed:
            return True

        stage: Literal["processing", "webhook"] = event.stage
        try:
            if stage == "processing":
                await self._process_gateway(event, token)
            stage = "webhook"
            await self._deliver_webhook(event, token)
        except LeaseLost:
            return False
        except (InvalidEvent, SQLAlchemyError):
            # A DB outage must retain the original delivery, not spend the application retry budget.
            raise
        except Exception as exc:
            retryable = isinstance(exc, (httpx.HTTPError, TimeoutError, ConnectionError))
            logger.log(
                logging.WARNING if retryable else logging.ERROR,
                "Payment operation failed: payment=%s stage=%s attempt=%s retryable=%s",
                event.payment_id,
                stage,
                event.attempt,
                retryable,
                exc_info=True,
            )
            await self._record_failure(event, token, stage, exc, retryable=retryable)
        finally:
            try:
                await self._release(event.payment_id, token)
            except SQLAlchemyError:
                logger.exception("Could not release payment lease: payment=%s", event.payment_id)
        return True

    async def _process_gateway(self, event: PaymentEvent, token: UUID) -> None:
        async with self.database.sessions() as session, session.begin():
            _, payment = await self._owned(session, event, token)
            if payment.status != PaymentStatus.PENDING:
                return
            payment.processing_attempts = max(payment.processing_attempts, event.attempt)
            payment.worker_lease_until = utcnow() + timedelta(seconds=self.settings.worker_lease_seconds)

        # Bound the operation to less than its lease; no DB session or row lock is held here.
        async with asyncio.timeout(self.settings.worker_lease_seconds / 2):
            result = PaymentStatus(await emulate_gateway())
        async with self.database.sessions() as session, session.begin():
            _, payment = await self._owned(session, event, token)
            payment.status = result
            payment.processed_at = utcnow()

    async def _deliver_webhook(self, event: PaymentEvent, token: UUID) -> None:
        async with self.database.sessions() as session, session.begin():
            record, payment = await self._owned(session, event, token)
            if payment.webhook_delivered_at is not None:
                record.consumed_at = utcnow()
                return
            if payment.processed_at is None:
                raise InvalidEvent("Webhook event refers to an unfinished payment")
            payment.webhook_attempts = max(payment.webhook_attempts, self._webhook_attempt(event, record))
            payment.worker_lease_until = utcnow() + timedelta(seconds=self.settings.worker_lease_seconds)
            url = payment.webhook_url
            payload = {
                "event": "payment.processed",
                "payment_id": str(payment.id),
                "status": payment.status,
                "amount": str(payment.amount),
                "currency": payment.currency,
                "description": payment.description,
                "metadata": payment.metadata_json,
                "created_at": payment.created_at.isoformat(),
                "processed_at": payment.processed_at.isoformat(),
            }

        async with asyncio.timeout(
            min(self.settings.webhook_timeout, self.settings.worker_lease_seconds / 2)
        ):
            response = await self.client.post(
                url,
                json=payload,
                headers={"Idempotency-Key": f"payment:{event.payment_id}:processed"},
                timeout=self.settings.webhook_timeout,
            )
            response.raise_for_status()
        async with self.database.sessions() as session, session.begin():
            record, payment = await self._owned(session, event, token)
            payment.webhook_delivered_at = utcnow()
            payment.last_error = None
            record.consumed_at = utcnow()

    @staticmethod
    def _webhook_attempt(event: PaymentEvent, record: Outbox) -> int:
        # Before stage-specific retries, the attempt counter was shared with webhook delivery.
        return event.attempt if event.stage == "webhook" or "stage" not in record.payload else 1

    async def _record_failure(
        self,
        event: PaymentEvent,
        token: UUID,
        stage: Literal["processing", "webhook"],
        error: Exception,
        *,
        retryable: bool,
    ) -> None:
        async with self.database.sessions() as session, session.begin():
            record, payment = await self._owned(session, event, token)
            attempt = event.attempt if stage == "processing" else self._webhook_attempt(event, record)
            error_text = type(error).__name__
            if isinstance(error, httpx.HTTPStatusError):
                error_text += f": HTTP {error.response.status_code}"
            payment.last_error = error_text
            record.consumed_at = utcnow()
            if retryable and attempt < MAX_ATTEMPTS:
                delay = self.settings.retry_base_delay * 2 ** (attempt - 1)
                session.add(
                    new_event(payment.id, attempt + 1, utcnow() + timedelta(seconds=delay), stage=stage)
                )
            else:
                dlq_id = uuid4()
                session.add(
                    Outbox(
                        id=dlq_id,
                        payment_id=payment.id,
                        routing_key=DLQ_QUEUE,
                        payload={
                            "event_id": str(dlq_id),
                            "original_event": event.model_dump(mode="json"),
                            "payment_id": str(payment.id),
                            "stage": stage,
                            "attempts": attempt,
                            "error": error_text,
                            "failed_at": utcnow().isoformat(),
                        },
                    )
                )
                if payment.status == PaymentStatus.PENDING:
                    payment.status = PaymentStatus.FAILED
                    payment.processed_at = utcnow()
                    # Persist notification intent together with the terminal result and DLQ.
                    session.add(new_event(payment.id, stage="webhook"))
