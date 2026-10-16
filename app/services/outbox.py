import asyncio
import logging
from datetime import timedelta
from uuid import uuid4

from faststream.rabbit import RabbitBroker
from sqlalchemy import func, or_, select

from app.core.config import Settings
from app.core.time import utcnow
from app.db.session import Database
from app.messaging.constants import DLQ_QUEUE
from app.messaging.topology import dead_exchange, payments_exchange
from app.models.outbox import Outbox

logger = logging.getLogger(__name__)


async def publish_one(database: Database, broker: RabbitBroker, settings: Settings) -> bool:
    async with database.sessions() as session, session.begin():
        event = (
            await session.execute(
                select(Outbox)
                .where(
                    Outbox.published_at.is_(None),
                    Outbox.available_at <= func.now(),
                    or_(Outbox.publish_lease_until.is_(None), Outbox.publish_lease_until <= func.now()),
                )
                .order_by(Outbox.available_at, Outbox.created_at)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
        ).scalar_one_or_none()
        if event is None:
            return False
        token = uuid4()
        event.publish_token = token
        event.publish_lease_until = utcnow() + timedelta(seconds=settings.worker_lease_seconds)
        event.publish_attempts += 1

    error_text = None
    try:
        async with asyncio.timeout(min(settings.publish_timeout, settings.worker_lease_seconds / 2)):
            await broker.publish(
                event.payload,
                exchange=dead_exchange if event.routing_key == DLQ_QUEUE else payments_exchange,
                routing_key=event.routing_key,
                message_id=str(event.id),
                persist=True,
                mandatory=True,
                timeout=settings.publish_timeout,
            )
    except Exception as exc:
        error_text = type(exc).__name__
        logger.exception("Publication failed: event=%s", event.id)

    async with database.sessions() as session, session.begin():
        record = await session.get(Outbox, event.id, with_for_update=True)
        if record is None or record.publish_token != token:
            # An expired claim may have been picked up by another publisher.
            return True
        record.last_error = error_text
        if error_text is None:
            # A crash after confirmation and before this commit can still cause a duplicate.
            record.published_at = utcnow()
        else:
            delay = min(60, settings.retry_base_delay * 2 ** min(record.publish_attempts - 1, 5))
            record.available_at = utcnow() + timedelta(seconds=delay)
        record.publish_token = None
        record.publish_lease_until = None
    return True
