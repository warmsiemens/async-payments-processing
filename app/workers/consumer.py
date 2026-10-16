import asyncio
import logging
from contextlib import AsyncExitStack

import httpx
from faststream import AckPolicy, FastStream
from faststream.message import StreamMessage
from faststream.rabbit import RabbitMessage
from pydantic import ValidationError

from app.core.config import Settings, get_settings
from app.core.exceptions import InvalidEvent, LeaseLost
from app.db.session import Database
from app.messaging.broker import make_broker
from app.messaging.topology import declare_topology, payments_exchange, payments_queue
from app.schemas.events import PaymentEvent
from app.services.processing import PaymentProcessor

logger = logging.getLogger(__name__)


async def raw_body(message: StreamMessage) -> bytes:
    # Decode inside the handler, where malformed JSON can be explicitly rejected.
    return message.body


class PaymentConsumer:
    def __init__(self, settings: Settings, processor: PaymentProcessor | None = None) -> None:
        self.settings = settings
        self.processor = processor

    async def handle_payment(self, body: bytes, message: RabbitMessage) -> None:
        try:
            event = PaymentEvent.model_validate_json(body)
            if self.processor is None:
                raise RuntimeError("Consumer has not been started")
            completed = await self.processor.process(event)
        except (ValidationError, InvalidEvent):
            logger.warning("Rejecting invalid payment event: message=%s", message.message_id)
            await message.reject(requeue=False)
        except LeaseLost:
            await self._requeue(message)
        except Exception:
            # Retry/DLQ could not commit: retain the original delivery, including its budget.
            logger.exception("Could not persist processing outcome: message=%s", message.message_id)
            await self._requeue(message)
        else:
            if completed:
                await message.ack()
            else:
                await self._requeue(message)

    async def _requeue(self, message: RabbitMessage) -> None:
        await asyncio.sleep(self.settings.retry_base_delay)
        await message.nack(requeue=True)


def create_consumer(settings: Settings | None = None) -> FastStream:
    settings = settings or get_settings()
    broker = make_broker(settings)
    application = FastStream(broker)
    handler = PaymentConsumer(settings)
    resources = AsyncExitStack()
    broker.subscriber(payments_queue, payments_exchange, ack_policy=AckPolicy.MANUAL, decoder=raw_body)(
        handler.handle_payment
    )

    @application.on_startup
    async def startup() -> None:
        try:
            database = Database(settings.database_url)
            resources.push_async_callback(database.close)
            client = await resources.enter_async_context(
                httpx.AsyncClient(follow_redirects=False, trust_env=False)
            )
            handler.processor = PaymentProcessor(database, client, settings)
            await broker.connect()
            await declare_topology(broker)
        except BaseException:
            handler.processor = None
            try:
                await broker.stop()
            finally:
                await resources.aclose()
            raise

    @application.after_shutdown
    async def shutdown() -> None:
        handler.processor = None
        await resources.aclose()

    return application


if __name__ == "__main__":
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    asyncio.run(create_consumer(settings).run())
