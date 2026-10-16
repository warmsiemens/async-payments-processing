from faststream.rabbit import Channel, RabbitBroker

from app.core.config import Settings


def make_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
        graceful_timeout=30,
    )
