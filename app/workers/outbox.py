import asyncio
import logging

from app.core.config import get_settings
from app.db.session import Database
from app.messaging.broker import make_broker
from app.messaging.topology import declare_topology
from app.services.outbox import publish_one

logger = logging.getLogger(__name__)


async def run_publisher() -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    database = Database(settings.database_url)
    broker = make_broker(settings)
    try:
        await broker.connect()
        await declare_topology(broker)
        while True:
            try:
                published = await publish_one(database, broker, settings)
            except Exception:
                logger.exception("Outbox transaction failed")
                published = False
            if not published:
                await asyncio.sleep(settings.outbox_poll_interval)
    finally:
        await broker.stop()
        await database.close()


if __name__ == "__main__":
    asyncio.run(run_publisher())
