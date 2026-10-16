from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.messaging.constants import DLQ_QUEUE, PAYMENTS_QUEUE

payments_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
dead_exchange = RabbitExchange("payments.dead", type=ExchangeType.DIRECT, durable=True)
payments_queue = RabbitQueue(
    PAYMENTS_QUEUE,
    durable=True,
    routing_key=PAYMENTS_QUEUE,
    arguments={
        "x-queue-type": "quorum",
        # Retry attempts are persisted in outbox; transport redeliveries do not consume that budget.
        "x-delivery-limit": -1,
        "x-dead-letter-exchange": dead_exchange.name,
        "x-dead-letter-routing-key": DLQ_QUEUE,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    },
)
dead_queue = RabbitQueue(DLQ_QUEUE, durable=True, routing_key=DLQ_QUEUE, arguments={"x-queue-type": "quorum"})


async def declare_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(payments_exchange)
    dlx = await broker.declare_exchange(dead_exchange)
    queue = await broker.declare_queue(payments_queue)
    dlq = await broker.declare_queue(dead_queue)
    await queue.bind(exchange, routing_key=PAYMENTS_QUEUE)
    await dlq.bind(dlx, routing_key=DLQ_QUEUE)
