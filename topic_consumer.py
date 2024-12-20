import asyncio
import os
import sys
from dataclasses import dataclass
from enum import Enum

import dotenv
from aio_pika import connect
from aio_pika.abc import AbstractIncomingMessage, ExchangeType

dotenv.load_dotenv(".env")
host = os.getenv("RMQ_HOST", "localhost")
user = os.getenv("RMQ_USER", "")
password = os.getenv("RMQ_PASSWORD", "")
default_queue_name = "test_topic_queue"


class LogLevels(Enum):
    debug = 1
    info = 2
    warn = 3
    error = 4
    critical = 5
    all = "all"


@dataclass
class Topic:
    app: str
    loglevel: LogLevels


async def worker(message: AbstractIncomingMessage) -> None:
    """
    on_message doesn't necessarily have to be defined as async.
    Here it is to show that it's possible.
    """
    print("Message body is: %r" % message.body)
    await asyncio.sleep(2)  # Represents async I/O operations
    print("Success!")
    await message.ack()


async def consumer(topic: Topic) -> None:
    connection = await connect(f"amqp://{user}:{password}@{host}/")

    value = topic.loglevel.value
    if value == LogLevels.all.value:
        value = "#"

    topic = f"{topic.app}.{value}"
    print(f"Starting consumer for topic: {topic}")

    async with connection:
        # Creating a channel
        channel = await connection.channel()

        # распределяет задачи по свободным консьюмерам, а не отдаёт их по очереди
        await channel.set_qos(prefetch_count=1)

        exchange = await channel.declare_exchange(
            "logs_topic_exchange",

            # FANOUT обеспечивает отправку сообщения во все известные ему очереди
            ExchangeType.TOPIC
        )

        queue = await channel.declare_queue(auto_delete=True)

        # подписываем очередь на exchange
        await queue.bind(exchange, routing_key=topic)

        await queue.consume(worker)
        await asyncio.Future()


if __name__ == "__main__":
    app = sys.argv[1] if len(sys.argv) > 0 else default_queue_name
    log_level_str = sys.argv[2] if len(sys.argv) > 1 else LogLevels.warn

    log_level = LogLevels.warn
    if log_level_str == LogLevels.all.value:
        log_level = LogLevels.all
    elif int(log_level_str) == LogLevels.debug.value:
        log_level = LogLevels.debug
    elif int(log_level_str) == LogLevels.info.value:
        log_level = LogLevels.info
    elif int(log_level_str) == LogLevels.warn.value:
        log_level = LogLevels.warn
    elif int(log_level_str) == LogLevels.error.value:
        log_level = LogLevels.error
    elif int(log_level_str) == LogLevels.critical.value:
        log_level = LogLevels.critical

    asyncio.run(consumer(Topic(app, log_level)))
