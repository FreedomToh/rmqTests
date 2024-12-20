import asyncio
import os
import sys
from enum import Enum

import dotenv
from aio_pika import connect
from aio_pika.abc import AbstractIncomingMessage, ExchangeType

dotenv.load_dotenv(".env")
host = os.getenv("RMQ_HOST", "localhost")
user = os.getenv("RMQ_USER", "")
password = os.getenv("RMQ_PASSWORD", "")
default_queue_name = "test_queue_1"

routing_key = "test_routing_key"


class LogLevels(Enum):
    debug = 1
    info = 2
    warn = 3
    error = 4
    critical = 5


async def worker(message: AbstractIncomingMessage) -> None:
    """
    on_message doesn't necessarily have to be defined as async.
    Here it is to show that it's possible.
    """
    print("Message body is: %r" % message.body)
    await asyncio.sleep(2)  # Represents async I/O operations
    print("Success!")
    await message.ack()


async def consumer(queue_name: str, log_level: LogLevels) -> None:
    connection = await connect(f"amqp://{user}:{password}@{host}/")
    print(f"Starting consumer for queue: {queue_name} with log level: {log_level}")

    async with connection:
        # Creating a channel
        channel = await connection.channel()

        # распределяет задачи по свободным консьюмерам, а не отдаёт их по очереди
        await channel.set_qos(prefetch_count=1)

        exchange = await channel.declare_exchange(
            "logs_exchange",

            # FANOUT обеспечивает отправку сообщения во все известные ему очереди
            ExchangeType.DIRECT
        )

        queue = await channel.declare_queue(
            queue_name,
            durable=True,  # обеспечивает долговечность сообщений, чтобы они не терялись при падении сервера
        )

        # подписываем очередь на exchange
        await queue.bind(exchange, routing_key=str(log_level.value))

        await queue.consume(worker)
        await asyncio.Future()


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 0 else default_queue_name
    log_level_str = sys.argv[2] if len(sys.argv) > 1 else LogLevels.warn

    log_level = LogLevels.warn
    if int(log_level_str) == LogLevels.debug.value:
        log_level = LogLevels.debug
    elif int(log_level_str) == LogLevels.info.value:
        log_level = LogLevels.info
    elif int(log_level_str) == LogLevels.warn.value:
        log_level = LogLevels.warn
    elif int(log_level_str) == LogLevels.error.value:
        log_level = LogLevels.error
    elif int(log_level_str) == LogLevels.critical.value:
        log_level = LogLevels.critical
    asyncio.run(consumer(name, log_level))
