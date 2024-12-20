import asyncio
import os
import sys

import dotenv
from aio_pika import connect
from aio_pika.abc import AbstractIncomingMessage, ExchangeType

dotenv.load_dotenv(".env")
host = os.getenv("RMQ_HOST", "localhost")
user = os.getenv("RMQ_USER", "")
password = os.getenv("RMQ_PASSWORD", "")
default_queue_name = "test_queue_1"

routing_key = "test_routing_key"


async def worker(message: AbstractIncomingMessage) -> None:
    """
    on_message doesn't necessarily have to be defined as async.
    Here it is to show that it's possible.
    """
    print("Message body is: %r" % message.body)
    await asyncio.sleep(2)  # Represents async I/O operations
    print("Success!")
    await message.ack()


async def consumer(queue_name: str):
    if not queue_name:
        queue_name = default_queue_name
    connection = await connect(f"amqp://{user}:{password}@{host}/")
    print(f"Starting consumer for queue: {queue_name}")

    async with connection:
        # Creating a channel
        channel = await connection.channel()

        # распределяет задачи по свободным консьюмерам, а не отдаёт их по очереди
        await channel.set_qos(prefetch_count=1)

        exchange = await channel.declare_exchange(
            "test_exchange",

            # FANOUT обеспечивает отправку сообщения во все известные ему очереди
            ExchangeType.FANOUT
        )

        queue = await channel.declare_queue(
            queue_name,
            durable=True,  # обеспечивает долговечность сообщений, чтобы они не терялись при падении сервера
        )

        # подписываем очередь на exchange
        await queue.bind(exchange)

        await queue.consume(worker)
        await asyncio.Future()


if __name__ == "__main__":
    name = None
    if len(sys.argv) > 1:
        name = sys.argv[1]
    asyncio.run(consumer(name))
