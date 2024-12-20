import asyncio
import os
import sys

import dotenv
from aio_pika import connect, Message, DeliveryMode, ExchangeType

dotenv.load_dotenv(".env")
host = os.getenv("RMQ_HOST", "localhost")
user = os.getenv("RMQ_USER", "")
password = os.getenv("RMQ_PASSWORD", "")

routing_key = "test_routing_key"


async def send_message(mess: str):
    connection = await connect(f"amqp://{user}:{password}@{host}/")

    async with connection:
        # Creating a channel
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            "test_exchange",

            # FANOUT обеспечивает отправку сообщения во все известные ему очереди
            ExchangeType.FANOUT
        )

        await exchange.publish(
            Message(
                mess.encode("utf-8"),

                # обеспечивает долговечность сообщений, чтобы они не терялись при падении сервера
                delivery_mode=DeliveryMode.PERSISTENT
            ),
            routing_key=routing_key,
        )

        # exchange = await channel.declare_exchange("tests_exchange")
        # await exchange.bind(exchange, routing_key=queue_one.name)
        # await exchange.bind(exchange, routing_key=queue_two.name)
        # await exchange.publish(Message(mess.encode("utf-8")), routing_key=queue_one.name)

if __name__ == "__main__":
    for message in sys.argv[1:]:
        asyncio.run(send_message(message))

    if len(sys.argv) < 2:
        asyncio.run(send_message("Test message"))