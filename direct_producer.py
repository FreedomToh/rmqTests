import asyncio
import os
import sys
from enum import Enum

import dotenv
from aio_pika import connect, Message, DeliveryMode, ExchangeType

dotenv.load_dotenv(".env")
host = os.getenv("RMQ_HOST", "localhost")
user = os.getenv("RMQ_USER", "")
password = os.getenv("RMQ_PASSWORD", "")

routing_key = "test_routing_key"


class LogLevels(Enum):
    debug = 1
    info = 2
    warn = 3
    error = 4
    critical = 5


async def send_message(mess: str, log_level: LogLevels = LogLevels.info):
    connection = await connect(f"amqp://{user}:{password}@{host}/")

    async with connection:
        # Creating a channel
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            "logs_exchange",

            # DIRECT обеспечивает отправку сообщения в очереди с определенным ключем маршрутизации
            ExchangeType.DIRECT
        )

        await exchange.publish(
            Message(
                mess.encode("utf-8"),

                # обеспечивает долговечность сообщений, чтобы они не терялись при падении сервера
                delivery_mode=DeliveryMode.PERSISTENT
            ),
            routing_key=str(log_level.value),
        )

        # exchange = await channel.declare_exchange("tests_exchange")
        # await exchange.bind(exchange, routing_key=queue_one.name)
        # await exchange.bind(exchange, routing_key=queue_two.name)
        # await exchange.publish(Message(mess.encode("utf-8")), routing_key=queue_one.name)

if __name__ == "__main__":
    message = sys.argv[1] if len(sys.argv) > 0 else "Debug"
    log_level_str = sys.argv[2] if len(sys.argv) > 1 else LogLevels.info.value

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

    asyncio.run(send_message(message, log_level))
