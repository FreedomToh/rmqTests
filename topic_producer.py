import asyncio
import os
import sys
from dataclasses import dataclass
from enum import Enum

import dotenv
from aio_pika import connect, Message, DeliveryMode, ExchangeType

dotenv.load_dotenv(".env")
host = os.getenv("RMQ_HOST", "localhost")
user = os.getenv("RMQ_USER", "")
password = os.getenv("RMQ_PASSWORD", "")


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


async def send_message(mess: str, topic: Topic):
    connection = await connect(f"amqp://{user}:{password}@{host}/")

    value = topic.loglevel.value
    if value == LogLevels.all.value:
        value = "#"

    topic = f"{topic.app}.{value}"
    print("Sending message with routing key:", topic)

    async with connection:
        # Creating a channel
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            "logs_topic_exchange",

            # TOPIC обеспечивает отправку сообщения в очереди с определенным топиком - темой
            ExchangeType.TOPIC
        )

        await exchange.publish(
            Message(
                mess.encode("utf-8"),

                # обеспечивает долговечность сообщений, чтобы они не терялись при падении сервера
                delivery_mode=DeliveryMode.PERSISTENT
            ),
            routing_key=topic,
        )

if __name__ == "__main__":
    message = sys.argv[1] if len(sys.argv) > 0 else "Debug"
    app = sys.argv[2] if len(sys.argv) > 1 else "root"
    log_level_str = sys.argv[3] if len(sys.argv) > 2 else LogLevels.info.value

    log_level = LogLevels.warn
    if log_level_str == LogLevels.all.value:
        log_level = LogLevels.all
    elif log_level_str == LogLevels.debug.value:
        log_level = LogLevels.debug
    elif int(log_level_str) == LogLevels.info.value:
        log_level = LogLevels.info
    elif int(log_level_str) == LogLevels.warn.value:
        log_level = LogLevels.warn
    elif int(log_level_str) == LogLevels.error.value:
        log_level = LogLevels.error
    elif int(log_level_str) == LogLevels.critical.value:
        log_level = LogLevels.critical

    asyncio.run(send_message(message, Topic(app, log_level)))
