import json
import logging
from time import time

from dataclasses import dataclass
from typing import Optional, Any, Union, Dict

import pika

from nauron import Nazgul
from nauron.utils import Response
from nauron.nazgul import BatchedNazgul

LOGGER = logging.getLogger(__name__)


@dataclass
class MQItem:
    delivery_tag: Optional[int]
    reply_to: Optional[str]
    correlation_id: Optional[str]
    body: Dict[str, Any]


class MQConsumer:
    def __init__(self, nazgul: Union[Nazgul, BatchedNazgul],
                 connection_parameters: pika.connection.ConnectionParameters, exchange_name: str,
                 queue_name: str, mq_max_priority: int = 10):
        self.nazgul = nazgul
        self.queue_name = queue_name

        # Initialize RabbitMQ connecton, channel and queue
        connection = pika.BlockingConnection(connection_parameters)

        self.channel = connection.channel()
        self.channel.queue_declare(queue=self.queue_name, arguments={'x-max-priority': mq_max_priority})
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        self.channel.queue_bind(exchange=exchange_name, queue=self.queue_name, routing_key=self.queue_name)

        # Start listening on channel with prefetch_count=1
        self.channel.basic_qos(prefetch_count=1)

        if isinstance(self.nazgul, BatchedNazgul) and self.nazgul.batch_size > 1:
            LOGGER.warning("Batch processing with RabbitMQ is an experimental feature.")
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.on_batch_request)
        else:
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.on_request)

    def start(self) -> None:
        self.channel.start_consuming()

    @staticmethod
    def respond(channel: pika.adapters.blocking_connection.BlockingChannel, mq_item: MQItem, response: Response):
        channel.basic_publish(exchange='',
                              routing_key=mq_item.reply_to,
                              properties=pika.BasicProperties(correlation_id=mq_item.correlation_id),
                              body=response.encode())
        channel.basic_ack(delivery_tag=mq_item.delivery_tag)

    def on_request(self, channel: pika.adapters.blocking_connection.BlockingChannel, method: pika.spec.Basic.Deliver,
                   properties: pika.BasicProperties, body: bytes) -> None:
        t1 = time()
        mq_item = MQItem(method.delivery_tag,
                         properties.reply_to,
                         properties.correlation_id,
                         json.loads(body))

        response = self.nazgul.process_request(mq_item.body)
        self.respond(channel, mq_item, response)
        t4 = time()
        LOGGER.debug(f"On_request took: {round(t4 - t1, 3)} s. ")

    def on_batch_request(self, channel: pika.adapters.blocking_connection.BlockingChannel,
                         method: pika.spec.Basic.Deliver, properties: pika.BasicProperties, body: bytes) -> None:
        t1 = time()
        batch = [MQItem(method.delivery_tag,
                        properties.reply_to,
                        properties.correlation_id,
                        json.loads(body))]

        while len(batch) < self.nazgul.batch_size:
            method, properties, body = channel.basic_get(queue=self.queue_name)
            if method:
                batch.append(MQItem(method.delivery_tag, properties.reply_to, properties.correlation_id,
                                    json.loads(body)))
            else:
                break

        requests = [mq_item.body for mq_item in batch]
        responses = self.nazgul.process_batch(requests)

        for mq_item, response in zip(batch, responses):
            self.respond(channel, mq_item, response)

        t4 = time()
        LOGGER.debug(f"On_batch_request took: {round(t4 - t1, 3)} s. Batch size: {len(batch)}.")
