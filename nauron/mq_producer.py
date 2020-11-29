import json
import uuid
import logging

from dataclasses import dataclass
from typing import Optional, Any, Dict, List

import pika

from nauron.utils import Response

LOGGER = logging.getLogger(__name__)


@dataclass
class MQItem:
    delivery_tag: Optional[int]
    reply_to: Optional[str]
    correlation_id: Optional[str]
    body: Dict[str, Any]


class MQProducer:
    def __init__(self, connection_parameters: pika.connection.Parameters, exchange_name: str):
        self.response = None
        self.correlation_id = None
        self.callback_queue = None

        self.mq_connection = pika.BlockingConnection(connection_parameters)
        self.exchange_name = exchange_name

        self.channel = self.mq_connection.channel()

    def init_callback(self):
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=True)

    def on_response(self, channel: pika.adapters.blocking_connection.BlockingChannel, method: pika.spec.Basic.Deliver,
                    properties: pika.spec.BasicProperties, body: bytes):
        if properties.correlation_id == self.correlation_id:
            self.response = Response(**json.loads(body))

    def publish_request(self, request: Dict[str, Any], queue_name: str, priority: int) -> Response:
        self.init_callback()
        self.correlation_id = str(uuid.uuid4())

        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=queue_name,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.correlation_id,
                priority=priority
            ),
            body=json.dumps(request)
        )

        while not self.response:
            self.mq_connection.process_data_events()
        return self.response


class MQMultiProducer(MQProducer):
    def on_response(self, channel: pika.adapters.blocking_connection.BlockingChannel, method: pika.spec.Basic.Deliver,
                    properties: pika.spec.BasicProperties, body: bytes):
        if properties.correlation_id in self.correlation_id:
            self.response[properties.correlation_id] = Response(**json.loads(body))

    def publish_request(self, requests: List[Dict[str, Any]], queue_name: str, priority: int) -> List[Response]:
        self.init_callback()
        self.correlation_id = []

        for request in requests:
            correlation_id = str(uuid.uuid4())
            self.correlation_id.append(correlation_id)

            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=queue_name,
                properties=pika.BasicProperties(
                    reply_to=self.callback_queue,
                    correlation_id=correlation_id,
                    priority=priority
                ),
                body=json.dumps(request)
            )

        while len(self.response) < len(self.correlation_id):
            self.mq_connection.process_data_events()

        results = []
        for correlation_id in self.correlation_id:
            results.append(self.response[correlation_id])

        return results
