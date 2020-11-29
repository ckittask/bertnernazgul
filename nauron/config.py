import logging
from abc import ABC

from dataclasses import dataclass, field
from typing import Dict, Union

from flask_restful.reqparse import RequestParser
import pika

from nauron import Nazgul

LOGGER = logging.getLogger(__name__)


@dataclass
class SauronConf(ABC):
    nazguls: Dict[str, Union[str, Nazgul]]
    application_required: bool
    parser: RequestParser = field(init=False)

    def __post_init__(self):
        self.parser = RequestParser()
        self.parser.add_argument('token', type=str, location='headers', default='public',
                                 help="A token which defines which service configuration is used.")
        self.parser.add_argument('application', type=str, location='headers', required=self.application_required,
                                 help='Name of the service or application where the request is made from. '
                                      'Depending on the service, there may be differences in how requests from '
                                      'different applications are processed.')


@dataclass
class MQSauronConf(SauronConf):
    nazguls: Dict[str, str]
    connection_parameters: pika.connection.ConnectionParameters
    exchange_name: str
    max_priority: int = 10
    max_length: int = 20000

    def __post_init__(self):
        super().__post_init__()
        connection = pika.BlockingConnection(self.connection_parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
        for queue in self.nazguls.values():
            channel.queue_declare(queue=queue, arguments={'x-max-priority': self.max_priority})
        channel.close()
        connection.close()


@dataclass
class LocalSauronConf(SauronConf):
    nazguls: Dict[str, Nazgul]
