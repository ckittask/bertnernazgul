import logging
import math
from abc import abstractmethod
from typing import Dict, Any, List, Union

from flask_restful import Resource, abort

from nauron import Response, LocalSauronConf, MQSauronConf
from nauron.mq_producer import MQProducer, MQMultiProducer

LOGGER = logging.getLogger(__name__)


class Sauron(Resource):
    def __init__(self, conf: Union[LocalSauronConf, MQSauronConf]):
        # Initiate RabbitMQ connection:
        self.conf = conf
        if isinstance(self.conf, LocalSauronConf):
            self.process = self.local_process
        else:
            self.process = self.mq_process

        self.add_arguments()

        self.request = None
        self.nazgul = None
        self.response = None

    def add_arguments(self):
        """
        Add service-specific arguments to the parser and return it.
        """
        self.conf.parser.add_argument('text', type=str, required=False, help='No text provided', location='json')

    def resolve_nazgul(self):
        """
        Resolves Nazgul instance or RabbitMQ queue name based on the "token" field in request header.
        """
        try:
            self.nazgul = self.conf.nazguls[self.request['token']]
            self.request.pop('token')
        except KeyError:
            abort(401, message="Invalid authentication token.")

    def pre_process(self):
        """
        Define any pre-processing steps to validate and modify the request if needed before forwarding it to Nazgul.
        By default, the request is sent as is.
        """
        pass

    def calculate_priority(self) -> int:
        """
        Calculate the priority of the request which will determine how requests are prioritized by Nazgul when using
        RabbitMQ.
        """
        length = len(self.request['text'])
        priority = int(math.ceil((self.conf.max_length - length + 1) / (self.conf.max_length / self.conf.max_priority)))
        if priority <= 0:
            abort(413, message="The text is too long ({} characters).".format(length))
        return priority

    def mq_process(self):
        priority = self.calculate_priority()
        producer = MQProducer(self.conf.connection_parameters, self.conf.exchange_name)
        self.response = producer.publish_request(self.request, queue_name=self.nazgul, priority=priority)

    def local_process(self):
        self.response = self.nazgul.process_request(self.request)

    def post_process(self):
        """
        Define any post-processing steps to modify the response from Nazgul before returning it to the end user.
        """
        pass

    def post(self):
        self.request = self.conf.parser.parse_args().copy()
        self.resolve_nazgul()

        self.pre_process()
        self.process()
        self.post_process()

        return self.response.rest_response()


class MultirequestSauron(Sauron):
    @abstractmethod
    def pre_process(self) -> List[Dict[str, Any]]:
        """
        Define any pre-processing steps to validate the request if needed and split it into subrequests,
        before forwarding these to Nazgul.
        """
        pass

    def calculate_priority(self) -> int:
        """
        Calculate the priority of the requests which will determine how requests are prioritized by Nazgul when using
        RabbitMQ. By default, priority is depends on the number of subrequests.
        """
        length = len(self.request)
        priority = int(math.ceil((self.conf.max_length - length + 1) / (self.conf.max_length / self.conf.max_priority)))
        if priority <= 0:
            abort(413, message="The request is too long ({} elements).".format(length))
        return priority

    def mq_process(self):
        priority = self.calculate_priority()
        producer = MQMultiProducer(self.conf.connection_parameters, self.conf.exchange_name)
        self.response = producer.publish_request(self.request, queue_name=self.nazgul, priority=priority)

    def local_process(self):
        self.response = self.nazgul.process_requests(self.request)

    @abstractmethod
    def post_process(self) -> Response:
        """
        Define any post-processing steps to modify the responses from Nazgul and merge them into a single request
        which will be returned to the end user.
        """
        pass

    def post(self):
        self.request = self.conf.parser.parse_args().copy()
        self.resolve_nazgul()

        self.request = self.pre_process()
        self.process()

        self.response = self.post_process()
        self.response.rest_response()
