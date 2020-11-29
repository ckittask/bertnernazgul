import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List

from nauron.utils import Response

LOGGER = logging.getLogger(__name__)


class Nazgul(ABC):
    """
    An abstract service logic class that responds to Sauron requests directly or via RabbitMQ.
    """
    @abstractmethod
    def process_request(self, request: Dict[str, Any]) -> Response:
        """
        Single request processing method. Unless a service-specific Sauron is used, the default request will have the
        following format:
            {
                'text': ...
                'application': ...
            }
        """
        pass

    def process_requests(self, requests: List[Dict[str, Any]]) -> List[Response]:
        return [self.process_request(request) for request in requests]


class BatchedNazgul(Nazgul):
    """
    An abstract service logic class that responds to Sauron requests directly or via RabbitMQ that also supports
    batching multiple requests.
    """
    def __init__(self, batch_size: int = 1):
        self.batch_size = batch_size
        if self.batch_size < 1:
            raise ValueError("Batch size cannot be negative.")

    @abstractmethod
    def process_batch(self, batch: List[Dict[str, Any]]) -> List[Response]:
        """
        Request batch processing. This is meant for processing multiple Sauron requests. If you split a single request
        into multiple subrequests inside Nazgul, then that logic should be implemented in the process_requests()
        method instead.
        """
        pass

    def process_requests(self, requests: List[Dict[str, Any]]) -> List[Response]:
        responses = []
        for i in range(0, len(requests), self.batch_size):
            responses += self.process_batch(requests[i:i+self.batch_size])
        return responses
