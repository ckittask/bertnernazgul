import logging
from nauron import Response, Nazgul, MQConsumer
import stanza
import pika, json
from typing import Dict, Any

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s : %(message)s")
logging.getLogger("pika").setLevel(level=logging.WARNING)

logger = logging.getLogger('mynazgul')


class StanzaTokenizerNazgul(Nazgul):
    def __init__(self, stanza_location='stanza_model'):
        logger.info("Loading Stanza tokenizer model...")
        self.tokenizer = stanza.Pipeline(lang='et', dir=stanza_location, processors='tokenize', logging_level='WARN')

    def process_request(self, request: Dict[str, Any]) -> Response:
        try:
            doc = self.tokenizer(request["text"])
            extracted_data = doc.to_dict()
            sentences = []
            for sentence in extracted_data:
                sentence_collected = []
                for word in sentence:
                    text = word.get('text')
                    sentence_collected.append(text)
                sentences.append(sentence_collected)
            return Response({'text':sentences}, mimetype="application/json")

        except ValueError:
            return Response(http_status_code=413,
                            content='Input is too long.')



if __name__ == "__main__":
    mq_parameters = pika.ConnectionParameters(host='localhost',
                                              port=5672,
                                              credentials=pika.credentials.PlainCredentials(username='guest',
                                                                                            password='guest'))

    service = MQConsumer(StanzaTokenizerNazgul(), mq_parameters, 'tokenize', queue_name='default')
    service.start()
