import logging
from nauron import Response, Nazgul, MQConsumer
from transformers import BertTokenizer, BertForTokenClassification
import torch
import pika, json
from typing import Dict, Any
from collections import Counter
import ast

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s : %(message)s")
logging.getLogger("pika").setLevel(level=logging.WARNING)

logger = logging.getLogger('mynazgul')


class BertNerNazgul(Nazgul):
    def __init__(self, bert_location='ner_bert'):
        logger.info("Loading BERT NER model...")
        self.bertner = BertForTokenClassification.from_pretrained(bert_location, return_dict=True)
        self.labelmap = {0: 'B-LOC', 1: 'B-ORG', 2: 'B-PER', 3: 'I-LOC', 4: 'I-ORG', 5: 'I-PER', 6: 'O'}
        self.tokenizer = BertTokenizer.from_pretrained(bert_location)

    def process_request(self, request: Dict[str, Any]) -> Response:
        try:
            sentences = ast.literal_eval(request['text'])
            tagged_sentences = []
            for sentence in sentences:
                entities = self.predict(sentence)
                words = []
                for word, entity in zip(sentence, entities):
                    subresult = {'word': word, 'ner': entity}
                    words.append(subresult)
                tagged_sentences.append(words)
            return Response({'result':tagged_sentences}, mimetype="application/json")
        except ValueError:
            return Response(http_status_code=413,
                            content='Input is too long.')

    def predict(self, sentence: list) -> list:
        grouped_inputs = [torch.LongTensor([self.tokenizer.cls_token_id])]
        subtokens_per_token = []
        for token in sentence:
            tokens = self.tokenizer.encode(
                token,
                return_tensors="pt",
                add_special_tokens=False,
            ).squeeze(axis=0)
            grouped_inputs.append(tokens)
            subtokens_per_token.append(len(tokens))

        grouped_inputs.append(torch.LongTensor([self.tokenizer.sep_token_id]))

        flattened_inputs = torch.cat(grouped_inputs)
        flattened_inputs = torch.unsqueeze(flattened_inputs, 0)
        predictions_tensor = self.bertner(flattened_inputs)[0]
        predictions_tensor = torch.argmax(predictions_tensor, dim=2)[0]
        preds = predictions_tensor[1:-1]
        predictions = [self.labelmap.get(int(pred)) for pred in preds]
        aligned_predictions = []
        ptr = 0
        for size in subtokens_per_token:
            group = predictions[ptr:ptr + size]
            aligned_predictions.append(group)
            ptr += size
        predicted_labels = []
        previous = 'O'
        for token, prediction_group in zip(sentence, aligned_predictions):
            label = Counter(prediction_group).most_common(1)[0][0]
            base = label.split('-')[-1]
            if previous == 'O' and label.startswith('I'):
                label = 'B-' + base
            previous = label
            predicted_labels.append(label)
        return predicted_labels


if __name__ == "__main__":
    mq_parameters = pika.ConnectionParameters(host='localhost',
                                              port=5672,
                                              credentials=pika.credentials.PlainCredentials(username='guest',
                                                                                            password='guest'))

    service = MQConsumer(BertNerNazgul(), mq_parameters, 'bertner', queue_name='default')
    service.start()
