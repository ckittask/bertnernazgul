from flask_restful import Api
from flask import Flask
from flask_cors import CORS

from nauron import Sauron, LocalSauronConf
from bert_ner_nazgul import BertNerNazgul

# Define Flask application
app = Flask(__name__)
api = Api(app)
CORS(app)


conf_bert = LocalSauronConf(nazguls={'public': BertNerNazgul()}, application_required=False)

# Define API endpoints
api.add_resource(Sauron, '/api/bertner', resource_class_args=(conf_bert, ))


if __name__ == '__main__':
    app.run()