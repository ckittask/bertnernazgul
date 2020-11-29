from flask_restful import Api
from flask import Flask
from flask_cors import CORS

from nauron import Sauron, LocalSauronConf
from two_class_version.stanza_tokenizer_nazgul import StanzaTokenizerNazgul

# Define Flask application
app = Flask(__name__)
api = Api(app)
CORS(app)


conf_stanza = LocalSauronConf(nazguls={'public': StanzaTokenizerNazgul()}, application_required=False)

# Define API endpoints
api.add_resource(Sauron, '/api/tokenize', resource_class_args=(conf_stanza, ))


if __name__ == '__main__':
    app.run()