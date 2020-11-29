import logging
import json
from io import BytesIO

from dataclasses import dataclass, asdict
from typing import Optional, Union, Dict

from flask.helpers import make_response, send_file
from flask import jsonify
from flask_restful import abort

LOGGER = logging.getLogger(__name__)


@dataclass
class Response:
    content: Optional[Union[bytes, str, Dict]]
    http_status_code: int = 200
    mimetype: str = 'application/json'

    def encode(self) -> bytes:
        if type(self.content) == bytes:
            self.content = self.content.decode('ISO-8859-1')
        return json.dumps(asdict(self)).encode("utf8")

    def rest_response(self):
        if self.http_status_code != 200:
            if self.content is None:
                abort(http_status_code=self.http_status_code)
            else:
                abort(http_status_code=self.http_status_code, message=self.content)

        if self.mimetype == 'application/json':
            return make_response(jsonify(self.content), self.http_status_code)
        else:
            if type(self.content) == str:
                self.content = self.content.encode('ISO-8859-1')
            return send_file(BytesIO(self.content), mimetype=self.mimetype)
