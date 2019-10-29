# -*- coding: utf-8 -*-
from six.moves.urllib.request import Request
from six.moves.urllib.request import urlopen

from py_zipkin.encoding import detect_span_version_and_encoding
from py_zipkin.encoding import Encoding
from py_zipkin.transport import BaseTransportHandler


class SimpleHTTPTransport(BaseTransportHandler):

    def __init__(self, address, port):
        """A simple HTTP transport for zipkin.

        This is not production ready (not async, no retries) but
        it's helpful for tests or people trying out py-zipkin.

        .. code-block:: python

            with zipkin_span(
                service_name='my_service',
                span_name='home',
                sample_rate=100,
                transport_handler=SimpleHTTPTransport('localhost', 9411),
                encoding=Encoding.V2_JSON,
            ):
                pass

        :param address: zipkin server address.
        :type address: str
        :param port: zipkin server port.
        :type port: int
        """
        super(SimpleHTTPTransport, self).__init__()
        self.address = address
        self.port = port

    def get_max_payload_bytes(self):
        return None

    def _get_path_content_type(self, payload):
        """Choose the right api path and content type depending on the encoding.

        This is not something you'd need to do generally when writing your own
        transport since in that case you'd know which encoding you're using.
        Since this is a generic transport, we need to make it compatible with
        any encoding instead.
        """
        encoding = detect_span_version_and_encoding(payload)

        if encoding == Encoding.V1_JSON:
            return '/api/v1/spans', 'application/json'
        elif encoding == Encoding.V1_THRIFT:
            return '/api/v1/spans', 'application/x-thrift'
        elif encoding == Encoding.V2_JSON:
            return '/api/v2/spans', 'application/json'
        elif encoding == Encoding.V2_PROTO3:
            return '/api/v2/spans', 'application/x-protobuf'

    def send(self, payload):
        path, content_type = self._get_path_content_type(payload)
        url = 'http://{}:{}{}'.format(self.address, self.port, path)

        req = Request(url, payload, {'Content-Type': content_type})
        response = urlopen(req)

        assert response.getcode() == 202
