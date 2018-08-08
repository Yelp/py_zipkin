import mock
import pytest

from py_zipkin.zipkin import ZipkinAttrs
from py_zipkin.transport import BaseTransportHandler
from py_zipkin._encoding_helpers import IEncoder


@pytest.fixture
def zipkin_attributes():
    return {
        'trace_id': '17133d482ba4f605',
        'span_id': '27133d482ba4f605',
        'parent_span_id': '37133d482ba4f605',
        'flags': '45',
    }


@pytest.fixture
def sampled_zipkin_attr(zipkin_attributes):
    return ZipkinAttrs(is_sampled=True, **zipkin_attributes)


@pytest.fixture
def unsampled_zipkin_attr(zipkin_attributes):
    return ZipkinAttrs(is_sampled=False, **zipkin_attributes)


class MockTransportHandler(BaseTransportHandler):

    def __init__(self, max_payload_bytes=None):
        self.max_payload_bytes = max_payload_bytes

    def send(self, payload):
        return payload

    def get_max_payload_bytes(self):
        return self.max_payload_bytes


class MockEncoder(IEncoder):

    def __init__(self, fits=True, encoded_span='', encoded_queue=''):
        self.fits = mock.Mock(return_value=fits)
        self.encode_span = mock.Mock(
            return_value=(encoded_span, len(encoded_span)),
        )
        self.encode_queue = mock.Mock(return_value=encoded_queue)
