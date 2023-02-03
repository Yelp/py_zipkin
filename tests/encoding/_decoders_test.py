import pytest

from py_zipkin.encoding._decoders import get_decoder
from py_zipkin.encoding._decoders import IDecoder
from py_zipkin.encoding._types import Encoding
from py_zipkin.exception import ZipkinError


def test_get_decoder():
    with pytest.raises(NotImplementedError):
        get_decoder(Encoding.V1_THRIFT)
    with pytest.raises(NotImplementedError):
        get_decoder(Encoding.V1_JSON)
    with pytest.raises(NotImplementedError):
        get_decoder(Encoding.V2_JSON)
    with pytest.raises(ZipkinError):
        get_decoder(None)


def test_idecoder_throws_not_implemented_errors():
    encoder = IDecoder()
    with pytest.raises(NotImplementedError):
        encoder.decode_spans(b"[]")
