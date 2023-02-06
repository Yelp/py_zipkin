import logging
from typing import List

from py_zipkin.encoding._helpers import Span
from py_zipkin.encoding._types import Encoding
from py_zipkin.exception import ZipkinError

log = logging.getLogger("py_zipkin.encoding")


def get_decoder(encoding: Encoding) -> "IDecoder":
    """Creates encoder object for the given encoding.
    :param encoding: desired output encoding protocol
    :type encoding: Encoding
    :return: corresponding IDecoder object
    :rtype: IDecoder
    """
    if encoding == Encoding.V1_THRIFT:
        raise NotImplementedError(f"{encoding} decoding no longer supported")
    if encoding == Encoding.V1_JSON:
        raise NotImplementedError(f"{encoding} decoding not yet implemented")
    if encoding == Encoding.V2_JSON:
        raise NotImplementedError(f"{encoding} decoding not yet implemented")
    raise ZipkinError(f"Unknown encoding: {encoding}")


class IDecoder:
    """Decoder interface."""

    def decode_spans(self, spans: bytes) -> List[Span]:
        """Decodes an encoded list of spans.
        :param spans: encoded list of spans
        :type spans: bytes
        :return: list of spans
        :rtype: list of Span
        """
        raise NotImplementedError()
