# -*- coding: utf-8 -*-
import json

import six

from py_zipkin.encoding._types import Encoding
from py_zipkin.encoding._decoders import get_decoder
from py_zipkin.encoding._encoders import get_encoder
from py_zipkin.exception import ZipkinError

_V2_ATTRIBUTES = ["tags", "localEndpoint", "remoteEndpoint", "shared", "kind"]


def detect_span_version_and_encoding(message):
    """Returns the span type and encoding for the message provided.

    The logic in this function is a Python port of
    https://github.com/openzipkin/zipkin/blob/master/zipkin/src/main/java/zipkin/internal/DetectingSpanDecoder.java

    :param message: span to perform operations on.
    :type message: byte array
    :returns: span encoding.
    :rtype: Encoding
    """
    # In case message is sent in as non-bytearray format,
    # safeguard convert to bytearray before handling
    if isinstance(message, six.string_types):
        message = six.b(message)

    if len(message) < 2:
        raise ZipkinError("Invalid span format. Message too short.")

    # Check for binary format
    if six.byte2int(message) <= 16:
        if message[0] == 10 and message[1] != 0:
            # Excluding from coverage since Protobuf encoding is
            # not yet implemented.
            return Encoding.V2_PROTOBUF  # pragma: no cover
        return Encoding.V1_THRIFT

    str_msg = message.decode('utf-8')

    # JSON case for list of spans
    if str_msg[0] == '[':
        span_list = json.loads(str_msg)
        if len(span_list) > 0:
            # Assumption: All spans in a list are the same version
            # Logic: Search for identifying fields in all spans, if any span can
            # be strictly identified to a version, return that version.
            # Otherwise, if no spans could be strictly identified, default to V2.
            for span in span_list:
                if any(word in span for word in _V2_ATTRIBUTES):
                    return Encoding.V2_JSON
                elif (
                    'binaryAnnotations' in span or
                    (
                        'annotations' in span and
                        'endpoint' in span['annotations']
                    )
                ):
                    return Encoding.V1_JSON
            return Encoding.V2_JSON

    raise ZipkinError("Unknown or unsupported span encoding")


def convert_spans(spans, output_encoding, input_encoding=None):
    """Converts encoded spans to a different encoding.

    param spans: encoded input spans.
    type spans: byte array
    param output_encoding: desired output encoding.
    type output_encoding: Encoding
    param input_encoding: optional input encoding. If this is not specified, it'll
        try to understand the encoding automatically by inspecting the input spans.
    type input_encoding: Encoding
    :returns: encoded spans.
    :rtype: byte array
    """
    if not isinstance(input_encoding, Encoding):
        input_encoding = detect_span_version_and_encoding(message=spans)

    if input_encoding == output_encoding:
        return spans

    decoder = get_decoder(input_encoding)
    encoder = get_encoder(output_encoding)
    span_builders = decoder.decode_spans(spans)
    output_spans = []

    # Encode each indivicual span
    for sb in span_builders:
        output_spans.append(encoder.encode_span(sb))

    # Outputs from encoder.encode_span() can be easily concatenated in a list
    return encoder.encode_queue(output_spans)
