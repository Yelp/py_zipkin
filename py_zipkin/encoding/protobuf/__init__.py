import socket
import struct
from typing import Dict
from typing import List
from typing import Optional

from typing_extensions import TypedDict
from typing_extensions import TypeGuard

from py_zipkin.encoding._helpers import Endpoint
from py_zipkin.encoding._helpers import Span
from py_zipkin.encoding._types import Kind
from py_zipkin.util import unsigned_hex_to_signed_int

try:
    from py_zipkin.encoding.protobuf import zipkin_pb2
except ImportError:  # pragma: no cover
    pass


def installed() -> bool:  # pragma: no cover
    """Checks whether the protobud library is installed and can be used.

    :return: True if everything's fine, False otherwise
    :rtype: bool
    """
    try:
        _ = zipkin_pb2
        return True
    except NameError:
        return False


def encode_pb_list(pb_spans: "List[zipkin_pb2.Span]") -> bytes:
    """Encode list of protobuf Spans to binary.

    :param pb_spans: list of protobuf Spans.
    :type pb_spans: list of zipkin_pb2.Span
    :return: encoded list.
    :rtype: bytes
    """
    pb_list = zipkin_pb2.ListOfSpans()
    pb_list.spans.extend(pb_spans)
    return pb_list.SerializeToString()


class ProtobufSpanArgsDict(TypedDict, total=False):
    trace_id: bytes
    parent_id: bytes
    id: bytes
    kind: "zipkin_pb2.Span._Kind.ValueType"
    name: str
    timestamp: int
    duration: int
    local_endpoint: "zipkin_pb2.Endpoint"
    remote_endpoint: "zipkin_pb2.Endpoint"
    annotations: "List[zipkin_pb2.Annotation]"
    tags: Dict[str, str]
    debug: bool
    shared: bool


def _is_dict_str_str(mapping: Dict[str, Optional[str]]) -> TypeGuard[Dict[str, str]]:
    return all(isinstance(value, str) for _, value in mapping.items())


def create_protobuf_span(span: Span) -> "zipkin_pb2.Span":
    """Converts a py_zipkin Span in a protobuf Span.

    :param span: py_zipkin Span to convert.
    :type span: py_zipkin.encoding.Span
    :return: protobuf's Span
    :rtype: zipkin_pb2.Span
    """

    # Protobuf's composite types (i.e. Span's local_endpoint) are immutable.
    # So we can't create a zipkin_pb2.Span here and then set the appropriate
    # fields since `pb_span.local_endpoint = zipkin_pb2.Endpoint` fails.
    # Instead we just create the kwargs and pass them in to the Span constructor.
    pb_kwargs: ProtobufSpanArgsDict = {}

    pb_kwargs["trace_id"] = _hex_to_bytes(span.trace_id)

    if span.parent_id:
        pb_kwargs["parent_id"] = _hex_to_bytes(span.parent_id)

    assert span.span_id is not None
    pb_kwargs["id"] = _hex_to_bytes(span.span_id)

    pb_kind = _get_protobuf_kind(span.kind)
    if pb_kind:
        pb_kwargs["kind"] = pb_kind

    if span.name:
        pb_kwargs["name"] = span.name
    if span.timestamp:
        pb_kwargs["timestamp"] = int(span.timestamp * 1000 * 1000)
    if span.duration:
        pb_kwargs["duration"] = int(span.duration * 1000 * 1000)

    if span.local_endpoint:
        pb_kwargs["local_endpoint"] = _convert_endpoint(span.local_endpoint)

    if span.remote_endpoint:
        pb_kwargs["remote_endpoint"] = _convert_endpoint(span.remote_endpoint)

    if len(span.annotations) > 0:
        pb_kwargs["annotations"] = _convert_annotations(span.annotations)

    if len(span.tags) > 0:
        assert _is_dict_str_str(span.tags)
        pb_kwargs["tags"] = span.tags

    if span.debug:
        pb_kwargs["debug"] = span.debug

    if span.shared:
        pb_kwargs["shared"] = span.shared

    return zipkin_pb2.Span(**pb_kwargs)


def _hex_to_bytes(hex_id: str) -> bytes:
    """Encodes to hexadecimal ids to big-endian binary.

    :param hex_id: hexadecimal id to encode.
    :type hex_id: str
    :return: binary representation.
    :type: bytes
    """
    if len(hex_id) <= 16:
        int_id = unsigned_hex_to_signed_int(hex_id)
        return struct.pack(">q", int_id)
    else:
        # There's no 16-bytes encoding in Python's struct. So we convert the
        # id as 2 64 bit ids and then concatenate the result.

        # NOTE: we count 16 chars from the right (:-16) rather than the left so
        # that ids with less than 32 chars will be correctly pre-padded with 0s.
        high_id = unsigned_hex_to_signed_int(hex_id[:-16])
        high_bin = struct.pack(">q", high_id)

        low_id = unsigned_hex_to_signed_int(hex_id[-16:])
        low_bin = struct.pack(">q", low_id)

        return high_bin + low_bin


def _get_protobuf_kind(kind: Kind) -> "Optional[zipkin_pb2.Span._Kind.ValueType]":
    """Converts py_zipkin's Kind to Protobuf's Kind.

    :param kind: py_zipkin's Kind.
    :type kind: py_zipkin.Kind
    :return: correcponding protobuf's kind value.
    :rtype: zipkin_pb2.Span._Kind.ValueType
    """
    if kind == Kind.CLIENT:
        return zipkin_pb2.Span.CLIENT
    elif kind == Kind.SERVER:
        return zipkin_pb2.Span.SERVER
    elif kind == Kind.PRODUCER:
        return zipkin_pb2.Span.PRODUCER
    elif kind == Kind.CONSUMER:
        return zipkin_pb2.Span.CONSUMER
    return None


def _convert_endpoint(endpoint: Endpoint) -> "zipkin_pb2.Endpoint":
    """Converts py_zipkin's Endpoint to Protobuf's Endpoint.

    :param endpoint: py_zipkins' endpoint to convert.
    :type endpoint: py_zipkin.encoding.Endpoint
    :return: corresponding protobuf's endpoint.
    :rtype: zipkin_pb2.Endpoint
    """
    pb_endpoint = zipkin_pb2.Endpoint()

    if endpoint.service_name:
        pb_endpoint.service_name = endpoint.service_name
    if endpoint.port and endpoint.port != 0:
        pb_endpoint.port = endpoint.port
    if endpoint.ipv4:
        pb_endpoint.ipv4 = socket.inet_pton(socket.AF_INET, endpoint.ipv4)
    if endpoint.ipv6:
        pb_endpoint.ipv6 = socket.inet_pton(socket.AF_INET6, endpoint.ipv6)

    return pb_endpoint


def _convert_annotations(
    annotations: Dict[str, Optional[float]]
) -> "List[zipkin_pb2.Annotation]":
    """Converts py_zipkin's annotations dict to protobuf.

    :param annotations: annotations dict.
    :type annotations: dict
    :return: corresponding protobuf's list of annotations.
    :rtype: list
    """
    pb_annotations = []
    for value, ts in annotations.items():
        assert ts is not None
        pb_annotations.append(
            zipkin_pb2.Annotation(timestamp=int(ts * 1000 * 1000), value=value)
        )
    return pb_annotations
