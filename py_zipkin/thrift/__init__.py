import os
import socket
import struct

import thriftpy2
from thriftpy2.protocol import TBinaryProtocol
from thriftpy2.protocol.binary import write_list_begin
from thriftpy2.thrift import TType
from thriftpy2.transport import TMemoryBuffer

from py_zipkin.util import unsigned_hex_to_signed_int


thrift_filepath = os.path.join(os.path.dirname(__file__), "zipkinCore.thrift")
zipkin_core = thriftpy2.load(thrift_filepath, module_name="zipkinCore_thrift")

SERVER_ADDR_VAL = "\x01"
LIST_HEADER_SIZE = 5  # size in bytes of the encoded list header

dummy_endpoint = zipkin_core.Endpoint()


def create_annotation(timestamp, value, host):
    """
    Create a zipkin annotation object

    :param timestamp: timestamp of when the annotation occured in microseconds
    :param value: name of the annotation, such as 'sr'
    :param host: zipkin endpoint object

    :returns: zipkin annotation object
    """
    return zipkin_core.Annotation(timestamp=timestamp, value=value, host=host)


def create_binary_annotation(key, value, annotation_type, host):
    """
    Create a zipkin binary annotation object

    :param key: name of the annotation, such as 'http.uri'
    :param value: value of the annotation, such as a URI
    :param annotation_type: type of annotation, such as AnnotationType.I32
    :param host: zipkin endpoint object

    :returns: zipkin binary annotation object
    """
    return zipkin_core.BinaryAnnotation(
        key=key,
        value=value,
        annotation_type=annotation_type,
        host=host,
    )


def create_endpoint(port=0, service_name="unknown", ipv4=None, ipv6=None):
    """Create a zipkin Endpoint object.

    An Endpoint object holds information about the network context of a span.

    :param port: int value of the port. Defaults to 0
    :param service_name: service name as a str. Defaults to 'unknown'
    :param ipv4: ipv4 host address
    :param ipv6: ipv6 host address
    :returns: thrift Endpoint object
    """
    ipv4_int = 0
    ipv6_binary = None

    # Convert ip address to network byte order
    if ipv4:
        ipv4_int = struct.unpack("!i", socket.inet_pton(socket.AF_INET, ipv4))[0]

    if ipv6:
        ipv6_binary = socket.inet_pton(socket.AF_INET6, ipv6)

    # Zipkin passes unsigned values in signed types because Thrift has no
    # unsigned types, so we have to convert the value.
    port = struct.unpack("h", struct.pack("H", port))[0]
    return zipkin_core.Endpoint(
        ipv4=ipv4_int,
        ipv6=ipv6_binary,
        port=port,
        service_name=service_name,
    )


def copy_endpoint_with_new_service_name(endpoint, service_name):
    """Copies a copy of a given endpoint with a new service name.
    This should be very fast, on the order of several microseconds.

    :param endpoint: existing zipkin_core.Endpoint object
    :param service_name: str of new service name
    :returns: zipkin Endpoint object
    """
    return zipkin_core.Endpoint(
        ipv4=endpoint.ipv4,
        port=endpoint.port,
        service_name=service_name,
    )


def annotation_list_builder(annotations, host):
    """
    Reformat annotations dict to return list of corresponding zipkin_core objects.

    :param annotations: dict containing key as annotation name,
                        value being timestamp in seconds(float).
    :type host: :class:`zipkin_core.Endpoint`
    :returns: a list of annotation zipkin_core objects
    :rtype: list
    """
    return [
        create_annotation(int(timestamp * 1000000), key, host)
        for key, timestamp in annotations.items()
    ]


def binary_annotation_list_builder(binary_annotations, host):
    """
    Reformat binary annotations dict to return list of zipkin_core objects. The
    value of the binary annotations MUST be in string format.

    :param binary_annotations: dict with key, value being the name and value
                               of the binary annotation being logged.
    :type host: :class:`zipkin_core.Endpoint`
    :returns: a list of binary annotation zipkin_core objects
    :rtype: list
    """
    # TODO: Remove the type hard-coding of STRING to take it as a param option.
    ann_type = zipkin_core.AnnotationType.STRING
    return [
        create_binary_annotation(key, str(value), ann_type, host)
        for key, value in binary_annotations.items()
    ]


def create_span(
    span_id,
    parent_span_id,
    trace_id,
    span_name,
    annotations,
    binary_annotations,
    timestamp_s,
    duration_s,
):
    """Takes a bunch of span attributes and returns a thriftpy2 representation
    of the span. Timestamps passed in are in seconds, they're converted to
    microseconds before thrift encoding.
    """
    # Check if trace_id is 128-bit. If so, record trace_id_high separately.
    trace_id_length = len(trace_id)
    trace_id_high = None
    if trace_id_length > 16:
        assert trace_id_length == 32
        trace_id, trace_id_high = trace_id[16:], trace_id[:16]

    if trace_id_high:
        trace_id_high = unsigned_hex_to_signed_int(trace_id_high)

    span_dict = {
        "trace_id": unsigned_hex_to_signed_int(trace_id),
        "name": span_name,
        "id": unsigned_hex_to_signed_int(span_id),
        "annotations": annotations,
        "binary_annotations": binary_annotations,
        "timestamp": int(timestamp_s * 1000000) if timestamp_s else None,
        "duration": int(duration_s * 1000000) if duration_s else None,
        "trace_id_high": trace_id_high,
    }
    if parent_span_id:
        span_dict["parent_id"] = unsigned_hex_to_signed_int(parent_span_id)
    return zipkin_core.Span(**span_dict)


def span_to_bytes(thrift_span):
    """
    Returns a TBinaryProtocol encoded Thrift span.

    :param thrift_span: thrift object to encode.
    :returns: thrift object in TBinaryProtocol format bytes.
    """
    transport = TMemoryBuffer()
    protocol = TBinaryProtocol(transport)
    thrift_span.write(protocol)

    return bytes(transport.getvalue())


def encode_bytes_list(binary_thrift_obj_list):  # pragma: no cover
    """
    Returns a TBinaryProtocol encoded list of Thrift objects.

    :param binary_thrift_obj_list: list of TBinaryProtocol objects to encode.
    :returns: bynary object representing the encoded list.
    """
    transport = TMemoryBuffer()
    write_list_begin(transport, TType.STRUCT, len(binary_thrift_obj_list))
    for thrift_bin in binary_thrift_obj_list:
        transport.write(thrift_bin)

    return bytes(transport.getvalue())
