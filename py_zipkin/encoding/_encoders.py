import json

from py_zipkin import thrift
from py_zipkin.encoding import protobuf
from py_zipkin.encoding._types import Encoding
from py_zipkin.encoding._types import Kind
from py_zipkin.exception import ZipkinError


def get_encoder(encoding):
    """Creates encoder object for the given encoding.

    :param encoding: desired output encoding protocol.
    :type encoding: Encoding
    :return: corresponding IEncoder object
    :rtype: IEncoder
    """
    if encoding == Encoding.V1_THRIFT:
        return _V1ThriftEncoder()
    if encoding == Encoding.V1_JSON:
        return _V1JSONEncoder()
    if encoding == Encoding.V2_JSON:
        return _V2JSONEncoder()
    if encoding == Encoding.V2_PROTO3:
        return _V2ProtobufEncoder()
    raise ZipkinError("Unknown encoding: {}".format(encoding))


class IEncoder:
    """Encoder interface."""

    def fits(self, current_count, current_size, max_size, new_span):
        """Returns whether the new span will fit in the list.

        :param current_count: number of spans already in the list.
        :type current_count: int
        :param current_size: sum of the sizes of all the spans already in the list.
        :type current_size: int
        :param max_size: max supported transport payload size.
        :type max_size: int
        :param new_span: encoded span object that we want to add the the list.
        :type new_span: str or bytes
        :return: True if the new span can be added to the list, False otherwise.
        :rtype: bool
        """
        raise NotImplementedError()

    def encode_span(self, span):
        """Encodes a single span.

        :param span: Span object representing the span.
        :type span: Span
        :return: encoded span.
        :rtype: str or bytes
        """
        raise NotImplementedError()

    def encode_queue(self, queue):
        """Encodes a list of pre-encoded spans.

        :param queue: list of encoded spans.
        :type queue: list
        :return: encoded list, type depends on the encoding.
        :rtype: str or bytes
        """
        raise NotImplementedError()


class _V1ThriftEncoder(IEncoder):
    """Thrift encoder for V1 spans."""

    def fits(self, current_count, current_size, max_size, new_span):
        """Checks if the new span fits in the max payload size.

        Thrift lists have a fixed-size header and no delimiters between elements
        so it's easy to compute the list size.
        """
        return thrift.LIST_HEADER_SIZE + current_size + len(new_span) <= max_size

    def encode_remote_endpoint(self, remote_endpoint, kind, binary_annotations):
        thrift_remote_endpoint = thrift.create_endpoint(
            remote_endpoint.port,
            remote_endpoint.service_name,
            remote_endpoint.ipv4,
            remote_endpoint.ipv6,
        )
        if kind == Kind.CLIENT:
            key = thrift.zipkin_core.SERVER_ADDR
        elif kind == Kind.SERVER:
            key = thrift.zipkin_core.CLIENT_ADDR

        binary_annotations.append(
            thrift.create_binary_annotation(
                key=key,
                value=thrift.SERVER_ADDR_VAL,
                annotation_type=thrift.zipkin_core.AnnotationType.BOOL,
                host=thrift_remote_endpoint,
            )
        )

    def encode_span(self, v2_span):
        """Encodes the current span to thrift."""
        span = v2_span.build_v1_span()

        thrift_endpoint = thrift.create_endpoint(
            span.endpoint.port,
            span.endpoint.service_name,
            span.endpoint.ipv4,
            span.endpoint.ipv6,
        )

        thrift_annotations = thrift.annotation_list_builder(
            span.annotations,
            thrift_endpoint,
        )

        thrift_binary_annotations = thrift.binary_annotation_list_builder(
            span.binary_annotations,
            thrift_endpoint,
        )

        # Add sa/ca binary annotations
        if v2_span.remote_endpoint:
            self.encode_remote_endpoint(
                v2_span.remote_endpoint,
                v2_span.kind,
                thrift_binary_annotations,
            )

        thrift_span = thrift.create_span(
            span.id,
            span.parent_id,
            span.trace_id,
            span.name,
            thrift_annotations,
            thrift_binary_annotations,
            span.timestamp,
            span.duration,
        )

        encoded_span = thrift.span_to_bytes(thrift_span)
        return encoded_span

    def encode_queue(self, queue):
        """Converts the queue to a thrift list"""
        return thrift.encode_bytes_list(queue)


class _BaseJSONEncoder(IEncoder):
    """V1 and V2 JSON encoders need many common helper functions"""

    def fits(self, current_count, current_size, max_size, new_span):
        """Checks if the new span fits in the max payload size.

        Json lists only have a 2 bytes overhead from '[]' plus 1 byte from
        ',' between elements
        """
        return 2 + current_count + current_size + len(new_span) <= max_size

    def _create_json_endpoint(self, endpoint, is_v1):
        """Converts an Endpoint to a JSON endpoint dict.

        :param endpoint: endpoint object to convert.
        :type endpoint: Endpoint
        :param is_v1: whether we're serializing a v1 span. This is needed since
            in v1 some fields default to an empty string rather than being
            dropped if they're not set.
        :type is_v1: bool
        :return: dict representing a JSON endpoint.
        :rtype: dict
        """
        json_endpoint = {}

        if endpoint.service_name:
            json_endpoint["serviceName"] = endpoint.service_name
        elif is_v1:
            # serviceName is mandatory in v1
            json_endpoint["serviceName"] = ""
        if endpoint.port and endpoint.port != 0:
            json_endpoint["port"] = endpoint.port
        if endpoint.ipv4 is not None:
            json_endpoint["ipv4"] = endpoint.ipv4
        if endpoint.ipv6 is not None:
            json_endpoint["ipv6"] = endpoint.ipv6

        return json_endpoint

    def encode_queue(self, queue):
        """Concatenates the list to a JSON list"""
        return "[" + ",".join(queue) + "]"


class _V1JSONEncoder(_BaseJSONEncoder):
    """JSON encoder for V1 spans."""

    def encode_remote_endpoint(self, remote_endpoint, kind, binary_annotations):
        json_remote_endpoint = self._create_json_endpoint(remote_endpoint, True)
        if kind == Kind.CLIENT:
            key = "sa"
        elif kind == Kind.SERVER:
            key = "ca"

        binary_annotations.append(
            {"key": key, "value": True, "endpoint": json_remote_endpoint}
        )

    def encode_span(self, v2_span):
        """Encodes a single span to JSON."""
        span = v2_span.build_v1_span()

        json_span = {
            "traceId": span.trace_id,
            "name": span.name,
            "id": span.id,
            "annotations": [],
            "binaryAnnotations": [],
        }

        if span.parent_id:
            json_span["parentId"] = span.parent_id
        if span.timestamp:
            json_span["timestamp"] = int(span.timestamp * 1000000)
        if span.duration:
            json_span["duration"] = int(span.duration * 1000000)

        v1_endpoint = self._create_json_endpoint(span.endpoint, True)

        for key, timestamp in span.annotations.items():
            json_span["annotations"].append(
                {
                    "endpoint": v1_endpoint,
                    "timestamp": int(timestamp * 1000000),
                    "value": key,
                }
            )

        for key, value in span.binary_annotations.items():
            json_span["binaryAnnotations"].append(
                {"key": key, "value": value, "endpoint": v1_endpoint}
            )

        # Add sa/ca binary annotations
        if v2_span.remote_endpoint:
            self.encode_remote_endpoint(
                v2_span.remote_endpoint,
                v2_span.kind,
                json_span["binaryAnnotations"],
            )

        encoded_span = json.dumps(json_span)

        return encoded_span


class _V2JSONEncoder(_BaseJSONEncoder):
    """JSON encoder for V2 spans."""

    def encode_span(self, span):
        """Encodes a single span to JSON."""

        json_span = {
            "traceId": span.trace_id,
            "id": span.span_id,
        }

        if span.name:
            json_span["name"] = span.name
        if span.parent_id:
            json_span["parentId"] = span.parent_id
        if span.timestamp:
            json_span["timestamp"] = int(span.timestamp * 1000000)
        if span.duration:
            json_span["duration"] = int(span.duration * 1000000)
        if span.shared is True:
            json_span["shared"] = True
        if span.kind and span.kind.value is not None:
            json_span["kind"] = span.kind.value
        if span.local_endpoint:
            json_span["localEndpoint"] = self._create_json_endpoint(
                span.local_endpoint,
                False,
            )
        if span.remote_endpoint:
            json_span["remoteEndpoint"] = self._create_json_endpoint(
                span.remote_endpoint,
                False,
            )
        if span.tags and len(span.tags) > 0:
            # Ensure that tags are all strings
            json_span["tags"] = {
                str(key): str(value) for key, value in span.tags.items()
            }

        if span.annotations:
            json_span["annotations"] = [
                {"timestamp": int(timestamp * 1000000), "value": key}
                for key, timestamp in span.annotations.items()
            ]

        encoded_span = json.dumps(json_span)

        return encoded_span


class _V2ProtobufEncoder(IEncoder):
    """Protobuf encoder for V2 spans."""

    def fits(self, current_count, current_size, max_size, new_span):
        """Checks if the new span fits in the max payload size."""
        return current_size + len(new_span) <= max_size

    def encode_span(self, span):
        """Encodes a single span to protobuf."""
        if not protobuf.installed():
            raise ZipkinError(
                "protobuf encoding requires installing the protobuf's extra "
                "requirements. Use py-zipkin[protobuf] in your requirements.txt."
            )

        pb_span = protobuf.create_protobuf_span(span)
        return protobuf.encode_pb_list([pb_span])

    def encode_queue(self, queue):
        """Concatenates the list to a protobuf list and encodes it to bytes"""
        return b"".join(queue)
