import json
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Union

from typing_extensions import TypedDict
from typing_extensions import TypeGuard

from py_zipkin import thrift
from py_zipkin.encoding import protobuf
from py_zipkin.encoding._helpers import Endpoint
from py_zipkin.encoding._helpers import Span
from py_zipkin.encoding._types import Encoding
from py_zipkin.encoding._types import Kind
from py_zipkin.exception import ZipkinError


def get_encoder(encoding: Encoding) -> "IEncoder":
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
    raise ZipkinError(f"Unknown encoding: {encoding}")


class IEncoder:
    """Encoder interface."""

    def fits(
        self,
        current_count: int,
        current_size: int,
        max_size: int,
        new_span: Union[str, bytes],
    ) -> bool:
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

    def encode_span(self, span: Span) -> Union[str, bytes]:
        """Encodes a single span.

        :param span: Span object representing the span.
        :type span: Span
        :return: encoded span.
        :rtype: str or bytes
        """
        raise NotImplementedError()

    def encode_queue(self, queue: List[Union[str, bytes]]) -> Union[str, bytes]:
        """Encodes a list of pre-encoded spans.

        :param queue: list of encoded spans.
        :type queue: list
        :return: encoded list, type depends on the encoding.
        :rtype: str or bytes
        """
        raise NotImplementedError()


def _is_mapping_str_float(
    mapping: Mapping[str, Optional[float]]
) -> TypeGuard[Mapping[str, float]]:
    return all(isinstance(value, float) for key, value in mapping.items())


def _is_dict_str_str(mapping: Dict[str, Optional[str]]) -> TypeGuard[Dict[str, str]]:
    return all(isinstance(value, str) for key, value in mapping.items())


class _V1ThriftEncoder(IEncoder):
    """Thrift encoder for V1 spans."""

    def fits(
        self,
        current_count: int,
        current_size: int,
        max_size: int,
        new_span: Union[str, bytes],
    ) -> bool:
        """Checks if the new span fits in the max payload size.

        Thrift lists have a fixed-size header and no delimiters between elements
        so it's easy to compute the list size.
        """
        return thrift.LIST_HEADER_SIZE + current_size + len(new_span) <= max_size

    def encode_remote_endpoint(
        self,
        remote_endpoint: Endpoint,
        kind: Kind,
        binary_annotations: List[thrift.zipkinCore.BinaryAnnotation],
    ) -> None:
        assert remote_endpoint.port is not None
        thrift_remote_endpoint = thrift.create_endpoint(
            remote_endpoint.port,
            remote_endpoint.service_name,
            remote_endpoint.ipv4,
            remote_endpoint.ipv6,
        )
        # these attributes aren't yet supported by thrift-pyi
        if kind == Kind.CLIENT:
            key = thrift.zipkinCore.SERVER_ADDR  # type: ignore[attr-defined]
        elif kind == Kind.SERVER:
            key = thrift.zipkinCore.CLIENT_ADDR  # type: ignore[attr-defined]

        binary_annotations.append(
            thrift.create_binary_annotation(
                key=key,
                value=thrift.SERVER_ADDR_VAL,
                annotation_type=thrift.zipkinCore.AnnotationType.BOOL,
                host=thrift_remote_endpoint,
            )
        )

    def encode_span(self, v2_span: Span) -> bytes:
        """Encodes the current span to thrift."""
        span = v2_span.build_v1_span()
        assert span.endpoint is not None
        assert span.endpoint.port is not None
        thrift_endpoint = thrift.create_endpoint(
            span.endpoint.port,
            span.endpoint.service_name,
            span.endpoint.ipv4,
            span.endpoint.ipv6,
        )

        assert _is_mapping_str_float(span.annotations)
        thrift_annotations = thrift.annotation_list_builder(
            span.annotations,
            thrift_endpoint,
        )

        assert _is_dict_str_str(span.binary_annotations)
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

        assert span.id is not None
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

    def encode_queue(self, queue: List[Union[str, bytes]]) -> bytes:
        """Converts the queue to a thrift list"""
        return thrift.encode_bytes_list(queue)


class JSONEndpoint(TypedDict, total=False):
    serviceName: Optional[str]
    port: Optional[int]
    ipv4: Optional[str]
    ipv6: Optional[str]


def _is_str_list(any_str_list: List[Union[str, bytes]]) -> TypeGuard[List[str]]:
    return all(isinstance(element, str) for element in any_str_list)


class _BaseJSONEncoder(IEncoder):
    """V1 and V2 JSON encoders need many common helper functions"""

    def fits(
        self,
        current_count: int,
        current_size: int,
        max_size: int,
        new_span: Union[str, bytes],
    ) -> bool:
        """Checks if the new span fits in the max payload size.

        Json lists only have a 2 bytes overhead from '[]' plus 1 byte from
        ',' between elements
        """
        return 2 + current_count + current_size + len(new_span) <= max_size

    def _create_json_endpoint(self, endpoint: Endpoint, is_v1: bool) -> JSONEndpoint:
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
        json_endpoint: JSONEndpoint = {}

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

    def encode_queue(self, queue: List[Union[str, bytes]]) -> str:
        """Concatenates the list to a JSON list"""
        assert _is_str_list(queue)
        return "[" + ",".join(queue) + "]"


class JSONv1BinaryAnnotation(TypedDict):
    key: str
    value: Union[str, bool, None]
    endpoint: JSONEndpoint


class JSONv1Annotation(TypedDict):
    endpoint: JSONEndpoint
    timestamp: int
    value: str


class JSONv1Span(TypedDict, total=False):
    traceId: str
    name: Optional[str]
    id: Optional[str]
    annotations: List[JSONv1Annotation]
    binaryAnnotations: List[JSONv1BinaryAnnotation]
    parentId: str
    timestamp: int
    duration: int


class _V1JSONEncoder(_BaseJSONEncoder):
    """JSON encoder for V1 spans."""

    def encode_remote_endpoint(
        self,
        remote_endpoint: Endpoint,
        kind: Kind,
        binary_annotations: List[JSONv1BinaryAnnotation],
    ) -> None:
        json_remote_endpoint = self._create_json_endpoint(remote_endpoint, True)
        if kind == Kind.CLIENT:
            key = "sa"
        elif kind == Kind.SERVER:
            key = "ca"

        binary_annotations.append(
            {"key": key, "value": True, "endpoint": json_remote_endpoint}
        )

    def encode_span(self, v2_span: Span) -> str:
        """Encodes a single span to JSON."""
        span = v2_span.build_v1_span()

        json_span: JSONv1Span = {
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

        assert span.endpoint is not None
        v1_endpoint = self._create_json_endpoint(span.endpoint, True)

        for key, timestamp in span.annotations.items():
            assert timestamp is not None
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


class JSONv2Annotation(TypedDict):
    timestamp: int
    value: str


class JSONv2Span(TypedDict, total=False):
    traceId: str
    id: Optional[str]
    name: str
    parentId: str
    timestamp: int
    duration: int
    shared: bool
    kind: str
    localEndpoint: JSONEndpoint
    remoteEndpoint: JSONEndpoint
    tags: Dict[str, str]
    annotations: List[JSONv2Annotation]


def _is_dict_str_float(
    mapping: Dict[str, Optional[float]]
) -> TypeGuard[Dict[str, float]]:
    return all(isinstance(value, float) for key, value in mapping.items())


class _V2JSONEncoder(_BaseJSONEncoder):
    """JSON encoder for V2 spans."""

    def encode_span(self, span: Span) -> str:
        """Encodes a single span to JSON."""

        json_span: JSONv2Span = {
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
            assert _is_dict_str_float(span.annotations)
            json_span["annotations"] = [
                {"timestamp": int(timestamp * 1000000), "value": key}
                for key, timestamp in span.annotations.items()
            ]

        encoded_span = json.dumps(json_span)

        return encoded_span


def _is_bytes_list(any_str_list: List[Union[str, bytes]]) -> TypeGuard[List[bytes]]:
    return all(isinstance(element, bytes) for element in any_str_list)


class _V2ProtobufEncoder(IEncoder):
    """Protobuf encoder for V2 spans."""

    def fits(
        self,
        current_count: int,
        current_size: int,
        max_size: int,
        new_span: Union[str, bytes],
    ) -> bool:
        """Checks if the new span fits in the max payload size."""
        return current_size + len(new_span) <= max_size

    def encode_span(self, span: Span) -> bytes:
        """Encodes a single span to protobuf."""
        if not protobuf.installed():
            raise ZipkinError(
                "protobuf encoding requires installing the protobuf's extra "
                "requirements. Use py-zipkin[protobuf] in your requirements.txt."
            )

        pb_span = protobuf.create_protobuf_span(span)
        return protobuf.encode_pb_list([pb_span])

    def encode_queue(self, queue: List[Union[str, bytes]]) -> bytes:
        """Concatenates the list to a protobuf list and encodes it to bytes"""
        assert _is_bytes_list(queue)
        return b"".join(queue)
