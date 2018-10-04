# -*- coding: utf-8 -*-
import json
import socket
from collections import namedtuple
from collections import OrderedDict

from enum import Enum

from py_zipkin import thrift
from py_zipkin.exception import ZipkinError


Endpoint = namedtuple(
    'Endpoint',
    ['service_name', 'ipv4', 'ipv6', 'port'],
)


_V1Span = namedtuple(
    'V1Span',
    ['trace_id', 'name', 'parent_id', 'id', 'timestamp', 'duration', 'endpoint',
     'annotations', 'binary_annotations', 'sa_endpoint'],
)


_V2Span = namedtuple(
    'V2Span',
    ['trace_id', 'name', 'parent_id', 'id', 'kind', 'timestamp',
     'duration', 'debug', 'shared', 'local_endpoint', 'remote_endpoint',
     'annotations', 'tags'],
)


class Kind(Enum):
    """Type of Span."""
    CLIENT = 'CLIENT'
    SERVER = 'SERVER'
    LOCAL = None


class Encoding(Enum):
    """Supported output encodings."""
    V1_THRIFT = 1
    V1_JSON = 2
    V2_JSON = 3


_DROP_ANNOTATIONS_BY_KIND = {
    Kind.CLIENT: {'ss', 'sr'},
    Kind.SERVER: {'cs', 'cr'},
}


class SpanBuilder(object):
    """Internal Span representation. It can generate both v1 and v2 spans.

    It doesn't exactly map to either V1 or V2, since an intermediate format
    makes it easier to convert to either format.
    """

    def __init__(
        self,
        trace_id,
        name,
        parent_id,
        span_id,
        timestamp,
        duration,
        annotations,
        tags,
        kind,
        local_endpoint=None,
        service_name=None,
        sa_endpoint=None,
        report_timestamp=True,
    ):
        """Creates a new SpanBuilder.

        :param trace_id: Trace id.
        :type trace_id: str
        :param name: Name of the span.
        :type name: str
        :param parent_id: Parent span id.
        :type parent_id: str
        :param span_id: Span id.
        :type span_id: str
        :param timestamp: start timestamp in seconds.
        :type timestamp: float
        :param duration: span duration in seconds.
        :type duration: float
        :param annotations: Optional dict of str -> timestamp annotations.
        :type annotations: dict
        :param tags: Optional dict of str -> str span tags.
        :type tags: dict
        :param kind: Span type (client, server, local, etc...)
        :type kind: Kind
        :param local_endpoint: The host that recorded this span.
        :type local_endpoint: Endpoint
        :param service_name: The name of the called service
        :type service_name: str
        :param sa_endpoint: Remote server in client spans.
        :type sa_endpoint: Endpoint
        :param report_timestamp: Whether the span should report
            timestamp and duration.
        :type report_timestamp: bool
        """
        self.trace_id = trace_id
        self.name = name
        self.parent_id = parent_id
        self.span_id = span_id
        self.kind = kind
        self.timestamp = timestamp
        self.duration = duration
        self.annotations = annotations
        self.tags = tags
        self.local_endpoint = local_endpoint
        self.service_name = service_name
        self.sa_endpoint = sa_endpoint
        self.report_timestamp = report_timestamp

        if not isinstance(kind, Kind):
            raise ZipkinError(
                'Invalid kind value {}. Must be of type Kind.'.format(kind))

    def build_v1_span(self):
        """Builds and returns a V1 Span.

        :return: newly generated _V1Span
        :rtype: _V1Span
        """
        # We are simulating a full two-part span locally, so set cs=sr and ss=cr
        full_annotations = OrderedDict([
            ('cs', self.timestamp),
            ('sr', self.timestamp),
            ('ss', self.timestamp + self.duration),
            ('cr', self.timestamp + self.duration),
        ])

        if self.kind != Kind.LOCAL:
            # If kind is not LOCAL, then we only want client or
            # server side annotations.
            for ann in _DROP_ANNOTATIONS_BY_KIND[self.kind]:
                del full_annotations[ann]

        # Add user-defined annotations. We write them in full_annotations
        # instead of the opposite so that user annotations will override
        # any automatically generated annotation.
        full_annotations.update(self.annotations)

        return _V1Span(
            trace_id=self.trace_id,
            name=self.name,
            parent_id=self.parent_id,
            id=self.span_id,
            timestamp=self.timestamp if self.report_timestamp else None,
            duration=self.duration if self.report_timestamp else None,
            endpoint=self.local_endpoint,
            annotations=full_annotations,
            binary_annotations=self.tags,
            sa_endpoint=self.sa_endpoint,
        )

    def build_v2_span(self):
        """Builds and returns a V2 Span.

        :return: newly generated _V2Span
        :rtype: _V2Span
        """
        remote_endpoint = None
        if self.sa_endpoint:
            remote_endpoint = self.sa_endpoint

        return _V2Span(
            trace_id=self.trace_id,
            name=self.name,
            parent_id=self.parent_id,
            id=self.span_id,
            kind=self.kind,
            timestamp=self.timestamp if self.report_timestamp else None,
            duration=self.duration if self.report_timestamp else None,
            debug=False,
            shared=False,
            local_endpoint=self.local_endpoint,
            remote_endpoint=remote_endpoint,
            annotations=self.annotations,
            tags=self.tags,
        )


def create_endpoint(port=0, service_name='unknown', host=None):
    """Creates a new Endpoint object.

    :param port: TCP/UDP port. Defaults to 0.
    :type port: int
    :param service_name: service name as a str. Defaults to 'unknown'.
    :type service_name: str
    :param host: ipv4 or ipv6 address of the host. Defaults to the
    current host ip.
    :type host: str
    :returns: zipkin Endpoint object
    """
    if host is None:
        try:
            host = socket.gethostbyname(socket.gethostname())
        except socket.gaierror:
            host = '127.0.0.1'

    ipv4 = None
    ipv6 = None

    # Check ipv4 or ipv6.
    try:
        socket.inet_pton(socket.AF_INET, host)
        ipv4 = host
    except socket.error:
        # If it's not an ipv4 address, maybe it's ipv6.
        try:
            socket.inet_pton(socket.AF_INET6, host)
            ipv6 = host
        except socket.error:
            # If it's neither ipv4 or ipv6, leave both ip addresses unset.
            pass

    return Endpoint(
        ipv4=ipv4,
        ipv6=ipv6,
        port=port,
        service_name=service_name,
    )


def copy_endpoint_with_new_service_name(endpoint, new_service_name):
    """Creates a copy of a given endpoint with a new service name.

    :param endpoint: existing Endpoint object
    :type endpoint: Endpoint
    :param new_service_name: new service name
    :type new_service_name: str
    :returns: zipkin new Endpoint object
    """
    return Endpoint(
        service_name=new_service_name,
        ipv4=endpoint.ipv4,
        ipv6=endpoint.ipv6,
        port=endpoint.port,
    )


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
    raise ZipkinError('Unknown encoding: {}'.format(encoding))


class IEncoder(object):
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

    def encode_span(self, span_builder):
        """Encodes a single span.

        :param span_builder: span_builder object representing the span.
        :type span_builder: SpanBuilder
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

    def encode_span(self, span_builder):
        """Encodes the current span to thrift."""
        span = span_builder.build_v1_span()

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

        # Add sa binary annotation
        if span.sa_endpoint is not None:
            thrift_sa_endpoint = thrift.create_endpoint(
                span.sa_endpoint.port,
                span.sa_endpoint.service_name,
                span.sa_endpoint.ipv4,
                span.sa_endpoint.ipv6,
            )
            thrift_binary_annotations.append(thrift.create_binary_annotation(
                key=thrift.zipkin_core.SERVER_ADDR,
                value=thrift.SERVER_ADDR_VAL,
                annotation_type=thrift.zipkin_core.AnnotationType.BOOL,
                host=thrift_sa_endpoint,
            ))

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
    """ V1 and V2 JSON encoders need many common helper functions """

    def fits(self, current_count, current_size, max_size, new_span):
        """Checks if the new span fits in the max payload size.

        Json lists only have a 2 bytes overhead from '[]' plus 1 byte from
        ',' between elements
        """
        return 2 + current_count + current_size + len(new_span) <= max_size

    def _create_json_endpoint(self, endpoint):
        """Converts an Endpoint to a JSON endpoint dict.

        :param endpoint: endpoint object to convert.
        :type endpoint: Endpoint
        :return: dict representing a JSON endpoint.
        :rtype: dict
        """
        json_endpoint = {
            'serviceName': endpoint.service_name,
            'port': endpoint.port,
        }
        if endpoint.ipv4 is not None:
            json_endpoint['ipv4'] = endpoint.ipv4
        if endpoint.ipv6 is not None:
            json_endpoint['ipv6'] = endpoint.ipv6

        return json_endpoint

    def encode_queue(self, queue):
        """Concatenates the list to a JSON list"""
        return '[' + ','.join(queue) + ']'


class _V1JSONEncoder(_BaseJSONEncoder):
    """JSON encoder for V1 spans."""

    def encode_span(self, span_builder):
        """Encodes a single span to JSON."""
        span = span_builder.build_v1_span()

        json_span = {
            'traceId': span.trace_id,
            'name': span.name,
            'id': span.id,
            'annotations': [],
            'binaryAnnotations': [],
        }

        if span.parent_id:
            json_span['parentId'] = span.parent_id
        if span.timestamp:
            json_span['timestamp'] = int(span.timestamp * 1000000)
        if span.duration:
            json_span['duration'] = int(span.duration * 1000000)

        v1_endpoint = self._create_json_endpoint(span.endpoint)

        for key, timestamp in span.annotations.items():
            json_span['annotations'].append({
                'endpoint': v1_endpoint,
                'timestamp': int(timestamp * 1000000),
                'value': key,
            })

        for key, value in span.binary_annotations.items():
            json_span['binaryAnnotations'].append({
                'key': key,
                'value': value,
                'endpoint': v1_endpoint,
            })

        # Add sa binary annotations
        if span.sa_endpoint is not None:
            json_sa_endpoint = self._create_json_endpoint(span.sa_endpoint)
            json_span['binaryAnnotations'].append({
                'key': 'sa',
                'value': '1',
                'endpoint': json_sa_endpoint,
            })

        encoded_span = json.dumps(json_span)

        return encoded_span


class _V2JSONEncoder(_BaseJSONEncoder):
    """JSON encoder for V2 spans."""

    def encode_span(self, span_builder):
        """Encodes a single span to JSON."""
        span = span_builder.build_v2_span()

        json_span = {
            'traceId': span.trace_id,
            'name': span.name,
            'id': span.id,
        }

        if span.parent_id:
            json_span['parentId'] = span.parent_id
        if span.timestamp:
            json_span['timestamp'] = int(span.timestamp * 1000000)
        if span.duration:
            json_span['duration'] = int(span.duration * 1000000)
        if span.kind and span.kind.value is not None:
            json_span['kind'] = span.kind.value
        if span.local_endpoint:
            json_span['localEndpoint'] = self._create_json_endpoint(
                span.local_endpoint,
            )
        if span.remote_endpoint:
            json_span['remoteEndpoint'] = self._create_json_endpoint(
                span.remote_endpoint,
            )
        if span.tags and len(span.tags) > 0:
            json_span['tags'] = span.tags

        if span.annotations:
            json_span['annotations'] = [
                {
                    'timestamp': int(timestamp * 1000000),
                    'value': key,
                }
                for key, timestamp in span.annotations.items()
            ]

        encoded_span = json.dumps(json_span)

        return encoded_span
