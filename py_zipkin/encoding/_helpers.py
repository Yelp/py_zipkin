# -*- coding: utf-8 -*-
import socket
from collections import namedtuple
from collections import OrderedDict

from py_zipkin.encoding._types import Kind
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
            timestamp=self.timestamp,
            duration=self.duration,
            debug=False,
            shared=self.report_timestamp is False,
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
