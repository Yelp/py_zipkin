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
     'annotations', 'binary_annotations', 'remote_endpoint'],
)


_DROP_ANNOTATIONS_BY_KIND = {
    Kind.CLIENT: {'ss', 'sr'},
    Kind.SERVER: {'cs', 'cr'},
}


class Span(object):
    """Internal V2 Span representation."""

    def __init__(
        self,
        trace_id,
        name,
        parent_id,
        span_id,
        kind,
        timestamp,
        duration,
        local_endpoint=None,
        remote_endpoint=None,
        debug=False,
        shared=False,
        annotations=None,
        tags=None,
    ):
        """Creates a new Span.

        :param trace_id: Trace id.
        :type trace_id: str
        :param name: Name of the span.
        :type name: str
        :param parent_id: Parent span id.
        :type parent_id: str
        :param span_id: Span id.
        :type span_id: str
        :param kind: Span type (client, server, local, etc...)
        :type kind: Kind
        :param timestamp: start timestamp in seconds.
        :type timestamp: float
        :param duration: span duration in seconds.
        :type duration: float
        :param local_endpoint: the host that recorded this span.
        :type local_endpoint: Endpoint
        :param remote_endpoint: the remote service.
        :type remote_endpoint: Endpoint
        :param debug: True is a request to store this span even if it
            overrides sampling policy.
        :type debug: bool
        :param shared: True if we are contributing to a span started by
            another tracer (ex on a different host).
        :type shared: bool
        :param annotations: Optional dict of str -> timestamp annotations.
        :type annotations: dict
        :param tags: Optional dict of str -> str span tags.
        :type tags: dict
        """
        self.trace_id = trace_id
        self.name = name
        self.parent_id = parent_id
        self.span_id = span_id
        self.kind = kind
        self.timestamp = timestamp
        self.duration = duration
        self.local_endpoint = local_endpoint
        self.remote_endpoint = remote_endpoint
        self.debug = debug
        self.shared = shared
        self.annotations = annotations or {}
        self.tags = tags or {}

        if not isinstance(kind, Kind):
            raise ZipkinError(
                'Invalid kind value {}. Must be of type Kind.'.format(kind))

        if local_endpoint and not isinstance(local_endpoint, Endpoint):
            raise ZipkinError(
                'Invalid local_endpoint value. Must be of type Endpoint.')

        if remote_endpoint and not isinstance(remote_endpoint, Endpoint):
            raise ZipkinError(
                'Invalid remote_endpoint value. Must be of type Endpoint.')

    def __eq__(self, other):  # pragma: no cover
        """Compare function to help assert span1 == span2 in py3"""
        return self.__dict__ == other.__dict__

    def __cmp__(self, other):  # pragma: no cover
        """Compare function to help assert span1 == span2 in py2"""
        return self.__dict__ == other.__dict__

    def __str__(self):  # pragma: no cover
        """Compare function to nicely print Span rather than just the pointer"""
        return str(self.__dict__)

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
            timestamp=self.timestamp if self.shared is False else None,
            duration=self.duration if self.shared is False else None,
            endpoint=self.local_endpoint,
            annotations=full_annotations,
            binary_annotations=self.tags,
            remote_endpoint=self.remote_endpoint,
        )


def create_endpoint(port=None, service_name=None, host=None, use_defaults=True):
    """Creates a new Endpoint object.

    :param port: TCP/UDP port. Defaults to 0.
    :type port: int
    :param service_name: service name as a str. Defaults to 'unknown'.
    :type service_name: str
    :param host: ipv4 or ipv6 address of the host. Defaults to the
    current host ip.
    :type host: str
    :param use_defaults: whether to use defaults.
    :type use_defaults: bool
    :returns: zipkin Endpoint object
    """
    if use_defaults:
        if port is None:
            port = 0
        if service_name is None:
            service_name = 'unknown'
        if host is None:
            try:
                host = socket.gethostbyname(socket.gethostname())
            except socket.gaierror:
                host = '127.0.0.1'

    ipv4 = None
    ipv6 = None

    if host:
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
