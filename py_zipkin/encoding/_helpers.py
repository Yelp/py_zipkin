import socket
from collections import OrderedDict
from typing import Dict
from typing import MutableMapping
from typing import NamedTuple
from typing import Optional

from py_zipkin.encoding._types import Kind
from py_zipkin.exception import ZipkinError


class Endpoint(NamedTuple):
    service_name: Optional[str]
    ipv4: Optional[str]
    ipv6: Optional[str]
    port: Optional[int]


class _V1Span(NamedTuple):
    trace_id: str
    name: Optional[str]
    parent_id: Optional[str]
    id: Optional[str]
    timestamp: Optional[float]
    duration: Optional[float]
    endpoint: Optional[Endpoint]
    annotations: MutableMapping[str, Optional[float]]
    binary_annotations: Dict[str, Optional[str]]
    remote_endpoint: Optional[Endpoint]


class Span:
    """Internal V2 Span representation."""

    def __init__(
        self,
        trace_id: str,
        name: Optional[str],
        parent_id: Optional[str],
        span_id: Optional[str],
        kind: Kind,
        timestamp: Optional[float],
        duration: Optional[float],
        local_endpoint: Optional[Endpoint] = None,
        remote_endpoint: Optional[Endpoint] = None,
        debug: bool = False,
        shared: bool = False,
        annotations: Optional[Dict[str, Optional[float]]] = None,
        tags: Optional[Dict[str, Optional[str]]] = None,
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
            raise ZipkinError(f"Invalid kind value {kind}. Must be of type Kind.")

        if local_endpoint and not isinstance(local_endpoint, Endpoint):
            raise ZipkinError("Invalid local_endpoint value. Must be of type Endpoint.")

        if remote_endpoint and not isinstance(remote_endpoint, Endpoint):
            raise ZipkinError(
                "Invalid remote_endpoint value. Must be of type Endpoint."
            )

    def __eq__(self, other: object) -> bool:  # pragma: no cover
        """Compare function to help assert span1 == span2 in py3"""
        return self.__dict__ == other.__dict__

    def __cmp__(self, other: "Span") -> int:  # pragma: no cover
        """Compare function to help assert span1 == span2 in py2"""
        return self.__dict__ == other.__dict__

    def __str__(self) -> str:  # pragma: no cover
        """Compare function to nicely print Span rather than just the pointer"""
        return str(self.__dict__)

    def build_v1_span(self) -> _V1Span:
        """Builds and returns a V1 Span.

        :return: newly generated _V1Span
        :rtype: _V1Span
        """
        annotations: MutableMapping[str, Optional[float]] = OrderedDict([])
        assert self.timestamp is not None
        if self.kind == Kind.CLIENT:
            assert self.duration is not None
            annotations["cs"] = self.timestamp
            annotations["cr"] = self.timestamp + self.duration
        elif self.kind == Kind.SERVER:
            assert self.duration is not None
            annotations["sr"] = self.timestamp
            annotations["ss"] = self.timestamp + self.duration
        elif self.kind == Kind.PRODUCER:
            annotations["ms"] = self.timestamp
        elif self.kind == Kind.CONSUMER:
            annotations["mr"] = self.timestamp

        # Add user-defined annotations. We write them in annotations
        # instead of the opposite so that user annotations will override
        # any automatically generated annotation.
        annotations.update(self.annotations)

        return _V1Span(
            trace_id=self.trace_id,
            name=self.name,
            parent_id=self.parent_id,
            id=self.span_id,
            timestamp=self.timestamp if self.shared is False else None,
            duration=self.duration if self.shared is False else None,
            endpoint=self.local_endpoint,
            annotations=annotations,
            binary_annotations=self.tags,
            remote_endpoint=self.remote_endpoint,
        )


def create_endpoint(
    port: Optional[int] = None,
    service_name: Optional[str] = None,
    host: Optional[str] = None,
    use_defaults: bool = True,
) -> Endpoint:
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
            service_name = "unknown"
        if host is None:
            try:
                host = socket.gethostbyname(socket.gethostname())
            except socket.gaierror:
                host = "127.0.0.1"

    ipv4 = None
    ipv6 = None

    if host:
        # Check ipv4 or ipv6.
        try:
            socket.inet_pton(socket.AF_INET, host)
            ipv4 = host
        except OSError:
            # If it's not an ipv4 address, maybe it's ipv6.
            try:
                socket.inet_pton(socket.AF_INET6, host)
                ipv6 = host
            except OSError:
                # If it's neither ipv4 or ipv6, leave both ip addresses unset.
                pass

    return Endpoint(ipv4=ipv4, ipv6=ipv6, port=port, service_name=service_name)


def copy_endpoint_with_new_service_name(
    endpoint: Endpoint,
    new_service_name: Optional[str],
) -> Endpoint:
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
