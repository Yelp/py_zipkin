import json
import socket
from collections import namedtuple
from enum import Enum

from py_zipkin import thrift

Endpoint = namedtuple(
    'Endpoint',
    ['service_name', 'ipv4', 'ipv6', 'port'],
)


class Encoding(Enum):
    THRIFT = 1
    JSON = 2


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
    """
    if encoding == Encoding.THRIFT:
        return _V1ThriftEncoder()
    elif encoding == Encoding.JSON:
        return _V1JSONEncoder()
    else:
        raise ValueError('Unknown encoding')


class IEncoder(object):
    """Encoder interface."""

    def fits(self, queue, current_size, max_size, new_span):
        """Returns the overhead in bytes of a list.

        The overhead is measured as the difference between
        the sum of the size in bytes of all objects and the
        size of their list concatenation.

        i.e. In JSON is 2 since

        :param queue: queue
        :type queue: list
        :returns: (int) overhead in bytes of a list object.
        """
        raise NotImplementedError()

    def encode_span(self, *argv):
        raise NotImplementedError()

    def encode_queue(self, queue):
        raise NotImplementedError()


class _V1ThriftEncoder(IEncoder):
    """Thrift encoder for V1 spans."""

    def fits(self, queue, current_size, max_size, new_span):
        return thrift.LIST_HEADER_SIZE + current_size + len(new_span) <= max_size

    def encode_span(
        self,
        span_id,
        parent_span_id,
        trace_id,
        span_name,
        annotations,
        binary_annotations,
        timestamp_s,
        duration_s,
        endpoint,
        sa_endpoint,
    ):
        thrift_endpoint = thrift.create_endpoint(
            endpoint.port,
            endpoint.service_name,
            endpoint.ipv4,
            endpoint.ipv6,
        )

        thrift_annotations = thrift.annotation_list_builder(
            annotations,
            thrift_endpoint,
        )

        # Binary annotations can be set through debug messages or the
        # set_extra_binary_annotations registry setting.
        thrift_binary_annotations = thrift.binary_annotation_list_builder(
            binary_annotations,
            thrift_endpoint,
        )

        # Add sa binary annotation
        if sa_endpoint is not None:
            thrift_sa_endpoint = thrift.create_endpoint(
                sa_endpoint.port,
                sa_endpoint.service_name,
                sa_endpoint.ipv4,
                sa_endpoint.ipv6,
            )
            thrift_binary_annotations.append(thrift.create_binary_annotation(
                key=thrift.zipkin_core.SERVER_ADDR,
                value=thrift.SERVER_ADDR_VAL,
                annotation_type=thrift.zipkin_core.AnnotationType.BOOL,
                host=thrift_sa_endpoint,
            ))

        thrift_span = thrift.create_span(
            span_id,
            parent_span_id,
            trace_id,
            span_name,
            thrift_annotations,
            thrift_binary_annotations,
            timestamp_s,
            duration_s,
        )

        encoded_span = thrift.span_to_bytes(thrift_span)

        return encoded_span, len(encoded_span)

    def encode_queue(self, queue):
        return thrift.encode_bytes_list(queue)


class _V1JSONEncoder(IEncoder):
    """JSON encoder for V1 spans."""

    def fits(self, queue, current_size, max_size, new_span):
        # Json lists only have a 2 bytes overhead from '[]' plus 2 bytes from
        # ', ' between elements
        return 2 + (len(queue) - 1) * 2 + current_size + len(new_span) <= max_size

    def _create_v1_endpoint(self, endpoint):
        """Converts an Endpoint to a v1 endpoint dict.

        :param endpoint: endpoint object to convert.
        :type endpoint: Endpoint
        :returns: dict representing a V1 endpoint.
        """
        v1_endpoint = {
            'serviceName': endpoint.service_name,
            'port': endpoint.port,
        }
        if endpoint.ipv4 is not None:
            v1_endpoint['ipv4'] = endpoint.ipv4
        if endpoint.ipv6 is not None:
            v1_endpoint['ipv6'] = endpoint.ipv6

        return v1_endpoint

    def encode_span(
        self,
        span_id,
        parent_span_id,
        trace_id,
        span_name,
        annotations,
        binary_annotations,
        timestamp_s,
        duration_s,
        endpoint,
        sa_endpoint,
    ):

        span = {
            'traceId': trace_id,
            'name': span_name,
            'id': span_id,
            'debug': False,
            'annotations': [],
            'binaryAnnotations': [],
        }

        if parent_span_id:
            span['parentId'] = parent_span_id
        if timestamp_s:
            span['timestamp'] = int(timestamp_s * 1000000)
        if duration_s:
            span['duration'] = int(duration_s * 1000000)

        v1_endpoint = self._create_v1_endpoint(endpoint)

        for key, timestamp in annotations.items():
            span['annotations'].append({
                'endpoint': v1_endpoint,
                'timestamp': int(timestamp * 1000000),
                'value': key,
            })

        for key, value in binary_annotations.items():
            span['binaryAnnotations'].append({
                'key': key,
                'value': value,
                'endpoint': v1_endpoint,
            })

        # Add sa binary annotations
        if sa_endpoint is not None:
            json_sa_endpoint = self._create_v1_endpoint(sa_endpoint)
            span['binaryAnnotations'].append({
                'key': 'sa',
                'value': '1',
                'endpoint': json_sa_endpoint,
            })

        encoded_span = json.dumps(span)

        return encoded_span, len(encoded_span)

    def encode_queue(self, queue):
        return '[' + ', '.join(queue) + ']'
