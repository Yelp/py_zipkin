import socket
from collections import namedtuple

Endpoint = namedtuple(
    'Endpoint',
    ['service_name', 'ipv4', 'ipv6', 'port'],
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
