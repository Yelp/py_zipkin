import socket
from collections import namedtuple

Endpoint = namedtuple(
    'Endpoint',
    ['service_name', 'ipv4', 'ipv6', 'port'],
)


def create_endpoint(port=0, service_name='unknown', host=None):
    if host is None:
        try:
            host = socket.gethostbyname(socket.gethostname())
        except socket.gaierror:
            host = '127.0.0.1'

    ipv4 = None
    ipv6 = None

    # Check ipv4 or ipv6
    try:
        socket.inet_pton(socket.AF_INET, host)
        ipv4 = host
    except socket.error:
        ipv6 = host

    return Endpoint(
        ipv4=ipv4,
        ipv6=ipv6,
        port=port,
        service_name=service_name,
    )


def copy_endpoint_with_new_service_name(endpoint, new_service_name):
    return Endpoint(
        service_name=new_service_name,
        ipv4=endpoint.ipv4,
        ipv6=endpoint.ipv6,
        port=endpoint.port,
    )
