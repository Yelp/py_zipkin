import socket

import mock

from py_zipkin import encoding


@mock.patch('socket.gethostbyname', autospec=True)
def test_create_endpoint_defaults_service_name(gethostbyname):
    gethostbyname.return_value = '0.0.0.0'
    endpoint = encoding.create_endpoint(port=8080)

    assert endpoint.service_name == 'unknown'
    assert endpoint.port == 8080
    assert endpoint.ipv4 == '0.0.0.0'
    assert endpoint.ipv6 is None


@mock.patch('socket.gethostbyname', autospec=True)
def test_create_endpoint_correct_host_ip(gethostbyname):
    gethostbyname.return_value = '1.2.3.4'
    endpoint = encoding.create_endpoint(host='0.0.0.0')

    assert endpoint.service_name == 'unknown'
    assert endpoint.port == 0
    assert endpoint.ipv4 == '0.0.0.0'
    assert endpoint.ipv6 is None


@mock.patch('socket.gethostbyname', autospec=True)
def test_create_endpoint_defaults_localhost(gethostbyname):
    gethostbyname.side_effect = socket.gaierror

    endpoint = encoding.create_endpoint(
        port=8080,
        service_name='foo',
    )
    assert endpoint.service_name == 'foo'
    assert endpoint.port == 8080
    assert endpoint.ipv4 == '127.0.0.1'
    assert endpoint.ipv6 is None


def test_create_endpoint_ipv6():
    endpoint = encoding.create_endpoint(
        port=8080,
        service_name='foo',
        host='2001:0db8:85a3:0000:0000:8a2e:0370:7334',
    )
    assert endpoint.service_name == 'foo'
    assert endpoint.port == 8080
    assert endpoint.ipv4 is None
    assert endpoint.ipv6 == '2001:0db8:85a3:0000:0000:8a2e:0370:7334'
