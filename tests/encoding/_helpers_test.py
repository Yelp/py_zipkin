import socket

import mock
import pytest

from py_zipkin.encoding._helpers import create_endpoint
from py_zipkin.encoding._helpers import Span
from py_zipkin.encoding._types import Kind
from py_zipkin.exception import ZipkinError
from py_zipkin.util import generate_random_64bit_string


def test_create_span_with_bad_kind():
    with pytest.raises(ZipkinError) as e:
        Span(
            trace_id=generate_random_64bit_string(),
            name='test span',
            parent_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            kind='client',
            timestamp=26.0,
            duration=4.0,
        )

    assert (
        'Invalid kind value client. Must be of type Kind.' in str(e.value)
    )


def test_create_span_with_bad_local_endpoint():
    with pytest.raises(ZipkinError) as e:
        Span(
            trace_id=generate_random_64bit_string(),
            name='test span',
            parent_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            kind=Kind.CLIENT,
            timestamp=26.0,
            duration=4.0,
            local_endpoint='my_service',
        )

    assert (
        'Invalid local_endpoint value. Must be of type Endpoint.' in str(e.value)
    )


def test_create_span_with_bad_remote_endpoint():
    with pytest.raises(ZipkinError) as e:
        Span(
            trace_id=generate_random_64bit_string(),
            name='test span',
            parent_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            kind=Kind.CLIENT,
            timestamp=26.0,
            duration=4.0,
            remote_endpoint='my_service',
        )

    assert (
        'Invalid remote_endpoint value. Must be of type Endpoint.' in str(e.value)
    )


@mock.patch('socket.gethostbyname', autospec=True)
def test_create_endpoint_defaults_service_name(gethostbyname):
    gethostbyname.return_value = '0.0.0.0'
    endpoint = create_endpoint(port=8080)

    assert endpoint.service_name == 'unknown'
    assert endpoint.port == 8080
    assert endpoint.ipv4 == '0.0.0.0'
    assert endpoint.ipv6 is None


@mock.patch('socket.gethostbyname', autospec=True)
def test_create_endpoint_correct_host_ip(gethostbyname):
    gethostbyname.return_value = '1.2.3.4'
    endpoint = create_endpoint(host='0.0.0.0')

    assert endpoint.service_name == 'unknown'
    assert endpoint.port == 0
    assert endpoint.ipv4 == '0.0.0.0'
    assert endpoint.ipv6 is None


@mock.patch('socket.gethostbyname', autospec=True)
def test_create_endpoint_defaults_localhost(gethostbyname):
    gethostbyname.side_effect = socket.gaierror

    endpoint = create_endpoint(
        port=8080,
        service_name='foo',
    )
    assert endpoint.service_name == 'foo'
    assert endpoint.port == 8080
    assert endpoint.ipv4 == '127.0.0.1'
    assert endpoint.ipv6 is None


def test_create_endpoint_ipv6():
    endpoint = create_endpoint(
        port=8080,
        service_name='foo',
        host='2001:0db8:85a3:0000:0000:8a2e:0370:7334',
    )
    assert endpoint.service_name == 'foo'
    assert endpoint.port == 8080
    assert endpoint.ipv4 is None
    assert endpoint.ipv6 == '2001:0db8:85a3:0000:0000:8a2e:0370:7334'


def test_malformed_host():
    endpoint = create_endpoint(
        port=8080,
        service_name='foo',
        host='80',
    )
    assert endpoint.service_name == 'foo'
    assert endpoint.port == 8080
    assert endpoint.ipv4 is None
    assert endpoint.ipv6 is None
