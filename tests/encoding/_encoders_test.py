import socket
from unittest import mock

import pytest

from py_zipkin import Encoding
from py_zipkin.encoding._encoders import _V1JSONEncoder
from py_zipkin.encoding._encoders import _V2JSONEncoder
from py_zipkin.encoding._encoders import _V2ProtobufEncoder
from py_zipkin.encoding._encoders import get_encoder
from py_zipkin.encoding._encoders import IEncoder
from py_zipkin.encoding._helpers import create_endpoint
from py_zipkin.encoding._helpers import Span
from py_zipkin.encoding._types import Kind
from py_zipkin.exception import ZipkinError


@mock.patch("socket.gethostbyname", autospec=True)
def test_create_endpoint_defaults_service_name(gethostbyname):
    gethostbyname.return_value = "0.0.0.0"
    endpoint = create_endpoint(port=8080)

    assert endpoint.service_name == "unknown"
    assert endpoint.port == 8080
    assert endpoint.ipv4 == "0.0.0.0"
    assert endpoint.ipv6 is None


@mock.patch("socket.gethostbyname", autospec=True)
def test_create_endpoint_correct_host_ip(gethostbyname):
    gethostbyname.return_value = "1.2.3.4"
    endpoint = create_endpoint(host="0.0.0.0")

    assert endpoint.service_name == "unknown"
    assert endpoint.port == 0
    assert endpoint.ipv4 == "0.0.0.0"
    assert endpoint.ipv6 is None


@mock.patch("socket.gethostbyname", autospec=True)
def test_create_endpoint_defaults_localhost(gethostbyname):
    gethostbyname.side_effect = socket.gaierror

    endpoint = create_endpoint(port=8080, service_name="foo")
    assert endpoint.service_name == "foo"
    assert endpoint.port == 8080
    assert endpoint.ipv4 == "127.0.0.1"
    assert endpoint.ipv6 is None


def test_create_endpoint_ipv6():
    endpoint = create_endpoint(
        port=8080,
        service_name="foo",
        host="2001:0db8:85a3:0000:0000:8a2e:0370:7334",
    )
    assert endpoint.service_name == "foo"
    assert endpoint.port == 8080
    assert endpoint.ipv4 is None
    assert endpoint.ipv6 == "2001:0db8:85a3:0000:0000:8a2e:0370:7334"


def test_malformed_host():
    endpoint = create_endpoint(port=8080, service_name="foo", host="80")
    assert endpoint.service_name == "foo"
    assert endpoint.port == 8080
    assert endpoint.ipv4 is None
    assert endpoint.ipv6 is None


def test_encoder():
    with pytest.raises(NotImplementedError):
        get_encoder(Encoding.V1_THRIFT)
    assert isinstance(get_encoder(Encoding.V1_JSON), _V1JSONEncoder)
    assert isinstance(get_encoder(Encoding.V2_JSON), _V2JSONEncoder)
    assert isinstance(get_encoder(Encoding.V2_PROTO3), _V2ProtobufEncoder)
    with pytest.raises(ZipkinError):
        get_encoder(None)


def test_iencoder_throws_not_implemented_errors():
    encoder = IEncoder()
    with pytest.raises(NotImplementedError):
        encoder.fits(0, 0, 0, "")
    with pytest.raises(NotImplementedError):
        encoder.encode_span(mock.ANY)
    with pytest.raises(NotImplementedError):
        encoder.encode_queue([])


class TestBaseJSONEncoder:
    @pytest.fixture
    def encoder(self):
        """Test encoder"""
        return get_encoder(Encoding.V1_JSON)

    def test_fits(self, encoder):
        # count=2, current_size = 30, max_size = 52, len(new_span) = 20
        # won't fit since we have the extra ', '
        assert encoder.fits(2, 30, 52, '{"trace_id": "1234"}') is False

        # with max_size = 56 it fits perfectly since there's space for the 2 ', '
        assert encoder.fits(2, 30, 56, '{"trace_id": "1234"}') is True

    def test_create_json_endpoint(self, encoder):
        ipv4_endpoint = create_endpoint(
            port=8888,
            service_name="test_service",
            host="127.0.0.1",
        )
        assert encoder._create_json_endpoint(ipv4_endpoint, False) == {
            "serviceName": "test_service",
            "port": 8888,
            "ipv4": "127.0.0.1",
        }

        ipv6_endpoint = create_endpoint(
            port=8888,
            service_name="test_service",
            host="2001:0db8:85a3:0000:0000:8a2e:0370:7334",
        )
        assert encoder._create_json_endpoint(ipv6_endpoint, False) == {
            "serviceName": "test_service",
            "port": 8888,
            "ipv6": "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
        }

        v1_endpoint = create_endpoint(
            port=8888,
            service_name=None,
            host="2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            use_defaults=False,
        )
        assert encoder._create_json_endpoint(v1_endpoint, True) == {
            "serviceName": "",
            "port": 8888,
            "ipv6": "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
        }

        v2_endpoint = create_endpoint(
            port=8888,
            service_name=None,
            host="2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            use_defaults=False,
        )
        assert encoder._create_json_endpoint(v2_endpoint, False) == {
            "port": 8888,
            "ipv6": "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
        }


class TestV1JSONEncoder:
    def test_remote_endpoint(self):
        encoder = get_encoder(Encoding.V1_JSON)
        remote_endpoint = create_endpoint(service_name="test_server", host="127.0.0.1")

        # For server spans, the remote endpoint is encoded as 'ca'
        binary_annotations = []
        encoder.encode_remote_endpoint(
            remote_endpoint,
            Kind.SERVER,
            binary_annotations,
        )
        assert binary_annotations == [
            {
                "endpoint": {"ipv4": "127.0.0.1", "serviceName": "test_server"},
                "key": "ca",
                "value": True,
            }
        ]

        # For client spans, the remote endpoint is encoded as 'sa'
        binary_annotations = []
        encoder.encode_remote_endpoint(
            remote_endpoint,
            Kind.CLIENT,
            binary_annotations,
        )
        assert binary_annotations == [
            {
                "endpoint": {"ipv4": "127.0.0.1", "serviceName": "test_server"},
                "key": "sa",
                "value": True,
            }
        ]


class TestV2ProtobufEncoder:
    @pytest.fixture
    def encoder(self):
        return get_encoder(Encoding.V2_PROTO3)

    def test_fits(self, encoder):
        span = Span("1", "name", "2", "3", Kind.CLIENT, 10, 10)
        pb_span = encoder.encode_span(span)
        span_len = len(pb_span)

        assert encoder.fits(None, 0, span_len * 2, pb_span) is True
        assert encoder.fits(None, span_len, span_len * 2, pb_span) is True
        assert encoder.fits(None, span_len + 1, span_len * 2, pb_span) is False

    @mock.patch("py_zipkin.encoding.protobuf.installed", autospec=True)
    def test_encode_span(self, mock_installed, encoder):
        span = Span("1", "name", "2", "3", Kind.CLIENT, 10, 10)

        mock_installed.return_value = False
        with pytest.raises(ZipkinError):
            encoder.encode_span(span)

        mock_installed.return_value = True
        pb_span = encoder.encode_span(span)
        assert isinstance(pb_span, bytes)

    def test_encode_queue(self, encoder):
        span = Span("1", "name", "2", "3", Kind.CLIENT, 10, 10)
        pb_span = encoder.encode_span(span)

        pb_list = encoder.encode_queue([pb_span])
        assert isinstance(pb_list, bytes)
