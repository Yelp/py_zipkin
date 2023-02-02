from unittest import mock

import pytest

from py_zipkin.encoding import Encoding
from py_zipkin.transport import SimpleHTTPTransport
from py_zipkin.zipkin import zipkin_span
from tests.test_helpers import MockTransportHandler


class TestBaseTransportHandler:
    def test_call_calls_send(self):
        with mock.patch.object(MockTransportHandler, "send", autospec=True):
            transport = MockTransportHandler()
            transport("foobar")

            assert transport.send.call_count == 1
            assert transport.send.call_args == mock.call(transport, "foobar")


class TestSimpleHTTPTransport:
    def test_get_max_payload_bytes(self):
        transport = SimpleHTTPTransport("localhost", 9411)
        assert transport.get_max_payload_bytes() is None

    @pytest.mark.parametrize(
        "encoding,path,content_type",
        [
            (Encoding.V1_JSON, "/api/v1/spans", "application/json"),
            (Encoding.V2_JSON, "/api/v2/spans", "application/json"),
            (Encoding.V2_PROTO3, "/api/v2/spans", "application/x-protobuf"),
        ],
    )
    def test__get_path_content_type(self, encoding, path, content_type):
        transport = MockTransportHandler()
        with zipkin_span(
            service_name="my_service",
            span_name="home",
            sample_rate=100,
            transport_handler=transport,
            encoding=encoding,
        ):
            pass

        spans = transport.get_payloads()[0]
        http_transport = SimpleHTTPTransport("localhost", 9411)
        assert http_transport._get_path_content_type(spans) == (path, content_type)

    @mock.patch("py_zipkin.transport.urlopen", autospec=True)
    def test_send(self, mock_urlopen):
        transport = SimpleHTTPTransport("localhost", 9411)
        with zipkin_span(
            service_name="my_service",
            span_name="home",
            sample_rate=100,
            transport_handler=transport,
            encoding=Encoding.V2_JSON,
        ):
            pass

        assert mock_urlopen.call_count == 1
        request = mock_urlopen.call_args[0][0]
        assert request.get_full_url() == "http://localhost:9411/api/v2/spans"
        # I don't understand why, but Type gets lowercased to type by urllib
        # Header keys are case insensitive anyway, so it's not a big deal
        assert request.get_header("Content-type") == "application/json"
