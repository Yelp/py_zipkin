import mock

from py_zipkin.encoding import protobuf
from py_zipkin.encoding._helpers import create_endpoint
from py_zipkin.encoding._helpers import Span
from py_zipkin.encoding._types import Kind
from py_zipkin.encoding.protobuf import zipkin_pb2


def test_installed():
    with mock.patch.object(protobuf, "zipkin_pb2", None):
        assert protobuf.installed() is False

    assert protobuf.installed() is True


@mock.patch("py_zipkin.encoding.protobuf.zipkin_pb2.ListOfSpans")
def test_encode_pb_list(mock_list):
    protobuf.encode_pb_list([])

    assert mock_list.call_count == 1
    assert mock_list.return_value.spans.extend.call_count == 1
    assert mock_list.return_value.SerializeToString.call_count == 1


def test_create_protobuf_span():
    span = Span(
        trace_id="1",
        name="name",
        parent_id="2",
        span_id="3",
        kind=Kind.CLIENT,
        timestamp=10,
        duration=10,
        local_endpoint=create_endpoint(service_name="service1", use_defaults=False),
        remote_endpoint=create_endpoint(service_name="service2", use_defaults=False),
        debug=True,
        shared=True,
        annotations={"foo": 1},
        tags={"key": "value"},
    )

    pb_span = protobuf.create_protobuf_span(span)

    assert pb_span == zipkin_pb2.Span(
        trace_id=b"\000\000\000\000\000\000\000\001",
        parent_id=b"\000\000\000\000\000\000\000\002",
        id=b"\000\000\000\000\000\000\000\003",
        kind=zipkin_pb2.Span.CLIENT,
        name="name",
        timestamp=10000000,
        duration=10000000,
        local_endpoint=zipkin_pb2.Endpoint(service_name="service1"),
        remote_endpoint=zipkin_pb2.Endpoint(service_name="service2"),
        debug=True,
        shared=True,
        annotations=[zipkin_pb2.Annotation(timestamp=1000000, value="foo")],
        tags={"key": "value"},
    )


def test_hex_to_bytes():
    assert protobuf._hex_to_bytes("6e611a263bd498a") == b"\x06\xe6\x11\xa2c\xbdI\x8a"
    assert (
        protobuf._hex_to_bytes("5c72e322656b5346e611a263bd498a")
        == b'\x00\\r\xe3"ekSF\xe6\x11\xa2c\xbdI\x8a'
    )
    assert (
        protobuf._hex_to_bytes("325c72e322656b5346e611a263bd498a")
        == b'2\\r\xe3"ekSF\xe6\x11\xa2c\xbdI\x8a'
    )


def test_get_protobuf_kind():
    assert protobuf._get_protobuf_kind(Kind.CLIENT) == zipkin_pb2.Span.CLIENT
    assert protobuf._get_protobuf_kind(Kind.SERVER) == zipkin_pb2.Span.SERVER
    assert protobuf._get_protobuf_kind(Kind.PRODUCER) == zipkin_pb2.Span.PRODUCER
    assert protobuf._get_protobuf_kind(Kind.CONSUMER) == zipkin_pb2.Span.CONSUMER
    assert protobuf._get_protobuf_kind(Kind.LOCAL) is None


def test_convert_endpoint():
    endpoint = create_endpoint(8888, "service1", "127.0.0.1")
    assert protobuf._convert_endpoint(endpoint) == zipkin_pb2.Endpoint(
        service_name="service1", port=8888, ipv4=b"\177\000\000\001", ipv6=None,
    )

    ipv6_endpoint = create_endpoint(0, "service1", "fe80::1ff:fe23:4567:890")
    assert protobuf._convert_endpoint(ipv6_endpoint) == zipkin_pb2.Endpoint(
        service_name="service1",
        ipv4=None,
        ipv6=b"\376\200\000\000\000\000\000\000\001\377\376#Eg\010\220",
    )


def test_convert_annotations():
    annotations = protobuf._convert_annotations({"foo": 123.456, "bar": 456.789})
    # The annotations dict is unordered in python < 3.6 so we need to sort the
    # output list to avoid making this test flaky.
    assert sorted(annotations, key=lambda a: a.timestamp) == [
        zipkin_pb2.Annotation(timestamp=123456000, value="foo"),
        zipkin_pb2.Annotation(timestamp=456789000, value="bar"),
    ]
