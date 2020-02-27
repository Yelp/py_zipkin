# -*- coding: utf-8 -*-
import mock
import pytest

from py_zipkin import thrift


def test_create_span():
    # Not much logic here so this is just a smoke test. The only
    # substantive thing is that hex IDs get converted to ints.
    span = thrift.create_span(
        span_id="0000000000000001",
        parent_span_id="0000000000000002",
        trace_id="000000000000000f",
        span_name="foo",
        annotations="ann",
        binary_annotations="binary_ann",
        timestamp_s=1485920381.2,
        duration_s=2.0,
    )
    assert span.id == 1
    assert span.parent_id == 2
    assert span.trace_id == 15
    assert span.name == "foo"
    assert span.annotations == "ann"
    assert span.binary_annotations == "binary_ann"
    assert span.timestamp == 1485920381.2 * 1000000
    assert span.duration == 2.0 * 1000000
    assert span.trace_id_high is None


def test_create_span_with_128_bit_trace_ids():
    span = thrift.create_span(
        span_id="0000000000000001",
        parent_span_id="0000000000000002",
        trace_id="000000000000000f000000000000000e",
        span_name="foo",
        annotations="ann",
        binary_annotations="binary_ann",
        timestamp_s=1485920381.2,
        duration_s=2.0,
    )
    assert span.trace_id == 14
    assert span.trace_id_high == 15


def test_create_span_fails_with_wrong_128_bit_trace_id_length():
    with pytest.raises(AssertionError):
        thrift.create_span(
            span_id="0000000000000001",
            parent_span_id="0000000000000002",
            trace_id="000000000000000f000000000000000",
            span_name="foo",
            annotations="ann",
            binary_annotations="binary_ann",
            timestamp_s=1485920381.2,
            duration_s=2.0,
        )

    with pytest.raises(AssertionError):
        thrift.create_span(
            span_id="0000000000000001",
            parent_span_id="0000000000000002",
            trace_id="000000000000000f000000000000000f0",
            span_name="foo",
            annotations="ann",
            binary_annotations="binary_ann",
            timestamp_s=1485920381.2,
            duration_s=2.0,
        )


@mock.patch("socket.gethostbyname", autospec=True)
def test_create_endpoint_creates_correct_endpoint(gethostbyname):
    gethostbyname.return_value = "0.0.0.0"
    endpoint = thrift.create_endpoint(port=8080, service_name="foo")
    assert endpoint.service_name == "foo"
    assert endpoint.port == 8080
    # An IP address of 0.0.0.0 unpacks to just 0
    assert endpoint.ipv4 == 0


def test_create_endpoint_ipv6():
    endpoint = thrift.create_endpoint(port=8080, service_name="foo", ipv6="::1")
    assert endpoint.service_name == "foo"
    assert endpoint.port == 8080
    assert endpoint.ipv4 == 0
    assert (
        endpoint.ipv6
        == b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01"
    )


@mock.patch("socket.gethostbyname", autospec=True)
def test_copy_endpoint_with_new_service_name(gethostbyname):
    gethostbyname.return_value = "0.0.0.0"
    endpoint = thrift.create_endpoint(port=8080, service_name="foo")
    new_endpoint = thrift.copy_endpoint_with_new_service_name(endpoint, "blargh")
    assert new_endpoint.port == 8080
    assert new_endpoint.service_name == "blargh"
    # An IP address of 0.0.0.0 unpacks to just 0
    assert endpoint.ipv4 == 0


def test_create_annotation():
    ann = thrift.create_annotation("foo", "bar", "baz")
    assert ("foo", "bar", "baz") == (ann.timestamp, ann.value, ann.host)


@mock.patch("py_zipkin.thrift.create_annotation", autospec=True)
def test_annotation_list_builder(ann_mock):
    ann_list = {"foo": 1, "bar": 2}
    thrift.annotation_list_builder(ann_list, "host")
    ann_mock.assert_any_call(1000000, "foo", "host")
    ann_mock.assert_any_call(2000000, "bar", "host")
    assert ann_mock.call_count == 2


@pytest.mark.parametrize(
    "value", [(b"binary", u"unicøde")],
)
def test_create_binary_annotation(value):
    bann = thrift.create_binary_annotation("foo", value, "baz", "bla")
    assert ("foo", value, "baz", "bla") == (
        bann.key,
        bann.value,
        bann.annotation_type,
        bann.host,
    )


@mock.patch("py_zipkin.thrift.create_binary_annotation", autospec=True)
def test_binary_annotation_list_builder(bann_mock):
    bann_list = {"key1": "val1", "key2": "val2"}
    thrift.binary_annotation_list_builder(bann_list, "host")
    bann_mock.assert_any_call("key1", "val1", 6, "host")
    bann_mock.assert_any_call("key2", "val2", 6, "host")
    assert bann_mock.call_count == 2


def test_binary_annotation_list_builder_with_nonstring_values():
    bann_list = {"test key": 5}
    banns = thrift.binary_annotation_list_builder(bann_list, "host")
    assert banns[0].key == "test key"
    assert banns[0].value == "5"
