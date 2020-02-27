# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json
from collections import OrderedDict

import mock
import pytest
from thriftpy2.protocol.binary import read_list_begin
from thriftpy2.protocol.binary import TBinaryProtocol
from thriftpy2.transport import TMemoryBuffer

from py_zipkin import Encoding
from py_zipkin import Kind
from py_zipkin import thrift
from py_zipkin import zipkin
from py_zipkin.thrift import zipkin_core
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.zipkin import ZipkinAttrs
from tests.test_helpers import MockTransportHandler


def _decode_binary_thrift_objs(obj):
    spans = []
    trans = TMemoryBuffer(obj)
    _, size = read_list_begin(trans)
    for _ in range(size):
        span = zipkin_core.Span()
        span.read(TBinaryProtocol(trans))
        spans.append(span)
    return spans


def us(seconds):
    return int(seconds * 1000 * 1000)


def check_v1_json(obj, zipkin_attrs, inner_span_id, ts):
    inner_span, root_span = json.loads(obj)

    endpoint = {
        "ipv4": "10.0.0.0",
        "port": 8080,
        "serviceName": "test_service_name",
    }
    assert root_span == {
        "traceId": zipkin_attrs.trace_id,
        "parentId": zipkin_attrs.parent_span_id,
        "name": "test_span_name",
        "id": zipkin_attrs.span_id,
        "binaryAnnotations": [
            {"endpoint": endpoint, "key": "some_key", "value": "some_value"},
            {
                "endpoint": {
                    "ipv6": "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
                    "port": 8888,
                    "serviceName": "sa_service",
                },
                "key": "sa",
                "value": True,
            },
        ],
        "annotations": [
            {"endpoint": endpoint, "timestamp": us(ts), "value": "cs"},
            {"endpoint": endpoint, "timestamp": us(ts + 10), "value": "cr"},
        ],
    }

    assert inner_span == {
        "traceId": zipkin_attrs.trace_id,
        "parentId": zipkin_attrs.span_id,
        "name": "inner_span",
        "id": inner_span_id,
        "timestamp": us(ts),
        "duration": us(5),
        "binaryAnnotations": [],
        "annotations": [
            {"endpoint": endpoint, "timestamp": us(ts), "value": "cs"},
            {"endpoint": endpoint, "timestamp": us(ts), "value": "sr"},
            {"endpoint": endpoint, "timestamp": us(ts + 5), "value": "ss"},
            {"endpoint": endpoint, "timestamp": us(ts + 5), "value": "cr"},
            {"endpoint": endpoint, "timestamp": us(ts), "value": "ws"},
        ],
    }


def check_v1_thrift(obj, zipkin_attrs, inner_span_id, ts):
    inner_span, root_span = _decode_binary_thrift_objs(obj)

    endpoint = thrift.create_endpoint(
        port=8080, service_name="test_service_name", ipv4="10.0.0.0",
    )
    binary_annotations = thrift.binary_annotation_list_builder(
        {"some_key": "some_value"}, endpoint,
    )
    binary_annotations.append(
        thrift.create_binary_annotation(
            "sa",
            "\x01",
            zipkin_core.AnnotationType.BOOL,
            thrift.create_endpoint(
                port=8888,
                service_name="sa_service",
                ipv6="2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            ),
        )
    )

    expected_root = thrift.create_span(
        span_id=zipkin_attrs.span_id,
        parent_span_id=zipkin_attrs.parent_span_id,
        trace_id=zipkin_attrs.trace_id,
        span_name="test_span_name",
        annotations=thrift.annotation_list_builder(
            OrderedDict([("cs", ts), ("cr", ts + 10)]), endpoint,
        ),
        binary_annotations=binary_annotations,
        timestamp_s=None,
        duration_s=None,
    )
    # py.test diffs of thrift Spans are pretty useless and hide many things
    # These prints would only appear on stdout if the test fails and help comparing
    # the 2 spans.
    print(root_span)
    print(expected_root)
    assert root_span == expected_root

    expected_inner = thrift.create_span(
        span_id=inner_span_id,
        parent_span_id=zipkin_attrs.span_id,
        trace_id=zipkin_attrs.trace_id,
        span_name="inner_span",
        annotations=thrift.annotation_list_builder(
            OrderedDict(
                [("cs", ts), ("sr", ts), ("ss", ts + 5), ("cr", ts + 5), ("ws", ts)]
            ),
            endpoint,
        ),
        binary_annotations=[],
        timestamp_s=ts,
        duration_s=5,
    )
    # py.test diffs of thrift Spans are pretty useless and hide many things
    # These prints would only appear on stdout if the test fails and help comparing
    # the 2 spans.
    print(inner_span)
    print(expected_inner)
    assert inner_span == expected_inner


def check_v2_json(obj, zipkin_attrs, inner_span_id, ts):
    inner_span, root_span = json.loads(obj)

    assert root_span == {
        "traceId": zipkin_attrs.trace_id,
        "name": "test_span_name",
        "parentId": zipkin_attrs.parent_span_id,
        "id": zipkin_attrs.span_id,
        "kind": "CLIENT",
        "timestamp": us(ts),
        "duration": us(10),
        "shared": True,
        "localEndpoint": {
            "ipv4": "10.0.0.0",
            "port": 8080,
            "serviceName": "test_service_name",
        },
        "remoteEndpoint": {
            "ipv6": "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            "port": 8888,
            "serviceName": "sa_service",
        },
        "tags": {"some_key": "some_value"},
    }

    assert inner_span == {
        "traceId": zipkin_attrs.trace_id,
        "name": "inner_span",
        "parentId": zipkin_attrs.span_id,
        "id": inner_span_id,
        "timestamp": us(ts),
        "duration": us(5),
        "localEndpoint": {
            "ipv4": "10.0.0.0",
            "port": 8080,
            "serviceName": "test_service_name",
        },
        "annotations": [{"timestamp": us(ts), "value": "ws"}],
    }


@pytest.mark.parametrize(
    "encoding,validate_fn",
    [
        (Encoding.V1_THRIFT, check_v1_thrift),
        (Encoding.V1_JSON, check_v1_json),
        (Encoding.V2_JSON, check_v2_json),
    ],
)
def test_encoding(encoding, validate_fn):
    zipkin_attrs = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=generate_random_64bit_string(),
        is_sampled=True,
        flags=None,
    )
    inner_span_id = generate_random_64bit_string()
    mock_transport_handler = MockTransportHandler(10000)
    # Let's hardcode the timestamp rather than call time.time() every time.
    # The issue with time.time() is that the convertion to int of the
    # returned float value * 1000000 is not precise and in the same test
    # sometimes returns N and sometimes N+1. This ts value doesn't have that
    # issue afaict, probably since it ends in zeros.
    ts = 1538544126.115900
    with mock.patch("time.time", autospec=True) as mock_time:
        # zipkin.py start, logging_helper.start, 3 x logging_helper.stop
        # I don't understand why logging_helper.stop would run 3 times, but
        # that's what I'm seeing in the test
        mock_time.side_effect = iter([ts, ts, ts + 10, ts + 10, ts + 10])
        with zipkin.zipkin_span(
            service_name="test_service_name",
            span_name="test_span_name",
            transport_handler=mock_transport_handler,
            binary_annotations={"some_key": "some_value"},
            encoding=encoding,
            zipkin_attrs=zipkin_attrs,
            host="10.0.0.0",
            port=8080,
            kind=Kind.CLIENT,
        ) as span:
            with mock.patch.object(
                zipkin, "generate_random_64bit_string", return_value=inner_span_id,
            ):
                with zipkin.zipkin_span(
                    service_name="test_service_name",
                    span_name="inner_span",
                    timestamp=ts,
                    duration=5,
                    annotations={"ws": ts},
                ):
                    span.add_sa_binary_annotation(
                        8888, "sa_service", "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
                    )

    output = mock_transport_handler.get_payloads()[0]
    validate_fn(output, zipkin_attrs, inner_span_id, ts)
