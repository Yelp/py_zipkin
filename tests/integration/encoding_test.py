import json
from unittest import mock

import pytest

from py_zipkin import Encoding
from py_zipkin import Kind
from py_zipkin import zipkin
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.zipkin import ZipkinAttrs
from tests.test_helpers import MockTransportHandler


def us(seconds):
    return int(seconds * 1000 * 1000)


def check_v1_json(obj, zipkin_attrs, inner_span_id, ts):
    inner_span, producer_span, root_span = json.loads(obj)

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
        "annotations": [{"endpoint": endpoint, "timestamp": us(ts), "value": "ws"}],
    }

    assert producer_span == {
        "traceId": zipkin_attrs.trace_id,
        "parentId": zipkin_attrs.span_id,
        "name": "producer_span",
        "id": inner_span_id,
        "timestamp": us(ts),
        "duration": us(10),
        "binaryAnnotations": [],
        "annotations": [{"endpoint": endpoint, "timestamp": us(ts), "value": "ms"}],
    }


def check_v2_json(obj, zipkin_attrs, inner_span_id, ts):
    inner_span, producer_span, root_span = json.loads(obj)

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

    assert producer_span == {
        "traceId": zipkin_attrs.trace_id,
        "name": "producer_span",
        "parentId": zipkin_attrs.span_id,
        "id": inner_span_id,
        "kind": "PRODUCER",
        "timestamp": us(ts),
        "duration": us(10),
        "localEndpoint": {
            "ipv4": "10.0.0.0",
            "port": 8080,
            "serviceName": "test_service_name",
        },
    }


@pytest.mark.parametrize(
    "encoding,validate_fn",
    [
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
        mock_time.side_effect = iter(
            [ts, ts, ts + 10, ts + 10, ts + 10, ts + 10, ts + 10]
        )
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
                zipkin,
                "generate_random_64bit_string",
                return_value=inner_span_id,
            ):
                with zipkin.zipkin_span(
                    service_name="test_service_name",
                    span_name="inner_span",
                    timestamp=ts,
                    duration=5,
                    annotations={"ws": ts},
                ):
                    span.add_sa_binary_annotation(
                        8888,
                        "sa_service",
                        "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
                    )
                with zipkin.zipkin_span(
                    service_name="test_service_name",
                    span_name="producer_span",
                    timestamp=ts,
                    duration=10,
                    kind=Kind.PRODUCER,
                ):
                    pass

    output = mock_transport_handler.get_payloads()[0]
    validate_fn(output, zipkin_attrs, inner_span_id, ts)
