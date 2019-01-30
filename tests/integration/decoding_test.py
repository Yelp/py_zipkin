# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json

from py_zipkin import Encoding
from py_zipkin.encoding import convert_spans
from tests.test_helpers import generate_list_of_spans


def us(seconds):
    return int(seconds * 1000 * 1000)


def test_encoding():
    thrift_spans, zipkin_attrs, inner_span_id, ts = \
        generate_list_of_spans(Encoding.V1_THRIFT)

    json_spans = convert_spans(thrift_spans, Encoding.V2_JSON)

    inner_span, root_span = json.loads(json_spans)

    assert root_span == {
        'traceId': zipkin_attrs.trace_id,
        'name': 'test_span_name',
        'parentId': zipkin_attrs.parent_span_id,
        'id': zipkin_attrs.span_id,
        'timestamp': us(ts),
        'duration': us(10),
        'kind': 'CLIENT',
        'localEndpoint': {
            'ipv4': '10.0.0.0',
            'port': 8080,
            'serviceName': 'test_service_name',
        },
        'remoteEndpoint': {
            'ipv6': '2001:db8:85a3::8a2e:370:7334',
            'port': 8888,
            'serviceName': 'sa_service',
        },
        'tags': {'some_key': 'some_value'},
    }

    assert inner_span == {
        'traceId': zipkin_attrs.trace_id,
        'name': 'inner_span',
        'parentId': zipkin_attrs.span_id,
        'id': inner_span_id,
        'timestamp': us(ts),
        'duration': us(5),
        'localEndpoint': {
            'ipv4': '10.0.0.0',
            'port': 8080,
            'serviceName': 'test_service_name',
        },
        'annotations': [{'timestamp': us(ts), 'value': 'ws'}],
    }
