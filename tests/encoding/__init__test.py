# -*- coding: utf-8 -*-
import pytest

from py_zipkin import Encoding
from py_zipkin.encoding import convert_spans
from py_zipkin.encoding import detect_span_version_and_encoding
from py_zipkin.exception import ZipkinError
from tests.test_helpers import generate_list_of_spans


@pytest.mark.parametrize('encoding', [
    Encoding.V1_THRIFT,
    Encoding.V1_JSON,
    Encoding.V2_JSON,
    Encoding.V2_PROTO3,
])
def test_detect_span_version_and_encoding(encoding):
    spans, _, _, _ = generate_list_of_spans(encoding)
    old_type = type(spans)

    assert detect_span_version_and_encoding(spans) == encoding

    if encoding in [Encoding.V1_JSON, Encoding.V2_JSON]:
        assert type(spans) == old_type
        spans = spans.encode()
        assert detect_span_version_and_encoding(spans) == encoding


def test_detect_span_version_and_encoding_incomplete_message():
    with pytest.raises(ZipkinError):
        detect_span_version_and_encoding('[')


def test_detect_span_version_and_encoding_ambiguous_json():
    """JSON spans that don't have any v1 or v2 keyword default to V2"""
    assert detect_span_version_and_encoding(
        '[{"traceId": "aaa", "id": "bbb"}]',
    ) == Encoding.V2_JSON


def test_detect_span_version_and_encoding_unknown_encoding():
    with pytest.raises(ZipkinError):
        detect_span_version_and_encoding('foobar')


def test_convert_spans_thrift_to_v2_json():
    spans, _, _, _ = generate_list_of_spans(Encoding.V1_THRIFT)

    converted_spans = convert_spans(spans=spans, output_encoding=Encoding.V2_JSON)

    assert detect_span_version_and_encoding(converted_spans) == Encoding.V2_JSON


def test_convert_spans_v2_json_to_v2_json():
    spans, _, _, _ = generate_list_of_spans(Encoding.V2_JSON)

    converted_spans = convert_spans(spans=spans, output_encoding=Encoding.V2_JSON)

    assert detect_span_version_and_encoding(converted_spans) == Encoding.V2_JSON
