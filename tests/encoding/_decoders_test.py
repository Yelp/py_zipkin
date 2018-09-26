# -*- coding: utf-8 -*-
from __future__ import absolute_import

import mock
import pytest

from py_zipkin import thrift
from py_zipkin.encoding._decoders import get_decoder
from py_zipkin.encoding._decoders import IDecoder
from py_zipkin.encoding._decoders import _V1ThriftDecoder
from py_zipkin.encoding._helpers import Endpoint
from py_zipkin.encoding._types import Encoding
from py_zipkin.encoding._types import Kind
from py_zipkin.exception import ZipkinError
from py_zipkin.thrift import create_binary_annotation
from py_zipkin.thrift import zipkin_core
from py_zipkin.util import generate_random_128bit_string
from py_zipkin.util import generate_random_64bit_string
from tests.test_helpers import generate_list_of_spans
from tests.test_helpers import generate_single_thrift_span

USEC = 1000 * 1000


@pytest.fixture
def thrift_endpoint():
    return thrift.create_endpoint(8888, 'test_service', '10.0.0.1', None)


def test_get_decoder():
    assert isinstance(get_decoder(Encoding.V1_THRIFT), _V1ThriftDecoder)
    with pytest.raises(NotImplementedError):
        get_decoder(Encoding.V1_JSON)
    with pytest.raises(NotImplementedError):
        get_decoder(Encoding.V2_JSON)
    with pytest.raises(ZipkinError):
        get_decoder(None)


def test_idecoder_throws_not_implemented_errors():
    encoder = IDecoder()
    with pytest.raises(NotImplementedError):
        encoder.decode_spans(b'[]')


class TestV1ThriftDecoder(object):

    def test_decode_spans_list(self):
        spans, _, _, _ = generate_list_of_spans(Encoding.V1_THRIFT)
        decoder = _V1ThriftDecoder()
        with mock.patch.object(decoder, '_decode_thrift_span') as mock_decode:
            decoder.decode_spans(spans)
            assert mock_decode.call_count == 2

    def test_decode_old_style_thrift_span(self):
        """Test it can handle single thrift spans (not a list with 1 span).

        Years ago you'd just thrift encode spans one by one and then write them to
        the transport singularly. The zipkin kafka consumer still supports this.
        Let's make sure we properly detect this case and don't just assume that
        it's a thrift list.
        """
        span = generate_single_thrift_span()
        decoder = _V1ThriftDecoder()
        with mock.patch.object(decoder, '_decode_thrift_span') as mock_decode:
            decoder.decode_spans(span)
            assert mock_decode.call_count == 1

    def test__convert_from_thrift_endpoint(self, thrift_endpoint):
        decoder = _V1ThriftDecoder()

        ipv4_endpoint = decoder._convert_from_thrift_endpoint(thrift_endpoint)
        assert ipv4_endpoint == Endpoint('test_service', '10.0.0.1', None, 8888)

        ipv6_thrift_endpoint = \
            thrift.create_endpoint(8888, 'test_service', None, '::1')
        ipv6_endpoint = decoder._convert_from_thrift_endpoint(ipv6_thrift_endpoint)
        assert ipv6_endpoint == Endpoint('test_service', None, '::1', 8888)

    def test__decode_thrift_annotations(self, thrift_endpoint):
        timestamp = 1.0
        decoder = _V1ThriftDecoder()
        thrift_annotations = thrift.annotation_list_builder(
            {
                'cs': timestamp,
                'cr': timestamp + 10,
                'my_annotation': timestamp + 15,
            },
            thrift_endpoint,
        )

        annotations, end, kind, ts, dur = decoder._decode_thrift_annotations(
            thrift_annotations,
        )
        assert annotations == {'my_annotation': 16.0}
        assert end == Endpoint('test_service', '10.0.0.1', None, 8888)
        assert kind == Kind.CLIENT
        assert ts == timestamp * USEC
        assert dur == 10 * USEC

    def test__decode_thrift_annotations_server_span(self, thrift_endpoint):
        timestamp = 1.0
        decoder = _V1ThriftDecoder()
        thrift_annotations = thrift.annotation_list_builder(
            {
                'sr': timestamp,
                'ss': timestamp + 10,
            },
            thrift_endpoint,
        )

        annotations, end, kind, ts, dur = decoder._decode_thrift_annotations(
            thrift_annotations,
        )
        assert annotations == {}
        assert end == Endpoint('test_service', '10.0.0.1', None, 8888)
        assert kind == Kind.SERVER
        assert ts == timestamp * USEC
        assert dur == 10 * USEC

    def test__decode_thrift_annotations_local_span(self, thrift_endpoint):
        timestamp = 1.0
        decoder = _V1ThriftDecoder()
        thrift_annotations = thrift.annotation_list_builder(
            {
                'cs': timestamp,
                'sr': timestamp,
                'ss': timestamp + 10,
                'cr': timestamp + 10,
            },
            thrift_endpoint,
        )

        annotations, end, kind, ts, dur = decoder._decode_thrift_annotations(
            thrift_annotations,
        )
        assert annotations == {}
        assert end == Endpoint('test_service', '10.0.0.1', None, 8888)
        assert kind == Kind.LOCAL
        # ts and dur are not computed for a local span since those always have
        # timestamp and duration set as span arguments.
        assert ts is None
        assert dur is None

    def test__convert_from_thrift_binary_annotations(self):
        decoder = _V1ThriftDecoder()
        local_host = thrift.create_endpoint(8888, 'test_service', '10.0.0.1', None)
        remote_host = thrift.create_endpoint(9999, 'rem_service', '10.0.0.2', None)
        ann_type = zipkin_core.AnnotationType
        thrift_binary_annotations = [
            create_binary_annotation('key1', True, ann_type.BOOL, local_host),
            create_binary_annotation('key2', 'val2', ann_type.STRING, local_host),
            create_binary_annotation('key3', False, ann_type.BOOL, local_host),
            create_binary_annotation('key4', b'04', ann_type.I16, local_host),
            create_binary_annotation('key5', b'0004', ann_type.I32, local_host),
            create_binary_annotation('sa', True, ann_type.BOOL, remote_host),
        ]

        tags, local_endpoint, remote_endpoint = \
            decoder._convert_from_thrift_binary_annotations(
                thrift_binary_annotations,
            )

        assert tags == {
            'key1': 'true',
            'key2': 'val2',
            'key3': 'false',
        }
        assert local_endpoint == Endpoint('test_service', '10.0.0.1', None, 8888)
        assert remote_endpoint == Endpoint('rem_service', '10.0.0.2', None, 9999)

    @pytest.mark.parametrize('trace_id_generator', [
        (generate_random_64bit_string),
        (generate_random_128bit_string),
    ])
    def test__convert_trace_id_to_string(self, trace_id_generator):
        decoder = _V1ThriftDecoder()
        trace_id = trace_id_generator()
        span = thrift.create_span(
            generate_random_64bit_string(),
            None,
            trace_id,
            'test_span',
            [],
            [],
            None,
            None,
        )
        assert decoder._convert_trace_id_to_string(
            span.trace_id,
            span.trace_id_high,
        ) == trace_id

    def test__convert_unsigned_long_to_lower_hex(self):
        decoder = _V1ThriftDecoder()
        span_id = generate_random_64bit_string()
        span = thrift.create_span(
            span_id,
            None,
            generate_random_64bit_string(),
            'test_span',
            [],
            [],
            None,
            None,
        )
        assert decoder._convert_unsigned_long_to_lower_hex(span.id) == span_id
