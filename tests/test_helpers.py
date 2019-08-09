# -*- coding: utf-8 -*-
import time

import mock
import six

from py_zipkin import Kind
from py_zipkin import thrift
from py_zipkin import zipkin
from py_zipkin.encoding._encoders import IEncoder
from py_zipkin.storage import Tracer
from py_zipkin.testing import MockTransportHandler
from py_zipkin.thrift import zipkin_core
from py_zipkin.util import generate_random_128bit_string
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.zipkin import ZipkinAttrs


class MockEncoder(IEncoder):

    def __init__(self, fits=True, encoded_span='', encoded_queue=''):
        self.fits_bool = fits
        self.encode_span = mock.Mock(
            return_value=(encoded_span, len(encoded_span)),
        )
        self.encode_queue = mock.Mock(return_value=encoded_queue)

    def fits(self, current_count, current_size, max_size, new_span):
        assert isinstance(current_count, int)
        assert isinstance(current_size, int)
        assert isinstance(max_size, int)
        assert isinstance(new_span, six.string_types)

        return self.fits_bool


class MockTracer(Tracer):
    def get_context(self):
        return self._context_stack


def generate_list_of_spans(encoding):
    zipkin_attrs = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=generate_random_64bit_string(),
        is_sampled=True,
        flags=None,
    )
    inner_span_id = generate_random_64bit_string()
    transport_handler = MockTransportHandler()
    # Let's hardcode the timestamp rather than call time.time() every time.
    # The issue with time.time() is that the convertion to int of the
    # returned float value * 1000000 is not precise and in the same test
    # sometimes returns N and sometimes N+1. This ts value doesn't have that
    # issue afaict, probably since it ends in zeros.
    ts = 1538544126.115900
    with mock.patch('time.time', autospec=True) as mock_time:
        # zipkin.py start, logging_helper.start, 3 x logging_helper.stop
        # I don't understand why logging_helper.stop would run 3 times, but
        # that's what I'm seeing in the test
        mock_time.side_effect = iter([ts, ts, ts + 10, ts + 10, ts + 10])
        with zipkin.zipkin_span(
                service_name='test_service_name',
                span_name='test_span_name',
                transport_handler=transport_handler,
                binary_annotations={'some_key': 'some_value'},
                encoding=encoding,
                zipkin_attrs=zipkin_attrs,
                host='10.0.0.0',
                port=8080,
                kind=Kind.CLIENT,
        ) as span:
            with mock.patch.object(
                    zipkin,
                    'generate_random_64bit_string',
                    return_value=inner_span_id,
            ):
                with zipkin.zipkin_span(
                        service_name='test_service_name',
                        span_name='inner_span',
                        timestamp=ts,
                        duration=5,
                        annotations={'ws': ts},
                ):
                    span.add_sa_binary_annotation(
                        8888,
                        'sa_service',
                        '2001:0db8:85a3:0000:0000:8a2e:0370:7334',
                    )

    return transport_handler.get_payloads()[0], zipkin_attrs, inner_span_id, ts


def generate_single_thrift_span():
    trace_id = generate_random_128bit_string()
    span_id = generate_random_64bit_string()
    timestamp_s = round(time.time(), 3)
    duration_s = 2.0
    host = thrift.create_endpoint(port=8000, service_name='host')
    host.ipv4 = 2130706433
    span = thrift.create_span(
        span_id=span_id,
        parent_span_id=None,
        trace_id=trace_id,
        span_name='foo',
        annotations=[
            thrift.create_annotation(1472470996199000, "cs", host),
        ],
        binary_annotations=[
            thrift.create_binary_annotation(
                "key",
                "value",
                zipkin_core.AnnotationType.STRING,
                host,
            ),
        ],
        timestamp_s=timestamp_s,
        duration_s=duration_s,
    )

    return thrift.span_to_bytes(span)
