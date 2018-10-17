# -*- coding: utf-8 -*-
import json
from collections import namedtuple

import pytest
from thriftpy.protocol.binary import read_list_begin
from thriftpy.protocol.binary import TBinaryProtocol
from thriftpy.transport import TMemoryBuffer

from py_zipkin import Encoding
from py_zipkin import Kind
from py_zipkin import storage
from py_zipkin import zipkin
from py_zipkin._encoding_helpers import Endpoint
from py_zipkin.logging_helper import LOGGING_END_KEY
from py_zipkin.thrift import zipkin_core
from py_zipkin.zipkin import ZipkinAttrs


USECS = 1000000


Annotation = namedtuple('Annotation', ['host', 'timestamp', 'value'])
BinaryAnnotation = namedtuple('BinaryAnnotation', ['key', 'value', 'host'])
V1Span = namedtuple('V1Span', [
    'trace_id',
    'name',
    'parent_id',
    'id',
    'timestamp',
    'duration',
    'debug',
    'annotations',
    'binary_annotations',
    'trace_id_high',
])


SUPPORTED_ENCODINGS = [
    Encoding.V1_THRIFT,
    Encoding.V1_JSON,
]


@pytest.fixture
def default_annotations():
    return {'ss', 'sr', LOGGING_END_KEY}


def mock_logger():
    mock_logs = []

    def mock_transport_handler(message):
        mock_logs.append(message)

    return mock_transport_handler, mock_logs


def decode(obj, encoding):
    if encoding == Encoding.V1_THRIFT:
        return _decode_binary_thrift_objs(obj)
    elif encoding == Encoding.V1_JSON:
        return _decode_json_v1_span(obj)
    else:
        raise ValueError("unknown encoding")


def _decode_binary_thrift_objs(obj):
    spans = []
    trans = TMemoryBuffer(obj)
    _, size = read_list_begin(trans)
    for _ in range(size):
        span = zipkin_core.Span()
        span.read(TBinaryProtocol(trans))
        spans.append(span)
    return spans


def _decode_json_v1_span(obj):
    json_spans = json.loads(obj)
    spans = []

    for json_span in json_spans:
        ann = json_span['annotations']
        new_ann = []
        for a in ann:
            new_ann.append(Annotation(
                Endpoint(
                    a['endpoint'].get('serviceName'),
                    a['endpoint'].get('ipv4'),
                    a['endpoint'].get('ipv6'),
                    a['endpoint'].get('port'),
                ),
                a['timestamp'],
                a['value'],
            ))

        old_bin = json_span['binaryAnnotations']
        new_bin = []
        for b in old_bin:
            new_bin.append(BinaryAnnotation(
                b['key'],
                b['value'],
                Endpoint(
                    b['endpoint'].get('serviceName'),
                    b['endpoint'].get('ipv4'),
                    b['endpoint'].get('ipv6'),
                    b['endpoint'].get('port'),
                ),
            ))

        spans.append(V1Span(
            json_span.get('traceId'),
            json_span.get('name'),
            json_span.get('parentId'),
            json_span.get('id'),
            json_span.get('timestamp'),
            json_span.get('duration'),
            json_span.get('debug'),
            new_ann,
            new_bin,
            None,
        ))

    return spans


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_starting_zipkin_trace_with_sampling_rate(
    encoding,
    default_annotations,
):
    mock_transport_handler, mock_logs = mock_logger()
    mock_firehose_handler, mock_firehose_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
        firehose_handler=mock_firehose_handler,
        encoding=encoding,
    ):
        pass

    def check_span(span):
        assert span.name == 'test_span_name'
        assert span.annotations[0].host.service_name == 'test_service_name'
        assert span.parent_id is None
        assert span.trace_id_high is None
        # timestamp and duration are microsecond conversions of time.time()
        assert span.timestamp is not None
        assert span.duration is not None
        assert span.binary_annotations[0].key == 'some_key'
        assert span.binary_annotations[0].value == 'some_value'
        assert set([ann.value for ann in span.annotations]) == default_annotations

    check_span(decode(mock_logs[0], encoding)[0])
    check_span(decode(mock_firehose_logs[0], encoding)[0])


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_starting_zipkin_trace_with_128bit_trace_id(encoding):
    mock_transport_handler, mock_logs = mock_logger()
    mock_firehose_handler, mock_firehose_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
        use_128bit_trace_id=True,
        firehose_handler=mock_firehose_handler,
        encoding=encoding,
    ):
        pass

    def check_span(span):
        assert span.trace_id is not None
        if encoding == Encoding.V1_THRIFT:
            assert span.trace_id_high is not None
        elif encoding == Encoding.V1_JSON:
            assert len(span.trace_id) == 32

    check_span(decode(mock_logs[0], encoding)[0])
    check_span(decode(mock_firehose_logs[0], encoding)[0])


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_span_inside_trace(encoding):
    mock_transport_handler, mock_logs = mock_logger()
    mock_firehose_handler, mock_firehose_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        firehose_handler=mock_firehose_handler,
        encoding=encoding,
    ):
        with zipkin.zipkin_span(
            service_name='nested_service',
            span_name='nested_span',
            annotations={'nested_annotation': 43},
            binary_annotations={'nested_key': 'nested_value'},
        ):
            pass

    def check_spans(spans):
        assert len(spans) == 2
        nested_span = spans[0]
        root_span = spans[1]
        assert nested_span.name == 'nested_span'
        assert nested_span.annotations[0].host.service_name == 'nested_service'
        assert nested_span.parent_id == root_span.id
        assert nested_span.binary_annotations[0].key == 'nested_key'
        assert nested_span.binary_annotations[0].value == 'nested_value'
        # Local nested spans report timestamp and duration
        assert nested_span.timestamp is not None
        assert nested_span.duration is not None
        assert len(nested_span.annotations) == 5
        assert set([ann.value for ann in nested_span.annotations]) == set([
            'ss', 'sr', 'cs', 'cr', 'nested_annotation'])
        for ann in nested_span.annotations:
            if ann.value == 'nested_annotation':
                assert ann.timestamp == 43 * USECS

    check_spans(decode(mock_logs[0], encoding))
    check_spans(decode(mock_firehose_logs[0], encoding))


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_annotation_override(encoding):
    """This is the same as above, but we override an annotation
    in the inner span
    """
    mock_transport_handler, mock_logs = mock_logger()
    mock_firehose_handler, mock_firehose_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        firehose_handler=mock_firehose_handler,
        encoding=encoding,
    ):
        with zipkin.zipkin_span(
            service_name='nested_service',
            span_name='nested_span',
            annotations={'nested_annotation': 43, 'cs': 100, 'cr': 300},
            binary_annotations={'nested_key': 'nested_value'},
        ):
            pass

    def check_spans(spans):
        nested_span = spans[0]
        root_span = spans[1]
        assert nested_span.name == 'nested_span'
        assert nested_span.annotations[0].host.service_name == 'nested_service'
        assert nested_span.parent_id == root_span.id
        assert nested_span.binary_annotations[0].key == 'nested_key'
        assert nested_span.binary_annotations[0].value == 'nested_value'
        # Local nested spans report timestamp and duration
        assert nested_span.timestamp == 100 * USECS
        assert nested_span.duration == 200 * USECS
        assert len(nested_span.annotations) == 5
        assert set([ann.value for ann in nested_span.annotations]) == set([
            'ss', 'sr', 'cs', 'cr', 'nested_annotation'])
        for ann in nested_span.annotations:
            if ann.value == 'nested_annotation':
                assert ann.timestamp == 43 * USECS
            elif ann.value == 'cs':
                assert ann.timestamp == 100 * USECS
            elif ann.value == 'cr':
                assert ann.timestamp == 300 * USECS

    check_spans(decode(mock_logs[0], encoding))
    check_spans(decode(mock_firehose_logs[0], encoding))


def test_sr_ss_annotation_override():
    """This is the same as above, but we override an annotation
    in the inner span
    """
    mock_transport_handler, mock_logs = mock_logger()
    mock_firehose_handler, mock_firehose_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        firehose_handler=mock_firehose_handler,
    ):
        with zipkin.zipkin_span(
            service_name='nested_service',
            span_name='nested_span',
            annotations={'nested_annotation': 43, 'sr': 100, 'ss': 300},
            binary_annotations={'nested_key': 'nested_value'},
            kind=Kind.SERVER,
        ):
            pass

    def check_spans(spans):
        nested_span = spans[0]
        root_span = spans[1]
        assert nested_span.name == 'nested_span'
        assert nested_span.annotations[0].host.service_name == 'nested_service'
        assert nested_span.parent_id == root_span.id
        assert nested_span.binary_annotations[0].key == 'nested_key'
        assert nested_span.binary_annotations[0].value == 'nested_value'
        # Local nested spans report timestamp and duration
        assert nested_span.timestamp == 100 * USECS
        assert nested_span.duration == 200 * USECS
        assert len(nested_span.annotations) == 3
        assert set([ann.value for ann in nested_span.annotations]) == set([
            'ss', 'sr', 'nested_annotation'])
        for ann in nested_span.annotations:
            if ann.value == 'nested_annotation':
                assert ann.timestamp == 43 * USECS
            elif ann.value == 'sr':
                assert ann.timestamp == 100 * USECS
            elif ann.value == 'ss':
                assert ann.timestamp == 300 * USECS
            else:
                raise ValueError('unexpected annotation {}'.format(ann))

    check_spans(_decode_binary_thrift_objs(mock_logs[0]))
    check_spans(_decode_binary_thrift_objs(mock_firehose_logs[0]))


def _verify_service_span(span, annotations):
    assert span.name == 'service_span'
    assert int(span.trace_id) == 0
    assert int(span.id) == 1
    assert span.annotations[0].host.service_name == 'test_service_name'
    assert span.annotations[0].host.port == 0
    assert int(span.parent_id) == 2
    assert span.binary_annotations[0].key == 'some_key'
    assert span.binary_annotations[0].value == 'some_value'
    assert set([ann.value for ann in span.annotations]) == annotations


def test_service_span(default_annotations):
    mock_transport_handler, mock_logs = mock_logger()
    mock_firehose_handler, mock_firehose_logs = mock_logger()
    zipkin_attrs = ZipkinAttrs(
        trace_id='0',
        span_id='1',
        parent_span_id='2',
        flags='0',
        is_sampled=True,
    )
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='service_span',
        zipkin_attrs=zipkin_attrs,
        transport_handler=mock_transport_handler,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
        firehose_handler=mock_firehose_handler,
        encoding=Encoding.V1_THRIFT,
    ):
        pass

    span = decode(mock_logs[0], Encoding.V1_THRIFT)[0]
    firehose_span = decode(mock_firehose_logs[0], Encoding.V1_THRIFT)[0]
    _verify_service_span(span, default_annotations)
    _verify_service_span(firehose_span, default_annotations)

    # Spans continued on the server don't log timestamp/duration, as it's
    # assumed the client part of the pair will log them.
    assert span.timestamp is None
    assert span.duration is None


def test_service_span_report_timestamp_override(default_annotations):
    mock_transport_handler, mock_logs = mock_logger()
    zipkin_attrs = ZipkinAttrs(
        trace_id='0',
        span_id='1',
        parent_span_id='2',
        flags='0',
        is_sampled=True,
    )
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='service_span',
        zipkin_attrs=zipkin_attrs,
        transport_handler=mock_transport_handler,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
        report_root_timestamp=True,
        encoding=Encoding.V1_THRIFT,
    ):
        pass

    span = decode(mock_logs[0], Encoding.V1_THRIFT)[0]
    _verify_service_span(span, default_annotations)
    assert span.timestamp is not None
    assert span.duration is not None


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_service_span_that_is_independently_sampled(
    encoding,
    default_annotations,
):
    mock_transport_handler, mock_logs = mock_logger()
    mock_firehose_handler, mock_firehose_logs = mock_logger()
    zipkin_attrs = ZipkinAttrs(
        trace_id='0',
        span_id='1',
        parent_span_id='2',
        flags='0',
        is_sampled=False,
    )
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='service_span',
        zipkin_attrs=zipkin_attrs,
        transport_handler=mock_transport_handler,
        port=45,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
        firehose_handler=mock_firehose_handler,
        encoding=encoding,
    ):
        pass

    def check_span(span):
        assert span.name == 'service_span'
        assert span.annotations[0].host.service_name == 'test_service_name'
        assert span.parent_id is None
        assert span.binary_annotations[0].key == 'some_key'
        assert span.binary_annotations[0].value == 'some_value'
        # Spans that are part of an unsampled trace which start their own sampling
        # should report timestamp/duration, as they're acting as root spans.
        assert span.timestamp is not None
        assert span.duration is not None
        assert set([ann.value for ann in span.annotations]) == default_annotations

    check_span(decode(mock_logs[0], encoding)[0])
    check_span(decode(mock_firehose_logs[0], encoding)[0])


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_zipkin_trace_with_no_sampling_no_firehose(encoding):
    mock_transport_handler, mock_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=None,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
        encoding=encoding,
    ):
        pass

    assert len(mock_logs) == 0


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_zipkin_trace_with_no_sampling_with_firehose(
    encoding,
    default_annotations
):
    mock_transport_handler, mock_logs = mock_logger()
    mock_firehose_handler, mock_firehose_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=None,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
        firehose_handler=mock_firehose_handler,
        encoding=encoding,
    ):
        pass

    def check_span(span):
        assert span.name == 'test_span_name'
        assert span.annotations[0].host.service_name == 'test_service_name'
        assert span.parent_id is None
        assert span.trace_id_high is None
        # timestamp and duration are microsecond conversions of time.time()
        assert span.timestamp is not None
        assert span.duration is not None
        assert span.binary_annotations[0].key == 'some_key'
        assert span.binary_annotations[0].value == 'some_value'
        assert set([ann.value for ann in span.annotations]) == default_annotations

    assert len(mock_logs) == 0
    check_span(decode(mock_firehose_logs[0], encoding)[0])


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_no_sampling_with_inner_span(encoding):
    mock_transport_handler, mock_logs = mock_logger()
    mock_firehose_handler, mock_firehose_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=None,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
        firehose_handler=mock_firehose_handler,
        encoding=encoding,
    ):
        with zipkin.zipkin_span(
            service_name='nested_service',
            span_name='nested_span',
            annotations={'nested_annotation': 43},
            binary_annotations={'nested_key': 'nested_value'},
        ):
            pass

        pass

    def check_spans(spans):
        nested_span = spans[0]
        root_span = spans[1]
        assert nested_span.name == 'nested_span'
        assert nested_span.annotations[0].host.service_name == 'nested_service'
        assert nested_span.parent_id == root_span.id
        assert nested_span.binary_annotations[0].key == 'nested_key'
        assert nested_span.binary_annotations[0].value == 'nested_value'
        # Local nested spans report timestamp and duration
        assert nested_span.timestamp is not None
        assert nested_span.duration is not None
        assert len(nested_span.annotations) == 5
        assert set([ann.value for ann in nested_span.annotations]) == set([
            'ss', 'sr', 'cs', 'cr', 'nested_annotation'])
        for ann in nested_span.annotations:
            if ann.value == 'nested_annotation':
                assert ann.timestamp == 43 * USECS

    assert len(mock_logs) == 0
    check_spans(decode(mock_firehose_logs[0], encoding))


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_client_span(encoding):
    mock_transport_handler, mock_logs = mock_logger()
    with zipkin.zipkin_client_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
        encoding=encoding,
    ):
        pass

    def check_spans(spans):
        client_span = spans[0]
        assert client_span.name == 'test_span_name'
        assert client_span.annotations[0].host.service_name == 'test_service_name'
        assert client_span.binary_annotations[0].key == 'some_key'
        assert client_span.binary_annotations[0].value == 'some_value'
        assert client_span.timestamp is not None
        assert client_span.duration is not None
        assert len(client_span.annotations) == 3
        assert set([ann.value for ann in client_span.annotations]) == {
            'cs', 'cr', 'py_zipkin.logging_end'}

    assert len(mock_logs) == 1
    check_spans(decode(mock_logs[0], encoding))


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_server_span(encoding):
    mock_transport_handler, mock_logs = mock_logger()
    with zipkin.zipkin_server_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
        encoding=encoding,
    ):
        with zipkin.zipkin_client_span(
            service_name='nested_service',
            span_name='nested_span',
            annotations={'nested_annotation': 43},
            binary_annotations={'nested_key': 'nested_value'},
        ):
            pass

        pass

    def check_spans(spans):
        client_span = spans[0]
        server_span = spans[1]
        assert server_span.name == 'test_span_name'
        assert server_span.annotations[0].host.service_name == 'test_service_name'
        assert server_span.binary_annotations[0].key == 'some_key'
        assert server_span.binary_annotations[0].value == 'some_value'
        # Local nested spans report timestamp and duration
        assert server_span.timestamp is not None
        assert server_span.duration is not None
        assert len(server_span.annotations) == 3
        assert set([ann.value for ann in server_span.annotations]) == {
            'ss', 'sr', 'py_zipkin.logging_end'}

        assert client_span.name == 'nested_span'
        assert client_span.annotations[0].host.service_name == 'nested_service'
        assert client_span.binary_annotations[0].key == 'nested_key'
        assert client_span.binary_annotations[0].value == 'nested_value'
        # Local nested spans report timestamp and duration
        assert client_span.timestamp is not None
        assert client_span.duration is not None
        assert len(client_span.annotations) == 3
        assert set([ann.value for ann in client_span.annotations]) == {
            'cs', 'cr', 'nested_annotation'}

    assert len(mock_logs) == 1
    check_spans(decode(mock_logs[0], encoding))


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_include_still_works(encoding):
    mock_transport_handler, mock_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
        encoding=encoding,
    ):
        with zipkin.zipkin_span(
            service_name='nested_service',
            span_name='nested_span',
            annotations={'nested_annotation': 43},
            binary_annotations={'nested_key': 'nested_value'},
            include={'client'},
        ):
            pass
        with zipkin.zipkin_span(
            service_name='nested_service',
            span_name='nested_span',
            annotations={'nested_annotation': 43},
            binary_annotations={'nested_key': 'nested_value'},
            include={'server'},
        ):
            pass
        with zipkin.zipkin_span(
            service_name='nested_service',
            span_name='nested_span',
            annotations={'nested_annotation': 43},
            binary_annotations={'nested_key': 'nested_value'},
            include={'client', 'server'},
        ):
            pass
        pass

    def check_spans(spans):
        client_span = spans[0]
        server_span = spans[1]
        local_span = spans[2]
        assert len(client_span.annotations) == 3
        assert set([ann.value for ann in client_span.annotations]) == {
            'cs', 'cr', 'nested_annotation'}
        assert len(server_span.annotations) == 3
        assert set([ann.value for ann in server_span.annotations]) == {
            'ss', 'sr', 'nested_annotation'}
        assert len(local_span.annotations) == 5
        assert set([ann.value for ann in local_span.annotations]) == {
            'cs', 'cr', 'ss', 'sr', 'nested_annotation'}

    assert len(mock_logs) == 1
    check_spans(decode(mock_logs[0], encoding))


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_can_set_sa_annotation(encoding):
    mock_transport_handler, mock_logs = mock_logger()
    with zipkin.zipkin_client_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
        encoding=encoding,
    ) as span:
        span.add_sa_binary_annotation(
            port=8888,
            service_name='sa_service',
            host='10.0.0.0',
        )

    assert len(mock_logs) == 1
    client_span = decode(mock_logs[0], encoding)[0]

    expected_sa_value = '1' if encoding == Encoding.V1_JSON else u'\x01'
    expected_ip = u'10.0.0.0' if encoding == Encoding.V1_JSON else 167772160

    assert set([ann.value for ann in client_span.annotations]) == {
        'cs', 'cr', 'py_zipkin.logging_end'}
    assert client_span.binary_annotations[0].key == 'some_key'
    assert client_span.binary_annotations[0].value == 'some_value'
    assert client_span.binary_annotations[1].key == 'sa'
    assert client_span.binary_annotations[1].value == expected_sa_value
    host = client_span.binary_annotations[1].host
    assert host.service_name == u'sa_service'
    assert host.ipv4 == expected_ip
    assert host.ipv6 is None
    assert host.port == 8888


def test_memory_leak():
    # In py_zipkin >= 0.13.0 and <= 0.14.0 this test fails since the
    # span_storage contains 10 spans once you exit the for loop.
    mock_transport_handler, mock_logs = mock_logger()
    assert len(storage.default_span_storage()) == 0
    for _ in range(10):
        with zipkin.zipkin_client_span(
            service_name='test_service_name',
            span_name='test_span_name',
            transport_handler=mock_transport_handler,
            sample_rate=0.0,
            binary_annotations={'some_key': 'some_value'},
            add_logging_annotation=True,
            encoding=Encoding.V1_JSON,
        ):
            with zipkin.zipkin_span(
                service_name='inner_service_name',
                span_name='inner_span_name',
            ):
                pass

    assert len(mock_logs) == 0
    assert len(storage.default_span_storage()) == 0
