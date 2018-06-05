import json
from collections import namedtuple

import pytest
from thriftpy.protocol.binary import read_list_begin
from thriftpy.protocol.binary import TBinaryProtocol
from thriftpy.transport import TMemoryBuffer

import py_zipkin
from py_zipkin import zipkin
from py_zipkin._encoding_helpers import Endpoint
from py_zipkin.logging_helper import LOGGING_END_KEY
from py_zipkin.logging_helper import zipkin_logger
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
    py_zipkin.Encoding.THRIFT,
    py_zipkin.Encoding.JSON,
]


@pytest.fixture
def default_annotations():
    return set([
        'ss', 'sr', LOGGING_END_KEY,
    ])


def mock_logger():
    mock_logs = []

    def mock_transport_handler(message):
        mock_logs.append(message)

    return mock_transport_handler, mock_logs


def decode(obj, encoding):
    if encoding == py_zipkin.Encoding.THRIFT:
        return _decode_binary_thrift_objs(obj)
    elif encoding == py_zipkin.Encoding.JSON:
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
        if encoding == py_zipkin.Encoding.THRIFT:
            assert span.trace_id_high is not None
        elif encoding == py_zipkin.Encoding.JSON:
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


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_service_span(encoding, default_annotations):
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
        encoding=encoding,
    ):
        pass

    span = decode(mock_logs[0], encoding)[0]
    firehose_span = decode(mock_firehose_logs[0], encoding)[0]
    _verify_service_span(span, default_annotations)
    _verify_service_span(firehose_span, default_annotations)

    # Spans continued on the server don't log timestamp/duration, as it's
    # assumed the client part of the pair will log them.
    assert span.timestamp is None
    assert span.duration is None


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_service_span_report_timestamp_override(
    encoding,
    default_annotations,
):
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
        encoding=encoding,
    ):
        pass

    span = decode(mock_logs[0], encoding)[0]
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
def test_log_debug_for_new_span(encoding):
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
        zipkin_logger.debug({
            'annotations': {
                'cs': 7,
                'cr': 8,
            },
            'binary_annotations': {
                'logged_binary_annotation': 'logged_value',
            },
            'name': 'logged_name',
            'service_name': 'logged_service_name',
        })
        pass

    def check_spans(spans):
        logged_span = spans[0]
        root_span = spans[1]
        assert logged_span.name == 'logged_name'
        assert logged_span.annotations[0] \
                          .host.service_name == 'logged_service_name'
        assert logged_span.parent_id == root_span.id
        assert logged_span.binary_annotations[0].key == 'logged_binary_annotation'
        assert logged_span.binary_annotations[0].value == 'logged_value'
        assert set(
            [ann.value for ann in logged_span.annotations]
        ) == set(['cs', 'cr'])

    check_spans(decode(mock_logs[0], encoding))
    check_spans(decode(mock_firehose_logs[0], encoding))


@pytest.mark.parametrize('encoding', SUPPORTED_ENCODINGS)
def test_log_debug_for_existing_span(encoding, default_annotations):
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
        zipkin_logger.debug({
            'annotations': {
                'test_annotation': 42,
            },
            'binary_annotations': {
                'extra_binary_annotation': 'extra_value',
            }
        })
        pass

    def check_span(span):
        assert span.name == 'test_span_name'
        assert span.annotations[0].host.service_name == 'test_service_name'
        assert span.parent_id is None
        assert len(span.annotations) == 4
        annotations = sorted(span.annotations, key=lambda ann: ann.value)
        assert annotations[3].value == 'test_annotation'
        assert annotations[3].timestamp == 42 * USECS
        default_annotations.add('test_annotation')
        assert set([ann.value for ann in annotations]) == default_annotations
        assert len(span.binary_annotations) == 2
        binary_annotations = sorted(
            span.binary_annotations, key=lambda bin_ann: bin_ann.key)
        assert binary_annotations[0].key == 'extra_binary_annotation'
        assert binary_annotations[0].value == 'extra_value'
        assert binary_annotations[1].key == 'some_key'
        assert binary_annotations[1].value == 'some_value'

    assert len(mock_logs) == 1
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
