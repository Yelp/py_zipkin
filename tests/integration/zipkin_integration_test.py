import pytest
from thriftpy.protocol.binary import TBinaryProtocol
from thriftpy.transport import TMemoryBuffer

from py_zipkin import zipkin
from py_zipkin.logging_helper import zipkin_logger
from py_zipkin.logging_helper import LOGGING_END_KEY
from py_zipkin.thrift import zipkin_core
from py_zipkin.zipkin import ZipkinAttrs


@pytest.fixture
def default_annotations():
    return set([
        'ss', 'sr', LOGGING_END_KEY,
    ])


@pytest.fixture
def mock_logger():
    mock_logs = []

    def mock_transport_handler(message):
        mock_logs.append(message)

    return mock_transport_handler, mock_logs


def _decode_binary_thrift_obj(obj):
    trans = TMemoryBuffer(obj)
    span = zipkin_core.Span()
    span.read(TBinaryProtocol(trans))
    return span


def test_starting_zipkin_trace_with_sampling_rate(
    mock_logger,
    default_annotations,
):
    mock_transport_handler, mock_logs = mock_logger
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
    ):
        pass

    span = _decode_binary_thrift_obj(mock_logs[0])

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


def test_starting_zipkin_trace_with_128bit_trace_id(
    mock_logger,
    default_annotations,
):
    mock_transport_handler, mock_logs = mock_logger
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
        use_128bit_trace_id=True,
    ):
        pass

    span = _decode_binary_thrift_obj(mock_logs[0])

    assert span.trace_id is not None
    assert span.trace_id_high is not None


def test_span_inside_trace(mock_logger):
    mock_transport_handler, mock_logs = mock_logger
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
    ):
        with zipkin.zipkin_span(
            service_name='nested_service',
            span_name='nested_span',
            annotations={'nested_annotation': 43},
            binary_annotations={'nested_key': 'nested_value'},
        ):
            pass

    root_span = _decode_binary_thrift_obj(mock_logs[1])
    nested_span = _decode_binary_thrift_obj(mock_logs[0])
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
            assert ann.timestamp == 43000000


def _verify_service_span(span, annotations):
    assert span.name == 'service_span'
    assert span.trace_id == 0
    assert span.id == 1
    assert span.annotations[0].host.service_name == 'test_service_name'
    assert span.annotations[0].host.port == 0
    assert span.parent_id == 2
    assert span.binary_annotations[0].key == 'some_key'
    assert span.binary_annotations[0].value == 'some_value'
    assert set([ann.value for ann in span.annotations]) == annotations


def test_service_span(mock_logger, default_annotations):
    mock_transport_handler, mock_logs = mock_logger
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
    ):
        pass

    span = _decode_binary_thrift_obj(mock_logs[0])
    _verify_service_span(span, default_annotations)
    # Spans continued on the server don't log timestamp/duration, as it's
    # assumed the client part of the pair will log them.
    assert span.timestamp is None
    assert span.duration is None


def test_service_span_report_timestamp_override(
    mock_logger,
    default_annotations,
):
    mock_transport_handler, mock_logs = mock_logger
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
    ):
        pass

    span = _decode_binary_thrift_obj(mock_logs[0])
    _verify_service_span(span, default_annotations)
    assert span.timestamp is not None
    assert span.duration is not None


def test_service_span_that_is_independently_sampled(
    mock_logger,
    default_annotations,
):
    mock_transport_handler, mock_logs = mock_logger
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
    ):
        pass

    span = _decode_binary_thrift_obj(mock_logs[0])
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


def test_log_debug_for_new_span(mock_logger):
    mock_transport_handler, mock_logs = mock_logger
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
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

    logged_span = _decode_binary_thrift_obj(mock_logs[0])
    root_span = _decode_binary_thrift_obj(mock_logs[1])
    assert logged_span.name == 'logged_name'
    assert logged_span.annotations[0].host.service_name == 'logged_service_name'
    assert logged_span.parent_id == root_span.id
    assert logged_span.binary_annotations[0].key == 'logged_binary_annotation'
    assert logged_span.binary_annotations[0].value == 'logged_value'
    assert set([ann.value for ann in logged_span.annotations]) == set(['cs', 'cr'])


def test_log_debug_for_existing_span(mock_logger, default_annotations):
    mock_transport_handler, mock_logs = mock_logger
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
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

    assert len(mock_logs) == 1
    span = _decode_binary_thrift_obj(mock_logs[0])
    assert span.name == 'test_span_name'
    assert span.annotations[0].host.service_name == 'test_service_name'
    assert span.parent_id is None
    assert len(span.annotations) == 4
    annotations = sorted(span.annotations, key=lambda ann: ann.value)
    assert annotations[3].value == 'test_annotation'
    assert annotations[3].timestamp == 42000000
    default_annotations.add('test_annotation')
    assert set([ann.value for ann in annotations]) == default_annotations
    assert len(span.binary_annotations) == 2
    binary_annotations = sorted(
        span.binary_annotations, key=lambda bin_ann: bin_ann.key)
    assert binary_annotations[0].key == 'extra_binary_annotation'
    assert binary_annotations[0].value == 'extra_value'
    assert binary_annotations[1].key == 'some_key'
    assert binary_annotations[1].value == 'some_value'
