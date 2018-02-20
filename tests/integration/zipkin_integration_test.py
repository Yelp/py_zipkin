import pytest
from thriftpy.protocol.binary import read_list_begin
from thriftpy.protocol.binary import TBinaryProtocol
from thriftpy.transport import TMemoryBuffer

from py_zipkin import zipkin
from py_zipkin.logging_helper import LOGGING_END_KEY
from py_zipkin.logging_helper import zipkin_logger
from py_zipkin.thrift import zipkin_core
from py_zipkin.zipkin import ZipkinAttrs


USECS = 1000000


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


def _decode_binary_thrift_obj(obj):
    spans = _decode_binary_thrift_objs(obj)
    return spans[0]


def _decode_binary_thrift_objs(obj):
    spans = []
    trans = TMemoryBuffer(obj)
    _, size = read_list_begin(trans)
    for _ in range(size):
        span = zipkin_core.Span()
        span.read(TBinaryProtocol(trans))
        spans.append(span)
    return spans


def test_starting_zipkin_trace_with_sampling_rate(
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

    check_span(_decode_binary_thrift_obj(mock_logs[0]))
    check_span(_decode_binary_thrift_obj(mock_firehose_logs[0]))


def test_starting_zipkin_trace_with_128bit_trace_id(
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
        use_128bit_trace_id=True,
        firehose_handler=mock_firehose_handler
    ):
        pass

    def check_span(span):
        assert span.trace_id is not None
        assert span.trace_id_high is not None

    check_span(_decode_binary_thrift_obj(mock_logs[0]))
    check_span(_decode_binary_thrift_obj(mock_firehose_logs[0]))


def test_span_inside_trace():
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

    check_spans(_decode_binary_thrift_objs(mock_logs[0]))
    check_spans(_decode_binary_thrift_objs(mock_firehose_logs[0]))


def test_annotation_override():
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

    check_spans(_decode_binary_thrift_objs(mock_logs[0]))
    check_spans(_decode_binary_thrift_objs(mock_firehose_logs[0]))


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
    ):
        pass

    span = _decode_binary_thrift_obj(mock_logs[0])
    firehose_span = _decode_binary_thrift_obj(mock_firehose_logs[0])
    _verify_service_span(span, default_annotations)
    _verify_service_span(firehose_span, default_annotations)

    # Spans continued on the server don't log timestamp/duration, as it's
    # assumed the client part of the pair will log them.
    assert span.timestamp is None
    assert span.duration is None


def test_service_span_report_timestamp_override(
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
    ):
        pass

    span = _decode_binary_thrift_obj(mock_logs[0])
    _verify_service_span(span, default_annotations)
    assert span.timestamp is not None
    assert span.duration is not None


def test_service_span_that_is_independently_sampled(
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

    check_span(_decode_binary_thrift_obj(mock_logs[0]))
    check_span(_decode_binary_thrift_obj(mock_firehose_logs[0]))


def test_log_debug_for_new_span():
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

    check_spans(_decode_binary_thrift_objs(mock_logs[0]))
    check_spans(_decode_binary_thrift_objs(mock_firehose_logs[0]))


def test_log_debug_for_existing_span(default_annotations):
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
    check_span(_decode_binary_thrift_obj(mock_logs[0]))
    check_span(_decode_binary_thrift_obj(mock_firehose_logs[0]))


def test_zipkin_trace_with_no_sampling_no_firehose(
    default_annotations
):
    mock_transport_handler, mock_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=None,
        binary_annotations={'some_key': 'some_value'},
        add_logging_annotation=True,
    ):
        pass

    assert len(mock_logs) == 0


def test_zipkin_trace_with_no_sampling_with_firehose(
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
    check_span(_decode_binary_thrift_obj(mock_firehose_logs[0]))


def test_no_sampling_with_inner_span():
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
    check_spans(_decode_binary_thrift_objs(mock_firehose_logs[0]))
