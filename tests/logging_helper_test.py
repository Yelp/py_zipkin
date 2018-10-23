import mock
import pytest

from py_zipkin import Encoding
from py_zipkin import Kind
from py_zipkin import logging_helper
from py_zipkin.encoding._encoders import get_encoder
from py_zipkin.encoding._helpers import create_endpoint
from py_zipkin.encoding._helpers import Endpoint
from py_zipkin.encoding._helpers import Span
from py_zipkin.encoding._helpers import create_endpoint
from py_zipkin.encoding._encoders import get_encoder
from py_zipkin.exception import ZipkinError
from py_zipkin.storage import SpanStorage
from py_zipkin.zipkin import ZipkinAttrs
from tests.test_helpers import MockEncoder
from tests.test_helpers import MockTransportHandler


@pytest.fixture
def context():
    attr = ZipkinAttrs(None, None, None, None, False)
    return logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        endpoint=create_endpoint(80, 'test_server', '127.0.0.1'),
        span_name='span_name',
        transport_handler=MockTransportHandler(),
        report_root_timestamp=False,
        span_storage=SpanStorage(),
        service_name='test_server',
        encoding=Encoding.V1_JSON,
    )


@pytest.fixture
def empty_tags():
    return {}


@pytest.fixture
def empty_annotations_dict():
    return {}


@pytest.fixture
def fake_endpoint():
    return Endpoint(
        service_name='test_server',
        ipv4='127.0.0.1',
        ipv6=None,
        port=80,
    )


@mock.patch('py_zipkin.logging_helper.time.time', autospec=True)
def test_zipkin_logging_context(time_mock, context):
    # Tests the context manager aspects of the ZipkinLoggingContext
    time_mock.return_value = 42
    # Ignore the actual logging part
    with mock.patch.object(context, 'emit_spans'):
        context.start()
        assert context.start_timestamp == 42
        context.stop()
        # Make sure the handler and the zipkin attrs are gone
        assert context.emit_spans.call_count == 1


@mock.patch('py_zipkin.logging_helper.time.time', autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.flush',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.add_span',
            autospec=True)
def test_zipkin_logging_server_context_emit_spans(
    add_span_mock, flush_mock, time_mock, fake_endpoint
):
    # This lengthy function tests that the logging context properly
    # logs both client and server spans.
    trace_id = '000000000000000f'
    parent_span_id = '0000000000000001'
    server_span_id = '0000000000000002'
    client_span_id = '0000000000000003'
    client_span_name = 'breadcrumbs'
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=server_span_id,
        parent_span_id=parent_span_id,
        flags=None,
        is_sampled=True,
    )
    span_storage = SpanStorage()

    client_span = Span(
        trace_id=trace_id,
        name=client_span_name,
        parent_id=server_span_id,
        span_id=client_span_id,
        kind=Kind.CLIENT,
        timestamp=26.0,
        duration=4.0,
        local_endpoint=create_endpoint(
            service_name='test_server',
        ),
        annotations={'ann2': 2, 'cs': 26, 'cr': 30},
        tags={'bann2': 'yiss'},
    )
    span_storage.append(client_span)

    transport_handler = mock.Mock()

    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        endpoint=fake_endpoint,
        span_name='GET /foo',
        transport_handler=transport_handler,
        report_root_timestamp=True,
        span_storage=span_storage,
        service_name='test_server',
        encoding=Encoding.V1_JSON,
    )

    context.start_timestamp = 24
    context.response_status_code = 200

    context.tags = {'k': 'v'}
    time_mock.return_value = 42

    context.emit_spans()
    client_log_call, server_log_call = add_span_mock.call_args_list
    assert server_log_call[0][1].build_v1_span() == Span(
        trace_id=trace_id,
        name='GET /foo',
        parent_id=parent_span_id,
        span_id=server_span_id,
        kind=Kind.SERVER,
        timestamp=24.0,
        duration=18.0,
        local_endpoint=fake_endpoint,
        annotations={'sr': 24, 'ss': 42},
        tags={'k': 'v'},
    ).build_v1_span()
    assert client_log_call[0][1] == client_span
    assert flush_mock.call_count == 1


@mock.patch('py_zipkin.logging_helper.time.time', autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.flush',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.add_span',
            autospec=True)
def test_zipkin_logging_server_context_emit_spans_with_firehose(
    add_span_mock, flush_mock, time_mock, fake_endpoint
):
    # This lengthy function tests that the logging context properly
    # logs both client and server spans.
    trace_id = '000000000000000f'
    parent_span_id = '0000000000000001'
    server_span_id = '0000000000000002'
    client_span_id = '0000000000000003'
    client_span_name = 'breadcrumbs'
    client_svc_name = 'svc'
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=server_span_id,
        parent_span_id=parent_span_id,
        flags=None,
        is_sampled=True,
    )

    span_storage = SpanStorage()

    client_span = Span(
        trace_id=trace_id,
        name=client_span_name,
        parent_id=server_span_id,
        span_id=client_span_id,
        kind=Kind.CLIENT,
        timestamp=26.0,
        duration=4.0,
        local_endpoint=create_endpoint(service_name=client_svc_name),
        annotations={'ann2': 2, 'cs': 26, 'cr': 30},
        tags={'bann2': 'yiss'},
    )
    span_storage.append(client_span)

    transport_handler = mock.Mock()
    firehose_handler = mock.Mock()

    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        endpoint=fake_endpoint,
        span_name='GET /foo',
        transport_handler=transport_handler,
        report_root_timestamp=True,
        span_storage=span_storage,
        firehose_handler=firehose_handler,
        service_name='test_server',
        encoding=Encoding.V1_JSON,
    )

    context.start_timestamp = 24
    context.response_status_code = 200

    context.tags = {'k': 'v'}
    time_mock.return_value = 42

    context.emit_spans()
    call_args = add_span_mock.call_args_list
    firehose_client_log_call, client_log_call = call_args[0], call_args[2]
    firehose_server_log_call, server_log_call = call_args[1], call_args[3]
    assert server_log_call[0][1].build_v1_span() == \
        firehose_server_log_call[0][1].build_v1_span()
    assert server_log_call[0][1].build_v1_span() == Span(
        trace_id=trace_id,
        name='GET /foo',
        parent_id=parent_span_id,
        span_id=server_span_id,
        kind=Kind.SERVER,
        timestamp=24.0,
        duration=18.0,
        local_endpoint=fake_endpoint,
        annotations={'sr': 24, 'ss': 42},
        tags={'k': 'v'},
    ).build_v1_span()
    assert client_log_call[0][1] == firehose_client_log_call[0][1] == client_span
    assert flush_mock.call_count == 2


@mock.patch('py_zipkin.logging_helper.time.time', autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.flush',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.add_span',
            autospec=True)
def test_zipkin_logging_client_context_emit_spans(
    add_span_mock, flush_mock, time_mock, fake_endpoint
):
    # This lengthy function tests that the logging context properly
    # logs root client span
    trace_id = '000000000000000f'
    client_span_id = '0000000000000003'
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=client_span_id,
        parent_span_id=None,
        flags=None,
        is_sampled=True,
    )

    span_storage = SpanStorage()
    transport_handler = mock.Mock()

    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        endpoint=fake_endpoint,
        span_name='GET /foo',
        transport_handler=transport_handler,
        report_root_timestamp=True,
        span_storage=span_storage,
        client_context=True,
        service_name='test_server',
        encoding=Encoding.V1_JSON,
    )

    context.start_timestamp = 24
    context.response_status_code = 200

    context.tags = {'k': 'v'}
    time_mock.return_value = 42

    context.emit_spans()
    log_call = add_span_mock.call_args_list[0]
    assert log_call[0][1].build_v1_span() == Span(
        trace_id=trace_id,
        name='GET /foo',
        parent_id=None,
        span_id=client_span_id,
        kind=Kind.CLIENT,
        timestamp=24.0,
        duration=18.0,
        local_endpoint=fake_endpoint,
        annotations={'cs': 24, 'cr': 42},
        tags={'k': 'v'},
    ).build_v1_span()
    assert flush_mock.call_count == 1


@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.flush',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.add_span',
            autospec=True)
def test_batch_sender_add_span_not_called_if_not_sampled(add_span_mock,
                                                         flush_mock):
    attr = ZipkinAttrs(
        trace_id='0000000000000001',
        span_id='0000000000000002',
        parent_span_id=None,
        flags=None,
        is_sampled=False,
    )
    span_storage = SpanStorage()
    transport_handler = mock.Mock()

    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        endpoint=create_endpoint(80, 'test_server', '127.0.0.1'),
        span_name='span_name',
        transport_handler=transport_handler,
        report_root_timestamp=False,
        span_storage=span_storage,
        service_name='test_server',
        encoding=Encoding.V1_JSON,
    )
    context.emit_spans()
    assert add_span_mock.call_count == 0
    assert flush_mock.call_count == 0


@mock.patch('py_zipkin.logging_helper.time.time', autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.flush',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.add_span',
            autospec=True)
def test_batch_sender_add_span_not_sampled_with_firehose(add_span_mock,
                                                         flush_mock,
                                                         time_mock):
    attr = ZipkinAttrs(
        trace_id='0000000000000001',
        span_id='0000000000000002',
        parent_span_id=None,
        flags=None,
        is_sampled=False,
    )
    span_storage = SpanStorage()
    transport_handler = mock.Mock()
    firehose_handler = mock.Mock()

    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        endpoint=create_endpoint(80, 'test_server', '127.0.0.1'),
        span_name='span_name',
        transport_handler=transport_handler,
        report_root_timestamp=False,
        span_storage=span_storage,
        firehose_handler=firehose_handler,
        service_name='test_server',
        encoding=Encoding.V1_JSON,
    )
    context.start_timestamp = 24
    context.response_status_code = 200

    context.tags = {'k': 'v'}
    time_mock.return_value = 42

    context.emit_spans()
    assert add_span_mock.call_count == 1
    assert flush_mock.call_count == 1


def test_batch_sender_add_span(
    empty_annotations_dict,
    empty_tags,
    fake_endpoint,
):
    # This test verifies it's possible to add 1 span without throwing errors.
    # It also checks that exiting the ZipkinBatchSender context manager
    # triggers a flush of all the already added spans.
    encoder = MockEncoder(encoded_queue='foobar')
    sender = logging_helper.ZipkinBatchSender(
        transport_handler=MockTransportHandler(),
        max_portion_size=None,
        encoder=encoder,
    )
    with sender:
        sender.add_span(Span(
            trace_id='000000000000000f',
            name='span',
            parent_id='0000000000000001',
            span_id='0000000000000002',
            kind=Kind.CLIENT,
            timestamp=26.0,
            duration=4.0,
            local_endpoint=fake_endpoint,
            annotations=empty_annotations_dict,
            tags=empty_tags,
        ))
    assert encoder.encode_queue.call_count == 1


def test_batch_sender_with_error_on_exit():
    sender = logging_helper.ZipkinBatchSender(
        MockTransportHandler(),
        None,
        MockEncoder(),
    )
    with pytest.raises(ZipkinError):
        with sender:
            raise Exception('Error!')


def test_batch_sender_add_span_many_times(
    empty_annotations_dict,
    empty_tags,
    fake_endpoint,
):
    # We create MAX_PORTION_SIZE * 2 + 1 spans, so we should trigger flush 3
    # times, once every MAX_PORTION_SIZE spans.
    encoder = MockEncoder()
    sender = logging_helper.ZipkinBatchSender(
        transport_handler=MockTransportHandler(),
        max_portion_size=None,
        encoder=encoder,
    )
    max_portion_size = logging_helper.ZipkinBatchSender.MAX_PORTION_SIZE
    with sender:
        for _ in range(max_portion_size * 2 + 1):
            sender.add_span(Span(
                trace_id='000000000000000f',
                name='span',
                parent_id='0000000000000001',
                span_id='0000000000000002',
                kind=Kind.CLIENT,
                timestamp=26.0,
                duration=4.0,
                local_endpoint=fake_endpoint,
                annotations=empty_annotations_dict,
                tags=empty_tags,
            ))

    assert encoder.encode_queue.call_count == 3
    assert len(encoder.encode_queue.call_args_list[0][0][0]) == max_portion_size
    assert len(encoder.encode_queue.call_args_list[1][0][0]) == max_portion_size
    assert len(encoder.encode_queue.call_args_list[2][0][0]) == 1


def test_batch_sender_add_span_too_big(
    empty_annotations_dict,
    empty_tags,
    fake_endpoint,
):
    # This time we set max_payload_bytes to 1000, so we have to send more batches.
    # Each encoded span is 175 bytes, so we can fit 5 of those in 1000 bytes.
    mock_transport_handler = mock.Mock(spec=MockTransportHandler)
    mock_transport_handler.get_max_payload_bytes = lambda: 1000
    sender = logging_helper.ZipkinBatchSender(
        mock_transport_handler,
        100,
        get_encoder(Encoding.V1_THRIFT),
    )
    with sender:
        for _ in range(201):
            sender.add_span(Span(
                trace_id='000000000000000f',
                name='span',
                parent_id='0000000000000001',
                span_id='0000000000000002',
                kind=Kind.CLIENT,
                timestamp=26.0,
                duration=4.0,
                local_endpoint=fake_endpoint,
                annotations=empty_annotations_dict,
                tags=empty_tags,
            ))

    # 5 spans per batch, means we need 201 / 4 = 41 batches to send them all.
    assert mock_transport_handler.call_count == 41
    for i in range(40):
        # The first 40 batches have 5 spans of 197 bytes + 5 bytes of
        # list headers = 990 bytes
        assert len(mock_transport_handler.call_args_list[i][0][0]) == 990
    # The last batch has a single remaining span of 197 bytes + 5 bytes of
    # list headers = 202 bytes
    assert len(mock_transport_handler.call_args_list[40][0][0]) == 202


def test_batch_sender_flush_calls_transport_handler_with_correct_params(
    empty_annotations_dict,
    empty_tags,
    fake_endpoint,
):
    # Tests that the transport handler is called with the value returned
    # by encoder.encode_queue.
    transport_handler = mock.Mock()
    transport_handler.get_max_payload_bytes = lambda: None
    encoder = MockEncoder(encoded_queue='foobar')
    sender = logging_helper.ZipkinBatchSender(
        transport_handler=transport_handler,
        max_portion_size=None,
        encoder=encoder,
    )
    with sender:
        sender.add_span(Span(
            trace_id='000000000000000f',
            name='span',
            parent_id='0000000000000001',
            span_id='0000000000000002',
            kind=Kind.CLIENT,
            timestamp=26.0,
            duration=4.0,
            local_endpoint=fake_endpoint,
            annotations=empty_annotations_dict,
            tags=empty_tags,
        ))
    transport_handler.assert_called_once_with('foobar')


def test_batch_sender_defensive_about_transport_handler(
    empty_annotations_dict,
    empty_tags,
    fake_endpoint,
):
    """Make sure log_span doesn't try to call the transport handler if it's
    None."""
    encoder = MockEncoder()
    sender = logging_helper.ZipkinBatchSender(
        transport_handler=None,
        max_portion_size=None,
        encoder=encoder,
    )
    with sender:
        sender.add_span(Span(
            trace_id='000000000000000f',
            name='span',
            parent_id='0000000000000001',
            span_id='0000000000000002',
            kind=Kind.CLIENT,
            timestamp=26.0,
            duration=4.0,
            local_endpoint=fake_endpoint,
            annotations=empty_annotations_dict,
            tags=empty_tags,
        ))
    assert encoder.encode_span.call_count == 1
    assert encoder.encode_queue.call_count == 0
