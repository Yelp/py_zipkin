import mock
import pytest

from tests.conftest import MockTransportHandler
from py_zipkin import logging_helper
from py_zipkin import _encoding_helpers
from py_zipkin.stack import Stack
from py_zipkin.exception import ZipkinError
from py_zipkin.zipkin import ZipkinAttrs


class SimpleStack(Stack):
    def __init__(self):
        super(SimpleStack, self).__init__([])


@pytest.fixture
def context():
    attr = ZipkinAttrs(None, None, None, None, False)
    return logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        endpoint=_encoding_helpers.create_endpoint(80, 'test_server', '127.0.0.1'),
        span_name='span_name',
        transport_handler=MockTransportHandler(),
        report_root_timestamp=False,
        span_store=SimpleStack()
    )


@pytest.fixture
def empty_binary_annotations_dict():
    return {}


@pytest.fixture
def empty_annotations_dict():
    return {}


@pytest.fixture
def fake_endpoint():
    return _encoding_helpers.Endpoint(
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
    with mock.patch.object(context, 'log_spans'):
        context.start()
        assert context.start_timestamp == 42
        context.stop()
        # Make sure the handler and the zipkin attrs are gone
        assert context.log_spans.call_count == 1


@mock.patch('py_zipkin.logging_helper.time.time', autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.flush',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.add_span',
            autospec=True)
def test_zipkin_logging_server_context_log_spans(
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
    span_store = SimpleStack()

    span_store.push({
        'trace_id': trace_id,
        'span_id': client_span_id,
        'parent_span_id': server_span_id,
        'span_name': client_span_name,
        'service_name': client_svc_name,
        'annotations': {'ann2': 2, 'cs': 26, 'cr': 30},
        'binary_annotations': {'bann2': 'yiss'},
        'sa_endpoint': None,
    })

    transport_handler = mock.Mock()

    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        endpoint=fake_endpoint,
        span_name='GET /foo',
        transport_handler=transport_handler,
        report_root_timestamp=True,
        span_store=span_store,
    )

    context.start_timestamp = 24
    context.response_status_code = 200

    context.binary_annotations_dict = {'k': 'v'}
    time_mock.return_value = 42

    expected_server_annotations = {'sr': 24, 'ss': 42}
    expected_server_bin_annotations = {'k': 'v'}

    expected_client_annotations = {'ann2': 2, 'cs': 26, 'cr': 30}
    expected_client_bin_annotations = {'bann2': 'yiss'}

    context.log_spans()
    client_log_call, server_log_call = add_span_mock.call_args_list
    assert server_log_call[1] == {
        'span_id': server_span_id,
        'parent_span_id': parent_span_id,
        'trace_id': trace_id,
        'span_name': 'GET /foo',
        'annotations': expected_server_annotations,
        'binary_annotations': expected_server_bin_annotations,
        'duration_s': 18,
        'timestamp_s': 24,
        'endpoint': fake_endpoint,
        'sa_endpoint': None,
    }
    assert client_log_call[1] == {
        'span_id': client_span_id,
        'parent_span_id': server_span_id,
        'trace_id': trace_id,
        'span_name': client_span_name,
        'annotations': expected_client_annotations,
        'binary_annotations': expected_client_bin_annotations,
        'duration_s': 4,
        'timestamp_s': 26,
        'endpoint': _encoding_helpers.create_endpoint(80, 'svc', '127.0.0.1'),
        'sa_endpoint': None,
    }
    assert flush_mock.call_count == 1


@mock.patch('py_zipkin.logging_helper.time.time', autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.flush',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.add_span',
            autospec=True)
def test_zipkin_logging_server_context_log_spans_with_firehose(
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

    span_store = SimpleStack()

    span_store.push({
        'trace_id': trace_id,
        'span_id': client_span_id,
        'parent_span_id': server_span_id,
        'span_name': client_span_name,
        'service_name': client_svc_name,
        'annotations': {'ann2': 2, 'cs': 26, 'cr': 30},
        'binary_annotations': {'bann2': 'yiss'},
        'sa_endpoint': None,
    })

    transport_handler = mock.Mock()
    firehose_handler = mock.Mock()

    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        endpoint=fake_endpoint,
        span_name='GET /foo',
        transport_handler=transport_handler,
        report_root_timestamp=True,
        span_store=span_store,
        firehose_handler=firehose_handler,
    )

    context.start_timestamp = 24
    context.response_status_code = 200

    context.binary_annotations_dict = {'k': 'v'}
    time_mock.return_value = 42

    expected_server_annotations = {'sr': 24, 'ss': 42}
    expected_server_bin_annotations = {'k': 'v'}

    expected_client_annotations = {'ann2': 2, 'cs': 26, 'cr': 30}
    expected_client_bin_annotations = {'bann2': 'yiss'}

    context.log_spans()
    call_args = add_span_mock.call_args_list
    firehose_client_log_call, client_log_call = call_args[0], call_args[2]
    firehose_server_log_call, server_log_call = call_args[1], call_args[3]
    assert server_log_call[1] == firehose_server_log_call[1] == {
        'span_id': server_span_id,
        'parent_span_id': parent_span_id,
        'trace_id': trace_id,
        'span_name': 'GET /foo',
        'annotations': expected_server_annotations,
        'binary_annotations': expected_server_bin_annotations,
        'duration_s': 18,
        'timestamp_s': 24,
        'endpoint': fake_endpoint,
        'sa_endpoint': None,
    }
    assert client_log_call[1] == firehose_client_log_call[1] == {
        'span_id': client_span_id,
        'parent_span_id': server_span_id,
        'trace_id': trace_id,
        'span_name': client_span_name,
        'annotations': expected_client_annotations,
        'binary_annotations': expected_client_bin_annotations,
        'duration_s': 4,
        'timestamp_s': 26,
        'endpoint': _encoding_helpers.create_endpoint(80, 'svc', '127.0.0.1'),
        'sa_endpoint': None,
    }
    assert flush_mock.call_count == 2


@mock.patch('py_zipkin.logging_helper.time.time', autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.flush',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.add_span',
            autospec=True)
def test_zipkin_logging_client_context_log_spans(
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

    span_store = SimpleStack()
    transport_handler = mock.Mock()

    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        endpoint=fake_endpoint,
        span_name='GET /foo',
        transport_handler=transport_handler,
        report_root_timestamp=True,
        span_store=span_store,
        client_context=True,
    )

    context.start_timestamp = 24
    context.response_status_code = 200

    context.binary_annotations_dict = {'k': 'v'}
    time_mock.return_value = 42

    expected_server_annotations = {'cs': 24, 'cr': 42}
    expected_server_bin_annotations = {'k': 'v'}

    context.log_spans()
    log_call = add_span_mock.call_args_list[0]
    assert log_call[1] == {
        'span_id': client_span_id,
        'parent_span_id': None,
        'trace_id': trace_id,
        'span_name': 'GET /foo',
        'annotations': expected_server_annotations,
        'binary_annotations': expected_server_bin_annotations,
        'duration_s': 18,
        'timestamp_s': 24,
        'endpoint': fake_endpoint,
        'sa_endpoint': None,
    }
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
    span_store = SimpleStack()
    transport_handler = mock.Mock()

    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        endpoint=_encoding_helpers.create_endpoint(80, 'test_server', '127.0.0.1'),
        span_name='span_name',
        transport_handler=transport_handler,
        report_root_timestamp=False,
        span_store=span_store,
    )
    context.log_spans()
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
    span_store = SimpleStack()
    transport_handler = mock.Mock()
    firehose_handler = mock.Mock()

    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        endpoint=_encoding_helpers.create_endpoint(80, 'test_server', '127.0.0.1'),
        span_name='span_name',
        transport_handler=transport_handler,
        report_root_timestamp=False,
        span_store=span_store,
        firehose_handler=firehose_handler,
    )
    context.start_timestamp = 24
    context.response_status_code = 200

    context.binary_annotations_dict = {'k': 'v'}
    time_mock.return_value = 42

    context.log_spans()
    assert add_span_mock.call_count == 1
    assert flush_mock.call_count == 1


@mock.patch('py_zipkin.logging_helper.thrift.encode_bytes_list', autospec=True)
def test_batch_sender_add_span(
    mock_encode_bytes_list,
    empty_annotations_dict,
    empty_binary_annotations_dict,
    fake_endpoint,
):
    # This test verifies it's possible to add 1 span without throwing errors.
    # It also checks that exiting the ZipkinBatchSender context manager
    # triggers a flush of all the already added spans.
    sender = logging_helper.ZipkinBatchSender(MockTransportHandler())
    with sender:
        sender.add_span(
            span_id='0000000000000002',
            parent_span_id='0000000000000001',
            trace_id='000000000000000f',
            span_name='span',
            annotations=empty_annotations_dict,
            binary_annotations=empty_binary_annotations_dict,
            timestamp_s=None,
            duration_s=None,
            endpoint=fake_endpoint,
            sa_endpoint=None,
        )
    assert mock_encode_bytes_list.call_count == 1


def test_batch_sender_with_error_on_exit():
    sender = logging_helper.ZipkinBatchSender(MockTransportHandler())
    with pytest.raises(ZipkinError):
        with sender:
            raise Exception('Error!')


@mock.patch('py_zipkin.logging_helper.thrift.encode_bytes_list', autospec=True)
def test_batch_sender_add_span_many_times(
    mock_encode_bytes_list,
    empty_annotations_dict,
    empty_binary_annotations_dict,
    fake_endpoint,
):
    # We create MAX_PORTION_SIZE * 2 + 1 spans, so we should trigger flush 3
    # times, once every MAX_PORTION_SIZE spans.
    sender = logging_helper.ZipkinBatchSender(MockTransportHandler())
    max_portion_size = logging_helper.ZipkinBatchSender.MAX_PORTION_SIZE
    with sender:
        for _ in range(max_portion_size * 2 + 1):
            sender.add_span(
                span_id='0000000000000002',
                parent_span_id='0000000000000001',
                trace_id='000000000000000f',
                span_name='span',
                annotations=empty_annotations_dict,
                binary_annotations=empty_binary_annotations_dict,
                timestamp_s=None,
                duration_s=None,
                endpoint=fake_endpoint,
                sa_endpoint=None,
            )
    assert mock_encode_bytes_list.call_count == 3
    assert len(mock_encode_bytes_list.call_args_list[0][0][0]) == max_portion_size
    assert len(mock_encode_bytes_list.call_args_list[1][0][0]) == max_portion_size
    assert len(mock_encode_bytes_list.call_args_list[2][0][0]) == 1


def test_batch_sender_add_span_too_big(
    empty_annotations_dict,
    empty_binary_annotations_dict,
    fake_endpoint,
):
    # This time we set max_payload_bytes to 1000, so we have to send more batches.
    # Each encoded span is 65 bytes, so we can fit 15 of those in 1000 bytes.
    mock_transport_handler = mock.Mock(spec=MockTransportHandler)
    mock_transport_handler.get_max_payload_bytes = lambda: 1000
    sender = logging_helper.ZipkinBatchSender(mock_transport_handler, 100)
    with sender:
        for _ in range(201):
            sender.add_span(
                span_id='0000000000000002',
                parent_span_id='0000000000000001',
                trace_id='000000000000000f',
                span_name='span',
                annotations=empty_annotations_dict,
                binary_annotations=empty_binary_annotations_dict,
                timestamp_s=None,
                duration_s=None,
                endpoint=fake_endpoint,
                sa_endpoint=None,
            )
    # 15 spans per batch, means we need 201 / 15 = 14 batches to send them all.
    assert mock_transport_handler.call_count == 14
    for i in range(13):
        # The first 13 batches have 15 spans of 65 bytes + 5 bytes of
        # list headers = 980 bytes
        assert len(mock_transport_handler.call_args_list[i][0][0]) == 980
    # The last batch has the 6 remaining spans of 65 bytes + 5 bytes of
    # list headers = 395 bytes
    assert len(mock_transport_handler.call_args_list[13][0][0]) == 395


@mock.patch('py_zipkin.logging_helper.thrift.encode_bytes_list', autospec=True)
def test_batch_sender_flush_calls_transport_handler_with_correct_params(
    mock_encode_bytes_list,
    empty_annotations_dict,
    empty_binary_annotations_dict,
    fake_endpoint,
):
    # Tests that the transport handler is called with the value returned
    # by thrift.encode_bytes_list.
    transport_handler = mock.Mock()
    transport_handler.get_max_payload_bytes = lambda: None
    sender = logging_helper.ZipkinBatchSender(transport_handler)
    with sender:
        sender.add_span(
            span_id='0000000000000002',
            parent_span_id='0000000000000001',
            trace_id='000000000000000f',
            span_name='span',
            annotations=empty_annotations_dict,
            binary_annotations=empty_binary_annotations_dict,
            timestamp_s=None,
            duration_s=None,
            endpoint=fake_endpoint,
            sa_endpoint=None,
        )
    transport_handler.assert_called_once_with(mock_encode_bytes_list.return_value)


@mock.patch('py_zipkin.logging_helper.thrift.create_span', autospec=True)
@mock.patch('py_zipkin.logging_helper.thrift.encode_bytes_list', autospec=True)
def test_batch_sender_defensive_about_transport_handler(
    mock_encode_bytes_list,
    create_sp,
    empty_annotations_dict,
    empty_binary_annotations_dict,
    fake_endpoint,
):
    """Make sure log_span doesn't try to call the transport handler if it's
    None."""
    sender = logging_helper.ZipkinBatchSender(transport_handler=None)
    with sender:
        sender.add_span(
            span_id='0000000000000002',
            parent_span_id='0000000000000001',
            trace_id='00000000000000015',
            span_name='span',
            annotations=empty_annotations_dict,
            binary_annotations=empty_binary_annotations_dict,
            timestamp_s=None,
            duration_s=None,
            endpoint=fake_endpoint,
            sa_endpoint=None,
        )
    assert create_sp.call_count == 1
    assert mock_encode_bytes_list.call_count == 0


def test_get_local_span_timestamp_and_duration_client():
    timestamp, duration = logging_helper.get_local_span_timestamp_and_duration(
        {'cs': 16, 'cr': 30},
    )
    assert timestamp == 16
    assert duration == 14


def test_get_local_span_timestamp_and_duration_server():
    timestamp, duration = logging_helper.get_local_span_timestamp_and_duration(
        {'sr': 12, 'ss': 30},
    )
    assert timestamp == 12
    assert duration == 18


def test_get_local_span_timestamp_and_duration_none():
    timestamp, duration = logging_helper.get_local_span_timestamp_and_duration(
        {'cs': 16, 'other': 5}
    )
    assert timestamp is None
    assert duration is None
