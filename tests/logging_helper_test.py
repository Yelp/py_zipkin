import mock
import pytest

from py_zipkin import logging_helper
from py_zipkin import thrift
from py_zipkin.exception import ZipkinError
from py_zipkin.zipkin import ZipkinAttrs


# This test _must_ be the first test in this file
def test_zipkin_doesnt_spew_on_first_log(capfd):
    zipkin_logger = logging_helper.zipkin_logger

    zipkin_logger.debug({
        'annotations': {'foo': 2},
        'name': 'bar',
    })

    out, err = capfd.readouterr()

    assert not err
    assert not out


class MockTransportHandler(object):

    def __init__(self, max_payload_bytes=None):
        self.max_payload_bytes = max_payload_bytes

    def __call__(self, *args, **kwargs):
        return args[0]

    def get_max_payload_bytes(self):
        return self.max_payload_bytes


@pytest.fixture
def context():
    attr = ZipkinAttrs(None, None, None, None, False)
    log_handler = logging_helper.ZipkinLoggerHandler(attr)
    return logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        thrift_endpoint='thrift_endpoint',
        log_handler=log_handler,
        span_name='span_name',
        transport_handler=MockTransportHandler(),
        report_root_timestamp=False,
    )


@mock.patch('py_zipkin.logging_helper.zipkin_logger', autospec=True)
@mock.patch('py_zipkin.logging_helper.time.time', autospec=True)
def test_zipkin_logging_context(time_mock, mock_logger, context):
    # Tests the context manager aspects of the ZipkinLoggingContext
    time_mock.return_value = 42
    # Ignore the actual logging part
    with mock.patch.object(context, 'log_spans'):
        context.start()
        mock_logger.addHandler.assert_called_once_with(context.log_handler)
        assert context.start_timestamp == 42
        context.stop()
        # Make sure the handler and the zipkin attrs are gone
        mock_logger.removeHandler.assert_called_with(context.log_handler)
        assert mock_logger.removeHandler.call_count == 2
        assert context.log_spans.call_count == 1


@mock.patch('py_zipkin.logging_helper.time.time', autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.flush',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.add_span',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.thrift.annotation_list_builder',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.thrift.binary_annotation_list_builder',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.thrift.copy_endpoint_with_new_service_name',
            autospec=True)
def test_zipkin_logging_server_context_log_spans(
    copy_endpoint_mock, bin_ann_list_builder, ann_list_builder,
    add_span_mock, flush_mock, time_mock
):
    # This lengthy function tests that the logging context properly
    # logs both client and server spans, while attaching extra annotations
    # logged throughout the context of the trace.
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
    handler = logging_helper.ZipkinLoggerHandler(attr)
    extra_server_annotations = {
        'parent_span_id': None,
        'annotations': {'foo': 1},
        'binary_annotations': {'what': 'whoa'},
    }
    extra_client_annotations = {
        'parent_span_id': client_span_id,
        'annotations': {'ann1': 1},
        'binary_annotations': {'bann1': 'aww'},
    }
    handler.extra_annotations = [
        extra_server_annotations,
        extra_client_annotations,
    ]
    handler.client_spans = [{
        'span_id': client_span_id,
        'parent_span_id': None,
        'span_name': client_span_name,
        'service_name': client_svc_name,
        'annotations': {'ann2': 2, 'cs': 26, 'cr': 30},
        'binary_annotations': {'bann2': 'yiss'},
    }]

    # Each of the thrift annotation helpers just reflects its first arg
    # so the annotation dicts can be checked.
    ann_list_builder.side_effect = lambda x, y: x
    bin_ann_list_builder.side_effect = lambda x, y: x

    transport_handler = mock.Mock()

    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        thrift_endpoint='thrift_endpoint',
        log_handler=handler,
        span_name='GET /foo',
        transport_handler=transport_handler,
        report_root_timestamp=True,
    )

    context.start_timestamp = 24
    context.response_status_code = 200

    context.binary_annotations_dict = {'k': 'v'}
    time_mock.return_value = 42

    expected_server_annotations = {'foo': 1, 'sr': 24, 'ss': 42}
    expected_server_bin_annotations = {'k': 'v', 'what': 'whoa'}

    expected_client_annotations = {'ann1': 1, 'ann2': 2, 'cs': 26, 'cr': 30}
    expected_client_bin_annotations = {'bann1': 'aww', 'bann2': 'yiss'}

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
    }
    assert flush_mock.call_count == 1


@mock.patch('py_zipkin.logging_helper.time.time', autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.flush',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.add_span',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.thrift.annotation_list_builder',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.thrift.binary_annotation_list_builder',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.thrift.copy_endpoint_with_new_service_name',
            autospec=True)
def test_zipkin_logging_server_context_log_spans_with_firehose(
    copy_endpoint_mock, bin_ann_list_builder, ann_list_builder,
    add_span_mock, flush_mock, time_mock
):
    # This lengthy function tests that the logging context properly
    # logs both client and server spans, while attaching extra annotations
    # logged throughout the context of the trace.
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
    handler = logging_helper.ZipkinLoggerHandler(attr)
    extra_server_annotations = {
        'parent_span_id': None,
        'annotations': {'foo': 1},
        'binary_annotations': {'what': 'whoa'},
    }
    extra_client_annotations = {
        'parent_span_id': client_span_id,
        'annotations': {'ann1': 1},
        'binary_annotations': {'bann1': 'aww'},
    }
    handler.extra_annotations = [
        extra_server_annotations,
        extra_client_annotations,
    ]
    handler.client_spans = [{
        'span_id': client_span_id,
        'parent_span_id': None,
        'span_name': client_span_name,
        'service_name': client_svc_name,
        'annotations': {'ann2': 2, 'cs': 26, 'cr': 30},
        'binary_annotations': {'bann2': 'yiss'},
    }]

    # Each of the thrift annotation helpers just reflects its first arg
    # so the annotation dicts can be checked.
    ann_list_builder.side_effect = lambda x, y: x
    bin_ann_list_builder.side_effect = lambda x, y: x

    transport_handler = mock.Mock()
    firehose_handler = mock.Mock()

    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        thrift_endpoint='thrift_endpoint',
        log_handler=handler,
        span_name='GET /foo',
        transport_handler=transport_handler,
        report_root_timestamp=True,
        firehose_handler=firehose_handler
    )

    context.start_timestamp = 24
    context.response_status_code = 200

    context.binary_annotations_dict = {'k': 'v'}
    time_mock.return_value = 42

    expected_server_annotations = {'foo': 1, 'sr': 24, 'ss': 42}
    expected_server_bin_annotations = {'k': 'v', 'what': 'whoa'}

    expected_client_annotations = {'ann1': 1, 'ann2': 2, 'cs': 26, 'cr': 30}
    expected_client_bin_annotations = {'bann1': 'aww', 'bann2': 'yiss'}

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
    }
    assert flush_mock.call_count == 2


@mock.patch('py_zipkin.logging_helper.time.time', autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.flush',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.ZipkinBatchSender.add_span',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.thrift.annotation_list_builder',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.thrift.binary_annotation_list_builder',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.thrift.copy_endpoint_with_new_service_name',
            autospec=True)
def test_zipkin_logging_client_context_log_spans(
    copy_endpoint_mock, bin_ann_list_builder, ann_list_builder,
    add_span_mock, flush_mock, time_mock
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
    handler = logging_helper.ZipkinLoggerHandler(attr)
    handler.client_spans = []

    # Each of the thrift annotation helpers just reflects its first arg
    # so the annotation dicts can be checked.
    ann_list_builder.side_effect = lambda x, y: x
    bin_ann_list_builder.side_effect = lambda x, y: x

    transport_handler = mock.Mock()

    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        thrift_endpoint='thrift_endpoint',
        log_handler=handler,
        span_name='GET /foo',
        transport_handler=transport_handler,
        report_root_timestamp=True,
        client_context=True
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
    log_handler = logging_helper.ZipkinLoggerHandler(attr)
    transport_handler = mock.Mock()
    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        thrift_endpoint='thrift_endpoint',
        log_handler=log_handler,
        span_name='span_name',
        transport_handler=transport_handler,
        report_root_timestamp=False,
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
    log_handler = logging_helper.ZipkinLoggerHandler(attr)
    transport_handler = mock.Mock()
    firehose_handler = mock.Mock()
    context = logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        thrift_endpoint='thrift_endpoint',
        log_handler=log_handler,
        span_name='span_name',
        transport_handler=transport_handler,
        report_root_timestamp=False,
        firehose_handler=firehose_handler,
    )
    context.start_timestamp = 24
    context.response_status_code = 200

    context.binary_annotations_dict = {'k': 'v'}
    time_mock.return_value = 42

    context.log_spans()
    assert add_span_mock.call_count == 1
    assert flush_mock.call_count == 1


def test_zipkin_handler_init():
    handler = logging_helper.ZipkinLoggerHandler('foo')
    assert handler.zipkin_attrs == 'foo'


def test_zipkin_handler_does_not_emit_unsampled_record(unsampled_zipkin_attr):
    handler = logging_helper.ZipkinLoggerHandler(unsampled_zipkin_attr)
    assert not handler.emit('bla')


def test_handler_stores_client_span_on_emit(sampled_zipkin_attr):
    record = mock.Mock()
    record.msg = {
        'annotations': 'ann1', 'binary_annotations': 'bann1',
        'name': 'foo', 'service_name': 'blargh',
    }
    handler = logging_helper.ZipkinLoggerHandler(sampled_zipkin_attr)
    assert handler.client_spans == []
    handler.emit(record)
    assert handler.client_spans == [{
        'span_name': 'foo',
        'service_name': 'blargh',
        'parent_span_id': None,
        'span_id': None,
        'annotations': 'ann1',
        'binary_annotations': 'bann1',
        'sa_binary_annotations': None,
    }]


def test_handler_stores_extra_annotations_on_emit(sampled_zipkin_attr):
    record = mock.Mock()
    record.msg = {'annotations': 'ann1', 'binary_annotations': 'bann1'}
    handler = logging_helper.ZipkinLoggerHandler(sampled_zipkin_attr)
    assert handler.extra_annotations == []
    handler.emit(record)
    assert handler.extra_annotations == [{
        'annotations': 'ann1',
        'binary_annotations': 'bann1',
        'parent_span_id': None,
    }]


def test_zipkin_handler_raises_exception_if_ann_and_bann_not_provided(
        sampled_zipkin_attr):
    record = mock.Mock(msg={'name': 'foo'})
    handler = logging_helper.ZipkinLoggerHandler(sampled_zipkin_attr)
    with pytest.raises(ZipkinError) as excinfo:
        handler.emit(record)
    assert ("At least one of annotation/binary annotation has to be provided"
            " for foo span" == str(excinfo.value))


@mock.patch('py_zipkin.logging_helper.thrift.encode_bytes_list', autospec=True)
def test_batch_sender_add_span(mock_encode_bytes_list):
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
            annotations=thrift.annotation_list_builder({}, thrift.create_endpoint()),
            binary_annotations=thrift.binary_annotation_list_builder({}, thrift.create_endpoint()),
            timestamp_s=None,
            duration_s=None,
        )
    assert mock_encode_bytes_list.call_count == 1


def test_batch_sender_with_error_on_exit():
    sender = logging_helper.ZipkinBatchSender(MockTransportHandler())
    with pytest.raises(ZipkinError):
        with sender:
            raise Exception('Error!')


@mock.patch('py_zipkin.logging_helper.thrift.encode_bytes_list', autospec=True)
def test_batch_sender_add_span_many_times(mock_encode_bytes_list):
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
                annotations=thrift.annotation_list_builder({}, thrift.create_endpoint()),
                binary_annotations=thrift.binary_annotation_list_builder({}, thrift.create_endpoint()),
                timestamp_s=None,
                duration_s=None,
            )
    assert mock_encode_bytes_list.call_count == 3
    assert len(mock_encode_bytes_list.call_args_list[0][0][0]) == max_portion_size
    assert len(mock_encode_bytes_list.call_args_list[1][0][0]) == max_portion_size
    assert len(mock_encode_bytes_list.call_args_list[2][0][0]) == 1


def test_batch_sender_add_span_too_big():
    # This time we set max_payload_bytes to 1000, so we end up sending more batches.
    # Each encoded span is 65 bytes, so we can fit 15 of those in 1000 bytes.
    mock_transport_handler = mock.Mock()
    mock_transport_handler.get_max_payload_bytes = lambda: 1000
    sender = logging_helper.ZipkinBatchSender(mock_transport_handler, 100)
    with sender:
        for _ in range(201):
            sender.add_span(
                span_id='0000000000000002',
                parent_span_id='0000000000000001',
                trace_id='000000000000000f',
                span_name='span',
                annotations=thrift.annotation_list_builder({}, thrift.create_endpoint()),
                binary_annotations=thrift.binary_annotation_list_builder({}, thrift.create_endpoint()),
                timestamp_s=None,
                duration_s=None,
            )
    # 15 spans per batch, means we need upper(201 / 15) = 14 batches to send them all.
    assert mock_transport_handler.call_count == 14
    for i in range(13):
        # The first 13 batches have 15 spans of 65 bytes + 5 bytes of list headers = 980 bytes
        assert len(mock_transport_handler.call_args_list[i][0][0]) == 980
    # The last batch has the 6 remaining spans of 65 bytes + 5 bytes of list headers = 395 bytes
    assert len(mock_transport_handler.call_args_list[13][0][0]) == 395


@mock.patch('py_zipkin.logging_helper.thrift.encode_bytes_list', autospec=True)
def test_batch_sender_flush_calls_transport_handler_with_correct_params(
    mock_encode_bytes_list,
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
            annotations=thrift.annotation_list_builder({}, thrift.create_endpoint()),
            binary_annotations=thrift.binary_annotation_list_builder({}, thrift.create_endpoint()),
            timestamp_s=None,
            duration_s=None,
        )
    transport_handler.assert_called_once_with(mock_encode_bytes_list.return_value)


@mock.patch('py_zipkin.logging_helper.thrift.create_span', autospec=True)
@mock.patch('py_zipkin.logging_helper.thrift.encode_bytes_list', autospec=True)
def test_batch_sender_defensive_about_transport_handler(
        mock_encode_bytes_list,
    create_sp
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
            annotations='ann',
            binary_annotations='binary_ann',
            timestamp_s=None,
            duration_s=None,
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
