import mock
import pytest

from py_zipkin import logging_helper
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


def mock_transport_handler(message):
    return message


@pytest.fixture
def context():
    attr = ZipkinAttrs(None, None, None, None, False)
    log_handler = logging_helper.ZipkinLoggerHandler(attr)
    return logging_helper.ZipkinLoggingContext(
        zipkin_attrs=attr,
        thrift_endpoint='thrift_endpoint',
        log_handler=log_handler,
        span_name='span_name',
        transport_handler=mock_transport_handler,
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
@mock.patch('py_zipkin.logging_helper.log_span', autospec=True)
@mock.patch('py_zipkin.logging_helper.annotation_list_builder',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.binary_annotation_list_builder',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.copy_endpoint_with_new_service_name',
            autospec=True)
def test_zipkin_logging_server_context_log_spans(
    copy_endpoint_mock, bin_ann_list_builder, ann_list_builder,
    log_span_mock, time_mock
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
    client_log_call, server_log_call = log_span_mock.call_args_list
    assert server_log_call[1] == {
        'span_id': server_span_id,
        'parent_span_id': parent_span_id,
        'trace_id': trace_id,
        'span_name': 'GET /foo',
        'annotations': expected_server_annotations,
        'binary_annotations': expected_server_bin_annotations,
        'transport_handler': transport_handler,
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
        'transport_handler': transport_handler,
        'duration_s': 4,
        'timestamp_s': 26,
    }


@mock.patch('py_zipkin.logging_helper.time.time', autospec=True)
@mock.patch('py_zipkin.logging_helper.log_span', autospec=True)
@mock.patch('py_zipkin.logging_helper.annotation_list_builder',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.binary_annotation_list_builder',
            autospec=True)
@mock.patch('py_zipkin.logging_helper.copy_endpoint_with_new_service_name',
            autospec=True)
def test_zipkin_logging_client_context_log_spans(
    copy_endpoint_mock, bin_ann_list_builder, ann_list_builder,
    log_span_mock, time_mock
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
    log_call = log_span_mock.call_args_list[0]
    assert log_call[1] == {
        'span_id': client_span_id,
        'parent_span_id': None,
        'trace_id': trace_id,
        'span_name': 'GET /foo',
        'annotations': expected_server_annotations,
        'binary_annotations': expected_server_bin_annotations,
        'transport_handler': transport_handler,
        'duration_s': 18,
        'timestamp_s': 24,
    }


@mock.patch('py_zipkin.logging_helper.log_span', autospec=True)
def test_log_span_not_called_if_not_sampled(log_span_mock):
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
    assert log_span_mock.call_count == 0


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


@mock.patch('py_zipkin.logging_helper.thrift_obj_in_bytes', autospec=True)
def test_log_span(thrift_obj):
    # Not much logic here, so this is basically a smoke test

    logging_helper.log_span(
        span_id='0000000000000002',
        parent_span_id='0000000000000001',
        trace_id='000000000000000f',
        span_name='span',
        annotations='ann',
        binary_annotations='binary_ann',
        timestamp_s=None,
        duration_s=None,
        transport_handler=mock_transport_handler,
    )
    assert thrift_obj.call_count == 1


@mock.patch('py_zipkin.logging_helper.create_span', autospec=True)
@mock.patch('py_zipkin.logging_helper.thrift_obj_in_bytes', autospec=True)
def test_log_span_calls_transport_handler_with_correct_params(
    thrift_obj,
    create_sp
):
    transport_handler = mock.Mock()
    logging_helper.log_span(
        span_id='0000000000000002',
        parent_span_id='0000000000000001',
        trace_id='00000000000000015',
        span_name='span',
        annotations='ann',
        binary_annotations='binary_ann',
        timestamp_s=None,
        duration_s=None,
        transport_handler=transport_handler,
    )
    transport_handler.assert_called_once_with(thrift_obj.return_value)


@mock.patch('py_zipkin.logging_helper.create_span', autospec=True)
@mock.patch('py_zipkin.logging_helper.thrift_obj_in_bytes', autospec=True)
def test_log_span_defensive_about_transport_handler(
    thrift_obj,
    create_sp
):
    """Make sure log_span doesn't try to call the transport handler if it's
    None."""
    logging_helper.log_span(
        span_id='0000000000000002',
        parent_span_id='0000000000000001',
        trace_id='00000000000000015',
        span_name='span',
        annotations='ann',
        binary_annotations='binary_ann',
        timestamp_s=None,
        duration_s=None,
        transport_handler=None,
    )
    assert thrift_obj.call_count == 0
    assert create_sp.call_count == 0


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
