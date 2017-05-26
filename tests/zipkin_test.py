import mock
import pytest

import py_zipkin.zipkin as zipkin
from py_zipkin.exception import ZipkinError
from py_zipkin.logging_helper import ZipkinLoggerHandler
from py_zipkin.thread_local import get_zipkin_attrs
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.zipkin import ZipkinAttrs


@mock.patch('py_zipkin.zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.create_attrs_for_span', autospec=True)
@mock.patch('py_zipkin.zipkin.create_endpoint')
@mock.patch('py_zipkin.zipkin.ZipkinLoggerHandler', autospec=True)
@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_for_new_trace(
    logging_context_cls_mock,
    logger_handler_cls_mock,
    create_endpoint_mock,
    create_attrs_for_span_mock,
    push_zipkin_attrs_mock,
    pop_zipkin_attrs_mock,
):
    transport_handler = mock.Mock()
    with zipkin.zipkin_span(
        service_name='some_service_name',
        span_name='span_name',
        transport_handler=transport_handler,
        port=5,
        sample_rate=100.0,
    ) as zipkin_context:
        assert zipkin_context.port == 5
        pass
    create_attrs_for_span_mock.assert_called_once_with(
        sample_rate=100.0,
        use_128bit_trace_id=False,
    )
    push_zipkin_attrs_mock.assert_called_once_with(
        create_attrs_for_span_mock.return_value)
    create_endpoint_mock.assert_called_once_with(5, 'some_service_name', None)
    logger_handler_cls_mock.assert_called_once_with(
        create_attrs_for_span_mock.return_value)
    logging_context_cls_mock.assert_called_once_with(
        create_attrs_for_span_mock.return_value,
        create_endpoint_mock.return_value,
        logger_handler_cls_mock.return_value,
        'span_name',
        transport_handler,
        report_root_timestamp=True,
        binary_annotations={},
        add_logging_annotation=False,
    )
    pop_zipkin_attrs_mock.assert_called_once_with()


@mock.patch('py_zipkin.zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.create_attrs_for_span', autospec=True)
@mock.patch('py_zipkin.zipkin.create_endpoint')
@mock.patch('py_zipkin.zipkin.ZipkinLoggerHandler', autospec=True)
@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_passed_sampled_attrs(
    logging_context_cls_mock,
    logger_handler_cls_mock,
    create_endpoint_mock,
    create_attrs_for_span_mock,
    push_zipkin_attrs_mock,
    pop_zipkin_attrs_mock,
):
    # Make sure that if zipkin_span is passed *sampled* ZipkinAttrs, but is
    # also configured to do sampling itself, the passed ZipkinAttrs are used.
    transport_handler = mock.Mock()
    zipkin_attrs = ZipkinAttrs(
        trace_id='0',
        span_id='1',
        parent_span_id=None,
        flags='0',
        is_sampled=True,
    )
    with zipkin.zipkin_span(
        service_name='some_service_name',
        span_name='span_name',
        transport_handler=transport_handler,
        port=5,
        sample_rate=100.0,
        zipkin_attrs=zipkin_attrs,
    ) as zipkin_context:
        assert zipkin_context.port == 5
    assert not create_attrs_for_span_mock.called
    push_zipkin_attrs_mock.assert_called_once_with(zipkin_attrs)
    create_endpoint_mock.assert_called_once_with(5, 'some_service_name', None)
    logger_handler_cls_mock.assert_called_once_with(zipkin_attrs)
    # Logging context should not report timestamp/duration for the server span,
    # since it's assumed that the client part of this span will do that.
    logging_context_cls_mock.assert_called_once_with(
        zipkin_attrs,
        create_endpoint_mock.return_value,
        logger_handler_cls_mock.return_value,
        'span_name',
        transport_handler,
        report_root_timestamp=False,
        binary_annotations={},
        add_logging_annotation=False,
    )
    pop_zipkin_attrs_mock.assert_called_once_with()


@mock.patch('py_zipkin.zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.create_attrs_for_span', autospec=True)
@mock.patch('py_zipkin.zipkin.create_endpoint')
@mock.patch('py_zipkin.zipkin.ZipkinLoggerHandler', autospec=True)
@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_trace_with_0_sample_rate(
    logging_context_cls_mock,
    logger_handler_cls_mock,
    create_endpoint_mock,
    create_attrs_for_span_mock,
    push_zipkin_attrs_mock,
    pop_zipkin_attrs_mock,
):
    create_attrs_for_span_mock.return_value = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags='0',
        is_sampled=False,
    )
    with zipkin.zipkin_span(
        service_name='some_service_name',
        span_name='span_name',
        transport_handler=mock.Mock(),
        sample_rate=0.0,
    ) as zipkin_context:
        assert zipkin_context.port == 0
        pass
    create_attrs_for_span_mock.assert_called_once_with(
        sample_rate=0.0,
        use_128bit_trace_id=False,
    )
    push_zipkin_attrs_mock.assert_called_once_with(
        create_attrs_for_span_mock.return_value)
    assert create_endpoint_mock.call_count == 0
    assert logger_handler_cls_mock.call_count == 0
    assert logging_context_cls_mock.call_count == 0
    pop_zipkin_attrs_mock.assert_called_once_with()


def test_zipkin_span_sample_rate_required_params():
    # Missing transport_handler
    with pytest.raises(ZipkinError):
        with zipkin.zipkin_span(
            service_name='some_service_name',
            span_name='span_name',
            port=5,
            sample_rate=100.0,
        ):
            pass


def test_zipkin_invalid_sample_rate():
    with pytest.raises(ZipkinError):
        with zipkin.zipkin_span(
            service_name='some_service_name',
            span_name='span_name',
            transport_handler=mock.Mock(),
            sample_rate=101.0,
        ):
            pass

    with pytest.raises(ZipkinError):
        with zipkin.zipkin_span(
            service_name='some_service_name',
            span_name='span_name',
            transport_handler=mock.Mock(),
            sample_rate=-0.1,
        ):
            pass


def test_zipkin_invalid_include():
    with pytest.raises(ZipkinError):
        with zipkin.zipkin_span(
            service_name='some_service_name',
            span_name='span_name',
            transport_handler=mock.Mock(),
            sample_rate=100.0,
            include=('clawyant',)
        ):
            pass


@pytest.mark.parametrize('span_func', [
    zipkin.zipkin_client_span,
    zipkin.zipkin_server_span,
])
@mock.patch('py_zipkin.zipkin.zipkin_span', autospec=True)
def test_zipkin_extraneous_include_raises(mock_zipkin_span, span_func):
    with pytest.raises(ValueError):
        with span_func(
            service_name='some_service_name',
            span_name='span_name',
            transport_handler=mock.Mock(),
            sample_rate=100.0,
            include=('foobar',)
        ):
            assert mock_zipkin_span.__init__.call_count == 0


@mock.patch('py_zipkin.zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.create_attrs_for_span', autospec=True)
@mock.patch('py_zipkin.zipkin.create_endpoint')
@mock.patch('py_zipkin.zipkin.ZipkinLoggerHandler', autospec=True)
@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_trace_with_no_sampling(
    logging_context_cls_mock,
    logger_handler_cls_mock,
    create_endpoint_mock,
    create_attrs_for_span_mock,
    push_zipkin_attrs_mock,
    pop_zipkin_attrs_mock,
):
    zipkin_attrs = ZipkinAttrs(
        trace_id='0',
        span_id='1',
        parent_span_id=None,
        flags='0',
        is_sampled=False,
    )
    with zipkin.zipkin_span(
        service_name='my_service',
        span_name='span_name',
        zipkin_attrs=zipkin_attrs,
        transport_handler=mock.Mock(),
        port=5,
    ):
        pass
    assert create_attrs_for_span_mock.call_count == 0
    push_zipkin_attrs_mock.assert_called_once_with(zipkin_attrs)
    assert create_endpoint_mock.call_count == 0
    assert logger_handler_cls_mock.call_count == 0
    assert logging_context_cls_mock.call_count == 0
    pop_zipkin_attrs_mock.assert_called_once_with()


def test_zipkin_span_with_zipkin_attrs_required_params():
    # Missing transport_handler
    with pytest.raises(ZipkinError):
        with zipkin.zipkin_span(
            service_name='some_service_name',
            span_name='span_name',
            zipkin_attrs=mock.Mock(),
            port=5,
        ):
            pass


@mock.patch('py_zipkin.zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.create_attrs_for_span', autospec=True)
@mock.patch('py_zipkin.zipkin.create_endpoint')
@mock.patch('py_zipkin.zipkin.ZipkinLoggerHandler', autospec=True)
@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_trace_context_attrs_is_always_popped(
    logging_context_cls_mock,
    logger_handler_cls_mock,
    create_endpoint_mock,
    create_attrs_for_span_mock,
    push_zipkin_attrs_mock,
    pop_zipkin_attrs_mock,
):
    with pytest.raises(Exception):
        with zipkin.zipkin_span(
            service_name='my_service',
            span_name='my_span_name',
            transport_handler=mock.Mock(),
            port=22,
            sample_rate=100.0,
        ):
            raise Exception
    pop_zipkin_attrs_mock.assert_called_once_with()


@mock.patch('py_zipkin.zipkin.get_zipkin_attrs', autospec=True)
def test_create_headers_for_new_span_empty_if_no_active_request(get_mock):
    get_mock.return_value = None
    assert {} == zipkin.create_http_headers_for_new_span()


@mock.patch('py_zipkin.zipkin.get_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.generate_random_64bit_string', autospec=True)
def test_create_headers_for_new_span_returns_header_if_active_request(
        gen_mock, get_mock):
    get_mock.return_value = mock.Mock(
        trace_id='27133d482ba4f605', span_id='37133d482ba4f605', is_sampled=True)
    gen_mock.return_value = '17133d482ba4f605'
    expected = {
        'X-B3-TraceId': '27133d482ba4f605',
        'X-B3-SpanId': '17133d482ba4f605',
        'X-B3-ParentSpanId': '37133d482ba4f605',
        'X-B3-Flags': '0',
        'X-B3-Sampled': '1',
    }
    assert expected == zipkin.create_http_headers_for_new_span()


@mock.patch('py_zipkin.zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.get_zipkin_attrs', autospec=True)
def test_span_context_no_zipkin_attrs(
    get_zipkin_attrs_mock,
    push_zipkin_attrs_mock,
    pop_zipkin_attrs_mock,
):
    # When not in a Zipkin context, don't do anything
    get_zipkin_attrs_mock.return_value = None
    context = zipkin.zipkin_span(service_name='my_service')
    with context:
        pass
    assert not pop_zipkin_attrs_mock.called
    assert not push_zipkin_attrs_mock.called


@mock.patch('py_zipkin.thread_local._thread_local', autospec=True)
@mock.patch('py_zipkin.zipkin.generate_random_64bit_string', autospec=True)
@mock.patch('py_zipkin.zipkin.zipkin_logger', autospec=True)
def test_span_context_sampled_no_handlers(
    zipkin_logger_mock,
    generate_string_mock,
    thread_local_mock,
):
    zipkin_attrs = ZipkinAttrs(
        trace_id='1111111111111111',
        span_id='2222222222222222',
        parent_span_id='3333333333333333',
        flags='flags',
        is_sampled=True,
    )
    thread_local_mock.zipkin_attrs = [zipkin_attrs]

    zipkin_logger_mock.handlers = []
    generate_string_mock.return_value = '1'

    context = zipkin.zipkin_span(
        service_name='my_service',
        port=5,
        transport_handler=mock.Mock(),
        sample_rate=0.0,
    )
    with context:
        # Assert that the new ZipkinAttrs were saved
        new_zipkin_attrs = get_zipkin_attrs()
        assert new_zipkin_attrs.span_id == '1'

    # Outside of the context, things should be returned to normal
    assert get_zipkin_attrs() == zipkin_attrs


@pytest.mark.parametrize('span_func, expected_annotations', [
    (zipkin.zipkin_span, ('cs', 'cr', 'ss', 'sr')),
    (zipkin.zipkin_client_span, ('cs', 'cr')),
    (zipkin.zipkin_server_span, ('ss', 'sr')),
])
@mock.patch('py_zipkin.thread_local._thread_local', autospec=True)
@mock.patch('py_zipkin.zipkin.generate_random_64bit_string', autospec=True)
@mock.patch('py_zipkin.zipkin.generate_random_128bit_string', autospec=True)
@mock.patch('py_zipkin.zipkin.zipkin_logger', autospec=True)
def test_span_context(
    zipkin_logger_mock,
    generate_string_128bit_mock,
    generate_string_mock,
    thread_local_mock,
    span_func,
    expected_annotations,
):
    zipkin_attrs = ZipkinAttrs(
        trace_id='1111111111111111',
        span_id='2222222222222222',
        parent_span_id='3333333333333333',
        flags='flags',
        is_sampled=True,
    )
    thread_local_mock.zipkin_attrs = [zipkin_attrs]
    logging_handler = ZipkinLoggerHandler(zipkin_attrs)
    assert logging_handler.parent_span_id is None
    assert logging_handler.client_spans == []

    zipkin_logger_mock.handlers = [logging_handler]
    generate_string_mock.return_value = '1'

    context = span_func(
        service_name='svc',
        span_name='span',
        annotations={'something': 1},
        binary_annotations={'foo': 'bar'},
    )
    with context:
        # Assert that the new ZipkinAttrs were saved
        new_zipkin_attrs = get_zipkin_attrs()
        assert new_zipkin_attrs.span_id == '1'
        # And that the logging handler has a parent_span_id
        assert logging_handler.parent_span_id == '1'

    # Outside of the context, things should be returned to normal,
    # except a new client span is saved in the handler
    assert logging_handler.parent_span_id is None
    assert get_zipkin_attrs() == zipkin_attrs

    client_span = logging_handler.client_spans.pop()
    assert logging_handler.client_spans == []
    # These reserved annotations are based on timestamps so pop em.
    # This also acts as a check that they exist.
    for annotation in expected_annotations:
        client_span['annotations'].pop(annotation)

    expected_client_span = {
        'span_name': 'span',
        'service_name': 'svc',
        'parent_span_id': None,
        'span_id': '1',
        'annotations': {'something': 1},
        'binary_annotations': {'foo': 'bar'},
    }
    assert client_span == expected_client_span

    assert generate_string_128bit_mock.call_count == 0


@mock.patch('py_zipkin.zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('py_zipkin.zipkin.create_attrs_for_span', autospec=True)
@mock.patch('py_zipkin.zipkin.create_endpoint')
@mock.patch('py_zipkin.zipkin.ZipkinLoggerHandler', autospec=True)
@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_decorator(
    logging_context_cls_mock,
    logger_handler_cls_mock,
    create_endpoint_mock,
    create_attrs_for_span_mock,
    push_zipkin_attrs_mock,
    pop_zipkin_attrs_mock,
):
    transport_handler = mock.Mock()

    @zipkin.zipkin_span(
        service_name='some_service_name',
        span_name='span_name',
        transport_handler=transport_handler,
        port=5,
        sample_rate=100.0,
        host='1.5.1.2',
    )
    def test_func(a, b):
        return a + b

    assert test_func(1, 2) == 3

    create_attrs_for_span_mock.assert_called_once_with(
        sample_rate=100.0,
        use_128bit_trace_id=False,
    )
    push_zipkin_attrs_mock.assert_called_once_with(
        create_attrs_for_span_mock.return_value)
    create_endpoint_mock.assert_called_once_with(5, 'some_service_name', '1.5.1.2')
    logger_handler_cls_mock.assert_called_once_with(
        create_attrs_for_span_mock.return_value)
    # The decorator was passed a sample rate and no Zipkin attrs, so it's
    # assumed to be the root of a trace and it should report timestamp/duration
    logging_context_cls_mock.assert_called_once_with(
        create_attrs_for_span_mock.return_value,
        create_endpoint_mock.return_value,
        logger_handler_cls_mock.return_value,
        'span_name',
        transport_handler,
        report_root_timestamp=True,
        binary_annotations={},
        add_logging_annotation=False,
    )
    pop_zipkin_attrs_mock.assert_called_once_with()


@mock.patch('py_zipkin.zipkin.create_endpoint', wraps=zipkin.create_endpoint)
def test_zipkin_span_decorator_many(create_endpoint_mock):
    @zipkin.zipkin_span(service_name='decorator')
    def test_func(a, b):
        return a + b

    assert test_func(1, 2) == 3
    assert create_endpoint_mock.call_count == 0
    with zipkin.zipkin_span(
        service_name='context_manager',
        transport_handler=mock.Mock(),
        sample_rate=100.0,
    ):
        assert test_func(1, 2) == 3
    assert create_endpoint_mock.call_count == 1
    assert test_func(1, 2) == 3
    assert create_endpoint_mock.call_count == 1


@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_add_logging_annotation(mock_context):
    with zipkin.zipkin_span(
        service_name='my_service',
        transport_handler=mock.Mock(),
        sample_rate=100.0,
        add_logging_annotation=True,
    ):
        pass
    _, kwargs = mock_context.call_args
    assert kwargs['add_logging_annotation']


def test_update_binary_annotations():
    zipkin_attrs = ZipkinAttrs(
        trace_id='0',
        span_id='1',
        parent_span_id=None,
        flags='0',
        is_sampled=True,
    )
    context = zipkin.zipkin_span(
        service_name='my_service',
        span_name='span_name',
        zipkin_attrs=zipkin_attrs,
        transport_handler=mock.Mock(),
        port=5,
    )

    with context:
        assert 'test' not in context.logging_context.binary_annotations_dict
        context.update_binary_annotations({'test': 'hi'})
        assert context.logging_context.binary_annotations_dict['test'] == 'hi'

        nested_context = zipkin.zipkin_span(
            service_name='my_service',
            span_name='nested_span',
            binary_annotations={'one': 'one'},
        )
        with nested_context:
            assert 'one' not in context.logging_context.binary_annotations_dict
            nested_context.update_binary_annotations({'two': 'two'})
            assert 'two' in nested_context.binary_annotations
            assert 'two' not in context.logging_context.binary_annotations_dict


def test_update_binary_annotations_should_not_error_if_not_tracing():
    zipkin_attrs = ZipkinAttrs(
        trace_id='0',
        span_id='1',
        parent_span_id=None,
        flags='0',
        is_sampled=False,
    )
    context = zipkin.zipkin_span(
        service_name='my_service',
        span_name='span_name',
        zipkin_attrs=zipkin_attrs,
        transport_handler=mock.Mock(),
        port=5,
    )

    with context:
        # A non-sampled request should result in a no-op
        context.update_binary_annotations({'test': 'hi'})


def test_update_binary_annotations_should_not_error_for_child_spans():
    non_tracing_context = zipkin.zipkin_span(
        service_name='my_service',
        span_name='span_name',
    )
    with non_tracing_context:
        # Updating the binary annotations for a non-tracing child span
        # should result in a no-op
        non_tracing_context.update_binary_annotations({'test': 'hi'})


@mock.patch('py_zipkin.zipkin.generate_random_128bit_string', autospec=True)
@mock.patch('py_zipkin.zipkin.generate_random_64bit_string', autospec=True)
def test_create_attrs_for_span(random_64bit_mock, random_128bit_mock):
    random_64bit_mock.return_value = '0000000000000042'
    expected_attrs = ZipkinAttrs(
        trace_id='0000000000000042',
        span_id='0000000000000042',
        parent_span_id=None,
        flags='0',
        is_sampled=True,
    )
    assert expected_attrs == zipkin.create_attrs_for_span()

    # Test overrides
    expected_attrs = ZipkinAttrs(
        trace_id='0000000000000045',
        span_id='0000000000000046',
        parent_span_id=None,
        flags='0',
        is_sampled=False,
    )
    assert expected_attrs == zipkin.create_attrs_for_span(
        sample_rate=0.0,
        trace_id='0000000000000045',
        span_id='0000000000000046',
    )

    random_128bit_mock.return_value = '00000000000000420000000000000042'
    expected_attrs = ZipkinAttrs(
        trace_id='00000000000000420000000000000042',
        span_id='0000000000000042',
        parent_span_id=None,
        flags='0',
        is_sampled=True,
    )
    assert expected_attrs == zipkin.create_attrs_for_span(
        use_128bit_trace_id=True,
    )
