import mock
import pytest

import python_zipkin as zipkin
from python_zipkin import ZipkinAttrs
from python_zipkin.logging_helper import ZipkinLoggerHandler
from python_zipkin.thread_local import get_zipkin_attrs


@mock.patch('python_zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.create_attrs_for_root_span', autospec=True)
@mock.patch('python_zipkin.create_endpoint')
@mock.patch('python_zipkin.ZipkinLoggerHandler', autospec=True)
@mock.patch('python_zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_trace_context(
    logging_context_cls_mock,
    logger_handler_cls_mock,
    create_endpoint_mock,
    create_attrs_for_root_span_mock,
    push_zipkin_attrs_mock,
    pop_zipkin_attrs_mock,
):
    transport_handler = mock.Mock()
    with zipkin.zipkin_trace_context(
        'span_name',
        transport_handler,
        port=5,
        service_name='some_service_name',
    ):
        pass
    create_attrs_for_root_span_mock.assert_called_once_with()
    push_zipkin_attrs_mock.assert_called_once_with(
        create_attrs_for_root_span_mock.return_value)
    create_endpoint_mock.assert_called_once_with(5, 'some_service_name')
    logger_handler_cls_mock.assert_called_once_with(
        create_attrs_for_root_span_mock.return_value)
    logging_context_cls_mock.assert_called_once_with(
        create_attrs_for_root_span_mock.return_value,
        create_endpoint_mock.return_value,
        logger_handler_cls_mock.return_value,
        'span_name',
        transport_handler,
    )
    pop_zipkin_attrs_mock.assert_called_once_with()


@mock.patch('python_zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.create_attrs_for_root_span', autospec=True)
@mock.patch('python_zipkin.create_endpoint')
@mock.patch('python_zipkin.ZipkinLoggerHandler', autospec=True)
@mock.patch('python_zipkin.ZipkinLoggingContext', autospec=True)
def test_non_sampled_zipkin_trace_context(
    logging_context_cls_mock,
    logger_handler_cls_mock,
    create_endpoint_mock,
    create_attrs_for_root_span_mock,
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
    transport_handler = mock.Mock()
    with zipkin.zipkin_trace_context('span_name', transport_handler, zipkin_attrs):
        pass
    assert create_attrs_for_root_span_mock.call_count == 0
    push_zipkin_attrs_mock.assert_called_once_with(zipkin_attrs)
    assert create_endpoint_mock.call_count == 0
    assert logger_handler_cls_mock.call_count == 0
    assert logging_context_cls_mock.call_count == 0
    pop_zipkin_attrs_mock.assert_called_once_with()


@mock.patch('python_zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.create_attrs_for_root_span', autospec=True)
@mock.patch('python_zipkin.create_endpoint')
@mock.patch('python_zipkin.ZipkinLoggerHandler', autospec=True)
@mock.patch('python_zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_trace_context_attrs_is_always_popped(
    logging_context_cls_mock,
    logger_handler_cls_mock,
    create_endpoint_mock,
    create_attrs_for_root_span_mock,
    push_zipkin_attrs_mock,
    pop_zipkin_attrs_mock,
):
    transport_handler = mock.Mock()
    with pytest.raises(Exception):
        with zipkin.zipkin_trace_context('span_name', transport_handler):
            raise Exception
    pop_zipkin_attrs_mock.assert_called_once_with()


@mock.patch('python_zipkin.get_zipkin_attrs', autospec=True)
def test_create_headers_for_new_span_empty_if_no_active_request(get_mock):
    get_mock.return_value = None
    assert {} == zipkin.create_http_headers_for_new_span()


@mock.patch('python_zipkin.get_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.generate_random_64bit_string', autospec=True)
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


@mock.patch('python_zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.get_zipkin_attrs', autospec=True)
def test_span_context_no_zipkin_attrs(
    get_zipkin_attrs_mock,
    push_zipkin_attrs_mock,
    pop_zipkin_attrs_mock,
):
    # When not in a Zipkin context, don't do anything
    get_zipkin_attrs_mock.return_value = None
    context = zipkin.SpanContext('svc', 'span')
    with context:
        pass
    assert not pop_zipkin_attrs_mock.called
    assert not push_zipkin_attrs_mock.called


@mock.patch('python_zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.get_zipkin_attrs', autospec=True)
def test_span_context_not_sampled(
    get_zipkin_attrs_mock,
    push_zipkin_attrs_mock,
    pop_zipkin_attrs_mock,
):
    # When ZipkinAttrs say this request isn't sampled, push new attrs
    # onto threadlocal stack, but do nothing else.
    get_zipkin_attrs_mock.return_value = ZipkinAttrs(
        'trace_id', 'span_id', 'parent_span_id', 'flags', False)
    context = zipkin.SpanContext('svc', 'span')
    with context:
        pass
    # Even in the not-sampled case, if the client context generates
    # new zipkin attrs, it should pop them off.
    assert pop_zipkin_attrs_mock.called
    assert push_zipkin_attrs_mock.called


@mock.patch('python_zipkin.thread_local._thread_local', autospec=True)
@mock.patch('python_zipkin.generate_random_64bit_string', autospec=True)
@mock.patch('python_zipkin.zipkin_logger', autospec=True)
def test_span_context_sampled_no_handlers(
    zipkin_logger_mock,
    generate_string_mock,
    thread_local_mock,
):
    zipkin_attrs = ZipkinAttrs(
        'trace_id', 'span_id', 'parent_span_id', 'flags', True)
    thread_local_mock.requests = [zipkin_attrs]

    zipkin_logger_mock.handlers = []
    generate_string_mock.return_value = '1'

    context = zipkin.SpanContext('svc', 'span')
    with context:
        # Assert that the new ZipkinAttrs were saved
        new_zipkin_attrs = get_zipkin_attrs()
        assert new_zipkin_attrs.span_id == '1'

    # Outside of the context, things should be returned to normal
    assert get_zipkin_attrs() == zipkin_attrs


@mock.patch('python_zipkin.thread_local._thread_local', autospec=True)
@mock.patch('python_zipkin.generate_random_64bit_string', autospec=True)
@mock.patch('python_zipkin.zipkin_logger', autospec=True)
def test_span_context(
    zipkin_logger_mock,
    generate_string_mock,
    thread_local_mock,
):
    zipkin_attrs = ZipkinAttrs(
        'trace_id', 'span_id', 'parent_span_id', 'flags', True)
    thread_local_mock.requests = [zipkin_attrs]
    logging_handler = ZipkinLoggerHandler(zipkin_attrs)
    assert logging_handler.parent_span_id is None
    assert logging_handler.client_spans == []

    zipkin_logger_mock.handlers = [logging_handler]
    generate_string_mock.return_value = '1'

    context = zipkin.SpanContext(
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
    for annotation in ('cs', 'cr', 'ss', 'sr'):
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


@mock.patch('python_zipkin.SpanContext', autospec=True)
def test_zipkin_span_decorator(mock_span_context):

    service_name = 'my_service'
    span_name = 'my_span'
    binary_annotations = {'a': '1'}

    @zipkin.zipkin_span(service_name, span_name, binary_annotations)
    def some_function(a, b):
        return a + b

    assert some_function(1, 2) == 3

    expected_call = mock.call(
        service_name=service_name,
        span_name=span_name,
        binary_annotations=binary_annotations,
    )
    assert expected_call == mock_span_context.call_args


@mock.patch('python_zipkin.SpanContext', autospec=True)
def test_decorator_default_span_name(mock_span_context):

    service_name = 'my_service'

    @zipkin.zipkin_span(service_name)
    def some_function(a, b):
        return a + b

    assert some_function(1, 2) == 3

    expected_call = mock.call(
        service_name=service_name,
        span_name='some_function',
        binary_annotations=None,
    )
    assert expected_call == mock_span_context.call_args


@mock.patch('python_zipkin.generate_random_64bit_string', autospec=True)
def test_create_attrs_for_root_span(random_mock):
    random_mock.return_value = 42
    expected_attrs = ZipkinAttrs(
        trace_id=42,
        span_id=42,
        parent_span_id=None,
        flags='0',
        is_sampled=True,
    )
    assert expected_attrs == zipkin.create_attrs_for_root_span()
