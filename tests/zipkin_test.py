import mock
import pytest

import python_zipkin.zipkin as zipkin
from python_zipkin.exception import ZipkinError
from python_zipkin.logging_helper import ZipkinLoggerHandler
from python_zipkin.logging_helper import zipkin_logger
from python_zipkin.thread_local import get_zipkin_attrs
from python_zipkin.thrift import zipkin_core
from python_zipkin.util import generate_random_64bit_string
from python_zipkin.zipkin import ZipkinAttrs
from thriftpy.protocol.binary import TBinaryProtocol
from thriftpy.transport import TMemoryBuffer


@mock.patch('python_zipkin.zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.zipkin.create_attrs_for_root_span', autospec=True)
@mock.patch('python_zipkin.zipkin.create_endpoint')
@mock.patch('python_zipkin.zipkin.ZipkinLoggerHandler', autospec=True)
@mock.patch('python_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_for_new_trace(
    logging_context_cls_mock,
    logger_handler_cls_mock,
    create_endpoint_mock,
    create_attrs_for_root_span_mock,
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
    create_attrs_for_root_span_mock.assert_called_once_with(sample_rate=100.0)
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
        {},
    )
    pop_zipkin_attrs_mock.assert_called_once_with()


@mock.patch('python_zipkin.zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.zipkin.create_attrs_for_root_span', autospec=True)
@mock.patch('python_zipkin.zipkin.create_endpoint')
@mock.patch('python_zipkin.zipkin.ZipkinLoggerHandler', autospec=True)
@mock.patch('python_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_trace_with_0_sample_rate(
    logging_context_cls_mock,
    logger_handler_cls_mock,
    create_endpoint_mock,
    create_attrs_for_root_span_mock,
    push_zipkin_attrs_mock,
    pop_zipkin_attrs_mock,
):
    create_attrs_for_root_span_mock.return_value = ZipkinAttrs(
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
    create_attrs_for_root_span_mock.assert_called_once_with(sample_rate=0.0)
    push_zipkin_attrs_mock.assert_called_once_with(
        create_attrs_for_root_span_mock.return_value)
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
            sample_rate=101.0,
        ):
            pass

    with pytest.raises(ZipkinError):
        with zipkin.zipkin_span(
            service_name='some_service_name',
            span_name='span_name',
            sample_rate=-0.1,
        ):
            pass


@mock.patch('python_zipkin.zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.zipkin.create_attrs_for_root_span', autospec=True)
@mock.patch('python_zipkin.zipkin.create_endpoint')
@mock.patch('python_zipkin.zipkin.ZipkinLoggerHandler', autospec=True)
@mock.patch('python_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_trace_with_no_sampling(
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
    with zipkin.zipkin_span(
        service_name='my_service',
        span_name='span_name',
        zipkin_attrs=zipkin_attrs,
        transport_handler=mock.Mock(),
        port=5,
        is_service=True,
    ):
        pass
    assert create_attrs_for_root_span_mock.call_count == 0
    push_zipkin_attrs_mock.assert_called_once_with(zipkin_attrs)
    assert create_endpoint_mock.call_count == 0
    assert logger_handler_cls_mock.call_count == 0
    assert logging_context_cls_mock.call_count == 0
    pop_zipkin_attrs_mock.assert_called_once_with()


def test_zipkin_span_is_service_required_params():
    # Missing zipkin_attrs
    with pytest.raises(ZipkinError):
        with zipkin.zipkin_span(
            service_name='some_service_name',
            span_name='span_name',
            port=5,
            transport_handler=mock.Mock(),
            is_service=True,
        ):
            pass

    # Missing port
    with pytest.raises(ZipkinError):
        with zipkin.zipkin_span(
            service_name='some_service_name',
            span_name='span_name',
            zipkin_attrs=mock.Mock(),
            transport_handler=mock.Mock(),
            is_service=True,
        ):
            pass

    # Missing transport_handler
    with pytest.raises(ZipkinError):
        with zipkin.zipkin_span(
            service_name='some_service_name',
            span_name='span_name',
            zipkin_attrs=mock.Mock(),
            port=5,
            is_service=True,
        ):
            pass


@mock.patch('python_zipkin.zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.zipkin.create_attrs_for_root_span', autospec=True)
@mock.patch('python_zipkin.zipkin.create_endpoint')
@mock.patch('python_zipkin.zipkin.ZipkinLoggerHandler', autospec=True)
@mock.patch('python_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_trace_context_attrs_is_always_popped(
    logging_context_cls_mock,
    logger_handler_cls_mock,
    create_endpoint_mock,
    create_attrs_for_root_span_mock,
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


@mock.patch('python_zipkin.zipkin.get_zipkin_attrs', autospec=True)
def test_create_headers_for_new_span_empty_if_no_active_request(get_mock):
    get_mock.return_value = None
    assert {} == zipkin.create_http_headers_for_new_span()


@mock.patch('python_zipkin.zipkin.get_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.zipkin.generate_random_64bit_string', autospec=True)
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


@mock.patch('python_zipkin.zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.zipkin.get_zipkin_attrs', autospec=True)
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


@mock.patch('python_zipkin.thread_local._thread_local', autospec=True)
@mock.patch('python_zipkin.zipkin.generate_random_64bit_string', autospec=True)
@mock.patch('python_zipkin.zipkin.zipkin_logger', autospec=True)
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


@mock.patch('python_zipkin.thread_local._thread_local', autospec=True)
@mock.patch('python_zipkin.zipkin.generate_random_64bit_string', autospec=True)
@mock.patch('python_zipkin.zipkin.zipkin_logger', autospec=True)
def test_span_context(
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
    logging_handler = ZipkinLoggerHandler(zipkin_attrs)
    assert logging_handler.parent_span_id is None
    assert logging_handler.client_spans == []

    zipkin_logger_mock.handlers = [logging_handler]
    generate_string_mock.return_value = '1'

    context = zipkin.zipkin_span(
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


@mock.patch('python_zipkin.zipkin.pop_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.zipkin.push_zipkin_attrs', autospec=True)
@mock.patch('python_zipkin.zipkin.create_attrs_for_root_span', autospec=True)
@mock.patch('python_zipkin.zipkin.create_endpoint')
@mock.patch('python_zipkin.zipkin.ZipkinLoggerHandler', autospec=True)
@mock.patch('python_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_decorator(
    logging_context_cls_mock,
    logger_handler_cls_mock,
    create_endpoint_mock,
    create_attrs_for_root_span_mock,
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
    )
    def test_func(a, b):
        return a + b

    assert test_func(1, 2) == 3

    create_attrs_for_root_span_mock.assert_called_once_with(sample_rate=100.0)
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
        {},
    )
    pop_zipkin_attrs_mock.assert_called_once_with()


def test_update_binary_annotations_for_root_span():
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
        is_service=True,
    )

    with context:
        assert 'test' not in context.logging_context.binary_annotations_dict
        context.update_binary_annotations_for_root_span({'test': 'hi'})
        assert context.logging_context.binary_annotations_dict['test'] == 'hi'


def test_update_binary_annotations_for_root_span_errors():
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
        is_service=True,
    )

    with context:
        # A non-sampled request should result in a no-op
        context.update_binary_annotations_for_root_span({'test': 'hi'})

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
        is_service=True,
    )
    # Updating binary annotations without logging set up should error
    with pytest.raises(ZipkinError):
        context.update_binary_annotations_for_root_span({'test': 'hi'})


@mock.patch('python_zipkin.zipkin.generate_random_64bit_string', autospec=True)
def test_create_attrs_for_root_span(random_mock):
    random_mock.return_value = '0000000000000042'
    expected_attrs = ZipkinAttrs(
        trace_id='0000000000000042',
        span_id='0000000000000042',
        parent_span_id=None,
        flags='0',
        is_sampled=True,
    )
    assert expected_attrs == zipkin.create_attrs_for_root_span()


mock_logger = []


def example_transport_handler(message):
    mock_logger.append(message)


def _decode_binary_thrift_obj(obj):
    trans = TMemoryBuffer(obj)
    span = zipkin_core.Span()
    span.read(TBinaryProtocol(trans))
    return span


def test_starting_zipkin_trace_with_sampling_rate():
    del mock_logger[:]
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=example_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
    ):
        pass

    span = _decode_binary_thrift_obj(mock_logger[0])

    assert span.name == 'test_span_name'
    assert span.annotations[0].host.service_name == 'test_service_name'
    assert span.parent_id is None
    assert span.binary_annotations[0].key == 'some_key'
    assert span.binary_annotations[0].value == 'some_value'
    assert set([ann.value for ann in span.annotations]) == set(['ss', 'sr'])


def test_span_inside_trace():
    del mock_logger[:]
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=example_transport_handler,
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

    root_span = _decode_binary_thrift_obj(mock_logger[1])
    nested_span = _decode_binary_thrift_obj(mock_logger[0])
    assert nested_span.name == 'nested_span'
    assert nested_span.annotations[0].host.service_name == 'nested_service'
    assert nested_span.parent_id == root_span.id
    assert nested_span.binary_annotations[0].key == 'nested_key'
    assert nested_span.binary_annotations[0].value == 'nested_value'
    assert len(nested_span.annotations) == 5
    assert set([ann.value for ann in nested_span.annotations]) == set([
        'ss', 'sr', 'cs', 'cr', 'nested_annotation'])
    for ann in nested_span.annotations:
        if ann.value == 'nested_annotation':
            assert ann.timestamp == 43000000


def test_service_span():
    del mock_logger[:]
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
        transport_handler=example_transport_handler,
        port=45,
        is_service=True,
        binary_annotations={'some_key': 'some_value'},
    ):
        pass

    span = _decode_binary_thrift_obj(mock_logger[0])
    assert span.name == 'service_span'
    assert span.annotations[0].host.service_name == 'test_service_name'
    assert span.parent_id == 2
    assert span.binary_annotations[0].key == 'some_key'
    assert span.binary_annotations[0].value == 'some_value'
    assert set([ann.value for ann in span.annotations]) == set(['ss', 'sr'])


def test_log_debug_for_new_span():
    del mock_logger[:]
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=example_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
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

    logged_span = _decode_binary_thrift_obj(mock_logger[0])
    root_span = _decode_binary_thrift_obj(mock_logger[1])
    assert logged_span.name == 'logged_name'
    assert logged_span.annotations[0].host.service_name == 'logged_service_name'
    assert logged_span.parent_id == root_span.id
    assert logged_span.binary_annotations[0].key == 'logged_binary_annotation'
    assert logged_span.binary_annotations[0].value == 'logged_value'
    assert set([ann.value for ann in logged_span.annotations]) == set(['cs', 'cr'])


def test_log_debug_for_existing_span():
    del mock_logger[:]
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=example_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
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

    assert len(mock_logger) == 1
    span = _decode_binary_thrift_obj(mock_logger[0])
    assert span.name == 'test_span_name'
    assert span.annotations[0].host.service_name == 'test_service_name'
    assert span.parent_id is None
    assert len(span.annotations) == 3
    annotations = sorted(span.annotations, key=lambda ann: ann.value)
    assert annotations[2].value == 'test_annotation'
    assert annotations[2].timestamp == 42000000
    assert set([ann.value for ann in annotations]) == set([
        'ss', 'sr', 'test_annotation',
    ])
    assert len(span.binary_annotations) == 2
    binary_annotations = sorted(
        span.binary_annotations, key=lambda bin_ann: bin_ann.key)
    assert binary_annotations[0].key == 'extra_binary_annotation'
    assert binary_annotations[0].value == 'extra_value'
    assert binary_annotations[1].key == 'some_key'
    assert binary_annotations[1].value == 'some_value'
