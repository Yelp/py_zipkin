# -*- coding: utf-8 -*-
import json
import time

import mock
import pytest

import py_zipkin.zipkin as zipkin
from py_zipkin import Encoding
from py_zipkin import Kind
from py_zipkin.encoding._helpers import _V1Span
from py_zipkin.encoding._helpers import create_endpoint
from py_zipkin.exception import ZipkinError
from py_zipkin.storage import SpanStorage
from py_zipkin.storage import ThreadLocalStack
from py_zipkin.thread_local import get_zipkin_attrs
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.zipkin import ZipkinAttrs
from tests.test_helpers import MockTransportHandler


@pytest.fixture
def mock_context_stack():
    with mock.patch(
        'py_zipkin.storage.Stack', autospec=True,
    ) as mock_context_stack:
        yield mock_context_stack


@mock.patch('py_zipkin.zipkin.create_attrs_for_span', autospec=True)
@mock.patch('py_zipkin.zipkin.create_endpoint')
@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_for_new_trace(
    logging_context_cls_mock,
    create_endpoint_mock,
    create_attrs_for_span_mock,
    mock_context_stack,
):
    transport_handler = MockTransportHandler()
    firehose_handler = mock.Mock()
    span_storage = SpanStorage()

    with zipkin.zipkin_span(
        service_name='some_service_name',
        span_name='span_name',
        transport_handler=transport_handler,
        port=5,
        sample_rate=100.0,
        context_stack=mock_context_stack,
        span_storage=span_storage,
        firehose_handler=firehose_handler,
    ) as zipkin_context:
        assert zipkin_context.port == 5
        pass

    create_attrs_for_span_mock.assert_called_once_with(
        sample_rate=100.0,
        use_128bit_trace_id=False,
    )
    mock_context_stack.push.assert_called_once_with(
        create_attrs_for_span_mock.return_value,
    )
    create_endpoint_mock.assert_called_once_with(5, 'some_service_name', None)
    assert logging_context_cls_mock.call_args == mock.call(
        create_attrs_for_span_mock.return_value,
        create_endpoint_mock.return_value,
        'span_name',
        transport_handler,
        True,
        span_storage,
        'some_service_name',
        binary_annotations={},
        add_logging_annotation=False,
        client_context=False,
        max_span_batch_size=None,
        firehose_handler=firehose_handler,
        encoding=Encoding.V1_THRIFT,
    )
    mock_context_stack.pop.assert_called_once_with()


@mock.patch('py_zipkin.zipkin.create_attrs_for_span', autospec=True)
@mock.patch('py_zipkin.zipkin.create_endpoint')
@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_passed_sampled_attrs(
    logging_context_cls_mock,
    create_endpoint_mock,
    create_attrs_for_span_mock,
    mock_context_stack,
):
    # Make sure that if zipkin_span is passed *sampled* ZipkinAttrs, but is
    # also configured to do sampling itself, the passed ZipkinAttrs are used.
    transport_handler = MockTransportHandler()
    zipkin_attrs = ZipkinAttrs(
        trace_id='0',
        span_id='1',
        parent_span_id=None,
        flags='0',
        is_sampled=True,
    )
    span_storage = SpanStorage()

    with zipkin.zipkin_span(
        service_name='some_service_name',
        span_name='span_name',
        transport_handler=transport_handler,
        port=5,
        sample_rate=100.0,
        zipkin_attrs=zipkin_attrs,
        context_stack=mock_context_stack,
        span_storage=span_storage,
    ) as zipkin_context:
        assert zipkin_context.port == 5

    assert not create_attrs_for_span_mock.called
    mock_context_stack.push.assert_called_once_with(zipkin_attrs)
    create_endpoint_mock.assert_called_once_with(5, 'some_service_name', None)
    # Logging context should not report timestamp/duration for the server span,
    # since it's assumed that the client part of this span will do that.
    logging_context_cls_mock.assert_called_once_with(
        zipkin_attrs,
        create_endpoint_mock.return_value,
        'span_name',
        transport_handler,
        False,
        span_storage,
        'some_service_name',
        binary_annotations={},
        add_logging_annotation=False,
        client_context=False,
        max_span_batch_size=None,
        firehose_handler=None,
        encoding=Encoding.V1_THRIFT,
    )
    mock_context_stack.pop.assert_called_once_with()


@pytest.mark.parametrize('firehose_enabled', [True, False])
@mock.patch('py_zipkin.zipkin.create_attrs_for_span', autospec=True)
@mock.patch('py_zipkin.zipkin.create_endpoint')
@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_trace_with_0_sample_rate(
    logging_context_cls_mock,
    create_endpoint_mock,
    create_attrs_for_span_mock,
    mock_context_stack,
    firehose_enabled,
):
    create_attrs_for_span_mock.return_value = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags='0',
        is_sampled=False,
    )
    transport_handler = MockTransportHandler()
    span_storage = SpanStorage()

    with zipkin.zipkin_span(
        service_name='some_service_name',
        span_name='span_name',
        transport_handler=transport_handler,
        sample_rate=0.0,
        context_stack=mock_context_stack,
        span_storage=span_storage,
        firehose_handler=mock.Mock() if firehose_enabled else None
    ) as zipkin_context:
        assert zipkin_context.port == 0

    create_attrs_for_span_mock.assert_called_once_with(
        sample_rate=0.0,
        use_128bit_trace_id=False,
    )
    mock_context_stack.push.assert_called_once_with(
        create_attrs_for_span_mock.return_value)

    # When firehose mode is on, we log regardless of sample rate
    assert create_endpoint_mock.call_count == (1 if firehose_enabled else 0)
    assert logging_context_cls_mock.call_count == (1 if firehose_enabled else 0)
    mock_context_stack.pop.assert_called_once_with()


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


def test_zipkin_span_span_storage_wrong_type():
    # Missing transport_handler
    with pytest.raises(ZipkinError):
        with zipkin.zipkin_span(
            service_name='some_service_name',
            span_name='span_name',
            transport_handler=MockTransportHandler(),
            port=5,
            sample_rate=100.0,
            span_storage=[],
        ):
            pass


def test_zipkin_invalid_sample_rate():
    with pytest.raises(ZipkinError):
        with zipkin.zipkin_span(
            service_name='some_service_name',
            span_name='span_name',
            transport_handler=MockTransportHandler(),
            sample_rate=101.0,
        ):
            pass

    with pytest.raises(ZipkinError):
        with zipkin.zipkin_span(
            service_name='some_service_name',
            span_name='span_name',
            transport_handler=MockTransportHandler(),
            sample_rate=-0.1,
        ):
            pass


def test_zipkin_invalid_kind():
    with pytest.raises(ZipkinError):
        with zipkin.zipkin_span(
            service_name='some_service_name',
            span_name='span_name',
            transport_handler=MockTransportHandler(),
            sample_rate=100.0,
        ):
            with zipkin.zipkin_span(
                service_name='nested_service',
                span_name='nested_span',
                kind='client',
            ):
                pass

            pass


@pytest.mark.parametrize('span_func', [
    zipkin.zipkin_client_span,
    zipkin.zipkin_server_span,
])
@mock.patch('py_zipkin.zipkin.zipkin_span', autospec=True)
def test_zipkin_extraneous_kind_raises(mock_zipkin_span, span_func):
    with pytest.raises(ValueError):
        with span_func(
            service_name='some_service_name',
            span_name='span_name',
            transport_handler=MockTransportHandler(),
            sample_rate=100.0,
            kind=Kind.LOCAL,
        ):
            pass


@mock.patch.object(zipkin, 'log', autospec=True)
def test_zipkin_setting_transport_twice(mock_log):
    with zipkin.zipkin_span(
        service_name='some_service_name',
        span_name='span_name',
        transport_handler=MockTransportHandler(),
        sample_rate=100.0,
        kind=Kind.LOCAL,
    ) as outer_span:
        with zipkin.zipkin_span(
            service_name='some_service_name',
            span_name='another_span',
            transport_handler=MockTransportHandler(),
            sample_rate=100.0,
            kind=Kind.LOCAL,
        ) as inner_span:
            assert mock_log.info.call_count == 1
            assert inner_span.logging_context is None
            assert outer_span.logging_context is not None


@mock.patch('py_zipkin.zipkin.create_attrs_for_span', autospec=True)
@mock.patch('py_zipkin.zipkin.create_endpoint')
@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_trace_with_no_sampling(
    logging_context_cls_mock,
    create_endpoint_mock,
    create_attrs_for_span_mock,
    mock_context_stack,
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
        transport_handler=MockTransportHandler(),
        port=5,
        context_stack=mock_context_stack,
        span_storage=SpanStorage(),
    ):
        pass

    assert create_attrs_for_span_mock.call_count == 0
    mock_context_stack.push.assert_called_once_with(
        zipkin_attrs,
    )
    assert create_endpoint_mock.call_count == 0
    assert logging_context_cls_mock.call_count == 0
    mock_context_stack.pop.assert_called_once_with()


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


@mock.patch('py_zipkin.zipkin.create_attrs_for_span', autospec=True)
@mock.patch('py_zipkin.zipkin.create_endpoint')
@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_trace_context_attrs_is_always_popped(
    logging_context_cls_mock,
    create_endpoint_mock,
    create_attrs_for_span_mock,
    mock_context_stack,
):
    with pytest.raises(Exception):
        with zipkin.zipkin_span(
            service_name='my_service',
            span_name='my_span_name',
            transport_handler=MockTransportHandler(),
            port=22,
            sample_rate=100.0,
            context_stack=mock_context_stack,
            span_storage=SpanStorage()
        ):
            raise Exception
    mock_context_stack.pop.assert_called_once_with()


def test_create_headers_for_new_span_empty_if_no_active_request(
    mock_context_stack,
):
    mock_context_stack.get.return_value = None
    assert {} == zipkin.create_http_headers_for_new_span()


@mock.patch('py_zipkin.zipkin.generate_random_64bit_string', autospec=True)
def test_create_headers_for_new_span_returns_header_if_active_request(gen_mock):
    mock_context_stack = mock.Mock()
    mock_context_stack.get.return_value = mock.Mock(
        trace_id='27133d482ba4f605',
        span_id='37133d482ba4f605',
        is_sampled=True,
    )
    gen_mock.return_value = '17133d482ba4f605'
    expected = {
        'X-B3-TraceId': '27133d482ba4f605',
        'X-B3-SpanId': '17133d482ba4f605',
        'X-B3-ParentSpanId': '37133d482ba4f605',
        'X-B3-Flags': '0',
        'X-B3-Sampled': '1',
    }
    assert expected == zipkin.create_http_headers_for_new_span(
        context_stack=mock_context_stack,
    )


def test_span_context_no_zipkin_attrs(mock_context_stack):
    # When not in a Zipkin context, don't do anything
    mock_context_stack.get.return_value = None
    context = zipkin.zipkin_span(service_name='my_service')
    with context:
        pass
    assert not mock_context_stack.pop.called
    assert not mock_context_stack.push.called


@mock.patch('py_zipkin.thread_local._thread_local', autospec=True)
@mock.patch('py_zipkin.zipkin.generate_random_64bit_string', autospec=True)
def test_span_context_sampled_no_handlers(
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

    generate_string_mock.return_value = '1'

    context = zipkin.zipkin_span(
        service_name='my_service',
        port=5,
        transport_handler=MockTransportHandler(),
        sample_rate=None,
    )
    with context:
        # Assert that the new ZipkinAttrs were saved
        new_zipkin_attrs = ThreadLocalStack().get()
        assert new_zipkin_attrs.span_id == '1'

    # Outside of the context, things should be returned to normal
    assert ThreadLocalStack().get() == zipkin_attrs


@pytest.mark.parametrize('span_func, expected_annotations', [
    (zipkin.zipkin_span, ('cs', 'cr', 'ss', 'sr')),
    (zipkin.zipkin_client_span, ('cs', 'cr')),
    (zipkin.zipkin_server_span, ('ss', 'sr')),
])
@mock.patch('py_zipkin.thread_local._thread_local', autospec=True)
@mock.patch('py_zipkin.zipkin.generate_random_64bit_string', autospec=True)
@mock.patch('py_zipkin.zipkin.generate_random_128bit_string', autospec=True)
def test_span_context(
    generate_string_128bit_mock,
    generate_string_mock,
    thread_local_mock,
    span_func,
    expected_annotations,
):
    span_storage = SpanStorage()

    generate_string_mock.return_value = '1'

    with zipkin.zipkin_span(
        service_name='root_span',
        span_name='root_span',
        sample_rate=100.0,
        transport_handler=MockTransportHandler(),
        span_storage=span_storage,
    ):
        zipkin_attrs = ZipkinAttrs(
            trace_id='1111111111111111',
            span_id='2222222222222222',
            parent_span_id='3333333333333333',
            flags='flags',
            is_sampled=True,
        )
        thread_local_mock.zipkin_attrs = [zipkin_attrs]
        ts = time.time()
        with mock.patch('time.time', return_value=ts):
            with span_func(
                service_name='svc',
                span_name='span',
                binary_annotations={'foo': 'bar'},
                span_storage=span_storage,
            ):
                # Assert that the new ZipkinAttrs were saved
                new_zipkin_attrs = get_zipkin_attrs()
                assert new_zipkin_attrs.span_id == '1'

        # Outside of the context, things should be returned to normal
        assert get_zipkin_attrs() == zipkin_attrs

        client_span = span_storage.pop().build_v1_span()
        # These reserved annotations are based on timestamps so pop em.
        # This also acts as a check that they exist.
        for annotation in expected_annotations:
            client_span.annotations.pop(annotation)

        expected_client_span = _V1Span(
            trace_id='1111111111111111',
            name='span',
            parent_id='2222222222222222',
            id='1',
            timestamp=ts,
            duration=0.0,
            endpoint=create_endpoint(service_name='svc'),
            annotations={},
            binary_annotations={'foo': 'bar'},
            remote_endpoint=None,
        )
        assert client_span == expected_client_span

        assert generate_string_128bit_mock.call_count == 0


@mock.patch('py_zipkin.zipkin.create_attrs_for_span', autospec=True)
@mock.patch('py_zipkin.zipkin.create_endpoint')
@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_server_span_decorator(
    logging_context_cls_mock,
    create_endpoint_mock,
    create_attrs_for_span_mock,
    mock_context_stack,
):
    transport_handler = MockTransportHandler()
    span_storage = SpanStorage()

    @zipkin.zipkin_span(
        service_name='some_service_name',
        span_name='span_name',
        transport_handler=transport_handler,
        port=5,
        sample_rate=100.0,
        host='1.5.1.2',
        context_stack=mock_context_stack,
        span_storage=span_storage,
    )
    def test_func(a, b):
        return a + b

    assert test_func(1, 2) == 3

    create_attrs_for_span_mock.assert_called_once_with(
        sample_rate=100.0,
        use_128bit_trace_id=False,
    )
    mock_context_stack.push.assert_called_once_with(
        create_attrs_for_span_mock.return_value,
    )
    create_endpoint_mock.assert_called_once_with(5, 'some_service_name', '1.5.1.2')
    # The decorator was passed a sample rate and no Zipkin attrs, so it's
    # assumed to be the root of a trace and it should report timestamp/duration
    assert logging_context_cls_mock.call_args == mock.call(
        create_attrs_for_span_mock.return_value,
        create_endpoint_mock.return_value,
        'span_name',
        transport_handler,
        True,
        span_storage,
        'some_service_name',
        binary_annotations={},
        add_logging_annotation=False,
        client_context=False,
        max_span_batch_size=None,
        firehose_handler=None,
        encoding=Encoding.V1_THRIFT,
    )
    mock_context_stack.pop.assert_called_once_with()


@mock.patch('py_zipkin.zipkin.create_attrs_for_span', autospec=True)
@mock.patch('py_zipkin.zipkin.create_endpoint')
@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_client_span_decorator(
    logging_context_cls_mock,
    create_endpoint_mock,
    create_attrs_for_span_mock,
    mock_context_stack,
):
    transport_handler = MockTransportHandler()
    span_storage = SpanStorage()

    @zipkin.zipkin_span(
        service_name='some_service_name',
        span_name='span_name',
        transport_handler=transport_handler,
        port=5,
        sample_rate=100.0,
        include=('client',),
        host='1.5.1.2',
        context_stack=mock_context_stack,
        span_storage=span_storage,
    )
    def test_func(a, b):
        return a + b

    assert test_func(1, 2) == 3

    create_attrs_for_span_mock.assert_called_once_with(
        sample_rate=100.0,
        use_128bit_trace_id=False,
    )
    mock_context_stack.push.assert_called_once_with(
        create_attrs_for_span_mock.return_value,
    )
    create_endpoint_mock.assert_called_once_with(5, 'some_service_name', '1.5.1.2')
    # The decorator was passed a sample rate and no Zipkin attrs, so it's
    # assumed to be the root of a trace and it should report timestamp/duration
    assert logging_context_cls_mock.call_args == mock.call(
        create_attrs_for_span_mock.return_value,
        create_endpoint_mock.return_value,
        'span_name',
        transport_handler,
        True,
        span_storage,
        'some_service_name',
        binary_annotations={},
        add_logging_annotation=False,
        client_context=True,
        max_span_batch_size=None,
        firehose_handler=None,
        encoding=Encoding.V1_THRIFT,
    )
    mock_context_stack.pop.assert_called_once_with()


@mock.patch('py_zipkin.zipkin.create_endpoint', wraps=zipkin.create_endpoint)
def test_zipkin_span_decorator_many(create_endpoint_mock):
    @zipkin.zipkin_span(service_name='decorator')
    def test_func(a, b):
        return a + b

    assert test_func(1, 2) == 3
    assert create_endpoint_mock.call_count == 0
    with zipkin.zipkin_span(
        service_name='context_manager',
        transport_handler=MockTransportHandler(),
        sample_rate=100.0,
    ):
        assert test_func(1, 2) == 3
    assert create_endpoint_mock.call_count == 2
    assert test_func(1, 2) == 3
    assert create_endpoint_mock.call_count == 2


@mock.patch('py_zipkin.zipkin.ZipkinLoggingContext', autospec=True)
def test_zipkin_span_add_logging_annotation(mock_context):
    with zipkin.zipkin_span(
        service_name='my_service',
        transport_handler=MockTransportHandler(),
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
        transport_handler=MockTransportHandler(),
        port=5,
    )

    with context:
        assert 'test' not in context.logging_context.tags
        context.update_binary_annotations({'test': 'hi'})
        assert context.logging_context.tags['test'] == 'hi'

        nested_context = zipkin.zipkin_span(
            service_name='my_service',
            span_name='nested_span',
            binary_annotations={'one': 'one'},
        )
        with nested_context:
            assert 'one' not in context.logging_context.tags
            nested_context.update_binary_annotations({'two': 'two'})
            assert 'two' in nested_context.binary_annotations
            assert 'two' not in context.logging_context.tags


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
        transport_handler=MockTransportHandler(),
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


def test_add_sa_binary_annotation():
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
        transport_handler=MockTransportHandler(),
        port=5,
        kind=Kind.CLIENT,
    )

    with context:
        assert context.logging_context.remote_endpoint is None
        context.add_sa_binary_annotation(
            port=123,
            service_name='test_service',
            host='1.2.3.4',
        )
        expected_remote_endpoint = create_endpoint(
            port=123,
            service_name='test_service',
            host='1.2.3.4',
        )
        assert context.logging_context.remote_endpoint == \
            expected_remote_endpoint

        nested_context = zipkin.zipkin_span(
            service_name='my_service',
            span_name='nested_span',
            kind=Kind.CLIENT,
        )
        with nested_context:
            nested_context.add_sa_binary_annotation(
                port=456,
                service_name='nested_service',
                host='5.6.7.8',
            )
            expected_nested_remote_endpoint = create_endpoint(
                port=456,
                service_name='nested_service',
                host='5.6.7.8',
            )
            assert nested_context.remote_endpoint == \
                expected_nested_remote_endpoint


def test_add_sa_binary_annotation_twice():
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
        transport_handler=MockTransportHandler(),
        port=5,
        kind=Kind.CLIENT,
    )

    with context:
        assert context.logging_context.remote_endpoint is None
        context.add_sa_binary_annotation(
            port=123,
            service_name='test_service',
            host='1.2.3.4',
        )

        with pytest.raises(ValueError):
            context.add_sa_binary_annotation(
                port=123,
                service_name='test_service',
                host='1.2.3.4',
            )

        nested_context = zipkin.zipkin_span(
            service_name='my_service',
            span_name='nested_span',
            kind=Kind.CLIENT,
        )
        with nested_context:
            nested_context.add_sa_binary_annotation(
                port=456,
                service_name='nested_service',
                host='5.6.7.8',
            )
            with pytest.raises(ValueError):
                nested_context.add_sa_binary_annotation(
                    port=456,
                    service_name='nested_service',
                    host='5.6.7.8',
                )


def test_adding_sa_binary_annotation_without_sampling():
    """Even if we're not sampling, we still want to add binary annotations
    since they're useful for firehose traces.
    """
    context = zipkin.zipkin_span(
        service_name='my_service',
        span_name='span_name',
        transport_handler=MockTransportHandler(),
        sample_rate=0.0,
        kind=Kind.CLIENT,
    )
    with context:
        context.add_sa_binary_annotation(
            port=123,
            service_name='test_service',
            host='1.2.3.4',
        )
        expected_remote_endpoint = create_endpoint(
            port=123,
            service_name='test_service',
            host='1.2.3.4',
        )

        assert context.remote_endpoint == expected_remote_endpoint


def test_adding_sa_binary_annotation_missing_zipkin_attrs():
    context = zipkin.zipkin_span(
        service_name='my_service',
        span_name='span_name',
    )
    with context:
        context.add_sa_binary_annotation(
            port=123,
            service_name='test_service',
            host='1.2.3.4',
        )
        assert context.remote_endpoint is None


def test_adding_sa_binary_annotation_for_non_client_spans():
    context = zipkin.zipkin_span(
        service_name='my_service',
        span_name='span_name',
        transport_handler=MockTransportHandler(),
        include=('server',),
        sample_rate=100.0,
    )
    with context:
        context.add_sa_binary_annotation(
            port=123,
            service_name='test_service',
            host='1.2.3.4',
        )
        assert context.logging_context.remote_endpoint is None


def test_override_span_name():
    transport = MockTransportHandler()
    with zipkin.zipkin_span(
        service_name='my_service',
        span_name='span_name',
        transport_handler=transport,
        kind=Kind.CLIENT,
        sample_rate=100.0,
        encoding=Encoding.V1_JSON,
    ) as context:
        context.override_span_name('new_span_name')

        with zipkin.zipkin_span(
            service_name='my_service',
            span_name='nested_span',
        ) as nested_context:
            nested_context.override_span_name('new_nested_span')

    spans = json.loads(transport.get_payloads()[0])
    assert len(spans) == 2
    assert spans[0]['name'] == 'new_nested_span'
    assert spans[1]['name'] == 'new_span_name'


@pytest.mark.parametrize(
    'exception_message, expected_error_string',
    (
        ('some value error', u'ValueError: some value error'),
        (u'unicøde error', u'ValueError: unicøde error'),
    ),
)
@mock.patch(
    'py_zipkin.zipkin.zipkin_span.update_binary_annotations', autospec=True)
def test_adding_error_annotation_on_exception(
    mock_update_binary_annotations,
    exception_message,
    expected_error_string,
):
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
        transport_handler=MockTransportHandler(),
        port=5,
    )
    with pytest.raises(ValueError):
        with context:
            raise ValueError(exception_message)
    assert mock_update_binary_annotations.call_count == 1
    call_args, _ = mock_update_binary_annotations.call_args
    assert 'error' in call_args[1]
    assert expected_error_string == call_args[1]['error']


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
