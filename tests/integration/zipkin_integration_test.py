# -*- coding: utf-8 -*-
import json

import pytest

from py_zipkin import Encoding
from py_zipkin import Kind
from py_zipkin import storage
from py_zipkin import zipkin
from py_zipkin.logging_helper import LOGGING_END_KEY
from py_zipkin.zipkin import ZipkinAttrs


USECS = 1000000


def mock_logger():
    mock_logs = []

    def mock_transport_handler(message):
        mock_logs.append(message)

    return mock_transport_handler, mock_logs


def test_starting_zipkin_trace_with_sampling_rate():
    """Test that a sampling rate of 100% will generate an output trace.

    The test also verifies that the same spans are sent to the firehose_handler.
    """
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
        encoding=Encoding.V2_JSON,
    ):
        pass

    def check_span(span):
        assert span['name'] == 'test_span_name'
        assert span['localEndpoint']['serviceName'] == 'test_service_name'
        assert 'parentId' not in span
        # timestamp and duration are microsecond conversions of time.time()
        assert span['timestamp'] is not None
        assert span['duration'] is not None
        assert span['tags']['some_key'] == 'some_value'
        assert sorted([ann['value'] for ann in span['annotations']]) == [
            LOGGING_END_KEY]

    check_span(json.loads(mock_logs[0])[0])
    check_span(json.loads(mock_firehose_logs[0])[0])


def test_starting_zipkin_trace_with_128bit_trace_id():
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
        firehose_handler=mock_firehose_handler,
        encoding=Encoding.V2_JSON,
    ):
        pass

    def check_span(span):
        assert 'traceId' in span
        assert len(span['traceId']) == 32

    check_span(json.loads(mock_logs[0])[0])
    check_span(json.loads(mock_firehose_logs[0])[0])


def test_span_inside_trace():
    """This tests that nested spans work correctly"""
    mock_transport_handler, mock_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        encoding=Encoding.V2_JSON,
    ):
        with zipkin.zipkin_span(
            service_name='nested_service',
            span_name='nested_span',
            annotations={'nested_annotation': 43},
            binary_annotations={'nested_key': 'nested_value'},
        ):
            pass

    spans = json.loads(mock_logs[0])

    assert len(spans) == 2
    nested_span = spans[0]
    root_span = spans[1]

    assert nested_span['name'] == 'nested_span'
    assert nested_span['localEndpoint']['serviceName'] == 'nested_service'
    assert nested_span['parentId'] == root_span['id']
    assert nested_span['tags']['nested_key'] == 'nested_value'
    # Local nested spans report timestamp and duration
    assert nested_span['timestamp'] is not None
    assert nested_span['duration'] is not None
    assert len(nested_span['annotations']) == 1
    assert 'kind' not in nested_span
    assert sorted([ann['value'] for ann in nested_span['annotations']]) == sorted([
        'nested_annotation'])
    for ann in nested_span['annotations']:
        if ann['value'] == 'nested_annotation':
            assert ann['timestamp'] == 43 * USECS


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
        encoding=Encoding.V1_JSON,
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
        assert nested_span['name'] == 'nested_span'
        assert nested_span['annotations'][0]['endpoint']['serviceName'] == \
            'nested_service'
        assert nested_span['parentId'] == root_span['id']
        assert nested_span['binaryAnnotations'][0]['key'] == 'nested_key'
        assert nested_span['binaryAnnotations'][0]['value'] == 'nested_value'
        # Local nested spans report timestamp and duration
        assert nested_span['timestamp'] is not None
        assert nested_span['duration'] is not None
        assert len(nested_span['annotations']) == 5
        assert sorted([ann['value'] for ann in nested_span['annotations']]) == \
            sorted(['ss', 'sr', 'cs', 'cr', 'nested_annotation'])
        for ann in nested_span['annotations']:
            if ann['value'] == 'nested_annotation':
                assert ann['timestamp'] == 43 * USECS
            elif ann['value'] == 'cs':
                assert ann['timestamp'] == 100 * USECS
            elif ann['value'] == 'cr':
                assert ann['timestamp'] == 300 * USECS

    check_spans(json.loads(mock_logs[0]))
    check_spans(json.loads(mock_firehose_logs[0]))


@pytest.mark.parametrize('encoding', [Encoding.V1_JSON, Encoding.V2_JSON])
def test_sr_ss_annotation_override(encoding):
    """Here we override ss and sr and expect the output span to contain the
    values we're passing in.
    """
    mock_transport_handler, mock_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        binary_annotations={'some_key': 'some_value'},
        encoding=encoding,
    ):
        with zipkin.zipkin_span(
            service_name='nested_service',
            span_name='nested_span',
            annotations={'nested_annotation': 43, 'sr': 100, 'ss': 300},
            binary_annotations={'nested_key': 'nested_value'},
            kind=Kind.SERVER,
        ):
            pass

    spans = json.loads(mock_logs[0])

    nested_span = spans[0]
    root_span = spans[1]

    assert nested_span['parentId'] == root_span['id']
    assert nested_span['timestamp'] == 100 * USECS
    assert nested_span['duration'] == 200 * USECS

    # In V1, we also add sr and ss to the list of annotations
    if encoding == Encoding.V1_JSON:
        assert len(nested_span['annotations']) == 3
        for ann in nested_span['annotations']:
            if ann['value'] == 'nested_annotation':
                assert ann['timestamp'] == 43 * USECS
            elif ann['value'] == 'sr':
                assert ann['timestamp'] == 100 * USECS
            elif ann['value'] == 'ss':
                assert ann['timestamp'] == 300 * USECS


@pytest.mark.parametrize('encoding', [Encoding.V1_JSON, Encoding.V2_JSON])
def test_service_span(encoding):
    """Tests that zipkin_attrs can be passed in"""
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
        encoding=encoding,
    ):
        pass

    span = json.loads(mock_logs[0])[0]
    assert span['name'] == 'service_span'
    assert span['traceId'] == '0'
    assert span['id'] == '1'
    assert span['parentId'] == '2'

    if encoding == Encoding.V1_JSON:
        # Spans continued on the server don't log timestamp/duration, as it's
        # assumed the client part of the pair will log them.
        assert 'timestamp' not in span
        assert 'duration' not in span
    elif encoding == Encoding.V2_JSON:
        assert span['shared'] is True


def test_service_span_report_timestamp_override():
    """Tests that timestamp and duration are set if report_root_timestamp=True"""
    mock_transport_handler, mock_logs = mock_logger()
    # We need to pass in zipkin_attrs so that py_zipkin doesn't think this is the
    # root span (ts and duration are always set for the root span)
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
        report_root_timestamp=True,
        encoding=Encoding.V1_JSON,
    ):
        pass

    span = json.loads(mock_logs[0])[0]
    assert 'timestamp' in span
    assert 'duration' in span


@pytest.mark.parametrize('encoding', [Encoding.V1_JSON, Encoding.V2_JSON])
def test_service_span_that_is_independently_sampled(encoding):
    """Tests that sample_rate has can turn on sampling for a trace.

    This is the same case as an intermediate service wanting to have an higher
    sampling rate.
    """
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
        firehose_handler=mock_firehose_handler,
        encoding=encoding,
    ):
        pass

    def check_span(span):
        assert span['traceId'] == '0'
        assert span['name'] == 'service_span'
        assert 'parentId' not in span
        # Spans that are part of an unsampled trace which start their own sampling
        # should report timestamp/duration, as they're acting as root spans.
        assert 'timestamp' in span
        assert 'duration' in span
        if encoding == Encoding.V2_JSON:
            # BUG: this should fail in the firehose trace since there was already
            # an ongoing trace and we have a parent. However we emit the exact
            # same span for both the normal transport and firehose, so this is not
            # something we can handle right now.
            assert 'shared' not in span

    check_span(json.loads(mock_logs[0])[0])
    check_span(json.loads(mock_firehose_logs[0])[0])


def test_zipkin_trace_with_no_sampling_no_firehose():
    mock_transport_handler, mock_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=None,
    ):
        pass

    assert len(mock_logs) == 0


def test_zipkin_trace_with_no_sampling_with_firehose():
    """Tests that firehose traces are emitted even if sampling is False"""
    mock_transport_handler, mock_logs = mock_logger()
    mock_firehose_handler, mock_firehose_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=None,
        firehose_handler=mock_firehose_handler,
        encoding=Encoding.V2_JSON,
    ):
        pass

    assert len(mock_logs) == 0
    assert len(mock_firehose_logs) == 1
    firehose_span = json.loads(mock_firehose_logs[0])[0]

    assert firehose_span['name'] == 'test_span_name'
    assert firehose_span['localEndpoint']['serviceName'] == 'test_service_name'


def test_no_sampling_with_inner_span():
    """Tests that even inner spans are sent to firehose if sampling=False."""
    mock_transport_handler, mock_logs = mock_logger()
    mock_firehose_handler, mock_firehose_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=None,
        firehose_handler=mock_firehose_handler,
        encoding=Encoding.V2_JSON,
    ):
        with zipkin.zipkin_span(
            service_name='nested_service',
            span_name='nested_span',
        ):
            pass

        pass

    assert len(mock_logs) == 0
    assert len(mock_firehose_logs) == 1
    firehose_spans = json.loads(mock_firehose_logs[0])
    assert len(firehose_spans) == 2

    nested_span = firehose_spans[0]
    root_span = firehose_spans[1]
    assert nested_span['name'] == 'nested_span'
    assert nested_span['localEndpoint']['serviceName'] == 'nested_service'
    assert nested_span['parentId'] == root_span['id']


@pytest.mark.parametrize('encoding', [Encoding.V1_JSON, Encoding.V2_JSON])
def test_client_span(encoding):
    """Tests the zipkin_client_span helper."""
    mock_transport_handler, mock_logs = mock_logger()
    with zipkin.zipkin_client_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        encoding=encoding,
    ):
        pass

    assert len(mock_logs) == 1
    span = json.loads(mock_logs[0])[0]

    assert span['name'] == 'test_span_name'
    if encoding == Encoding.V1_JSON:
        assert sorted([ann['value'] for ann in span['annotations']]) == \
            ['cr', 'cs']
    elif encoding == Encoding.V2_JSON:
        assert span['kind'] == 'CLIENT'


@pytest.mark.parametrize('encoding', [Encoding.V1_JSON, Encoding.V2_JSON])
def test_server_span(encoding):
    mock_transport_handler, mock_logs = mock_logger()
    with zipkin.zipkin_server_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        encoding=encoding,
    ):
        with zipkin.zipkin_client_span(
            service_name='nested_service',
            span_name='nested_span',
        ):
            pass

        pass

    assert len(mock_logs) == 1
    spans = json.loads(mock_logs[0])
    client_span = spans[0]
    server_span = spans[1]

    assert server_span['name'] == 'test_span_name'
    # Local nested spans report timestamp and duration
    assert 'timestamp' in server_span
    assert 'duration' in server_span
    if encoding == Encoding.V1_JSON:
        assert len(server_span['annotations']) == 2
        assert sorted([ann['value'] for ann in server_span['annotations']]) == [
            'sr', 'ss']
    elif encoding == Encoding.V2_JSON:
        assert server_span['kind'] == 'SERVER'

    assert client_span['name'] == 'nested_span'
    # Local nested spans report timestamp and duration
    assert 'timestamp' in client_span
    assert 'duration' in client_span
    if encoding == Encoding.V1_JSON:
        assert len(client_span['annotations']) == 2
        assert sorted([ann['value'] for ann in client_span['annotations']]) == [
            'cr', 'cs']
    elif encoding == Encoding.V2_JSON:
        assert client_span['kind'] == 'CLIENT'


@pytest.mark.parametrize('encoding', [Encoding.V1_JSON, Encoding.V2_JSON])
def test_include_still_works(encoding):
    mock_transport_handler, mock_logs = mock_logger()
    with zipkin.zipkin_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        encoding=encoding,
    ):
        with zipkin.zipkin_span(
            service_name='nested_service',
            span_name='nested_span',
            include={'client'},
        ):
            pass
        with zipkin.zipkin_span(
            service_name='nested_service',
            span_name='nested_span',
            include={'server'},
        ):
            pass
        with zipkin.zipkin_span(
            service_name='nested_service',
            span_name='nested_span',
            include={'client', 'server'},
        ):
            pass
        pass

    assert len(mock_logs) == 1
    spans = json.loads(mock_logs[0])

    client_span = spans[0]
    server_span = spans[1]
    local_span = spans[2]
    if encoding == Encoding.V1_JSON:
        assert len(client_span['annotations']) == 2
        assert sorted([ann['value'] for ann in client_span['annotations']]) == [
            'cr', 'cs']
        assert len(server_span['annotations']) == 2
        assert sorted([ann['value'] for ann in server_span['annotations']]) == [
            'sr', 'ss']
        assert len(local_span['annotations']) == 4
        assert sorted([ann['value'] for ann in local_span['annotations']]) == [
            'cr', 'cs', 'sr', 'ss']
    elif encoding == Encoding.V2_JSON:
        assert client_span['kind'] == 'CLIENT'
        assert server_span['kind'] == 'SERVER'
        assert 'kind' not in local_span


@pytest.mark.parametrize('encoding', [Encoding.V1_JSON, Encoding.V2_JSON])
def test_can_set_sa_annotation(encoding):
    mock_transport_handler, mock_logs = mock_logger()
    with zipkin.zipkin_client_span(
        service_name='test_service_name',
        span_name='test_span_name',
        transport_handler=mock_transport_handler,
        sample_rate=100.0,
        encoding=encoding,
    ) as span:
        span.add_sa_binary_annotation(
            port=8888,
            service_name='sa_service',
            host='10.0.0.0',
        )

    assert len(mock_logs) == 1
    client_span = json.loads(mock_logs[0])[0]

    if encoding == Encoding.V1_JSON:
        assert client_span['binaryAnnotations'][0]['key'] == 'sa'
        assert client_span['binaryAnnotations'][0]['value'] is True
        host = client_span['binaryAnnotations'][0]['endpoint']
    elif encoding == Encoding.V2_JSON:
        host = client_span['remoteEndpoint']

    assert host['serviceName'] == u'sa_service'
    assert host['ipv4'] == '10.0.0.0'
    assert 'ipv6' not in host
    assert host['port'] == 8888


def test_memory_leak():
    # In py_zipkin >= 0.13.0 and <= 0.14.0 this test fails since the
    # span_storage contains 10 spans once you exit the for loop.
    mock_transport_handler, mock_logs = mock_logger()
    assert len(storage.default_span_storage()) == 0
    for _ in range(10):
        with zipkin.zipkin_client_span(
            service_name='test_service_name',
            span_name='test_span_name',
            transport_handler=mock_transport_handler,
            sample_rate=0.0,
            encoding=Encoding.V2_JSON,
        ):
            with zipkin.zipkin_span(
                service_name='inner_service_name',
                span_name='inner_span_name',
            ):
                pass

    assert len(mock_logs) == 0
    assert len(storage.default_span_storage()) == 0
