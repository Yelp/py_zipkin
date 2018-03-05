import pytest
import mock
import socket
import py_zipkin.zipkin as zipkin


class UDPTransportHandler(object):

    def __init__(self):
        self.send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.recv_socket.bind(('', 0))

    def __bool__(self):
        return True

    def __nonzero__(self):
        return True

    def __call__(self, message):
        self.send_socket.sendto(message, self.recv_socket.getsockname())


class TCPTransportHandler(object):

    def __init__(self):
        self.send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.recv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.recv_socket.bind(('', 0))
        self.recv_socket.listen(1)
        self.send_socket.connect(self.recv_socket.getsockname())

    def __bool__(self):
        return True

    def __nonzero__(self):
        return True

    def __call__(self, message):
        self.send_socket.send(message)


@pytest.mark.parametrize('transport_handler', [
    mock.Mock(),
    UDPTransportHandler(),
    TCPTransportHandler(),
])
@pytest.mark.parametrize('firehose_handler', [
    None,
    UDPTransportHandler(),
    TCPTransportHandler(),
])
@pytest.mark.parametrize('sample_rate', [0.15, 100])
def test_zipkin_span_thread_local(
    benchmark,
    transport_handler,
    firehose_handler,
    sample_rate,
):
    def benchmark_zipkin_span():
        context = zipkin.zipkin_span(
            service_name='my_service',
            span_name='my_span_name',
            transport_handler=transport_handler,
            firehose_handler=firehose_handler,
            port=42,
            sample_rate=sample_rate,
        )
        context.start()

    benchmark(benchmark_zipkin_span)


@pytest.mark.parametrize('transport_handler', [
    mock.Mock(),
    UDPTransportHandler(),
    TCPTransportHandler(),
])
@pytest.mark.parametrize('firehose_handler', [
    None,
    UDPTransportHandler(),
    TCPTransportHandler(),
])
@pytest.mark.parametrize('sample_rate', [0.15, 100])
@pytest.mark.parametrize('num_spans', [1, 10, 100])
def test_zipkin_span_logging(
    benchmark,
    transport_handler,
    firehose_handler,
    sample_rate,
    num_spans,
):
    def build_trace():
        context = zipkin.zipkin_span(
            service_name='my_service',
            span_name='my_span_name',
            transport_handler=transport_handler,
            firehose_handler=firehose_handler,
            port=42,
            sample_rate=sample_rate,
        )
        context.start()
        for _ in range(num_spans):
            with zipkin.zipkin_span(
                service_name='my_service',
                span_name='my_span_name',
            ):
                pass

        return (), {'context': context}

    def benchmark_zipkin_span(context):
        context.stop()

    benchmark.pedantic(benchmark_zipkin_span, setup=build_trace)


@pytest.mark.parametrize('use_128', [False, True])
def test_create_attrs_for_span(benchmark, use_128):
    benchmark(
        zipkin.create_attrs_for_span,
        use_128bit_trace_id=use_128,
    )
