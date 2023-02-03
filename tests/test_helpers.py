import threading
from unittest import mock

from py_zipkin import Kind
from py_zipkin import zipkin
from py_zipkin.encoding import Encoding
from py_zipkin.encoding._encoders import IEncoder
from py_zipkin.instrumentations import python_threads
from py_zipkin.storage import Tracer
from py_zipkin.testing import MockTransportHandler
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.zipkin import ZipkinAttrs


class MockEncoder(IEncoder):
    def __init__(self, fits=True, encoded_span="", encoded_queue=""):
        self.fits_bool = fits
        self.encode_span = mock.Mock(return_value=(encoded_span, len(encoded_span)))
        self.encode_queue = mock.Mock(return_value=encoded_queue)

    def fits(self, current_count, current_size, max_size, new_span):
        assert isinstance(current_count, int)
        assert isinstance(current_size, int)
        assert isinstance(max_size, int)
        assert isinstance(new_span, str)

        return self.fits_bool


class MockTracer(Tracer):
    def get_context(self):
        return self._context_stack


def generate_list_of_spans(encoding):
    zipkin_attrs = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=generate_random_64bit_string(),
        is_sampled=True,
        flags=None,
    )
    inner_span_id = generate_random_64bit_string()
    transport_handler = MockTransportHandler()
    # Let's hardcode the timestamp rather than call time.time() every time.
    # The issue with time.time() is that the convertion to int of the
    # returned float value * 1000000 is not precise and in the same test
    # sometimes returns N and sometimes N+1. This ts value doesn't have that
    # issue afaict, probably since it ends in zeros.
    ts = 1538544126.115900

    if encoding == Encoding.V1_THRIFT:
        return (
            b"\x0c\x00\x00\x00\x02\n\x00\x01\xb5ZX\x14\x81.\x07\xfe\x0b\x00\x03\x00"
            + b"\x00\x00\ninner_span\n\x00\x04\xc6\xcd\x0c\x1fZ\xb0_\xd1\n\x00\x05BM"
            + b"\xb5\x02p\xed\x8e\xb1\x0f\x00\x06\x0c\x00\x00\x00\x01\n\x00\x01\x00"
            + b"\x05wL8\x1b\xc0<\x0b\x00\x02\x00\x00\x00\x02ws\x0c\x00\x03\x08\x00"
            + b"\x01\n\x00\x00\x00\x06\x00\x02\x1f\x90\x0b\x00\x03\x00\x00\x00\x11"
            + b"test_service_name\x00\x00\x0f\x00\x08\x0c\x00\x00\x00\x00\x02\x00\t"
            + b"\x00\n\x00\n\x00\x05wL8\x1b\xc0<\n\x00\x0b\x00\x00\x00\x00\x00LK@\x00"
            + b"\n\x00\x01\xb5ZX\x14\x81.\x07\xfe\x0b\x00\x03\x00\x00\x00\x0e"
            + b"test_span_name\n\x00\x04BM\xb5\x02p\xed\x8e\xb1\n\x00\x05\xa1kn\xd3"
            + b"\x0cA\xba\xbd\x0f\x00\x06\x0c\x00\x00\x00\x02\n\x00\x01\x00\x05wL8"
            + b"\x1b\xc0<\x0b\x00\x02\x00\x00\x00\x02cs\x0c\x00\x03\x08\x00\x01\n\x00"
            + b"\x00\x00\x06\x00\x02\x1f\x90\x0b\x00\x03\x00\x00\x00\x11"
            + b"test_service_name\x00\x00\n\x00\x01\x00\x05wL8\xb4V\xbc\x0b\x00\x02"
            + b"\x00\x00\x00\x02cr\x0c\x00\x03\x08\x00\x01\n\x00\x00\x00\x06\x00\x02"
            + b"\x1f\x90\x0b\x00\x03\x00\x00\x00\x11test_service_name\x00\x00\x0f\x00"
            + b"\x08\x0c\x00\x00\x00\x02\x0b\x00\x01\x00\x00\x00\x08some_key\x0b\x00"
            + b"\x02\x00\x00\x00\nsome_value\x08\x00\x03\x00\x00\x00\x06\x0c\x00\x04"
            + b"\x08\x00\x01\n\x00\x00\x00\x06\x00\x02\x1f\x90\x0b\x00\x03\x00\x00"
            + b"\x00\x11test_service_name\x00\x00\x0b\x00\x01\x00\x00\x00\x02sa\x0b"
            + b"\x00\x02\x00\x00\x00\x01\x01\x08\x00\x03\x00\x00\x00\x00\x0c\x00\x04"
            + b'\x08\x00\x01\x00\x00\x00\x00\x06\x00\x02"\xb8\x0b\x00\x03\x00\x00\x00'
            + b"\nsa_service\x0b\x00\x04\x00\x00\x00\x10 \x01\r\xb8\x85\xa3\x00\x00"
            + b"\x00\x00\x8a.\x03ps4\x00\x00\x02\x00\t\x00\x00",
            zipkin_attrs,
            inner_span_id,
            ts,
        )

    with mock.patch("time.time", autospec=True) as mock_time:
        # zipkin.py start, logging_helper.start, 3 x logging_helper.stop
        # I don't understand why logging_helper.stop would run 3 times, but
        # that's what I'm seeing in the test
        mock_time.side_effect = iter([ts, ts, ts + 10, ts + 10, ts + 10])
        with zipkin.zipkin_span(
            service_name="test_service_name",
            span_name="test_span_name",
            transport_handler=transport_handler,
            binary_annotations={"some_key": "some_value"},
            encoding=encoding,
            zipkin_attrs=zipkin_attrs,
            host="10.0.0.0",
            port=8080,
            kind=Kind.CLIENT,
        ) as span:
            with mock.patch.object(
                zipkin,
                "generate_random_64bit_string",
                return_value=inner_span_id,
            ):
                with zipkin.zipkin_span(
                    service_name="test_service_name",
                    span_name="inner_span",
                    timestamp=ts,
                    duration=5,
                    annotations={"ws": ts},
                ):
                    span.add_sa_binary_annotation(
                        8888,
                        "sa_service",
                        "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
                    )

    return transport_handler.get_payloads()[0], zipkin_attrs, inner_span_id, ts


class TracingThread(threading.Thread):
    """A tracing-aware Thread subclass.

    This just helps us test the two pieces of Thread monkey patching.
    """

    def start(self):
        python_threads._Thread_pre_start(self)
        super().start()

    def run(self):
        python_threads._Thread_wrap_run(self, super().run)
