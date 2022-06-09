import inspect
import time
from unittest import mock

import pytest

import py_zipkin.zipkin as zipkin
from py_zipkin import Encoding
from py_zipkin import Kind
from py_zipkin.encoding._helpers import create_endpoint
from py_zipkin.encoding._helpers import Span
from py_zipkin.exception import ZipkinError
from py_zipkin.storage import default_span_storage
from py_zipkin.storage import get_default_tracer
from py_zipkin.storage import SpanStorage
from py_zipkin.storage import Stack
from py_zipkin.storage import ThreadLocalStack
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.zipkin import ZipkinAttrs
from tests.test_helpers import MockTracer
from tests.test_helpers import MockTransportHandler


@pytest.fixture(autouse=True)
def clean_thread_local():
    yield

    stack = ThreadLocalStack()
    while stack.pop():
        pass

    default_span_storage().clear()

    while get_default_tracer().pop_zipkin_attrs():
        pass
    get_default_tracer()._span_storage.clear()


class TestZipkinSpan:
    @mock.patch.object(zipkin.zipkin_span, "_generate_kind", autospec=True)
    def test_init(self, mock_generate_kind):
        # Test that all arguments are correctly saved
        zipkin_attrs = ZipkinAttrs(None, None, None, None, None)
        transport = MockTransportHandler()
        firehose = MockTransportHandler()
        stack = Stack([])
        span_storage = SpanStorage()
        tracer = MockTracer()

        context = tracer.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            zipkin_attrs=zipkin_attrs,
            transport_handler=transport,
            max_span_batch_size=10,
            annotations={"test_annotation": 1},
            binary_annotations={"status": "200"},
            port=80,
            sample_rate=100.0,
            include=("cs", "cr"),
            add_logging_annotation=True,
            report_root_timestamp=True,
            use_128bit_trace_id=True,
            host="127.0.0.255",
            context_stack=stack,
            span_storage=span_storage,
            firehose_handler=firehose,
            kind=Kind.CLIENT,
            timestamp=1234,
            duration=10,
            encoding=Encoding.V2_JSON,
        )

        assert context.service_name == "test_service"
        assert context.span_name == "test_span"
        assert context.zipkin_attrs_override == zipkin_attrs
        assert context.transport_handler == transport
        assert context.max_span_batch_size == 10
        assert context.annotations == {"test_annotation": 1}
        assert context.binary_annotations == {"status": "200"}
        assert context.port == 80
        assert context.sample_rate == 100.0
        assert context.add_logging_annotation is True
        assert context.report_root_timestamp_override is True
        assert context.use_128bit_trace_id is True
        assert context.host == "127.0.0.255"
        assert context._context_stack == stack
        assert context._span_storage == span_storage
        assert context.firehose_handler == firehose
        assert mock_generate_kind.call_count == 1
        assert mock_generate_kind.call_args == mock.call(
            context,
            Kind.CLIENT,
            ("cs", "cr"),
        )
        assert context.timestamp == 1234
        assert context.duration == 10
        assert context.encoding == Encoding.V2_JSON
        assert context._tracer == tracer
        # Check for backward compatibility
        assert tracer.get_spans() == span_storage
        assert tracer.get_context() == stack

    @mock.patch.object(zipkin.storage, "default_span_storage", autospec=True)
    @mock.patch.object(zipkin.zipkin_span, "_generate_kind", autospec=True)
    def test_init_defaults(self, mock_generate_kind, mock_storage):
        # Test that special arguments are properly defaulted
        mock_storage.return_value = SpanStorage()
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
        )

        assert context.service_name == "test_service"
        assert context.span_name == "test_span"
        assert context.annotations == {}
        assert context.binary_annotations == {}
        assert mock_generate_kind.call_args == mock.call(context, None, None)

    @mock.patch.object(zipkin.log, "warning", autospec=True)
    def test_init_override_timestamp_by_sr_ss(self, mock_log):
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            annotations={"sr": 100, "ss": 500},
        )

        assert context.timestamp == 100
        assert context.duration == 400
        assert mock_log.call_count == 1

    @mock.patch.object(zipkin.log, "warning", autospec=True)
    def test_init_override_timestamp_by_cr_cs(self, mock_log):
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            annotations={"cs": 100, "cr": 500},
        )

        assert context.timestamp == 100
        assert context.duration == 400
        assert mock_log.call_count == 1

    def test_init_not_local_root(self):
        # Normal spans are not marked as local root since they're not responsible
        # to log the resulting spans
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
        )

        assert context._is_local_root_span is False

    def test_init_local_root_transport_sample_rate(self):
        # If transport_handler and sample_rate are set, then this is a local root
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=MockTransportHandler(),
            sample_rate=100.0,
        )

        assert context._is_local_root_span is True

    def test_init_local_root_transport_zipkin_attrs(self):
        # If transport_handler and zipkin_attrs are set, then this is a local root
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=MockTransportHandler(),
            zipkin_attrs=zipkin.create_attrs_for_span(),
        )

        assert context._is_local_root_span is True

    def test_init_missing_transport(self):
        # Missing transport_handler
        with pytest.raises(ZipkinError):
            with zipkin.zipkin_span(
                service_name="some_service_name",
                span_name="span_name",
                port=5,
                sample_rate=100.0,
            ):
                pass

    def test_init_local_root_firehose(self):
        # If firehose_handler is set, then this is a local root even if there's
        # no sample_rate or zipkin_attrs
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            firehose_handler=MockTransportHandler(),
        )

        assert context._is_local_root_span is True

    def test_init_span_storage_wrong_type(self):
        # Missing transport_handler
        with pytest.raises(ZipkinError):
            with zipkin.zipkin_span(
                service_name="some_service_name",
                span_name="span_name",
                transport_handler=MockTransportHandler(),
                port=5,
                sample_rate=100.0,
                span_storage=[],
            ):
                pass

    def test_initinvalid_sample_rate(self):
        with pytest.raises(ZipkinError):
            with zipkin.zipkin_span(
                service_name="some_service_name",
                span_name="span_name",
                transport_handler=MockTransportHandler(),
                sample_rate=101.0,
            ):
                pass

        with pytest.raises(ZipkinError):
            with zipkin.zipkin_span(
                service_name="some_service_name",
                span_name="span_name",
                transport_handler=MockTransportHandler(),
                sample_rate=-0.1,
            ):
                pass

    def test_context_manager(self):
        # Test that the context manager passes all the possible arguments
        # to the zipkin_span constructor.

        # getfullargspec returns the signature of the function
        signature = inspect.getfullargspec(zipkin.zipkin_span).args

        @zipkin.zipkin_span("test_service", "test_span")
        def fn():
            pass

        with mock.patch.object(zipkin, "zipkin_span") as mock_ctx:
            fn()

            # call_args[1] returns the kwargs and we only check that the
            # list of keys is exactly the same as the signature.
            # We skip the first element of the signature since that's self.
            assert list(mock_ctx.call_args[1].keys()).sort() == signature[1:].sort()

    def test_enter(self):
        # Test that __enter__ calls self.start
        context = zipkin.zipkin_span("test_service", "test_span")
        with mock.patch.object(context, "start", autospec=True) as mock_start:
            context.__enter__()
            assert mock_start.call_count == 1

    def test_generate_kind(self):
        context = zipkin.zipkin_span("test_service", "test_span")

        assert context._generate_kind(Kind.SERVER, ("client",)) == Kind.SERVER
        assert context._generate_kind(None, ("client",)) == Kind.CLIENT
        assert context._generate_kind(None, ("server",)) == Kind.SERVER
        assert context._generate_kind(None, ("client", "server")) == Kind.LOCAL
        assert context._generate_kind(None, None) == Kind.LOCAL

    @mock.patch.object(zipkin, "create_attrs_for_span", autospec=True)
    def test_get_current_context_root_sample_rate_override_not_sampled(
        self,
        mock_create_attr,
    ):
        # Root span, with custom zipkin_attrs, not sampled and sample_rate
        zipkin_attrs = ZipkinAttrs(
            trace_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            parent_span_id=generate_random_64bit_string(),
            flags=None,
            is_sampled=False,
        )
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=MockTransportHandler(),
            zipkin_attrs=zipkin_attrs,
            sample_rate=100.0,
        )

        report_root, _ = context._get_current_context()

        assert mock_create_attr.call_args == mock.call(
            sample_rate=100.0,
            trace_id=zipkin_attrs.trace_id,
        )
        # It wasn't sampled before and now it is, so this is the trace root
        assert report_root is True

    @mock.patch.object(zipkin, "create_attrs_for_span", autospec=True)
    def test_get_current_context_root_sample_rate_override_sampled(
        self,
        mock_create_attr,
    ):
        # Root span, with custom zipkin_attrs, sampled
        # Just return the custom zipkin_attrs.
        zipkin_attrs = ZipkinAttrs(
            trace_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            parent_span_id=generate_random_64bit_string(),
            flags=None,
            is_sampled=True,
        )
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=MockTransportHandler(),
            zipkin_attrs=zipkin_attrs,
            sample_rate=100.0,
        )

        report_root, current_attrs = context._get_current_context()

        assert mock_create_attr.call_count == 0
        assert current_attrs == zipkin_attrs
        # The override was set and was already sampled, so this is probably
        # not the trace root.
        assert report_root is False

    @mock.patch.object(zipkin, "create_attrs_for_span", autospec=True)
    def test_get_current_context_root_sample_rate_no_override(
        self,
        mock_create_attr,
    ):
        # Root span, with sample_rate, no override
        # Just generate new zipkin_attrs
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=MockTransportHandler(),
            sample_rate=100.0,
        )

        report_root, _ = context._get_current_context()

        assert mock_create_attr.call_args == mock.call(
            sample_rate=100.0,
            use_128bit_trace_id=False,
        )
        # No override, which means this is for sure the trace root
        assert report_root is True

    @mock.patch.object(zipkin, "create_attrs_for_span", autospec=True)
    def test_get_current_context_root_no_sample_rate_no_override_firehose(
        self,
        mock_create_attr,
    ):
        # Root span, no sample_rate, no override, firehose set
        # Just generate new zipkin_attrs with sample_rate 0
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            firehose_handler=MockTransportHandler(),
        )

        report_root, _ = context._get_current_context()

        assert mock_create_attr.call_args == mock.call(
            sample_rate=0.0,
            use_128bit_trace_id=False,
        )
        # No override, which means this is for sure the trace root
        assert report_root is True

    @mock.patch.object(zipkin, "create_attrs_for_span", autospec=True)
    def test_get_current_context_non_root_existing(
        self,
        mock_create_attr,
    ):
        # Non root, zipkin_attrs in context stack.
        # Return existing zipkin_attrs with the current one as parent
        zipkin_attrs = ZipkinAttrs(
            trace_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            parent_span_id=generate_random_64bit_string(),
            flags=None,
            is_sampled=True,
        )
        tracer = MockTracer()
        context = tracer.zipkin_span(
            service_name="test_service",
            span_name="test_span",
        )
        tracer._context_stack.push(zipkin_attrs)

        _, current_attrs = context._get_current_context()

        assert mock_create_attr.call_count == 0
        assert current_attrs == ZipkinAttrs(
            trace_id=zipkin_attrs.trace_id,
            span_id=mock.ANY,
            parent_span_id=zipkin_attrs.span_id,
            flags=zipkin_attrs.flags,
            is_sampled=zipkin_attrs.is_sampled,
        )

    @mock.patch.object(zipkin, "create_attrs_for_span", autospec=True)
    def test_get_current_context_non_root_non_existing(
        self,
        mock_create_attr,
    ):
        # Non root, nothing in the stack
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
        )

        _, current_attrs = context._get_current_context()

        assert mock_create_attr.call_count == 0
        assert current_attrs is None

    def test_start_no_current_context(self):
        # No current context in the stack, let's exit immediately
        context = zipkin.zipkin_span("test_service", "test_span")

        with mock.patch.object(
            context.get_tracer()._context_stack, "push"
        ) as mock_push:
            context.start()
            assert mock_push.call_count == 0

    @mock.patch.object(zipkin, "ZipkinLoggingContext", autospec=True)
    @mock.patch("time.time", autospec=True, return_value=123)
    def test_start_root_span(self, mock_time, mock_log_ctx):
        transport = MockTransportHandler()
        firehose = MockTransportHandler()
        tracer = MockTracer()

        context = tracer.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=transport,
            firehose_handler=firehose,
            sample_rate=100.0,
            max_span_batch_size=50,
            encoding=Encoding.V2_JSON,
        )

        context.start()

        assert context.zipkin_attrs is not None
        assert context.start_timestamp == 123
        assert mock_log_ctx.call_args == mock.call(
            context.zipkin_attrs,
            mock.ANY,
            "test_span",
            transport,
            True,
            context.get_tracer,
            "test_service",
            binary_annotations={},
            add_logging_annotation=False,
            client_context=False,
            max_span_batch_size=50,
            firehose_handler=firehose,
            encoding=Encoding.V2_JSON,
            annotations={},
        )
        assert mock_log_ctx.return_value.start.call_count == 1
        assert tracer.is_transport_configured() is True

    @mock.patch.object(zipkin, "ZipkinLoggingContext", autospec=True)
    def test_start_root_span_not_sampled(self, mock_log_ctx):
        transport = MockTransportHandler()
        tracer = MockTracer()
        context = tracer.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=transport,
            sample_rate=0.0,
        )

        context.start()

        assert context.zipkin_attrs is not None
        assert mock_log_ctx.call_count == 0
        assert tracer.is_transport_configured() is False

    @mock.patch.object(zipkin, "ZipkinLoggingContext", autospec=True)
    def test_start_root_span_not_sampled_firehose(self, mock_log_ctx):
        # This request is not sampled, but firehose is setup. So we need to
        # setup the transport anyway.
        transport = MockTransportHandler()
        firehose = MockTransportHandler()
        tracer = MockTracer()
        context = tracer.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=transport,
            firehose_handler=firehose,
            sample_rate=0.0,
        )

        context.start()

        assert context.zipkin_attrs is not None
        assert mock_log_ctx.call_count == 1
        assert mock_log_ctx.return_value.start.call_count == 1
        assert tracer.is_transport_configured() is True

    @mock.patch.object(zipkin, "ZipkinLoggingContext", autospec=True)
    @mock.patch.object(zipkin.log, "info", autospec=True)
    def test_start_root_span_redef_transport(self, mock_log, mock_log_ctx):
        # Transport is already setup, so we should not override it
        # and log a message to inform the user.
        transport = MockTransportHandler()
        tracer = MockTracer()
        context = tracer.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=transport,
            sample_rate=100.0,
        )

        tracer.set_transport_configured(configured=True)

        context.start()

        assert context.zipkin_attrs is not None
        assert mock_log_ctx.call_count == 0
        assert tracer.is_transport_configured() is True
        assert mock_log.call_count == 1

    def test_exit(self):
        context = zipkin.zipkin_span("test_service", "test_span")

        with mock.patch.object(context, "stop", autospec=True) as mock_stop:
            context.__exit__(ValueError, "error", None)
            assert mock_stop.call_args == mock.call(ValueError, "error", None)

    def test_stop_no_transport(self):
        # Transport is not setup, exit immediately
        span_storage = SpanStorage()
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            span_storage=span_storage,
        )

        with mock.patch.object(context, "logging_context") as mock_log_ctx:
            context.start()
            context.stop()
            assert mock_log_ctx.stop.call_count == 0
            assert len(span_storage) == 0

    def test_stop_with_error(self):
        # Transport is not setup, exit immediately
        transport = MockTransportHandler()
        span_storage = SpanStorage()
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=transport,
            sample_rate=100.0,
            span_storage=span_storage,
        )

        with mock.patch.object(context, "update_binary_annotations") as mock_upd:
            context.start()
            context.stop(ValueError, "bad error")
            assert mock_upd.call_args == mock.call(
                {zipkin.ERROR_KEY: "ValueError: bad error"}
            )
            assert len(span_storage) == 0

    def test_stop_with_bad_error_that_cannot_be_stringified(self):
        class BadExceptionThatCannotBeStringified(Exception):
            def __str__(self):
                # Returning a non-str will cause a TypeError.
                return 42

            __unicode__ = __str__

        # Transport is not setup, exit immediately
        transport = MockTransportHandler()
        span_storage = SpanStorage()
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=transport,
            sample_rate=100.0,
            span_storage=span_storage,
        )

        with mock.patch.object(context, "update_binary_annotations") as mock_upd:
            context.start()
            context.stop(
                BadExceptionThatCannotBeStringified,
                BadExceptionThatCannotBeStringified(),
            )
            assert mock_upd.call_args == mock.call(
                {
                    zipkin.ERROR_KEY: "BadExceptionThatCannotBeStringified: BadExceptionThatCannotBeStringified()"  # noqa
                }
            )
            assert len(span_storage) == 0

    def test_error_stopping_log_context(self):
        """Tests if exception is raised while emitting traces that
        1. tracer is cleared
        2. excpetion is not passed up
        """
        transport = MockTransportHandler()
        tracer = MockTracer()
        context = tracer.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=transport,
            sample_rate=100.0,
        )
        context.start()

        with mock.patch.object(context, "logging_context") as mock_log_ctx:
            mock_log_ctx.stop.side_effect = Exception
            try:
                context.stop()
            except Exception:
                pytest.fail("Exception not expected to be thrown!")

            assert mock_log_ctx.stop.call_count == 1
            assert len(tracer.get_spans()) == 0

    def test_stop_root(self):
        transport = MockTransportHandler()
        tracer = MockTracer()
        context = tracer.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=transport,
            sample_rate=100.0,
        )
        context.start()

        with mock.patch.object(context, "logging_context") as mock_log_ctx:
            context.stop()
            assert mock_log_ctx.stop.call_count == 1
            # Test that we reset everything after calling stop()
            assert context.logging_context is None
            assert tracer.is_transport_configured() is False
            assert len(tracer.get_spans()) == 0

    @mock.patch("time.time", autospec=True, return_value=123)
    def test_stop_non_root(self, mock_time):
        tracer = MockTracer()
        tracer.set_transport_configured(configured=True)
        tracer.get_context().push(zipkin.create_attrs_for_span())
        context = tracer.zipkin_span(
            service_name="test_service",
            span_name="test_span",
        )
        context.start()

        context.stop()
        assert len(tracer.get_spans()) == 1
        endpoint = create_endpoint(service_name="test_service")
        assert tracer.get_spans()[0] == Span(
            trace_id=context.zipkin_attrs.trace_id,
            name="test_span",
            parent_id=context.zipkin_attrs.parent_span_id,
            span_id=context.zipkin_attrs.span_id,
            kind=Kind.LOCAL,
            timestamp=123,
            duration=0,
            annotations={},
            local_endpoint=endpoint,
            remote_endpoint=None,
            tags={},
        )

        assert tracer.is_transport_configured() is True

    def test_stop_non_root_ts_duration_overridden(self):
        tracer = MockTracer()
        tracer.set_transport_configured(configured=True)
        tracer.get_context().push(zipkin.create_attrs_for_span())
        ts = time.time()
        context = tracer.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            timestamp=ts,
            duration=25,
        )
        context.start()

        context.stop()
        assert len(tracer.get_spans()) == 1
        endpoint = create_endpoint(service_name="test_service")
        assert tracer.get_spans()[0] == Span(
            trace_id=context.zipkin_attrs.trace_id,
            name="test_span",
            parent_id=context.zipkin_attrs.parent_span_id,
            span_id=context.zipkin_attrs.span_id,
            kind=Kind.LOCAL,
            timestamp=ts,
            duration=25,
            annotations={},
            local_endpoint=endpoint,
            remote_endpoint=None,
            tags={},
        )

        assert tracer.is_transport_configured() is True

    def test_update_binary_annotations_root(self):
        with zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=MockTransportHandler(),
            sample_rate=100.0,
            binary_annotations={"region": "uswest-1"},
            add_logging_annotation=True,
        ) as span:
            span.update_binary_annotations({"status": "200"})

            assert span.logging_context.tags == {
                "region": "uswest-1",
                "status": "200",
            }

    def test_update_binary_annotations_non_root(self):
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            binary_annotations={"region": "uswest-1"},
        )
        context.get_tracer()._context_stack.push(zipkin.create_attrs_for_span())
        with context as span:
            span.update_binary_annotations({"status": "200"})

            assert span.binary_annotations == {
                "region": "uswest-1",
                "status": "200",
            }

    def test_update_binary_annotations_non_root_not_traced(self):
        # nothing happens if the request is not traced
        with zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            binary_annotations={"region": "uswest-1"},
        ) as span:
            span.update_binary_annotations({"status": "200"})

            assert span.binary_annotations == {
                "region": "uswest-1",
                "status": "200",
            }

    def test_add_annotation_root(self):
        with zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=MockTransportHandler(),
            sample_rate=100.0,
            annotations={"abc": 123},
            add_logging_annotation=True,
        ) as span:
            span.add_annotation("def", 345)
            span.add_annotation("ghi", timestamp=678)
            with mock.patch("py_zipkin.zipkin.time.time") as mock_time:
                mock_time.return_value = 91011
                span.add_annotation("jkl")

            assert span.logging_context.annotations == {
                "abc": 123,
                "def": 345,
                "ghi": 678,
                "jkl": 91011,
            }

    def test_add_annotation_non_root(self):
        context = zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            annotations={"abc": 123},
        )
        context.get_tracer()._context_stack.push(zipkin.create_attrs_for_span())
        with context as span:
            span.add_annotation("def", 345)
            span.add_annotation("ghi", timestamp=678)
            with mock.patch("py_zipkin.zipkin.time.time") as mock_time:
                mock_time.return_value = 91011
                span.add_annotation("jkl")

            assert span.annotations == {
                "abc": 123,
                "def": 345,
                "ghi": 678,
                "jkl": 91011,
            }

    def test_add_annotation_non_root_not_traced(self):
        # nothing happens if the request is not traced
        with zipkin.zipkin_span(
            service_name="test_service",
            span_name="test_span",
            annotations={"abc": 123},
        ) as span:
            span.add_annotation("def", 345)
            span.add_annotation("ghi", timestamp=678)
            with mock.patch("py_zipkin.zipkin.time.time") as mock_time:
                mock_time.return_value = 91011
                span.add_annotation("jkl")

            assert span.annotations == {
                "abc": 123,
                "def": 345,
                "ghi": 678,
                "jkl": 91011,
            }

    def test_add_sa_binary_annotation_non_client(self):
        # Nothing happens if this is not a client span
        context = zipkin.zipkin_span("test_service", "test_span")

        context.add_sa_binary_annotation(80, "remote_service", "10.1.2.3")

        assert context.remote_endpoint is None

    def test_add_sa_binary_annotation_non_root(self):
        # Nothing happens if this is not a client span
        with zipkin.zipkin_client_span("test_service", "test_span") as span:

            span.add_sa_binary_annotation(80, "remote_service", "10.1.2.3")

            expected_endpoint = create_endpoint(80, "remote_service", "10.1.2.3")
            assert span.remote_endpoint == expected_endpoint

            # setting it again throws an error
            with pytest.raises(ValueError):
                span.add_sa_binary_annotation(80, "remote_service", "10.1.2.3")

    def test_add_sa_binary_annotation_root(self):
        # Nothing happens if this is not a client span
        with zipkin.zipkin_client_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=MockTransportHandler(),
            sample_rate=100.0,
        ) as span:

            span.add_sa_binary_annotation(80, "remote_service", "10.1.2.3")

            expected_endpoint = create_endpoint(80, "remote_service", "10.1.2.3")
            assert span.logging_context.remote_endpoint == expected_endpoint

            # setting it again throws an error
            with pytest.raises(ValueError):
                span.add_sa_binary_annotation(80, "remote_service", "10.1.2.3")

    def test_override_span_name(self):
        with zipkin.zipkin_client_span(
            service_name="test_service",
            span_name="test_span",
            transport_handler=MockTransportHandler(),
            sample_rate=100.0,
        ) as span:
            span.override_span_name("new_name")

            assert span.span_name == "new_name"
            assert span.logging_context.span_name == "new_name"


def test_zipkin_client_span():
    context = zipkin.zipkin_client_span("test_service", "test_span")

    assert context.kind == Kind.CLIENT

    with pytest.raises(ValueError):
        zipkin.zipkin_client_span("test_service", "test_span", kind=Kind.LOCAL)


def test_zipkin_server_span():
    context = zipkin.zipkin_server_span("test_service", "test_span")

    assert context.kind == Kind.SERVER

    with pytest.raises(ValueError):
        zipkin.zipkin_server_span("test_service", "test_span", kind=Kind.LOCAL)


def test_create_headers_for_new_span_empty_if_no_active_request():
    with mock.patch.object(get_default_tracer(), "get_zipkin_attrs") as mock_ctx:
        mock_ctx.return_value = None
        assert {} == zipkin.create_http_headers_for_new_span()


@mock.patch("py_zipkin.zipkin.create_http_headers", autospec=True)
def test_create_headers_for_new_span(mock_create_http_headers):
    tracer = MockTracer()
    context_stack = Stack()
    zipkin.create_http_headers_for_new_span(context_stack, tracer)

    assert mock_create_http_headers.call_count == 1
    assert mock_create_http_headers.call_args == mock.call(
        context_stack,
        tracer,
        True,
    )
