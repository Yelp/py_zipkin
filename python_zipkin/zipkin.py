# -*- coding: utf-8 -*-
import functools
import time
from collections import namedtuple

from python_zipkin.exception import ZipkinError
from python_zipkin.logging_helper import zipkin_logger
from python_zipkin.logging_helper import ZipkinLoggerHandler
from python_zipkin.logging_helper import ZipkinLoggingContext
from python_zipkin.thread_local import get_zipkin_attrs
from python_zipkin.thread_local import pop_zipkin_attrs
from python_zipkin.thread_local import push_zipkin_attrs
from python_zipkin.thrift import create_endpoint
from python_zipkin.util import generate_random_64bit_string


"""
Holds the basic attributes needed to log a zipkin trace

:param trace_id: Unique trace id
:param span_id: Span Id of the current request span
:param parent_span_id: Parent span Id of the current request span
:param flags: stores flags header. Currently unused
:param is_sampled: pre-computed boolean whether the trace should be logged
"""
ZipkinAttrs = namedtuple(
    'ZipkinAttrs',
    ['trace_id', 'span_id', 'parent_span_id', 'flags', 'is_sampled'],
)


class zipkin_span(object):
    """Context manager/decorator for all of your zipkin tracing needs.

    Usage #1: Start a trace with a given sampling rate

    This begins the zipkin trace and also records the root span. The required
    params are service_name, transport_handler, port, and sample_rate.

    # Start a trace with do_stuff() as the root span
    def some_batch_job(a, b):
        with zipkin_span(
            service_name='my_service',
            span_name='my_span_name',
            transport_handler=some_handler,
            port=22,
            sample_rate=0.05,
        ):
            do_stuff()

    Usage #2: Trace a service call.

    The typical use case is instrumenting a framework like Pyramid or Django. Only
    ss and sr times are recorded for the root span. Required params are
    service_name, zipkin_attrs, transport_handler, port, and is_service (which is
    set to True).

    # Used in a pyramid tween
    def tween(request):
        zipkin_attrs = some_zipkin_attr_creator(request)
        with zipkin_span(
            service_name='my_service,'
            span_name='my_span_name',
            zipkin_attrs=zipkin_attrs,
            transport_handler=some_handler,
            port=22,
            is_service=True,
        ) as zipkin_context:
            response = handler(request)
            zipkin_context.update_binary_annotations_for_root_span(
                some_binary_annotations)
            return response

    Usage #3: Log a span within the context of a zipkin trace

    If you're already in a zipkin trace, you can use this to log a span inside. The
    only required param is service_name.

    # As a decorator
    @zipkin_span(service_name='my_service', span_name='my_function')
    def my_function():
        do_stuff()

    # As a context manager
    def my_function():
        with zipkin_span(service_name='my_service', span_name='do_stuff'):
            do_stuff()

    :param service_name: Name of the "service" for the to-be-logged span
    :type service_name: string
    :param span_name: Name of the span to be logged. Defaults to func.__name__
    :type span_name: string
    :param binary_annotations: Additional span binary annotations
    :type binary_annotations: dict of str -> str
    """
    def __init__(
        self,
        service_name,
        span_name='span',
        zipkin_attrs=None,
        transport_handler=None,
        annotations=None,
        binary_annotations=None,
        port=None,
        is_service=False,
        sample_rate=None,
    ):
        """Logs a zipkin span. If this is the root span, then a zipkin
        trace is started as well.

        :param service_name: The name of the called service
        :type service_name: string
        :param span_name: Optional name of span, defaults to 'span'
        :type span_name: string
        :param zipkin_attrs: Optional set of zipkin attributes to be used
        :type zipkin_attrs: ZipkinAttrs
        :param annotations: Optional dict of str -> timestamp annotations
        :type annotations: dict of str -> int
        :param binary_annotations: Optional dict of str -> str span attrs
        :type binary_annotations: dict of str -> str
        :param port: The port number of the service
        :type port: int
        :param is_service: True if this is the root span of a service call
        :type is_service: bool
        :param sample_rate: Custom sampling rate (between 100.0 and 0.0) if
                            this is the root of the trace
        :type sample_rate: float
        """
        self.service_name = service_name
        self.span_name = span_name
        self.zipkin_attrs = zipkin_attrs
        self.transport_handler = transport_handler
        self.annotations = annotations or {}
        self.binary_annotations = binary_annotations or {}
        self.port = port
        self.is_service = is_service
        self.sample_rate = sample_rate
        self.logging_context = None

    def __call__(self, f):
        @functools.wraps(f)
        def decorated(*args, **kwargs):
            with self:
                return f(*args, **kwargs)
        return decorated

    def __enter__(self):
        """Enter the new span context. All spans/annotations logged inside this
        context will be attributed to this span.

        In the unsampled case, this context still generates new span IDs and
        pushes them onto the threadlocal stack, so downstream services calls
        made will pass the correct headers. However, the logging handler is
        never attached in the unsampled case, so it is left alone.
        """
        self.do_pop_attrs = False
        self.is_root = False
        if self.sample_rate is not None:
            # Treat this as root span and evaluate sampling rate
            self.is_root = True
            if self.transport_handler is None:
                raise ZipkinError(
                    'Sample rate requires a transport handler to be given')
            if self.port is None:
                raise ZipkinError('Port number is required')
            self.zipkin_attrs = create_attrs_for_root_span(
                sample_rate=self.sample_rate,
            )
        elif self.is_service:
            # Meant to be the root span of a service trace
            self.is_root = True
            if self.zipkin_attrs is None:
                raise ZipkinError(
                    'Service trace requires zipkin attrs to be passed in')
            if self.port is None:
                raise ZipkinError('Port number is required')
            if self.transport_handler is None:
                raise ZipkinError(
                    'Sample rate requires a transport handler to be given')
        else:
            # This span is inside the context of an existing trace
            existing_zipkin_attrs = get_zipkin_attrs()
            if existing_zipkin_attrs:
                self.zipkin_attrs = ZipkinAttrs(
                    trace_id=existing_zipkin_attrs.trace_id,
                    span_id=generate_random_64bit_string(),
                    parent_span_id=existing_zipkin_attrs.span_id,
                    flags=existing_zipkin_attrs.flags,
                    is_sampled=existing_zipkin_attrs.is_sampled,
                )

        # Don't do anything if zipkin attributes are not set up
        if not self.zipkin_attrs:
            return self

        push_zipkin_attrs(self.zipkin_attrs)
        self.do_pop_attrs = True

        self.start_timestamp = time.time()

        # Set up logging if this is the root span
        if self.is_root:
            # Don't set up any logging if we're not sampling
            if not self.zipkin_attrs.is_sampled:
                return self

            endpoint = create_endpoint(self.port, self.service_name)
            self.log_handler = ZipkinLoggerHandler(self.zipkin_attrs)
            self.logging_context = ZipkinLoggingContext(
                self.zipkin_attrs,
                endpoint,
                self.log_handler,
                self.span_name,
                self.transport_handler,
            )
            self.logging_context.__enter__()
            return self
        else:
            # In the sampled case, patch the ZipkinLoggerHandler.
            if self.zipkin_attrs.is_sampled:
                # Be defensive about logging setup. Since ZipkinAttrs are local
                # the a thread, multithreaded frameworks can get in strange states.
                # The logging is not going to be correct in these cases, so we set
                # a flag that turns off logging on __exit__.
                if len(zipkin_logger.handlers) > 0:
                    # Put span ID on logging handler. Assume there's only a single
                    # handler, since all logging should be set up in this package.
                    self.log_handler = zipkin_logger.handlers[0]
                    # Store the old parent_span_id, probably None, in case we have
                    # nested zipkin_spans
                    self.old_parent_span_id = self.log_handler.parent_span_id
                    self.log_handler.parent_span_id = self.zipkin_attrs.span_id

            return self

    def __exit__(self, _exc_type, _exc_value, _exc_traceback):
        """Exit the span context. Zipkin attrs are pushed onto the
        threadlocal stack regardless of sampling, so they always need to be
        popped off. The actual logging of spans depends on sampling and that
        the logging was correctly set up.
        """
        # Always remove the stored zipkin_attrs
        if self.do_pop_attrs:
            pop_zipkin_attrs()

        # Exit early if this request is not being sampled
        if not self.zipkin_attrs or not self.zipkin_attrs.is_sampled:
            return

        # If this is the root span, exit the context (which will handle logging)
        if self.logging_context:
            self.logging_context.__exit__(_exc_type, _exc_value, _exc_traceback)
            return

        end_timestamp = time.time()

        # Put the old parent_span_id back on the handler
        self.log_handler.parent_span_id = self.old_parent_span_id

        # To get a full span we just set cs=sr and ss=cr.
        self.annotations.update({
            'cs': self.start_timestamp,
            'sr': self.start_timestamp,
            'ss': end_timestamp,
            'cr': end_timestamp,
        })

        # Store this span on the logging handler object.
        self.log_handler.store_client_span(
            span_name=self.span_name,
            service_name=self.service_name,
            annotations=self.annotations,
            binary_annotations=self.binary_annotations,
            span_id=self.zipkin_attrs.span_id,
        )

    def update_binary_annotations_for_root_span(self, extra_annotations):
        """Updates the binary annotations for the root span of the trace.

        If this trace is not being sampled then this is a no-op.
        """
        if not self.zipkin_attrs.is_sampled:
            return
        if not self.logging_context:
            raise ZipkinError('No logging context available')
        self.logging_context.binary_annotations_dict.update(extra_annotations)


def create_attrs_for_root_span(sample_rate=100.0):
    """Creates a set of zipkin attributes for the root span of a trace.

    :param sample_rate: Float between 0.0 and 100.0 to determine sampling rate
    :type sample_rate: float
    """
    # Calculate if this trace is sampled based on the sample rate
    trace_id = generate_random_64bit_string()
    if sample_rate == 0.0:
        is_sampled = False
    else:
        inverse_frequency = int((1.0 / sample_rate) * 100)
        is_sampled = (int(trace_id, 16) % inverse_frequency) == 0

    return ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags='0',
        is_sampled=is_sampled,
    )


def create_http_headers_for_new_span():
    """
    Generate the headers for a new zipkin span.

    .. note::

        If the method is not called from within a zipkin_trace conext,
        empty dict will be returned back.

    :returns: dict containing (X-B3-TraceId, X-B3-SpanId, X-B3-ParentSpanId,
                X-B3-Flags and X-B3-Sampled) keys OR an empty dict.
    """
    zipkin_attrs = get_zipkin_attrs()

    if not zipkin_attrs:
        return {}

    return {
        'X-B3-TraceId': zipkin_attrs.trace_id,
        'X-B3-SpanId': generate_random_64bit_string(),
        'X-B3-ParentSpanId': zipkin_attrs.span_id,
        'X-B3-Flags': '0',
        'X-B3-Sampled': '1' if zipkin_attrs.is_sampled else '0',
    }
