import time
from collections import namedtuple
from contextlib import contextmanager

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


@contextmanager
def zipkin_trace_context(
    span_name,
    transport_handler,
    zipkin_attrs=None,
    port=0,
    service_name='unknown',
):
    """Convenient contextmanager that builds the root span of a Zipkin trace.

    When this context is exited, any spans stored in its context will be logged
    to zipkin's scribe handler.

    If zipkin_attrs is not specified, a default set of attributes is generated
    with sampling turned on.

    :param span_name: The name of the root span
    :type span_name: str
    :param transport_handler: Callback function that handles publishing messages
                            to a stream
    :type transport_handler: function
    :param zipkin_attrs: Optional Zipkin attributes for the root span
    :type zipkin_attrs: ZipkinAttr or None
    :param port: The port number of this service
    :type port: int
    :param service_name: The name of this service
    :type service_name: str
    """
    if not zipkin_attrs:
        zipkin_attrs = create_attrs_for_root_span()

    push_zipkin_attrs(zipkin_attrs)

    try:
        if not zipkin_attrs.is_sampled:
            yield
        else:
            endpoint = create_endpoint(port, service_name)
            log_handler = ZipkinLoggerHandler(zipkin_attrs)
            with ZipkinLoggingContext(
                zipkin_attrs,
                endpoint,
                log_handler,
                span_name,
                transport_handler,
            ):
                yield
    finally:
        # Regardless of what happens in the request we want to pop attrs
        pop_zipkin_attrs()


def zipkin_span(service_name, span_name=None, binary_annotations=None):
    """Decorator utility for logging a function as a Zipkin span.
    Usage:

    @zipkin_span('my_service', 'my_span_name')
    def my_funky_function(a, b):
        return a + b

    :param service_name: Name of the "service" for the to-be-logged span
    :type service_name: string
    :param span_name: Name of the span to be logged. Defaults to func.__name__
    :type span_name: string
    :param binary_annotations: Additional span binary annotations
    :type binary_annotations: dict of str -> str
    """
    def outer(func):
        def inner(*args, **kwargs):
            with SpanContext(
                service_name=service_name,
                span_name=span_name or func.__name__,
                binary_annotations=binary_annotations,
            ):
                return func(*args, **kwargs)
        return inner
    return outer


class SpanContext(object):
    """This contextmanager creates a new span (with cs, sr, ss, and cr)
    annotations. Each additional client span logged inside this context will
    have their parent span set to this new span's ID. It accomplishes that by
    attaching this span's ID to the logger handler.

    Note: this contextmanager ONLY works within a ZipkinLoggingContext
    context. Otherwise, the proper logging handlers will not be set up.
    """

    def __init__(
        self,
        service_name,
        span_name='span',
        annotations=None,
        binary_annotations=None,
    ):
        """Enter the client context. Initializes a bunch of state related
        to this span.

        :param service_name: The name of the called service
        :param span_name: Optional name of span, defaults to 'span'
        :param annotations: Optional dict of str -> timestamp annotations
        :param binary_annotations: Optional dict of str -> str span attrs
        """
        self.service_name = service_name
        self.span_name = span_name
        self.annotations = annotations or {}
        self.binary_annotations = binary_annotations or {}
        self.logging_initialized = False

    def __enter__(self):
        """Enter the new span context. All spans/annotations logged inside this
        context will be attributed to this span.

        In the unsampled case, this context still generates new span IDs and
        pushes them onto the threadlocal stack, so downstream services calls
        made will pass the correct headers. However, the logging handler is
        never attached in the unsampled case, so it is left alone.
        """
        zipkin_attrs = get_zipkin_attrs()
        self.is_sampled = zipkin_attrs is not None and zipkin_attrs.is_sampled
        self.span_id = generate_random_64bit_string()
        self.start_timestamp = time.time()
        # Push new zipkin attributes onto the threadlocal stack, so that
        # create_headers_for_new_span() performs as expected in this context.
        # The only difference is that span_id is this new span's ID
        # and parent_span_id is the old span's ID. Checking for a None
        # zipkin_attrs value is protecting against calling this outside of
        # a zipkin logging context entirely (e.g. in a batch). If new attrs
        # are stored, set a flag to pop them off at context exit.
        self.do_pop_attrs = False
        if zipkin_attrs is not None:
            new_zipkin_attrs = ZipkinAttrs(
                trace_id=zipkin_attrs.trace_id,
                span_id=self.span_id,
                parent_span_id=zipkin_attrs.span_id,
                flags=zipkin_attrs.flags,
                is_sampled=zipkin_attrs.is_sampled,
            )
            push_zipkin_attrs(new_zipkin_attrs)
            self.do_pop_attrs = True
        # In the sampled case, patch the ZipkinLoggerHandler.
        if self.is_sampled:
            # Be defensive about logging setup. Since ZipkinAttrs are local
            # the a thread, multithreaded frameworks can get in strange states.
            # The logging is not going to be correct in these cases, so we set
            # a flag that turns off logging on __exit__.
            if len(zipkin_logger.handlers) > 0:
                # Put span ID on logging handler. Assume there's only a single
                # handler, since all logging should be set up in this package.
                self.log_handler = zipkin_logger.handlers[0]
                # Store the old parent_span_id, probably None, in case we have
                # nested SpanContexts
                self.old_parent_span_id = self.log_handler.parent_span_id
                self.log_handler.parent_span_id = self.span_id
                self.logging_initialized = True

        return self

    def __exit__(self, _exc_type, _exc_value, _exc_traceback):
        """Exit the span context. The new zipkin attrs are pushed onto the
        threadlocal stack regardless of sampling, so they always need to be
        popped off. The actual logging of spans depends on sampling and that
        the logging was correctly set up.
        """
        if self.do_pop_attrs:
            pop_zipkin_attrs()
        if not (self.is_sampled and self.logging_initialized):
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
            span_id=self.span_id,
        )


def create_attrs_for_root_span():
    return ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags='0',
        is_sampled=True,
    )


def create_http_headers_for_new_span():
    """
    Generate the headers for a new zipkin span.

    .. note::

        If the method is not called from within a pyramid service call OR
        pyramid_zipkin is not included as a pyramid tween, empty dict will be
        returned back.

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
