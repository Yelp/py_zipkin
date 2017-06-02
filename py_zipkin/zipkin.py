# -*- coding: utf-8 -*-
import functools
import random
import time
from collections import namedtuple

from py_zipkin.exception import ZipkinError
from py_zipkin.logging_helper import zipkin_logger
from py_zipkin.logging_helper import ZipkinLoggerHandler
from py_zipkin.logging_helper import ZipkinLoggingContext
from py_zipkin.thread_local import get_zipkin_attrs
from py_zipkin.thread_local import pop_zipkin_attrs
from py_zipkin.thread_local import push_zipkin_attrs
from py_zipkin.thrift import SERVER_ADDR_VAL
from py_zipkin.thrift import create_binary_annotation
from py_zipkin.thrift import create_endpoint
from py_zipkin.thrift import zipkin_core
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.util import generate_random_128bit_string


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


STANDARD_ANNOTATIONS = {
    'client': {'cs', 'cr'},
    'server': {'ss', 'sr'},
}
STANDARD_ANNOTATIONS_KEYS = frozenset(STANDARD_ANNOTATIONS.keys())


class zipkin_span(object):
    """Context manager/decorator for all of your zipkin tracing needs.

    Usage #1: Start a trace with a given sampling rate

    This begins the zipkin trace and also records the root span. The required
    params are service_name, transport_handler, and sample_rate.

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
    service_name, zipkin_attrs, transport_handler, and port.

    # Used in a pyramid tween
    def tween(request):
        zipkin_attrs = some_zipkin_attr_creator(request)
        with zipkin_span(
            service_name='my_service,'
            span_name='my_span_name',
            zipkin_attrs=zipkin_attrs,
            transport_handler=some_handler,
            port=22,
        ) as zipkin_context:
            response = handler(request)
            zipkin_context.update_binary_annotations(
                some_binary_annotations)
            return response

    Usage #3: Log a span within the context of a zipkin trace

    If you're already in a zipkin trace, you can use this to log a span inside. The
    only required param is service_name. If you're not in a zipkin trace, this
    won't do anything.

    # As a decorator
    @zipkin_span(service_name='my_service', span_name='my_function')
    def my_function():
        do_stuff()

    # As a context manager
    def my_function():
        with zipkin_span(service_name='my_service', span_name='do_stuff'):
            do_stuff()
    """

    def __init__(
        self,
        service_name,
        span_name='span',
        zipkin_attrs=None,
        transport_handler=None,
        annotations=None,
        binary_annotations=None,
        port=0,
        sample_rate=None,
        include=('client', 'server'),
        add_logging_annotation=False,
        report_root_timestamp=False,
        use_128bit_trace_id=False,
        host=None
    ):
        """Logs a zipkin span. If this is the root span, then a zipkin
        trace is started as well.

        :param service_name: The name of the called service
        :type service_name: string
        :param span_name: Optional name of span, defaults to 'span'
        :type span_name: string
        :param zipkin_attrs: Optional set of zipkin attributes to be used
        :type zipkin_attrs: ZipkinAttrs
        :param transport_handler: Callback function that takes a message parameter
            and handles logging it
        :type transport_handler: function
        :param annotations: Optional dict of str -> timestamp annotations
        :type annotations: dict of str -> int
        :param binary_annotations: Optional dict of str -> str span attrs
        :type binary_annotations: dict of str -> str
        :param port: The port number of the service. Defaults to 0.
        :type port: int
        :param sample_rate: Rate at which to sample; 0.0 - 100.0. If passed-in
            zipkin_attrs have is_sampled=False and the sample_rate param is > 0,
            a new span will be generated at this rate. This means that if you
            propagate sampling decisions to downstream services, but still have
            sample_rate > 0 in those services, the actual rate of generated
            spans for those services will be > sampling_rate.
        :type sample_rate: float
        :param include: which annotations to include
            can be one of {'client', 'server'}
            corresponding to ('cs', 'cr') and ('ss', 'sr') respectively
        :type include: iterable
        :param add_logging_annotation: Whether to add a 'logging_end'
            annotation when py_zipkin finishes logging spans
        :type add_logging_annotation: boolean
        :param report_root_timestamp: Whether the span should report timestamp
            and duration. Only applies to "root" spans in this local context,
            so spans created inside other span contexts will always log
            timestamp/duration. Note that this is only an override for spans
            that have zipkin_attrs passed in. Spans that make their own
            sampling decisions (i.e. are the root spans of entire traces) will
            always report timestamp/duration.
        :type report_root_timestamp: boolean
        :param use_128bit_trace_id: If true, generate 128-bit trace_ids
        :type use_128bit_trace_id: boolean
        :param host: Contains the ipv4 value of the host. The ipv4 value isn't
            automatically determined in a docker environment
        :type host: string
        """
        self.service_name = service_name
        self.span_name = span_name
        self.zipkin_attrs = zipkin_attrs
        self.transport_handler = transport_handler
        self.annotations = annotations or {}
        self.binary_annotations = binary_annotations or {}
        self.port = port
        self.logging_context = None
        self.sample_rate = sample_rate
        self.include = include
        self.add_logging_annotation = add_logging_annotation
        self.report_root_timestamp_override = report_root_timestamp
        self.use_128bit_trace_id = use_128bit_trace_id
        self.host = host

        # Spans that log a 'cs' timestamp can additionally record
        # 'sa' binary annotations that show where the request is going.
        # This holds a list of 'sa' binary annotations.
        self.sa_binary_annotations = []

        # Validation checks
        if self.zipkin_attrs or self.sample_rate is not None:
            if self.transport_handler is None:
                raise ZipkinError(
                    'Root spans require a transport handler to be given')

        if self.sample_rate is not None and not (0.0 <= self.sample_rate <= 100.0):
            raise ZipkinError('Sample rate must be between 0.0 and 100.0')

        if not set(include).issubset(STANDARD_ANNOTATIONS_KEYS):
            raise ZipkinError(
                'Only %s are supported as annotations' %
                STANDARD_ANNOTATIONS_KEYS
            )
        else:
            # get a list of all of the mapped annotations
            self.annotation_filter = set()
            for include_name in include:
                self.annotation_filter.update(STANDARD_ANNOTATIONS[include_name])

    def __call__(self, f):
        @functools.wraps(f)
        def decorated(*args, **kwargs):
            with zipkin_span(
                service_name=self.service_name,
                span_name=self.span_name,
                zipkin_attrs=self.zipkin_attrs,
                transport_handler=self.transport_handler,
                annotations=self.annotations,
                binary_annotations=self.binary_annotations,
                port=self.port,
                sample_rate=self.sample_rate,
                include=self.include,
                host=self.host
            ):
                return f(*args, **kwargs)
        return decorated

    def __enter__(self):
        return self.start()

    def start(self):
        """Enter the new span context. All annotations logged inside this
        context will be attributed to this span. All new spans generated
        inside this context will have this span as their parent.

        In the unsampled case, this context still generates new span IDs and
        pushes them onto the threadlocal stack, so downstream services calls
        made will pass the correct headers. However, the logging handler is
        never attached in the unsampled case, so the spans are never logged.
        """
        self.do_pop_attrs = False
        # If zipkin_attrs are passed in or this span is doing its own sampling,
        # it will need to actually log spans at __exit__.
        self.perform_logging = self.zipkin_attrs or self.sample_rate is not None
        report_root_timestamp = False

        if self.sample_rate is not None:
            if self.zipkin_attrs and not self.zipkin_attrs.is_sampled:
                report_root_timestamp = True
                self.zipkin_attrs = create_attrs_for_span(
                    sample_rate=self.sample_rate,
                    trace_id=self.zipkin_attrs.trace_id,
                    use_128bit_trace_id=self.use_128bit_trace_id,
                )
            elif not self.zipkin_attrs:
                report_root_timestamp = True
                self.zipkin_attrs = create_attrs_for_span(
                    sample_rate=self.sample_rate,
                    use_128bit_trace_id=self.use_128bit_trace_id,
                )

        if not self.zipkin_attrs:
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

        # If zipkin_attrs are not set up by now, that means this span is not
        # configured to perform logging itself, and it's not in an existing
        # Zipkin trace. That means there's nothing else to do and it can exit
        # early.
        if not self.zipkin_attrs:
            return self

        push_zipkin_attrs(self.zipkin_attrs)
        self.do_pop_attrs = True

        self.start_timestamp = time.time()

        if self.perform_logging:
            # Don't set up any logging if we're not sampling
            if not self.zipkin_attrs.is_sampled:
                return self
            endpoint = create_endpoint(self.port, self.service_name, self.host)
            self.log_handler = ZipkinLoggerHandler(self.zipkin_attrs)
            self.logging_context = ZipkinLoggingContext(
                self.zipkin_attrs,
                endpoint,
                self.log_handler,
                self.span_name,
                self.transport_handler,
                report_root_timestamp or self.report_root_timestamp_override,
                binary_annotations=self.binary_annotations,
                add_logging_annotation=self.add_logging_annotation,
            )
            self.logging_context.start()
            return self
        else:
            # In the sampled case, patch the ZipkinLoggerHandler.
            if self.zipkin_attrs.is_sampled:
                # Be defensive about logging setup. Since ZipkinAttrs are local to
                # the thread, multithreaded frameworks can get in strange states.
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
        self.stop(_exc_type, _exc_value, _exc_traceback)

    def stop(self, _exc_type=None, _exc_value=None, _exc_traceback=None):
        """Exit the span context. Zipkin attrs are pushed onto the
        threadlocal stack regardless of sampling, so they always need to be
        popped off. The actual logging of spans depends on sampling and that
        the logging was correctly set up.
        """
        if self.do_pop_attrs:
            pop_zipkin_attrs()

        if not self.zipkin_attrs or not self.zipkin_attrs.is_sampled:
            return

        # Logging context is only initialized for "root" spans of the local
        # process (i.e. this zipkin_span not inside of any other local
        # zipkin_spans)
        if self.logging_context:
            self.logging_context.stop()
            self.logging_context = None
            return

        # If we've gotten here, that means that this span is a child span of
        # this context's root span (i.e. it's a zipkin_span inside another
        # zipkin_span).
        end_timestamp = time.time()

        self.log_handler.parent_span_id = self.old_parent_span_id

        # We are simulating a full two-part span locally, so set cs=sr and ss=cr
        full_annotations = {
            'cs': self.start_timestamp,
            'sr': self.start_timestamp,
            'ss': end_timestamp,
            'cr': end_timestamp,
        }
        # But we filter down if we only want to emit some of the annotations
        filtered_annotations = {
            k: v for k, v in full_annotations.items()
            if k in self.annotation_filter
        }

        self.annotations.update(filtered_annotations)

        self.log_handler.store_local_span(
            span_name=self.span_name,
            service_name=self.service_name,
            annotations=self.annotations,
            binary_annotations=self.binary_annotations,
            sa_binary_annotations=self.sa_binary_annotations,
            span_id=self.zipkin_attrs.span_id,
        )

    def update_binary_annotations(self, extra_annotations):
        """Updates the binary annotations for the current span.

        If this trace is not being sampled then this is a no-op.
        """
        if not self.zipkin_attrs:
            return
        if not self.zipkin_attrs.is_sampled:
            return
        if not self.logging_context:
            # This is not the root span, so binary annotations will be added
            # to the log handler when this span context exits.
            self.binary_annotations.update(extra_annotations)
        else:
            # Otherwise, we're in the context of the root span, so just update
            # the binary annotations for the logging context directly.
            self.logging_context.binary_annotations_dict.update(extra_annotations)

    def add_sa_binary_annotation(
        self,
        port=0,
        service_name='unknown',
        host='127.0.0.1',
    ):
        """Adds a 'sa' binary annotation to the current span.

        'sa' binary annotations are useful for situations where you need to log
        where a request is going but the destination doesn't support zipkin.

        Note that the span must have 'cs'/'cr' annotations.

        :param port: The port number of the destination
        :type port: int
        :param service_name: The name of the destination service
        :type service_name: str
        :param host: Host address of the destination
        :type host: str
        """
        if not self.zipkin_attrs or not self.zipkin_attrs.is_sampled:
            return

        if 'client' not in self.include:
            # TODO: trying to set a sa binary annotation for a non-client span
            # should result in a logged error
            return

        sa_endpoint = create_endpoint(
            port=port,
            service_name=service_name,
            host=host,
        )
        sa_binary_annotation = create_binary_annotation(
            key=zipkin_core.SERVER_ADDR,
            value=SERVER_ADDR_VAL,
            annotation_type=zipkin_core.AnnotationType.BOOL,
            host=sa_endpoint,
        )
        if not self.logging_context:
            self.sa_binary_annotations.append(sa_binary_annotation)
        else:
            self.logging_context.sa_binary_annotations.append(sa_binary_annotation)


def _validate_args(kwargs):
    if 'include' in kwargs:
        raise ValueError(
            '"include" is not valid in this context. '
            'You probably want to use zipkin_span()'
        )


class zipkin_client_span(zipkin_span):
    """Logs a client-side zipkin span.

    Subclass of :class:`zipkin_span` using only annotations relevant to clients
    """

    def __init__(self, *args, **kwargs):
        """Logs a zipkin span with client annotations.

        See :class:`zipkin_span` for arguments
        """
        _validate_args(kwargs)

        kwargs['include'] = ('client',)
        super(zipkin_client_span, self).__init__(*args, **kwargs)


class zipkin_server_span(zipkin_span):
    """Logs a server-side zipkin span.

    Subclass of :class:`zipkin_span` using only annotations relevant to servers
    """

    def __init__(self, *args, **kwargs):
        """Logs a zipkin span with server annotations.

        See :class:`zipkin_span` for arguments
        """
        _validate_args(kwargs)

        kwargs['include'] = ('server',)
        super(zipkin_server_span, self).__init__(*args, **kwargs)


def create_attrs_for_span(
    sample_rate=100.0,
    trace_id=None,
    span_id=None,
    use_128bit_trace_id=False,
):
    """Creates a set of zipkin attributes for a span.

    :param sample_rate: Float between 0.0 and 100.0 to determine sampling rate
    :type sample_rate: float
    :param trace_id: Optional 16-character hex string representing a trace_id.
                    If this is None, a random trace_id will be generated.
    :type trace_id: str
    :param span_id: Optional 16-character hex string representing a span_id.
                    If this is None, a random span_id will be generated.
    :type span_id: str
    """
    # Calculate if this trace is sampled based on the sample rate
    if trace_id is None:
        if use_128bit_trace_id:
            trace_id = generate_random_128bit_string()
        else:
            trace_id = generate_random_64bit_string()
    if span_id is None:
        span_id = generate_random_64bit_string()
    if sample_rate == 0.0:
        is_sampled = False
    else:
        is_sampled = (random.random() * 100) < sample_rate

    return ZipkinAttrs(
        trace_id=trace_id,
        span_id=span_id,
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
