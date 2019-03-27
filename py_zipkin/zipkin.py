# -*- coding: utf-8 -*-
import functools
import logging
import random
import time
from collections import namedtuple

from py_zipkin import Encoding
from py_zipkin import Kind
from py_zipkin import storage
from py_zipkin.encoding._helpers import create_endpoint
from py_zipkin.encoding._helpers import Span
from py_zipkin.exception import ZipkinError
from py_zipkin.logging_helper import ZipkinLoggingContext
from py_zipkin.storage import get_default_tracer
from py_zipkin.util import generate_random_128bit_string
from py_zipkin.util import generate_random_64bit_string

log = logging.getLogger(__name__)

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

ERROR_KEY = 'error'


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
        max_span_batch_size=None,
        annotations=None,
        binary_annotations=None,
        port=0,
        sample_rate=None,
        include=None,
        add_logging_annotation=False,
        report_root_timestamp=False,
        use_128bit_trace_id=False,
        host=None,
        context_stack=None,
        span_storage=None,
        firehose_handler=None,
        kind=None,
        timestamp=None,
        duration=None,
        encoding=Encoding.V1_THRIFT,
        _tracer=None,
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
        :type transport_handler: BaseTransportHandler
        :param max_span_batch_size: Spans in a trace are sent in batches,
            max_span_batch_size defines max size of one batch
        :type max_span_batch_size: int
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
            corresponding to ('cs', 'cr') and ('ss', 'sr') respectively.
            DEPRECATED: use kind instead. `include` will be removed in 1.0.
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
        :param use_128bit_trace_id: If true, generate 128-bit trace_ids.
        :type use_128bit_trace_id: boolean
        :param host: Contains the ipv4 or ipv6 value of the host. The ip value
            isn't automatically determined in a docker environment.
        :type host: string
        :param context_stack: explicit context stack for storing
            zipkin attributes
        :type context_stack: object
        :param span_storage: explicit Span storage for storing zipkin spans
            before they're emitted.
        :type span_storage: py_zipkin.storage.SpanStorage
        :param firehose_handler: [EXPERIMENTAL] Similar to transport_handler,
            except that it will receive 100% of the spans regardless of trace
            sampling rate.
        :type firehose_handler: BaseTransportHandler
        :param kind: Span type (client, server, local, etc...).
        :type kind: Kind
        :param timestamp: Timestamp in seconds, defaults to `time.time()`.
            Set this if you want to use a custom timestamp.
        :type timestamp: float
        :param duration: Duration in seconds, defaults to the time spent in the
            context. Set this if you want to use a custom duration.
        :type duration: float
        :param encoding: Output encoding format, defaults to V1 thrift spans.
        :type encoding: Encoding
        :param _tracer: Current tracer object. This argument is passed in
            automatically when you create a zipkin_span from a Tracer.
        :type _tracer: Tracer
        """
        self.service_name = service_name
        self.span_name = span_name
        self.zipkin_attrs_override = zipkin_attrs
        self.transport_handler = transport_handler
        self.max_span_batch_size = max_span_batch_size
        self.annotations = annotations or {}
        self.binary_annotations = binary_annotations or {}
        self.port = port
        self.sample_rate = sample_rate
        self.add_logging_annotation = add_logging_annotation
        self.report_root_timestamp_override = report_root_timestamp
        self.use_128bit_trace_id = use_128bit_trace_id
        self.host = host
        self._context_stack = context_stack
        self._span_storage = span_storage
        self.firehose_handler = firehose_handler
        self.kind = self._generate_kind(kind, include)
        self.timestamp = timestamp
        self.duration = duration
        self.encoding = encoding
        self._tracer = _tracer

        self._is_local_root_span = False
        self.logging_context = None
        self.do_pop_attrs = False
        # Spans that log a 'cs' timestamp can additionally record a
        # 'sa' binary annotation that shows where the request is going.
        self.remote_endpoint = None
        self.zipkin_attrs = None

        # It used to  be possible to override timestamp and duration by passing
        # in the cs/cr or sr/ss annotations. We want to keep backward compatibility
        # for now, so this logic overrides self.timestamp and self.duration in the
        # same way.
        # This doesn't fit well with v2 spans since those annotations are gone, so
        # we also log a deprecation warning.
        if 'sr' in self.annotations and 'ss' in self.annotations:
            self.duration = self.annotations['ss'] - self.annotations['sr']
            self.timestamp = self.annotations['sr']
            log.warning(
                "Manually setting 'sr'/'ss' annotations is deprecated. Please "
                "use the timestamp and duration parameters."
            )
        if 'cr' in self.annotations and 'cs' in self.annotations:
            self.duration = self.annotations['cr'] - self.annotations['cs']
            self.timestamp = self.annotations['cs']
            log.warning(
                "Manually setting 'cr'/'cs' annotations is deprecated. Please "
                "use the timestamp and duration parameters."
            )

        # Root spans have transport_handler and at least one of
        # zipkin_attrs_override or sample_rate.
        if self.zipkin_attrs_override or self.sample_rate is not None:
            # transport_handler is mandatory for root spans
            if self.transport_handler is None:
                raise ZipkinError(
                    'Root spans require a transport handler to be given')

            self._is_local_root_span = True

        # If firehose_handler than this is a local root span.
        if self.firehose_handler:
            self._is_local_root_span = True

        if self.sample_rate is not None and not (0.0 <= self.sample_rate <= 100.0):
            raise ZipkinError('Sample rate must be between 0.0 and 100.0')

        if self._span_storage is not None and \
                not isinstance(self._span_storage, storage.SpanStorage):
            raise ZipkinError('span_storage should be an instance '
                              'of py_zipkin.storage.SpanStorage')

        if self._span_storage is not None:
            log.warning('span_storage is deprecated. Set local_storage instead.')
            self.get_tracer()._span_storage = self._span_storage

        if self._context_stack is not None:
            log.warning('context_stack is deprecated. Set local_storage instead.')
            self.get_tracer()._context_stack = self._context_stack

    def __call__(self, f):
        @functools.wraps(f)
        def decorated(*args, **kwargs):
            with zipkin_span(
                service_name=self.service_name,
                span_name=self.span_name,
                zipkin_attrs=self.zipkin_attrs,
                transport_handler=self.transport_handler,
                max_span_batch_size=self.max_span_batch_size,
                annotations=self.annotations,
                binary_annotations=self.binary_annotations,
                port=self.port,
                sample_rate=self.sample_rate,
                include=None,
                add_logging_annotation=self.add_logging_annotation,
                report_root_timestamp=self.report_root_timestamp_override,
                use_128bit_trace_id=self.use_128bit_trace_id,
                host=self.host,
                context_stack=self._context_stack,
                span_storage=self._span_storage,
                firehose_handler=self.firehose_handler,
                kind=self.kind,
                timestamp=self.timestamp,
                duration=self.duration,
                encoding=self.encoding,
                _tracer=self._tracer,
            ):
                return f(*args, **kwargs)
        return decorated

    def get_tracer(self):
        if self._tracer is not None:
            return self._tracer
        else:
            return get_default_tracer()

    def __enter__(self):
        return self.start()

    def _generate_kind(self, kind, include):
        # If `kind` is not set, then we generate it from `include`.
        # This code maintains backward compatibility with old versions of py_zipkin
        # which used include rather than kind to identify client / server spans.
        if kind:
            return kind
        else:
            if include:
                # If `include` contains only one of `client` or `server`
                # than it's a client or server span respectively.
                # If neither or both are present, then it's a local span
                # which is represented by kind = None.
                log.warning(
                    'The include argument is deprecated. Please use kind.'
                )
                if 'client' in include and 'server' not in include:
                    return Kind.CLIENT
                elif 'client' not in include and 'server' in include:
                    return Kind.SERVER
                else:
                    return Kind.LOCAL

        # If both kind and include are unset, then it's a local span.
        return Kind.LOCAL

    def _get_current_context(self):
        """Returns the current ZipkinAttrs and generates new ones if needed.

        :returns: (report_root_timestamp, zipkin_attrs)
        :rtype: (bool, ZipkinAttrs)
        """
        # This check is technically not necessary since only root spans will have
        # sample_rate, zipkin_attrs or a transport set. But it helps making the
        # code clearer by separating the logic for a root span from the one for a
        # child span.
        if self._is_local_root_span:

            # If sample_rate is set, we need to (re)generate a trace context.
            # If zipkin_attrs (trace context) were passed in as argument there are
            # 2 possibilities:
            # is_sampled = False --> we keep the same trace_id but re-roll the dice
            #                        for is_sampled.
            # is_sampled = True  --> we don't want to stop sampling halfway through
            #                        a sampled trace, so we do nothing.
            # If no zipkin_attrs were passed in, we generate new ones and start a
            # new trace.
            if self.sample_rate is not None:

                # If this trace is not sampled, we re-roll the dice.
                if self.zipkin_attrs_override and \
                        not self.zipkin_attrs_override.is_sampled:
                    # This will be the root span of the trace, so we should
                    # set timestamp and duration.
                    return True, create_attrs_for_span(
                        sample_rate=self.sample_rate,
                        trace_id=self.zipkin_attrs_override.trace_id,
                    )

                # If zipkin_attrs_override was not passed in, we simply generate
                # new zipkin_attrs to start a new trace.
                elif not self.zipkin_attrs_override:
                    return True, create_attrs_for_span(
                        sample_rate=self.sample_rate,
                        use_128bit_trace_id=self.use_128bit_trace_id,
                    )

            if self.firehose_handler and not self.zipkin_attrs_override:
                # If it has gotten here, the only thing that is
                # causing a trace is the firehose. So we force a trace
                # with sample rate of 0
                return True, create_attrs_for_span(
                    sample_rate=0.0,
                    use_128bit_trace_id=self.use_128bit_trace_id,
                )

            # If we arrive here it means the sample_rate was not set while
            # zipkin_attrs_override was, so let's simply return that.
            return False, self.zipkin_attrs_override

        else:
            # Check if there's already a trace context in _context_stack.
            existing_zipkin_attrs = self.get_tracer().get_zipkin_attrs()
            # If there's an existing context, let's create new zipkin_attrs
            # with that context as parent.
            if existing_zipkin_attrs:
                return False, ZipkinAttrs(
                    trace_id=existing_zipkin_attrs.trace_id,
                    span_id=generate_random_64bit_string(),
                    parent_span_id=existing_zipkin_attrs.span_id,
                    flags=existing_zipkin_attrs.flags,
                    is_sampled=existing_zipkin_attrs.is_sampled,
                )

        return False, None

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

        report_root_timestamp, self.zipkin_attrs = self._get_current_context()

        # If zipkin_attrs are not set up by now, that means this span is not
        # configured to perform logging itself, and it's not in an existing
        # Zipkin trace. That means there's nothing else to do and it can exit
        # early.
        if not self.zipkin_attrs:
            return self

        self.get_tracer().push_zipkin_attrs(self.zipkin_attrs)
        self.do_pop_attrs = True

        self.start_timestamp = time.time()

        if self._is_local_root_span:
            # Don't set up any logging if we're not sampling
            if not self.zipkin_attrs.is_sampled and not self.firehose_handler:
                return self
            # If transport is already configured don't override it. Doing so would
            # cause all previously recorded spans to never be emitted as exiting
            # the inner logging context will reset transport_configured to False.
            if self.get_tracer().is_transport_configured():
                log.info('Transport was already configured, ignoring override'
                         'from span {}'.format(self.span_name))
                return self
            endpoint = create_endpoint(self.port, self.service_name, self.host)
            self.logging_context = ZipkinLoggingContext(
                self.zipkin_attrs,
                endpoint,
                self.span_name,
                self.transport_handler,
                report_root_timestamp or self.report_root_timestamp_override,
                self.get_tracer,
                self.service_name,
                binary_annotations=self.binary_annotations,
                add_logging_annotation=self.add_logging_annotation,
                client_context=self.kind == Kind.CLIENT,
                max_span_batch_size=self.max_span_batch_size,
                firehose_handler=self.firehose_handler,
                encoding=self.encoding,
            )
            self.logging_context.start()
            self.get_tracer().set_transport_configured(configured=True)

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
            self.get_tracer().pop_zipkin_attrs()

        # If no transport is configured, there's no reason to create a new Span.
        # This also helps avoiding memory leaks since without a transport nothing
        # would pull spans out of get_tracer().
        if not self.get_tracer().is_transport_configured():
            return

        # Add the error annotation if an exception occurred
        if any((_exc_type, _exc_value, _exc_traceback)):
            error_msg = u'{0}: {1}'.format(_exc_type.__name__, _exc_value)
            self.update_binary_annotations({
                ERROR_KEY: error_msg,
            })

        # Logging context is only initialized for "root" spans of the local
        # process (i.e. this zipkin_span not inside of any other local
        # zipkin_spans)
        if self.logging_context:
            try:
                self.logging_context.stop()
            except Exception as ex:
                err_msg = 'Error emitting zipkin trace. {}'.format(
                    repr(ex),
                )
                log.error(err_msg)
            finally:
                self.logging_context = None
                self.get_tracer().clear()
                self.get_tracer().set_transport_configured(configured=False)
                return

        # If we've gotten here, that means that this span is a child span of
        # this context's root span (i.e. it's a zipkin_span inside another
        # zipkin_span).
        end_timestamp = time.time()
        # If self.duration is set, it means the user wants to override it
        if self.duration:
            duration = self.duration
        else:
            duration = end_timestamp - self.start_timestamp

        endpoint = create_endpoint(self.port, self.service_name, self.host)
        self.get_tracer().add_span(Span(
            trace_id=self.zipkin_attrs.trace_id,
            name=self.span_name,
            parent_id=self.zipkin_attrs.parent_span_id,
            span_id=self.zipkin_attrs.span_id,
            kind=self.kind,
            timestamp=self.timestamp if self.timestamp else self.start_timestamp,
            duration=duration,
            annotations=self.annotations,
            local_endpoint=endpoint,
            remote_endpoint=self.remote_endpoint,
            tags=self.binary_annotations,
        ))

    def update_binary_annotations(self, extra_annotations):
        """Updates the binary annotations for the current span."""
        if not self.logging_context:
            # This is not the root span, so binary annotations will be added
            # to the log handler when this span context exits.
            self.binary_annotations.update(extra_annotations)
        else:
            # Otherwise, we're in the context of the root span, so just update
            # the binary annotations for the logging context directly.
            self.logging_context.tags.update(extra_annotations)

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
        if self.kind != Kind.CLIENT:
            # TODO: trying to set a sa binary annotation for a non-client span
            # should result in a logged error
            return

        remote_endpoint = create_endpoint(
            port=port,
            service_name=service_name,
            host=host,
        )
        if not self.logging_context:
            if self.remote_endpoint is not None:
                raise ValueError('SA annotation already set.')
            self.remote_endpoint = remote_endpoint
        else:
            if self.logging_context.remote_endpoint is not None:
                raise ValueError('SA annotation already set.')
            self.logging_context.remote_endpoint = remote_endpoint

    def override_span_name(self, name):
        """Overrides the current span name.

        This is useful if you don't know the span name yet when you create the
        zipkin_span object. i.e. pyramid_zipkin doesn't know which route the
        request matched until the function wrapped by the context manager
        completes.

        :param name: New span name
        :type name: str
        """
        self.span_name = name
        if self.logging_context:
            self.logging_context.span_name = name


def _validate_args(kwargs):
    if 'kind' in kwargs:
        raise ValueError(
            '"kind" is not valid in this context. '
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

        kwargs['kind'] = Kind.CLIENT
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

        kwargs['kind'] = Kind.SERVER
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
    :param use_128bit_trace_id: If true, generate 128-bit trace_ids
    :type use_128bit_trace_id: boolean
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


def create_http_headers_for_new_span(context_stack=None, tracer=None):
    """
    Generate the headers for a new zipkin span.

    .. note::

        If the method is not called from within a zipkin_trace context,
        empty dict will be returned back.

    :returns: dict containing (X-B3-TraceId, X-B3-SpanId, X-B3-ParentSpanId,
                X-B3-Flags and X-B3-Sampled) keys OR an empty dict.
    """
    if tracer:
        zipkin_attrs = tracer.get_zipkin_attrs()
    elif context_stack:
        zipkin_attrs = context_stack.get()
    else:
        zipkin_attrs = get_default_tracer().get_zipkin_attrs()

    if not zipkin_attrs:
        return {}

    return {
        'X-B3-TraceId': zipkin_attrs.trace_id,
        'X-B3-SpanId': generate_random_64bit_string(),
        'X-B3-ParentSpanId': zipkin_attrs.span_id,
        'X-B3-Flags': '0',
        'X-B3-Sampled': '1' if zipkin_attrs.is_sampled else '0',
    }
