import os
import time

from py_zipkin import Kind
from py_zipkin.encoding._encoders import get_encoder
from py_zipkin.encoding._helpers import copy_endpoint_with_new_service_name
from py_zipkin.encoding._helpers import Span
from py_zipkin.exception import ZipkinError
from py_zipkin.transport import BaseTransportHandler


LOGGING_END_KEY = "py_zipkin.logging_end"


class ZipkinLoggingContext:
    """A logging context specific to a Zipkin trace. If the trace is sampled,
    the logging context sends serialized Zipkin spans to a transport_handler.
    The logging context sends root "server" or "client" span, as well as all
    local child spans collected within this context.

    This class should only be used by the main `zipkin_span` entrypoint.
    """

    def __init__(
        self,
        zipkin_attrs,
        endpoint,
        span_name,
        transport_handler,
        report_root_timestamp,
        get_tracer,
        service_name,
        binary_annotations=None,
        add_logging_annotation=False,
        client_context=False,
        max_span_batch_size=None,
        firehose_handler=None,
        encoding=None,
        annotations=None,
    ):
        self.zipkin_attrs = zipkin_attrs
        self.endpoint = endpoint
        self.span_name = span_name
        self.transport_handler = transport_handler
        self.response_status_code = 0
        self._get_tracer = get_tracer
        self.service_name = service_name
        self.report_root_timestamp = report_root_timestamp
        self.tags = binary_annotations or {}
        self.add_logging_annotation = add_logging_annotation
        self.client_context = client_context
        self.max_span_batch_size = max_span_batch_size
        self.firehose_handler = firehose_handler
        self.annotations = annotations or {}

        self.remote_endpoint = None
        self.encoder = get_encoder(encoding)

    def start(self):
        """Actions to be taken before request is handled."""

        # Record the start timestamp.
        self.start_timestamp = time.time()
        return self

    def stop(self):
        """Actions to be taken post request handling."""

        self.emit_spans()

    def emit_spans(self):
        """Main function to log all the annotations stored during the entire
        request. This is done if the request is sampled and the response was
        a success. It also logs the service (`ss` and `sr`) or the client
        ('cs' and 'cr') annotations.
        """

        # FIXME: Should have a single aggregate handler
        if self.firehose_handler:
            # FIXME: We need to allow different batching settings per handler
            self._emit_spans_with_span_sender(
                ZipkinBatchSender(
                    self.firehose_handler, self.max_span_batch_size, self.encoder
                )
            )

        if not self.zipkin_attrs.is_sampled:
            self._get_tracer().clear()
            return

        span_sender = ZipkinBatchSender(
            self.transport_handler, self.max_span_batch_size, self.encoder
        )

        self._emit_spans_with_span_sender(span_sender)
        self._get_tracer().clear()

    def _emit_spans_with_span_sender(self, span_sender):
        with span_sender:
            end_timestamp = time.time()

            # Collect, annotate, and log client spans from the logging handler
            for span in self._get_tracer()._span_storage:
                span.local_endpoint = copy_endpoint_with_new_service_name(
                    self.endpoint,
                    span.local_endpoint.service_name,
                )

                span_sender.add_span(span)

            if self.add_logging_annotation:
                self.annotations[LOGGING_END_KEY] = time.time()

            span_sender.add_span(
                Span(
                    trace_id=self.zipkin_attrs.trace_id,
                    name=self.span_name,
                    parent_id=self.zipkin_attrs.parent_span_id,
                    span_id=self.zipkin_attrs.span_id,
                    kind=Kind.CLIENT if self.client_context else Kind.SERVER,
                    timestamp=self.start_timestamp,
                    duration=end_timestamp - self.start_timestamp,
                    local_endpoint=self.endpoint,
                    remote_endpoint=self.remote_endpoint,
                    shared=not self.report_root_timestamp,
                    annotations=self.annotations,
                    tags=self.tags,
                )
            )


class ZipkinBatchSender:

    MAX_PORTION_SIZE = 100

    def __init__(self, transport_handler, max_portion_size, encoder):
        self.transport_handler = transport_handler
        self.max_portion_size = max_portion_size or self.MAX_PORTION_SIZE
        self.encoder = encoder

        if isinstance(self.transport_handler, BaseTransportHandler):
            self.max_payload_bytes = self.transport_handler.get_max_payload_bytes()
        else:
            self.max_payload_bytes = None

    def __enter__(self):
        self._reset_queue()
        return self

    def __exit__(self, _exc_type, _exc_value, _exc_traceback):
        if any((_exc_type, _exc_value, _exc_traceback)):
            filename = os.path.split(_exc_traceback.tb_frame.f_code.co_filename)[1]
            error = "({}:{}) {}: {}".format(
                filename,
                _exc_traceback.tb_lineno,
                _exc_type.__name__,
                _exc_value,
            )
            raise ZipkinError(error)
        else:
            self.flush()

    def _reset_queue(self):
        self.queue = []
        self.current_size = 0

    def add_span(self, internal_span):
        encoded_span = self.encoder.encode_span(internal_span)

        # If we've already reached the max batch size or the new span doesn't
        # fit in max_payload_bytes, send what we've collected until now and
        # start a new batch.
        is_over_size_limit = (
            self.max_payload_bytes is not None
            and not self.encoder.fits(
                current_count=len(self.queue),
                current_size=self.current_size,
                max_size=self.max_payload_bytes,
                new_span=encoded_span,
            )
        )
        is_over_portion_limit = len(self.queue) >= self.max_portion_size
        if is_over_size_limit or is_over_portion_limit:
            self.flush()

        self.queue.append(encoded_span)
        self.current_size += len(encoded_span)

    def flush(self):
        if self.transport_handler and len(self.queue) > 0:

            message = self.encoder.encode_queue(self.queue)
            self.transport_handler(message)
        self._reset_queue()
