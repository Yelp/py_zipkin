# -*- coding: utf-8 -*-
import time

from py_zipkin.exception import ZipkinError
from py_zipkin.thrift import annotation_list_builder
from py_zipkin.thrift import binary_annotation_list_builder
from py_zipkin.thrift import copy_endpoint_with_new_service_name
from py_zipkin.thrift import create_span
from py_zipkin.thrift import thrift_objs_in_bytes
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.util import get_local_span_timestamp_and_duration

LOGGING_END_KEY = 'py_zipkin.logging_end'


class ZipkinLoggingContext(object):
    """A logging context specific to a Zipkin trace. If the trace is sampled,
    the logging context sends serialized Zipkin spans to a transport_handler.
    The logging context sends root "server" or "client" span, as well as all
    local child spans collected within this context.

    This class should only be used by the main `zipkin_span` entrypoint.
    """

    def __init__(
        self,
        zipkin_attrs,
        thrift_endpoint,
        span_name,
        transport_handler,
        report_root_timestamp,
        annotations=None,
        binary_annotations=None,
        add_logging_annotation=False,
        client_context=False,
        max_span_batch_size=None,
        firehose_handler=None,
    ):
        self.zipkin_attrs = zipkin_attrs
        self.thrift_endpoint = thrift_endpoint
        self.span_name = span_name
        self.transport_handler = transport_handler
        self.response_status_code = 0
        self.report_root_timestamp = report_root_timestamp
        self.annotations = annotations or {}
        self.binary_annotations = binary_annotations or {}
        self.sa_binary_annotations = []
        self.add_logging_annotation = add_logging_annotation
        self.client_context = client_context
        self.max_span_batch_size = max_span_batch_size
        self.firehose_handler = firehose_handler

    def start(self):
        self.start_timestamp = time.time()
        return self

    def stop(self):
        self.log_spans()

    def log_spans(self):
        """Main function to log all the annotations stored during the entire
        request. This is done if the request is sampled and the response was
        a success. It also logs the service (`ss` and `sr`) or the client
        ('cs' and 'cr') annotations.
        """

        # FIXME: Should have a single aggregate handler
        if self.firehose_handler:
            # FIXME: We need to allow different batching settings per handler
            self._log_spans_with_span_sender(
                ZipkinBatchSender(self.firehose_handler,
                                  self.max_span_batch_size)
            )

        if not self.zipkin_attrs.is_sampled:
            return

        span_sender = ZipkinBatchSender(self.transport_handler,
                                        self.max_span_batch_size)

        self._log_spans_with_span_sender(span_sender)

    def _log_spans_with_span_sender(self, span_sender):
        with span_sender:
            end_timestamp = time.time()

            # Collect, annotate, and log client spans from the logging handler
            for span in self.zipkin_attrs.client_spans:
                # The parent_span_id is either the parent ID set in the
                # logging handler or the current Zipkin context's span ID.
                parent_span_id = (
                    span['parent_span_id'] or
                    self.zipkin_attrs.span_id
                )
                # A new client span's span ID can be overridden
                span_id = span['span_id'] or generate_random_64bit_string()
                endpoint = copy_endpoint_with_new_service_name(
                    self.thrift_endpoint, span['service_name']
                )
                # Collect annotations both logged with the new spans and
                # logged in separate log messages.
                annotations = span['annotations']
                binary_annotations = span['binary_annotations']
                timestamp, duration = get_local_span_timestamp_and_duration(
                    annotations
                )
                # Create serializable thrift objects of annotations
                thrift_annotations = annotation_list_builder(
                    annotations, endpoint
                )
                thrift_binary_annotations = binary_annotation_list_builder(
                    binary_annotations, endpoint
                )
                if span.get('sa_binary_annotations'):
                    thrift_binary_annotations += span['sa_binary_annotations']

                span_sender.add_span(
                    span_id=span_id,
                    parent_span_id=parent_span_id,
                    trace_id=self.zipkin_attrs.trace_id,
                    span_name=span['span_name'],
                    annotations=thrift_annotations,
                    binary_annotations=thrift_binary_annotations,
                    timestamp_s=timestamp,
                    duration_s=duration,
                )

            if self.client_context:
                k1, k2 = ('cs', 'cr')
            else:
                k1, k2 = ('sr', 'ss')

            self.annotations.update({k1: self.start_timestamp, k2: end_timestamp})

            if self.add_logging_annotation:
                self.annotations[LOGGING_END_KEY] = time.time()

            thrift_annotations = annotation_list_builder(
                self.annotations,
                self.thrift_endpoint,
            )

            # Binary annotations can be set through debug messages or the
            # set_extra_binary_annotations registry setting.
            thrift_binary_annotations = binary_annotation_list_builder(
                self.binary_annotations,
                self.thrift_endpoint,
            )
            if self.sa_binary_annotations:
                thrift_binary_annotations += self.sa_binary_annotations

            if self.report_root_timestamp:
                timestamp = self.start_timestamp
                duration = end_timestamp - self.start_timestamp
            else:
                timestamp = duration = None

            span_sender.add_span(
                span_id=self.zipkin_attrs.span_id,
                parent_span_id=self.zipkin_attrs.parent_span_id,
                trace_id=self.zipkin_attrs.trace_id,
                span_name=self.span_name,
                annotations=thrift_annotations,
                binary_annotations=thrift_binary_annotations,
                timestamp_s=timestamp,
                duration_s=duration,
            )


class ZipkinBatchSender(object):

    MAX_PORTION_SIZE = 100

    def __init__(self, transport_handler, max_portion_size=None):
        self.transport_handler = transport_handler
        self.max_portion_size = max_portion_size or self.MAX_PORTION_SIZE

    def __enter__(self):
        self.queue = []
        return self

    def __exit__(self, _exc_type, _exc_value, _exc_traceback):
        if any((_exc_type, _exc_value, _exc_traceback)):
            error = '{0}: {1}'.format(_exc_type.__name__, _exc_value)
            raise ZipkinError(error)
        else:
            self.flush()

    def add_span(
        self,
        span_id,
        parent_span_id,
        trace_id,
        span_name,
        annotations,
        binary_annotations,
        timestamp_s,
        duration_s,
    ):
        thrift_span = create_span(
            span_id,
            parent_span_id,
            trace_id,
            span_name,
            annotations,
            binary_annotations,
            timestamp_s,
            duration_s,
        )

        self.queue.append(thrift_span)
        if len(self.queue) >= self.max_portion_size:
            self.flush()

    def flush(self):
        if self.transport_handler and len(self.queue) > 0:
            message = thrift_objs_in_bytes(self.queue)
            self.transport_handler(message)
            self.queue = []
