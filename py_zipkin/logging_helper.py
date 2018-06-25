# -*- coding: utf-8 -*-
import time

from py_zipkin import _encoding_helpers
from py_zipkin import thrift
from py_zipkin.exception import ZipkinError
from py_zipkin.transport import BaseTransportHandler


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
        endpoint,
        span_name,
        transport_handler,
        report_root_timestamp,
        span_storage,
        binary_annotations=None,
        add_logging_annotation=False,
        client_context=False,
        max_span_batch_size=None,
        firehose_handler=None,
    ):
        self.zipkin_attrs = zipkin_attrs
        self.endpoint = endpoint
        self.span_name = span_name
        self.transport_handler = transport_handler
        self.response_status_code = 0
        self.span_storage = span_storage
        self.report_root_timestamp = report_root_timestamp
        self.binary_annotations_dict = binary_annotations or {}
        self.add_logging_annotation = add_logging_annotation
        self.client_context = client_context
        self.max_span_batch_size = max_span_batch_size
        self.firehose_handler = firehose_handler

        self.sa_endpoint = None

    def start(self):
        """Actions to be taken before request is handled."""

        # Record the start timestamp.
        self.start_timestamp = time.time()
        return self

    def stop(self):
        """Actions to be taken post request handling.
        """

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
                ZipkinBatchSender(self.firehose_handler,
                                  self.max_span_batch_size)
            )

        if not self.zipkin_attrs.is_sampled:
            self.span_storage.clear()
            return

        span_sender = ZipkinBatchSender(self.transport_handler,
                                        self.max_span_batch_size)

        self._emit_spans_with_span_sender(span_sender)
        self.span_storage.clear()

    def _emit_spans_with_span_sender(self, span_sender):
        with span_sender:
            end_timestamp = time.time()

            # Collect, annotate, and log client spans from the logging handler
            for span in self.span_storage:

                endpoint = _encoding_helpers.copy_endpoint_with_new_service_name(
                    self.endpoint, span['service_name']
                )

                timestamp, duration = get_local_span_timestamp_and_duration(
                    span['annotations']
                )

                span_sender.add_span(
                    span_id=span['span_id'],
                    parent_span_id=span['parent_span_id'],
                    trace_id=span['trace_id'],
                    span_name=span['span_name'],
                    annotations=span['annotations'],
                    binary_annotations=span['binary_annotations'],
                    timestamp_s=timestamp,
                    duration_s=duration,
                    endpoint=endpoint,
                    sa_endpoint=span['sa_endpoint'],
                )

            k1, k2 = ('sr', 'ss')
            if self.client_context:
                k1, k2 = ('cs', 'cr')
            annotations = {k1: self.start_timestamp, k2: end_timestamp}

            if self.add_logging_annotation:
                annotations[LOGGING_END_KEY] = time.time()

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
                annotations=annotations,
                binary_annotations=self.binary_annotations_dict,
                timestamp_s=timestamp,
                duration_s=duration,
                endpoint=self.endpoint,
                sa_endpoint=self.sa_endpoint,
            )


def get_local_span_timestamp_and_duration(annotations):
    if 'cs' in annotations and 'cr' in annotations:
        return annotations['cs'], annotations['cr'] - annotations['cs']
    elif 'sr' in annotations and 'ss' in annotations:
        return annotations['sr'], annotations['ss'] - annotations['sr']
    return None, None


class ZipkinBatchSender(object):

    MAX_PORTION_SIZE = 100

    def __init__(self, transport_handler, max_portion_size=None):
        self.transport_handler = transport_handler
        self.max_portion_size = max_portion_size or self.MAX_PORTION_SIZE

        if isinstance(self.transport_handler, BaseTransportHandler):
            self.max_payload_bytes = self.transport_handler.get_max_payload_bytes()
        else:
            self.max_payload_bytes = None

    def __enter__(self):
        self._reset_queue()
        return self

    def __exit__(self, _exc_type, _exc_value, _exc_traceback):
        if any((_exc_type, _exc_value, _exc_traceback)):
            error = '{0}: {1}'.format(_exc_type.__name__, _exc_value)
            raise ZipkinError(error)
        else:
            self.flush()

    def _reset_queue(self):
        self.queue = []
        self.current_size = thrift.LIST_HEADER_SIZE

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
        endpoint,
        sa_endpoint,
    ):
        thrift_endpoint = thrift.create_endpoint(
            endpoint.port,
            endpoint.service_name,
            endpoint.ipv4,
            endpoint.ipv6,
        )

        thrift_annotations = thrift.annotation_list_builder(
            annotations,
            thrift_endpoint,
        )

        # Binary annotations can be set through debug messages or the
        # set_extra_binary_annotations registry setting.
        thrift_binary_annotations = thrift.binary_annotation_list_builder(
            binary_annotations,
            thrift_endpoint,
        )

        # Add sa binary annotation
        if sa_endpoint is not None:
            thrift_sa_endpoint = thrift.create_endpoint(
                sa_endpoint.port,
                sa_endpoint.service_name,
                sa_endpoint.ipv4,
                sa_endpoint.ipv6,
            )
            thrift_binary_annotations.append(thrift.create_binary_annotation(
                key=thrift.zipkin_core.SERVER_ADDR,
                value=thrift.SERVER_ADDR_VAL,
                annotation_type=thrift.zipkin_core.AnnotationType.BOOL,
                host=thrift_sa_endpoint,
            ))

        thrift_span = thrift.create_span(
            span_id,
            parent_span_id,
            trace_id,
            span_name,
            thrift_annotations,
            thrift_binary_annotations,
            timestamp_s,
            duration_s,
        )

        encoded_span = thrift.span_to_bytes(thrift_span)

        # If we've already reached the max batch size or the new span doesn't
        # fit in max_payload_bytes, send what we've collected until now and
        # start a new batch.
        is_over_size_limit = (
            self.max_payload_bytes is not None and
            self.current_size + len(encoded_span) > self.max_payload_bytes
        )
        is_over_portion_limit = len(self.queue) >= self.max_portion_size
        if is_over_size_limit or is_over_portion_limit:
            self.flush()

        self.queue.append(encoded_span)
        self.current_size += len(encoded_span)

    def flush(self):
        if self.transport_handler and len(self.queue) > 0:

            message = thrift.encode_bytes_list(self.queue)
            self.transport_handler(message)
        self._reset_queue()
