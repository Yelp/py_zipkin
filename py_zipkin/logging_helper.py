# -*- coding: utf-8 -*-
import logging
import time
from collections import defaultdict

from py_zipkin.exception import ZipkinError
from py_zipkin.thrift import annotation_list_builder
from py_zipkin.thrift import binary_annotation_list_builder
from py_zipkin.thrift import copy_endpoint_with_new_service_name
from py_zipkin.thrift import create_span
from py_zipkin.thrift import thrift_obj_in_bytes
from py_zipkin.util import generate_random_64bit_string


try:  # Python 2.7+
    from logging import NullHandler
except ImportError:  # pragma: no cover
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

null_handler = NullHandler()
zipkin_logger = logging.getLogger('py_zipkin.logger')
zipkin_logger.addHandler(null_handler)
zipkin_logger.setLevel(logging.DEBUG)

LOGGING_END_KEY = 'py_zipkin.logging_end'


class ZipkinLoggingContext(object):
    """A logging context specific to a Zipkin trace. If the trace is sampled,
    the logging context sends serialized Zipkin spans to a transport_handler.
    This is meant to only be used server-side, so the logging context sends
    a single "server" span, as well as all local child spans collected within
    the server context.

    This class should only be used by the main `zipkin_span` entrypoint.
    """

    def __init__(
        self,
        zipkin_attrs,
        thrift_endpoint,
        log_handler,
        span_name,
        transport_handler,
        report_root_timestamp,
        binary_annotations=None,
        add_logging_annotation=False,
    ):
        self.zipkin_attrs = zipkin_attrs
        self.thrift_endpoint = thrift_endpoint
        self.log_handler = log_handler
        self.span_name = span_name
        self.transport_handler = transport_handler
        self.response_status_code = 0
        self.report_root_timestamp = report_root_timestamp
        self.binary_annotations_dict = binary_annotations or {}
        self.sa_binary_annotations = []
        self.add_logging_annotation = add_logging_annotation

    def start(self):
        """Actions to be taken before request is handled.
        1) Attach `zipkin_logger` to :class:`ZipkinLoggerHandler` object.
        2) Record the start timestamp.
        """
        zipkin_logger.removeHandler(null_handler)
        zipkin_logger.addHandler(self.log_handler)
        self.start_timestamp = time.time()
        return self

    def stop(self):
        """Actions to be taken post request handling.
        1) Log the service annotations to scribe.
        2) Detach `zipkin_logger` handler.
        """
        self.log_spans()
        zipkin_logger.removeHandler(self.log_handler)
        zipkin_logger.addHandler(null_handler)

    def log_spans(self):
        """Main function to log all the annotations stored during the entire
        request. This is done if the request is sampled and the response was
        a success. It also logs the service `ss` and `sr` annotations.
        """
        if self.zipkin_attrs.is_sampled:
            server_end_time = time.time()
            # Collect additional annotations from the logging handler
            annotations_by_span_id = defaultdict(dict)
            binary_annotations_by_span_id = defaultdict(dict)
            for msg in self.log_handler.extra_annotations:
                span_id = msg['parent_span_id'] or self.zipkin_attrs.span_id
                # This should check if these are non-None
                annotations_by_span_id[span_id].update(msg['annotations'])
                binary_annotations_by_span_id[span_id].update(
                    msg['binary_annotations']
                )

            # Collect, annotate, and log client spans from the logging handler
            for span in self.log_handler.client_spans:
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
                annotations.update(annotations_by_span_id[span_id])
                binary_annotations = span['binary_annotations']
                binary_annotations.update(
                    binary_annotations_by_span_id[span_id])

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

                log_span(
                    span_id=span_id,
                    parent_span_id=parent_span_id,
                    trace_id=self.zipkin_attrs.trace_id,
                    span_name=span['span_name'],
                    annotations=thrift_annotations,
                    binary_annotations=thrift_binary_annotations,
                    transport_handler=self.transport_handler,
                    timestamp_s=timestamp,
                    duration_s=duration,
                )

            # Collect extra annotations for server span
            extra_annotations = annotations_by_span_id[
                self.zipkin_attrs.span_id]
            extra_binary_annotations = binary_annotations_by_span_id[
                self.zipkin_attrs.span_id
            ]
            logging_end = time.time()
            annotations = dict(
                sr=self.start_timestamp,
                ss=server_end_time,
                **extra_annotations
            )
            if self.add_logging_annotation:
                annotations[LOGGING_END_KEY] = logging_end

            thrift_annotations = annotation_list_builder(
                annotations,
                self.thrift_endpoint,
            )

            # Binary annotations can be set through debug messages or the
            # set_extra_binary_annotations registry setting.
            self.binary_annotations_dict.update(extra_binary_annotations)
            thrift_binary_annotations = binary_annotation_list_builder(
                self.binary_annotations_dict,
                self.thrift_endpoint,
            )
            if self.sa_binary_annotations:
                thrift_binary_annotations += self.sa_binary_annotations

            if self.report_root_timestamp:
                timestamp = self.start_timestamp
                duration = server_end_time - self.start_timestamp
            else:
                timestamp = duration = None

            log_span(
                span_id=self.zipkin_attrs.span_id,
                parent_span_id=self.zipkin_attrs.parent_span_id,
                trace_id=self.zipkin_attrs.trace_id,
                span_name=self.span_name,
                annotations=thrift_annotations,
                binary_annotations=thrift_binary_annotations,
                timestamp_s=timestamp,
                duration_s=duration,
                transport_handler=self.transport_handler,
            )


def get_local_span_timestamp_and_duration(annotations):
    if 'cs' in annotations and 'cr' in annotations:
        return annotations['cs'], annotations['cr'] - annotations['cs']
    elif 'sr' in annotations and 'ss' in annotations:
        return annotations['sr'], annotations['ss'] - annotations['sr']
    return None, None


class ZipkinLoggerHandler(logging.StreamHandler, object):
    """Logger Handler to log span annotations or additional client spans to
    scribe. To connect to the handler, logger name must be
    'py_zipkin.logger'.

    :param zipkin_attrs: ZipkinAttrs namedtuple object
    """

    def __init__(self, zipkin_attrs):
        super(ZipkinLoggerHandler, self).__init__()
        # If parent_span_id is set, the application is in a logging context
        # where each additional client span logged has this span as its parent.
        # This is to allow logging of hierarchies of spans instead of just
        # single client spans. See the SpanContext class.
        self.parent_span_id = None
        self.zipkin_attrs = zipkin_attrs
        self.client_spans = []
        self.extra_annotations = []

    def store_local_span(
        self,
        span_name,
        service_name,
        annotations,
        binary_annotations,
        sa_binary_annotations=None,
        span_id=None,
    ):
        """Convenience method for storing a local child span (a zipkin_span
        inside other zipkin_spans) to be logged when the outermost zipkin_span
        exits.
        """
        self.client_spans.append({
            'span_name': span_name,
            'service_name': service_name,
            'parent_span_id': self.parent_span_id,
            'span_id': span_id,
            'annotations': annotations,
            'binary_annotations': binary_annotations,
            'sa_binary_annotations': sa_binary_annotations,
        })

    def emit(self, record):
        """Handle each record message. This function is called whenever
        zipkin_logger.debug() is called.

        :param record: object containing the `msg` object.
            Structure of record.msg should be the following:
            ::

            {
                "annotations": {
                    "cs": ts1,
                    "cr": ts2,
                },
                "binary_annotations": {
                    "http.uri": "/foo/bar",
                },
                "name": "foo_span",
                "service_name": "myService",
            }

            Keys:
            - annotations: str -> timestamp annotations
            - binary_annotations: str -> str binary annotations
              (One of either annotations or binary_annotations is required)
            - name: str of new span name; only used if service-name is also
              specified.
            - service_name: str of new client span's service name.

            If service_name is specified, this log msg is considered to
            represent a new client span. If service_name is omitted, this is
            considered additional annotation for the currently active
            "parent span" (either the server span or the parent client span
            inside a SpanContext).
        """
        if not self.zipkin_attrs.is_sampled:
            return
        span_name = record.msg.get('name', 'span')
        annotations = record.msg.get('annotations', {})
        binary_annotations = record.msg.get('binary_annotations', {})
        if not annotations and not binary_annotations:
            raise ZipkinError(
                "At least one of annotation/binary annotation has"
                " to be provided for {0} span".format(span_name)
            )
        service_name = record.msg.get('service_name', None)
        # Presence of service_name means this is to be a new local span.
        if service_name is not None:
            self.store_local_span(
                span_name=span_name,
                service_name=service_name,
                annotations=annotations,
                binary_annotations=binary_annotations,
            )
        else:
            self.extra_annotations.append({
                'annotations': annotations,
                'binary_annotations': binary_annotations,
                'parent_span_id': self.parent_span_id,
            })


def log_span(
    span_id,
    parent_span_id,
    trace_id,
    span_name,
    annotations,
    binary_annotations,
    timestamp_s,
    duration_s,
    transport_handler,
):
    """Creates a span and logs it using the given transport_handler."""
    # Be defensive about the lack of a transport handler
    if not transport_handler:
        return

    span = create_span(
        span_id,
        parent_span_id,
        trace_id,
        span_name,
        annotations,
        binary_annotations,
        timestamp_s,
        duration_s,
    )
    message = thrift_obj_in_bytes(span)
    transport_handler(message)
