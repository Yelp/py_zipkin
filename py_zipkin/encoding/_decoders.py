# -*- coding: utf-8 -*-
import logging
import socket
import struct

import six
from thriftpy2.protocol.binary import read_list_begin
from thriftpy2.protocol.binary import TBinaryProtocol
from thriftpy2.thrift import TType
from thriftpy2.transport import TMemoryBuffer

from py_zipkin.encoding._helpers import Endpoint
from py_zipkin.encoding._helpers import Span
from py_zipkin.encoding._types import Encoding
from py_zipkin.encoding._types import Kind
from py_zipkin.exception import ZipkinError
from py_zipkin.thrift import zipkin_core

_HEX_DIGITS = "0123456789abcdef"
_DROP_ANNOTATIONS = {'cs', 'sr', 'ss', 'cr'}

log = logging.getLogger('py_zipkin.encoding')


def get_decoder(encoding):
    """Creates encoder object for the given encoding.

    :param encoding: desired output encoding protocol
    :type encoding: Encoding
    :return: corresponding IEncoder object
    :rtype: IEncoder
    """
    if encoding == Encoding.V1_THRIFT:
        return _V1ThriftDecoder()
    if encoding == Encoding.V1_JSON:
        raise NotImplementedError(
            '{} decoding not yet implemented'.format(encoding))
    if encoding == Encoding.V2_JSON:
        raise NotImplementedError(
            '{} decoding not yet implemented'.format(encoding))
    raise ZipkinError('Unknown encoding: {}'.format(encoding))


class IDecoder(object):
    """Decoder interface."""

    def decode_spans(self, spans):
        """Decodes an encoded list of spans.

        :param spans: encoded list of spans
        :type spans: bytes
        :return: list of spans
        :rtype: list of Span
        """
        raise NotImplementedError()


class _V1ThriftDecoder(IDecoder):

    def decode_spans(self, spans):
        """Decodes an encoded list of spans.

        :param spans: encoded list of spans
        :type spans: bytes
        :return: list of spans
        :rtype: list of Span
        """
        decoded_spans = []
        transport = TMemoryBuffer(spans)

        if six.byte2int(spans) == TType.STRUCT:
            _, size = read_list_begin(transport)
        else:
            size = 1

        for _ in range(size):
            span = zipkin_core.Span()
            span.read(TBinaryProtocol(transport))
            decoded_spans.append(self._decode_thrift_span(span))
        return decoded_spans

    def _convert_from_thrift_endpoint(self, thrift_endpoint):
        """Accepts a thrift decoded endpoint and converts it to an Endpoint.

        :param thrift_endpoint: thrift encoded endpoint
        :type thrift_endpoint: thrift endpoint
        :returns: decoded endpoint
        :rtype: Encoding
        """
        ipv4 = None
        ipv6 = None
        port = struct.unpack('H', struct.pack('h', thrift_endpoint.port))[0]

        if thrift_endpoint.ipv4 != 0:
            ipv4 = socket.inet_ntop(
                socket.AF_INET,
                struct.pack('!i', thrift_endpoint.ipv4),
            )

        if thrift_endpoint.ipv6:
            ipv6 = socket.inet_ntop(socket.AF_INET6, thrift_endpoint.ipv6)

        return Endpoint(
            service_name=thrift_endpoint.service_name,
            ipv4=ipv4,
            ipv6=ipv6,
            port=port,
        )

    def _decode_thrift_annotations(self, thrift_annotations):
        """Accepts a thrift annotation and converts it to a v1 annotation.

        :param thrift_annotations: list of thrift annotations.
        :type thrift_annotations: list of zipkin_core.Span.Annotation
        :returns: (annotations, local_endpoint, kind)
        """
        local_endpoint = None
        kind = Kind.LOCAL
        all_annotations = {}
        timestamp = None
        duration = None

        for thrift_annotation in thrift_annotations:
            all_annotations[thrift_annotation.value] = thrift_annotation.timestamp
            if thrift_annotation.host:
                local_endpoint = self._convert_from_thrift_endpoint(
                    thrift_annotation.host,
                )

        if 'cs' in all_annotations and 'sr' not in all_annotations:
            kind = Kind.CLIENT
            timestamp = all_annotations['cs']
            duration = all_annotations['cr'] - all_annotations['cs']
        elif 'cs' not in all_annotations and 'sr' in all_annotations:
            kind = Kind.SERVER
            timestamp = all_annotations['sr']
            duration = all_annotations['ss'] - all_annotations['sr']

        annotations = {
            name: self.seconds(ts) for name, ts in all_annotations.items()
            if name not in _DROP_ANNOTATIONS
        }

        return annotations, local_endpoint, kind, timestamp, duration

    def _convert_from_thrift_binary_annotations(self, thrift_binary_annotations):
        """Accepts a thrift decoded binary annotation and converts it
        to a v1 binary annotation.
        """
        tags = {}
        local_endpoint = None
        remote_endpoint = None

        for binary_annotation in thrift_binary_annotations:
            if binary_annotation.key == 'sa':
                remote_endpoint = self._convert_from_thrift_endpoint(
                    thrift_endpoint=binary_annotation.host,
                )
            else:
                key = binary_annotation.key

                annotation_type = binary_annotation.annotation_type
                value = binary_annotation.value

                if annotation_type == zipkin_core.AnnotationType.BOOL:
                    tags[key] = "true" if value == 1 else "false"
                elif annotation_type == zipkin_core.AnnotationType.STRING:
                    tags[key] = value
                else:
                    log.warning('Only STRING and BOOL binary annotations are '
                                'supported right now and can be properly decoded.')

                if binary_annotation.host:
                    local_endpoint = self._convert_from_thrift_endpoint(
                        thrift_endpoint=binary_annotation.host,
                    )

        return tags, local_endpoint, remote_endpoint

    def seconds(self, us):
        if us is None:
            return None
        return round(float(us) / 1000 / 1000, 6)

    def _decode_thrift_span(self, thrift_span):
        """Decodes a thrift span.

        :param thrift_span: thrift span
        :type thrift_span: thrift Span object
        :returns: span builder representing this span
        :rtype: Span
        """
        parent_id = None
        local_endpoint = None
        annotations = {}
        tags = {}
        kind = Kind.LOCAL
        remote_endpoint = None
        timestamp = None
        duration = None

        if thrift_span.parent_id:
            parent_id = self._convert_unsigned_long_to_lower_hex(
                thrift_span.parent_id,
            )

        if thrift_span.annotations:
            annotations, local_endpoint, kind, timestamp, duration = \
                self._decode_thrift_annotations(thrift_span.annotations)

        if thrift_span.binary_annotations:
            tags, local_endpoint, remote_endpoint = \
                self._convert_from_thrift_binary_annotations(
                    thrift_span.binary_annotations,
                )

        trace_id = self._convert_trace_id_to_string(
            thrift_span.trace_id,
            thrift_span.trace_id_high,
        )

        return Span(
            trace_id=trace_id,
            name=thrift_span.name,
            parent_id=parent_id,
            span_id=self._convert_unsigned_long_to_lower_hex(thrift_span.id),
            kind=kind,
            timestamp=self.seconds(timestamp or thrift_span.timestamp),
            duration=self.seconds(duration or thrift_span.duration),
            local_endpoint=local_endpoint,
            remote_endpoint=remote_endpoint,
            shared=(kind == Kind.SERVER and thrift_span.timestamp is None),
            annotations=annotations,
            tags=tags,
        )

    def _convert_trace_id_to_string(self, trace_id, trace_id_high=None):
        """
        Converts the provided traceId hex value with optional high bits
        to a string.

        :param trace_id: the value of the trace ID
        :type trace_id: int
        :param trace_id_high: the high bits of the trace ID
        :type trace_id: int
        :returns: trace_id_high + trace_id as a string
        """
        if trace_id_high is not None:
            result = bytearray(32)
            self._write_hex_long(result, 0, trace_id_high)
            self._write_hex_long(result, 16, trace_id)
            return result.decode("utf8")

        result = bytearray(16)
        self._write_hex_long(result, 0, trace_id)
        return result.decode("utf8")

    def _convert_unsigned_long_to_lower_hex(self, value):
        """
        Converts the provided unsigned long value to a hex string.

        :param value: the value to convert
        :type value: unsigned long
        :returns: value as a hex string
        """
        result = bytearray(16)
        self._write_hex_long(result, 0, value)
        return result.decode("utf8")

    def _write_hex_long(self, data, pos, value):
        """
        Writes an unsigned long value across a byte array.

        :param data: the buffer to write the value to
        :type data: bytearray
        :param pos: the starting position
        :type pos: int
        :param value: the value to write
        :type value: unsigned long
        """
        self._write_hex_byte(data, pos + 0, (value >> 56) & 0xff)
        self._write_hex_byte(data, pos + 2, (value >> 48) & 0xff)
        self._write_hex_byte(data, pos + 4, (value >> 40) & 0xff)
        self._write_hex_byte(data, pos + 6, (value >> 32) & 0xff)
        self._write_hex_byte(data, pos + 8, (value >> 24) & 0xff)
        self._write_hex_byte(data, pos + 10, (value >> 16) & 0xff)
        self._write_hex_byte(data, pos + 12, (value >> 8) & 0xff)
        self._write_hex_byte(data, pos + 14, (value & 0xff))

    def _write_hex_byte(self, data, pos, byte):
        data[pos + 0] = ord(_HEX_DIGITS[int((byte >> 4) & 0xf)])
        data[pos + 1] = ord(_HEX_DIGITS[int(byte & 0xf)])
