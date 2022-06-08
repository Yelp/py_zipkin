import logging

from py_zipkin.storage import get_default_tracer
from py_zipkin.util import _should_sample
from py_zipkin.util import create_attrs_for_span
from py_zipkin.util import generate_random_64bit_string
from py_zipkin.util import ZipkinAttrs

log = logging.getLogger(__name__)


def _parse_single_header(b3_header):
    """
    Parse out and return the data necessary for generating ZipkinAttrs.

    Returns a dict with the following keys:
        'trace_id':             str or None
        'span_id':              str or None
        'parent_span_id':       str or None
        'sampled_str':          '0', '1', 'd', or None (defer)
    """
    parsed = dict.fromkeys(("trace_id", "span_id", "parent_span_id", "sampled_str"))

    # b3={TraceId}-{SpanId}-{SamplingState}-{ParentSpanId}
    #      (last 2 fields optional)
    # OR
    # b3={SamplingState}
    bits = b3_header.split("-")

    # Handle the lone-sampling-decision case:
    if len(bits) == 1:
        if bits[0] in ("0", "1", "d"):
            parsed["sampled_str"] = bits[0]
            return parsed
        raise ValueError("Invalid sample-only value: %r" % bits[0])
    if len(bits) > 4:
        # Too many segments
        raise ValueError("Too many segments in b3 header: %r" % b3_header)
    parsed["trace_id"] = bits[0]
    if not parsed["trace_id"]:
        raise ValueError("Bad or missing TraceId")
    parsed["span_id"] = bits[1]
    if not parsed["span_id"]:
        raise ValueError("Bad or missing SpanId")
    if len(bits) > 3:
        parsed["parent_span_id"] = bits[3]
        if not parsed["parent_span_id"]:
            raise ValueError("Got empty ParentSpanId")
    if len(bits) > 2:
        # Empty-string means "missing" which means "Defer"
        if bits[2]:
            parsed["sampled_str"] = bits[2]
            if parsed["sampled_str"] not in ("0", "1", "d"):
                raise ValueError("Bad SampledState: %r" % parsed["sampled_str"])
    return parsed


def _parse_multi_header(headers):
    """
    Parse out and return the data necessary for generating ZipkinAttrs.

    Returns a dict with the following keys:
        'trace_id':             str or None
        'span_id':              str or None
        'parent_span_id':       str or None
        'sampled_str':          '0', '1', 'd', or None (defer)
    """
    parsed = {
        "trace_id": headers.get("X-B3-TraceId", None),
        "span_id": headers.get("X-B3-SpanId", None),
        "parent_span_id": headers.get("X-B3-ParentSpanId", None),
        "sampled_str": headers.get("X-B3-Sampled", None),
    }
    # Normalize X-B3-Flags and X-B3-Sampled to None, '0', '1', or 'd'
    if headers.get("X-B3-Flags") == "1":
        parsed["sampled_str"] = "d"
    if parsed["sampled_str"] == "true":
        parsed["sampled_str"] = "1"
    elif parsed["sampled_str"] == "false":
        parsed["sampled_str"] = "0"
    if parsed["sampled_str"] not in (None, "1", "0", "d"):
        raise ValueError("Got invalid X-B3-Sampled: %s" % parsed["sampled_str"])
    for k in ("trace_id", "span_id", "parent_span_id"):
        if parsed[k] == "":
            raise ValueError("Got empty-string %r" % k)
    if parsed["trace_id"] and not parsed["span_id"]:
        raise ValueError("Got X-B3-TraceId but not X-B3-SpanId")
    elif parsed["span_id"] and not parsed["trace_id"]:
        raise ValueError("Got X-B3-SpanId but not X-B3-TraceId")

    # Handle the common case of no headers at all
    if not parsed["trace_id"] and not parsed["sampled_str"]:
        raise ValueError()  # won't trigger a log message

    return parsed


def extract_zipkin_attrs_from_headers(
    headers, sample_rate=100.0, use_128bit_trace_id=False
):
    """
    Implements extraction of B3 headers per:
        https://github.com/openzipkin/b3-propagation

    The input headers can be any dict-like container that supports "in"
    membership test and a .get() method that accepts a default value.

    Returns a ZipkinAttrs instance or None
    """
    try:
        if "b3" in headers:
            parsed = _parse_single_header(headers["b3"])
        else:
            parsed = _parse_multi_header(headers)
    except ValueError as e:
        if str(e):
            log.warning(e)
        return None

    # Handle the lone-sampling-decision case:
    if not parsed["trace_id"]:
        if parsed["sampled_str"] in ("1", "d"):
            sample_rate = 100.0
        else:
            sample_rate = 0.0
        attrs = create_attrs_for_span(
            sample_rate=sample_rate,
            use_128bit_trace_id=use_128bit_trace_id,
            flags="1" if parsed["sampled_str"] == "d" else "0",
        )
        return attrs

    # Handle any sampling decision, including if it was deferred
    if parsed["sampled_str"]:
        # We have 1==Accept, 0==Deny, d==Debug
        if parsed["sampled_str"] in ("1", "d"):
            is_sampled = True
        else:
            is_sampled = False
    else:
        # sample flag missing; means "Defer" and we're responsible for
        # rolling fresh dice
        is_sampled = _should_sample(sample_rate)

    return ZipkinAttrs(
        parsed["trace_id"],
        parsed["span_id"],
        parsed["parent_span_id"],
        "1" if parsed["sampled_str"] == "d" else "0",
        is_sampled,
    )


def create_http_headers(
    context_stack=None,
    tracer=None,
    new_span_id=False,
):
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

    # If zipkin_attrs is still not set then we're not in a trace context
    if not zipkin_attrs:
        return {}

    if new_span_id:
        span_id = generate_random_64bit_string()
        parent_span_id = zipkin_attrs.span_id
    else:
        span_id = zipkin_attrs.span_id
        parent_span_id = zipkin_attrs.parent_span_id

    return {
        "X-B3-TraceId": zipkin_attrs.trace_id,
        "X-B3-SpanId": span_id,
        "X-B3-ParentSpanId": parent_span_id,
        "X-B3-Flags": "0",
        "X-B3-Sampled": "1" if zipkin_attrs.is_sampled else "0",
    }
