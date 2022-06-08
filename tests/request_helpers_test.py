from unittest import mock

import pytest

from py_zipkin import request_helpers
from py_zipkin.request_helpers import ZipkinAttrs
from tests.test_helpers import MockTracer


def test_extract_zipkin_attrs_from_headers_single_sample_only():
    attrs = request_helpers.extract_zipkin_attrs_from_headers(
        {"b3": "0"}, use_128bit_trace_id=False
    )
    assert 16 == len(attrs.trace_id)
    assert not attrs.is_sampled

    attrs = request_helpers.extract_zipkin_attrs_from_headers(
        {"b3": "0"}, use_128bit_trace_id=True
    )
    assert 32 == len(attrs.trace_id)
    assert not attrs.is_sampled

    # I _think_ from reading https://github.com/openzipkin/b3-propagation that
    # sending only a "yes" sampling decision is legit and should, I guess,
    # result in a fresh trace with a sample_rate pegged to 1.0
    attrs = request_helpers.extract_zipkin_attrs_from_headers(
        {"b3": "1"}, use_128bit_trace_id=False
    )
    assert 16 == len(attrs.trace_id)
    assert attrs.is_sampled

    attrs = request_helpers.extract_zipkin_attrs_from_headers(
        {"b3": "1"}, use_128bit_trace_id=True
    )
    assert 32 == len(attrs.trace_id)
    assert attrs.is_sampled

    # Gibberish gets a None
    assert None is request_helpers.extract_zipkin_attrs_from_headers({"b3": "yakka!"})


@pytest.mark.parametrize(
    "no_sample_string,yes_sample_string", [("0", "1"), ("false", "true")]
)
def test_extract_zipkin_attrs_from_headers_multi_sample_only(
    no_sample_string, yes_sample_string
):
    # While you shouldn't encode X-B3-Sampled as true or false, a lenient
    # implementation may accept them
    attrs = request_helpers.extract_zipkin_attrs_from_headers(
        {"X-B3-Sampled": no_sample_string}, use_128bit_trace_id=False
    )
    assert 16 == len(attrs.trace_id)
    assert not attrs.is_sampled

    attrs = request_helpers.extract_zipkin_attrs_from_headers(
        {"X-B3-Sampled": no_sample_string}, use_128bit_trace_id=True
    )
    assert 32 == len(attrs.trace_id)
    assert not attrs.is_sampled

    # A specified debug flag should probably override any sample header,
    # though this is a bit pathological of the sender to send us.
    attrs = request_helpers.extract_zipkin_attrs_from_headers(
        {"X-B3-Sampled": no_sample_string, "X-B3-Flags": "1"},
        use_128bit_trace_id=False,
    )
    assert 16 == len(attrs.trace_id)
    assert attrs.is_sampled

    # I _think_ from reading https://github.com/openzipkin/b3-propagation that
    # sending only a "yes" sampling decision is legit and should, I guess,
    # result in a fresh trace with a sample_rate pegged to 1.0
    attrs = request_helpers.extract_zipkin_attrs_from_headers(
        {"X-B3-Sampled": yes_sample_string}, use_128bit_trace_id=False
    )
    assert 16 == len(attrs.trace_id)
    assert attrs.is_sampled

    attrs = request_helpers.extract_zipkin_attrs_from_headers(
        {"X-B3-Sampled": yes_sample_string}, use_128bit_trace_id=True
    )
    assert 32 == len(attrs.trace_id)
    assert attrs.is_sampled

    attrs = request_helpers.extract_zipkin_attrs_from_headers(
        {"X-B3-Flags": "1"}, use_128bit_trace_id=False
    )
    assert 16 == len(attrs.trace_id)
    assert attrs.is_sampled

    attrs = request_helpers.extract_zipkin_attrs_from_headers(
        {"X-B3-Flags": "1"}, use_128bit_trace_id=True
    )
    assert 32 == len(attrs.trace_id)
    assert attrs.is_sampled

    # Gibberish gets a None
    assert None is request_helpers.extract_zipkin_attrs_from_headers(
        {"X-B3-Sampled": "yakka!"}
    )


def test_extract_zipkin_attrs_from_headers_invalids():
    span_id = "7e5991634df0c66e"
    parent_span_id = "987d98239475e0a8"

    for invalid in [
        "",
        "a",
        "-".join(("a", "")),
        "-".join(("", "b")),
        "-".join(("a", "", "whee")),
        "-".join(("a", span_id, "whee")),
        "-".join(("a", span_id, "whee", parent_span_id)),
        "-".join(("a", span_id, "whee", "")),
        "-".join(("a", span_id, "1", "c", "d?!")),
    ]:
        print("bad b3 header: %r" % invalid)
        assert None is request_helpers.extract_zipkin_attrs_from_headers(
            {"b3": invalid}, sample_rate=88.2
        ), invalid

        # Convert the (bad) input b3 header into multiple-header format input
        bits = invalid.split("-")
        if len(bits) == 1:
            bad_headers = {"X-B3-TraceId": bits[0]}
        elif len(bits) == 2:
            bad_headers = {"X-B3-TraceId": bits[0], "X-B3-SpanId": bits[1]}
        elif len(bits) == 3:
            bad_headers = {
                "X-B3-TraceId": bits[0],
                "X-B3-SpanId": bits[1],
                "X-B3-Sampled": bits[2],
            }
        elif len(bits) == 4:
            bad_headers = {
                "X-B3-TraceId": bits[0],
                "X-B3-SpanId": bits[1],
                "X-B3-Sampled": bits[2],
                "X-B3-ParentSpanId": bits[3],
            }

        print("bad_headers: %r" % bad_headers)
        assert None is request_helpers.extract_zipkin_attrs_from_headers(
            bad_headers,
            sample_rate=88.2,
        )

    # I'm pretty sure a provided X-B3-Sampled with empty-string "value" should
    # be considered invalid (bit of an edge case).
    assert None is request_helpers.extract_zipkin_attrs_from_headers(
        {
            "X-B3-TraceId": "a",
            "X-B3-SpanId": span_id,
            "X-B3-Sampled": "",
            "X-B3-ParentSpanId": parent_span_id,
        },
        sample_rate=88.2,
    )

    # Catch edge case of SpanId provided but not TraceId
    assert None is request_helpers.extract_zipkin_attrs_from_headers(
        {"X-B3-SpanId": span_id}, sample_rate=88.2
    )


@pytest.mark.parametrize("no_trace_str,yes_trace_str", [("0", "1"), ("false", "true")])
@mock.patch("py_zipkin.util.random.random")
def test_extract_zipkin_attrs_from_headers_multi(
    mock_random, no_trace_str, yes_trace_str
):
    trace_id = "not_actually_validated"
    span_id = "7e5991634df0c66e"
    parent_span_id = "987d98239475e0a8"

    # No B3-headers; return None to get a new local-root span
    assert None is request_helpers.extract_zipkin_attrs_from_headers(
        {}, sample_rate=88.2
    )

    # Just 2 bits is a sample-rate defer; die-roll is "no":
    mock_random.return_value = 0.883
    assert ZipkinAttrs(
        trace_id,
        span_id,
        None,
        "0",
        False,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {"X-B3-TraceId": trace_id, "X-B3-SpanId": span_id}, sample_rate=88.2
    )

    # Just 2 bits is a sample-rate defer; die-roll is "yes":
    mock_random.return_value = 0.881
    assert ZipkinAttrs(
        trace_id,
        span_id,
        None,
        "0",
        True,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {"X-B3-TraceId": trace_id, "X-B3-SpanId": span_id}, sample_rate=88.2
    )

    # defer with a parent span; die-roll is "no":
    mock_random.return_value = 0.883
    assert ZipkinAttrs(
        trace_id,
        span_id,
        parent_span_id,
        "0",
        False,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {
            "X-B3-TraceId": trace_id,
            "X-B3-SpanId": span_id,
            "X-B3-ParentSpanId": parent_span_id,
        },
        sample_rate=88.2,
    )

    # defer with a parent span; die-roll is "yes":
    mock_random.return_value = 0.881
    assert ZipkinAttrs(
        trace_id,
        span_id,
        parent_span_id,
        "0",
        True,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {
            "X-B3-TraceId": trace_id,
            "X-B3-SpanId": span_id,
            "X-B3-ParentSpanId": parent_span_id,
        },
        sample_rate=88.2,
    )

    # non-defer no-trace, no parent span
    mock_random.return_value = 0.881  # a "yes" if it were used
    assert ZipkinAttrs(
        trace_id,
        span_id,
        None,
        "0",
        False,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {
            "X-B3-TraceId": trace_id,
            "X-B3-SpanId": span_id,
            "X-B3-Sampled": no_trace_str,
        },
        sample_rate=88.2,
    )

    # non-defer yes-trace, no parent span
    mock_random.return_value = 0.883  # a "no" if it were used
    assert ZipkinAttrs(
        trace_id,
        span_id,
        None,
        "0",
        True,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {
            "X-B3-TraceId": trace_id,
            "X-B3-SpanId": span_id,
            "X-B3-Sampled": yes_trace_str,
        },
        sample_rate=88.2,
    )

    # non-defer debug-trace, no parent span
    mock_random.return_value = 0.883  # a "no" if it were used
    assert ZipkinAttrs(
        trace_id,
        span_id,
        None,
        "1",
        True,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {"X-B3-TraceId": trace_id, "X-B3-SpanId": span_id, "X-B3-Flags": "1"},
        sample_rate=88.2,
    )

    # non-defer no-trace, yes parent span
    mock_random.return_value = 0.881  # a "yes" if it were used
    assert ZipkinAttrs(
        trace_id,
        span_id,
        parent_span_id,
        "0",
        False,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {
            "X-B3-TraceId": trace_id,
            "X-B3-SpanId": span_id,
            "X-B3-Sampled": no_trace_str,
            "X-B3-ParentSpanId": parent_span_id,
        },
        sample_rate=88.2,
    )

    # non-defer yes-trace, yes parent span
    mock_random.return_value = 0.883  # a "no" if it were used
    assert ZipkinAttrs(
        trace_id,
        span_id,
        parent_span_id,
        "0",
        True,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {
            "X-B3-TraceId": trace_id,
            "X-B3-SpanId": span_id,
            "X-B3-Sampled": yes_trace_str,
            "X-B3-ParentSpanId": parent_span_id,
        },
        sample_rate=88.2,
    )

    # non-defer debug-trace, yes parent span
    mock_random.return_value = 0.883  # a "no" if it were used
    assert ZipkinAttrs(
        trace_id,
        span_id,
        parent_span_id,
        "1",
        True,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {
            "X-B3-TraceId": trace_id,
            "X-B3-SpanId": span_id,
            "X-B3-Flags": "1",
            "X-B3-ParentSpanId": parent_span_id,
        },
        sample_rate=88.2,
    )

    # flags (debug) trump Sampled, if they conflict--another edge case
    mock_random.return_value = 0.883  # a "no" if it were used
    assert ZipkinAttrs(
        trace_id,
        span_id,
        parent_span_id,
        "1",
        True,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {
            "X-B3-TraceId": trace_id,
            "X-B3-SpanId": span_id,
            "X-B3-Flags": "1",
            "X-B3-Sampled": no_trace_str,
            "X-B3-ParentSpanId": parent_span_id,
        },
        sample_rate=88.2,
    )


@pytest.mark.parametrize("yes_trace_str", ("1", "d"))
@mock.patch("py_zipkin.util.random.random")
def test_extract_zipkin_attrs_from_headers_single(mock_random, yes_trace_str):
    # b3={TraceId}-{SpanId}-{SamplingState}-{ParentSpanId}
    # where the last two fields are optional.
    # The header can also just transmit a sample decision, but that's tested
    # elsewhere.
    trace_id = "not_actually_validated"
    span_id = "7e5991634df0c66e"
    parent_span_id = "987d98239475e0a8"

    # Just 2 bits is a sample-rate defer; die-roll is "no":
    mock_random.return_value = 0.883
    assert ZipkinAttrs(
        trace_id,
        span_id,
        None,
        "0",
        False,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {"b3": "-".join((trace_id, span_id))}, sample_rate=88.2
    )

    # Just 2 bits is a sample-rate defer; die-roll is "yes":
    mock_random.return_value = 0.881
    assert ZipkinAttrs(
        trace_id,
        span_id,
        None,
        "0",
        True,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {"b3": "-".join((trace_id, span_id))}, sample_rate=88.2
    )

    # defer with a parent span; die-roll is "no":
    mock_random.return_value = 0.883
    assert ZipkinAttrs(
        trace_id,
        span_id,
        parent_span_id,
        "0",
        False,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {"b3": "-".join((trace_id, span_id, "", parent_span_id))}, sample_rate=88.2
    )

    # defer with a parent span; die-roll is "yes":
    mock_random.return_value = 0.881
    assert ZipkinAttrs(
        trace_id,
        span_id,
        parent_span_id,
        "0",
        True,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {"b3": "-".join((trace_id, span_id, "", parent_span_id))}, sample_rate=88.2
    )

    # non-defer no-trace, no parent span
    mock_random.return_value = 0.881  # a "yes" if it were used
    assert ZipkinAttrs(
        trace_id,
        span_id,
        None,
        "0",
        False,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {"b3": "-".join((trace_id, span_id, "0"))}, sample_rate=88.2
    )

    # non-defer yes/debug-trace, no parent span
    mock_random.return_value = 0.883  # a "no" if it were used
    assert ZipkinAttrs(
        trace_id,
        span_id,
        None,
        "1" if yes_trace_str == "d" else "0",
        True,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {"b3": "-".join((trace_id, span_id, yes_trace_str))}, sample_rate=88.2
    )

    # non-defer no-trace, yes parent span
    mock_random.return_value = 0.881  # a "yes" if it were used
    assert ZipkinAttrs(
        trace_id,
        span_id,
        parent_span_id,
        "0",
        False,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {"b3": "-".join((trace_id, span_id, "0", parent_span_id))}, sample_rate=88.2
    )

    # non-defer yes/debug-trace, yes parent span
    mock_random.return_value = 0.883  # a "no" if it were used
    assert ZipkinAttrs(
        trace_id,
        span_id,
        parent_span_id,
        "1" if yes_trace_str == "d" else "0",
        True,
    ) == request_helpers.extract_zipkin_attrs_from_headers(
        {"b3": "-".join((trace_id, span_id, yes_trace_str, parent_span_id))},
        sample_rate=88.2,
    )


def test_create_http_headers_context_stack():
    mock_context_stack = mock.Mock()
    mock_context_stack.get.return_value = ZipkinAttrs(
        trace_id="17133d482ba4f605",
        span_id="37133d482ba4f605",
        is_sampled=True,
        parent_span_id="27133d482ba4f605",
        flags=None,
    )
    expected = {
        "X-B3-TraceId": "17133d482ba4f605",
        "X-B3-SpanId": "37133d482ba4f605",
        "X-B3-ParentSpanId": "27133d482ba4f605",
        "X-B3-Flags": "0",
        "X-B3-Sampled": "1",
    }
    assert expected == request_helpers.create_http_headers(
        context_stack=mock_context_stack,
    )


def test_create_http_headers_custom_tracer():
    tracer = MockTracer()
    tracer.push_zipkin_attrs(
        ZipkinAttrs(
            trace_id="17133d482ba4f605",
            span_id="37133d482ba4f605",
            is_sampled=True,
            parent_span_id="27133d482ba4f605",
            flags=None,
        )
    )
    expected = {
        "X-B3-TraceId": "17133d482ba4f605",
        "X-B3-SpanId": "37133d482ba4f605",
        "X-B3-ParentSpanId": "27133d482ba4f605",
        "X-B3-Flags": "0",
        "X-B3-Sampled": "1",
    }
    assert expected == request_helpers.create_http_headers(tracer=tracer)


@mock.patch("py_zipkin.request_helpers.generate_random_64bit_string", autospec=True)
def test_create_http_headers_new_span_id(gen_mock):
    tracer = MockTracer()
    tracer.push_zipkin_attrs(
        ZipkinAttrs(
            trace_id="17133d482ba4f605",
            span_id="37133d482ba4f605",
            is_sampled=True,
            parent_span_id="27133d482ba4f605",
            flags=None,
        )
    )
    gen_mock.return_value = "47133d482ba4f605"

    # if new_span_id = True we generate a new span id
    assert request_helpers.create_http_headers(tracer=tracer, new_span_id=True) == {
        "X-B3-TraceId": "17133d482ba4f605",
        "X-B3-SpanId": "47133d482ba4f605",
        "X-B3-ParentSpanId": "37133d482ba4f605",
        "X-B3-Flags": "0",
        "X-B3-Sampled": "1",
    }

    # by default we keep the same span id as the current span
    assert request_helpers.create_http_headers(tracer=tracer) == {
        "X-B3-TraceId": "17133d482ba4f605",
        "X-B3-SpanId": "37133d482ba4f605",
        "X-B3-ParentSpanId": "27133d482ba4f605",
        "X-B3-Flags": "0",
        "X-B3-Sampled": "1",
    }
