import pytest

import py_zipkin.zipkin as zipkin


@pytest.mark.parametrize('use_128', [False, True])
def test_create_attrs_for_span(benchmark, use_128):
    benchmark(
        zipkin.create_attrs_for_span,
        use_128bit_trace_id=use_128,
    )
