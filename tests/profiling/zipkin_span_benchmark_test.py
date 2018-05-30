import pytest

import py_zipkin.zipkin as zipkin
from tests.conftest import MockTransportHandler


def _create_root_span(is_sampled, firehose_enabled):
    return (), {
        'root_span': zipkin.zipkin_span(
            service_name='my_service',
            span_name='my_span_name',
            transport_handler=MockTransportHandler(),
            firehose_handler=MockTransportHandler() if firehose_enabled else None,
            port=42,
            sample_rate=0 if not is_sampled else 100,
        )
    }


def _start_zipkin_trace(is_sampled, firehose_enabled, num_child_spans):
    _, args = _create_root_span(is_sampled, firehose_enabled)
    args['root_span'].start()
    for _ in range(num_child_spans):
        with zipkin.zipkin_span(
            service_name='my_service',
            span_name='my_child_span_name',
        ):
            pass

    return (), args


def _start_and_stop_zipkin_trace(is_sampled, firehose_enabled, num_child_spans):
    _, args = _start_zipkin_trace(is_sampled, firehose_enabled, num_child_spans)
    args['root_span'].stop()


@pytest.mark.parametrize(
    'firehose_enabled',
    [False, True],
    ids=["firehose_enabled=False", "firehose_enabled=True"],
)
@pytest.mark.parametrize(
    'is_sampled',
    [False, True],
    ids=["is_sampled=False", "is_sampled=True"],
)
def test_creating_span(benchmark, firehose_enabled, is_sampled):
    benchmark(
        _create_root_span,
        firehose_enabled=firehose_enabled,
        is_sampled=is_sampled,
    )


@pytest.mark.parametrize(
    'firehose_enabled',
    [False, True],
    ids=["firehose_enabled=False", "firehose_enabled=True"],
)
@pytest.mark.parametrize(
    'is_sampled',
    [False, True],
    ids=["is_sampled=False", "is_sampled=True"],
)
def test_starting_span_context(benchmark, firehose_enabled, is_sampled):
    benchmark.pedantic(
        lambda root_span: root_span.start(),
        setup=lambda: _create_root_span(
            is_sampled=is_sampled,
            firehose_enabled=firehose_enabled,
        ),
        rounds=50,
    )


@pytest.mark.parametrize(
    'firehose_enabled',
    [False, True],
    ids=["firehose_enabled=False", "firehose_enabled=True"],
)
@pytest.mark.parametrize(
    'num_child_spans',
    [100, 1000],
    ids=["num_child_spans=100", "num_child_spans=1000"],
)
@pytest.mark.parametrize(
    'is_sampled',
    [False, True],
    ids=["is_sampled=False", "is_sampled=True"],
)
def test_logging_spans(benchmark, firehose_enabled, num_child_spans, is_sampled):
    benchmark.pedantic(
        lambda root_span: root_span.stop(),
        setup=lambda: _start_zipkin_trace(
            is_sampled=is_sampled,
            firehose_enabled=firehose_enabled,
            num_child_spans=num_child_spans,
        ),
        rounds=50,
    )


@pytest.mark.parametrize(
    'firehose_enabled',
    [False, True],
    ids=["firehose_enabled=False", "firehose_enabled=True"],
)
@pytest.mark.parametrize(
    'num_child_spans',
    [100, 1000],
    ids=["num_child_spans=100", "num_child_spans=1000"],
)
@pytest.mark.parametrize(
    'is_sampled',
    [False, True],
    ids=["is_sampled=False", "is_sampled=True"],
)
def test_zipkin_span(benchmark, firehose_enabled, num_child_spans, is_sampled):
    benchmark(
        _start_and_stop_zipkin_trace,
        is_sampled=is_sampled,
        firehose_enabled=firehose_enabled,
        num_child_spans=num_child_spans,
    )


@pytest.mark.parametrize(
    'use_128',
    [False, True],
    ids=["use_128=False", "use_128=True"],
)
def test_create_attrs_for_span(benchmark, use_128):
    benchmark(
        zipkin.create_attrs_for_span,
        use_128bit_trace_id=use_128,
    )
