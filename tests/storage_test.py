import mock

from py_zipkin import storage


@mock.patch.object(storage.thread_local, '_thread_local')
@mock.patch.object(storage, 'Tracer')
def test_get_thread_local_tracer_no_tracer(mock_tracer, mock_tl):
    del mock_tl.tracer
    tracer = storage.get_thread_local_tracer()

    assert mock_tracer.call_count == 1
    assert tracer == mock_tracer.return_value


@mock.patch.object(storage.thread_local, '_thread_local')
@mock.patch.object(storage, 'Tracer')
def test_get_thread_local_tracer_existing_tracer(mock_tracer, mock_tl):
    tracer = storage.get_thread_local_tracer()

    assert mock_tracer.call_count == 0
    assert tracer == mock_tl.tracer


def test_default_span_storage_warns():
    with mock.patch.object(storage.log, 'warning') as mock_log:
        storage.default_span_storage()
        assert mock_log.call_count == 1


@mock.patch.object(storage, '_contextvars_tracer')
def test_get_default_tracer(mock_contextvar):
    # We're in python 3.7+
    assert storage.get_default_tracer() == storage._contextvars_tracer.get()

    storage._contextvars_tracer = None

    # We're in python 2.7 to 3.6
    assert storage.get_default_tracer() == \
        storage.thread_local._thread_local.tracer
