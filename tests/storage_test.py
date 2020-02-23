import mock

from py_zipkin import storage
from tests.test_helpers import MockTracer


@mock.patch.object(storage, "_thread_local_tracer")
@mock.patch.object(storage, "Tracer")
def test_get_thread_local_tracer_no_tracer(mock_tracer, mock_tl):
    del mock_tl.tracer

    tracer = storage._get_thread_local_tracer()

    assert mock_tracer.call_count == 1
    assert tracer == mock_tracer.return_value


@mock.patch.object(storage, "_thread_local_tracer")
def test_set_thread_local_tracer(mock_tl):
    tracer = MockTracer()
    storage._set_thread_local_tracer(tracer)

    assert storage._get_thread_local_tracer() == tracer


@mock.patch.object(storage, "_thread_local_tracer")
@mock.patch.object(storage, "Tracer")
def test_get_thread_local_tracer_existing_tracer(mock_tracer, mock_tl):
    tracer = storage._get_thread_local_tracer()

    assert mock_tracer.call_count == 0
    assert tracer == mock_tl.tracer


def test_default_span_storage_warns():
    with mock.patch.object(storage.log, "warning") as mock_log:
        storage.default_span_storage()
        assert mock_log.call_count == 1


@mock.patch.object(storage, "_contextvars_tracer")
def test_get_default_tracer(mock_contextvar):
    # We're in python 3.7+
    assert storage.get_default_tracer() == storage._contextvars_tracer.get()

    storage._contextvars_tracer = None

    # We're in python 2.7 to 3.6
    assert storage.get_default_tracer() == storage._thread_local_tracer.tracer


@mock.patch.object(storage, "_contextvars_tracer")
def test_set_default_tracer(mock_contextvar):
    tracer = MockTracer()
    # We're in python 3.7+
    storage.set_default_tracer(tracer)
    assert mock_contextvar.set.call_args == mock.call(tracer)

    storage._contextvars_tracer = None

    # We're in python 2.7 to 3.6
    storage.set_default_tracer(tracer)
    assert storage._thread_local_tracer.tracer == tracer


@mock.patch.object(storage, "_thread_local_tracer")
@mock.patch.object(storage, "Tracer")
@mock.patch.object(storage, "_contextvars_tracer")
def test_has_default_tracer(mock_contextvar, mock_tracer, mock_tl):
    # shut up the py2 "yes" answer while testing the py3.7+ cases
    del mock_tl.tracer

    # We're in python 3.7+
    mock_contextvar.get.side_effect = LookupError
    assert not storage.has_default_tracer()
    mock_contextvar.get.side_effect = None
    mock_contextvar.get.return_value = "does not matter"

    assert storage.get_default_tracer() == storage._contextvars_tracer.get()
    assert storage.has_default_tracer()

    storage._contextvars_tracer = None

    # We're in python 2.7 to 3.6
    assert not storage.has_default_tracer()

    tracer = storage._get_thread_local_tracer()

    assert storage.has_default_tracer()
    assert mock_tracer.call_count == 1
    assert tracer == mock_tracer.return_value


def test_tracer_copy():
    tracer = storage.Tracer()
    tracer.add_span("span1")
    tracer.add_span("span2")
    tracer.push_zipkin_attrs("attrs1")
    tracer.push_zipkin_attrs("attrs2")

    the_copy = tracer.copy()
    the_copy.add_span("span3")
    the_copy.push_zipkin_attrs("will be popped -- copy")

    tracer.add_span("span4")
    tracer.push_zipkin_attrs("will be popped -- tracer")

    assert "will be popped -- copy" == the_copy.pop_zipkin_attrs()
    the_copy.push_zipkin_attrs("attrs3")

    assert "will be popped -- tracer" == tracer.pop_zipkin_attrs()
    tracer.push_zipkin_attrs("attrs4")

    assert ["span1", "span2", "span3", "span4"] == list(tracer._span_storage)
    assert ["attrs1", "attrs2", "attrs3"] == the_copy._context_stack._storage
    assert ["attrs1", "attrs2", "attrs4"] == tracer._context_stack._storage
