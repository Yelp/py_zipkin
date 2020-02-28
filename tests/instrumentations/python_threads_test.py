import mock
import pytest

from tests.test_helpers import TracingThread


@mock.patch("tests.test_helpers.threading.Thread.start")
@mock.patch("tests.test_helpers.threading.Thread.run")
@mock.patch("py_zipkin.instrumentations.python_threads.storage.set_default_tracer")
@mock.patch("py_zipkin.instrumentations.python_threads.storage.has_default_tracer")
def test_tracing_thread_with_no_tracer(
    mock_has_tracer, mock_set_tracer, mock_run, mock_start
):
    mock_has_tracer.return_value = False

    thread = TracingThread(target="t", args=("a",), kwargs={"b": "c"})
    with pytest.raises(AttributeError):
        thread._orig_tracer

    thread.start()
    assert not thread._orig_tracer

    thread.run()
    assert [] == mock_set_tracer.mock_calls
    assert [mock.call()] == mock_start.mock_calls
    assert [mock.call()] == mock_run.mock_calls


@mock.patch("tests.test_helpers.threading.Thread.start")
@mock.patch("tests.test_helpers.threading.Thread.run")
@mock.patch("py_zipkin.instrumentations.python_threads.storage.set_default_tracer")
@mock.patch("py_zipkin.instrumentations.python_threads.storage.get_default_tracer")
@mock.patch("py_zipkin.instrumentations.python_threads.storage.has_default_tracer")
def test_tracing_thread_with_a_tracer(
    mock_has_tracer, mock_get_tracer, mock_set_tracer, mock_run, mock_start
):
    mock_has_tracer.return_value = True
    mock_get_tracer.return_value.copy.return_value = "stub_copy"

    thread = TracingThread(target="t", args=("a",), kwargs={"b": "c"})
    # __init__() doesn't make the copy...
    with pytest.raises(AttributeError):
        thread._orig_tracer
    assert [] == mock_get_tracer.mock_calls

    # start() does
    thread.start()
    assert thread._orig_tracer == "stub_copy"

    thread.run()
    assert [mock.call("stub_copy")] == mock_set_tracer.mock_calls
    assert [mock.call()] == mock_start.mock_calls
    assert [mock.call()] == mock_run.mock_calls
    # make sure the double-under instance attribute got cleaned up
    with pytest.raises(AttributeError):
        thread._orig_tracer
