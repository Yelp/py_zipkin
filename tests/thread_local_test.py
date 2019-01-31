import mock

from py_zipkin import thread_local
from py_zipkin.storage import SpanStorage

# Can't patch an attribute that doesn't yet exist
thread_local._thread_local.zipkin_attrs = []
thread_local._thread_local.span_storage = SpanStorage()


@mock.patch('py_zipkin.thread_local._thread_local.zipkin_attrs', ['foo'])
def test_get_thread_local_zipkin_attrs_returns_back_zipkin_attrs_if_present():
    assert thread_local.get_thread_local_zipkin_attrs() == ['foo']


def test_get_thread_local_zipkin_attrs_creates_empty_list_if_not_attached():
    delattr(thread_local._thread_local, "zipkin_attrs")
    assert not hasattr(thread_local._thread_local, "zipkin_attrs")
    assert thread_local.get_thread_local_zipkin_attrs() == []
    assert hasattr(thread_local._thread_local, "zipkin_attrs")


@mock.patch(
    'py_zipkin.thread_local._thread_local.span_storage', SpanStorage(['foo'])
)
def test_get_thread_local_span_storage_present():
    assert thread_local.get_thread_local_span_storage() == SpanStorage(['foo'])


def test_get_thread_local_span_storage_creates_empty_list_if_not_attached():
    delattr(thread_local._thread_local, "span_storage")
    assert not hasattr(thread_local._thread_local, "span_storage")
    assert thread_local.get_thread_local_span_storage() == SpanStorage()
    assert hasattr(thread_local._thread_local, "span_storage")


@mock.patch('py_zipkin.thread_local._thread_local.zipkin_attrs', ['foo'])
def test_get_zipkin_attrs_returns_the_last_of_the_list():
    assert 'foo' == thread_local.get_zipkin_attrs()


@mock.patch(
    'py_zipkin.thread_local._thread_local.zipkin_attrs', ['foo', 'bar']
)
def test_pop_zipkin_attrs_removes_the_last_zipkin_attrs():
    assert 'bar' == thread_local.pop_zipkin_attrs()
    assert 'foo' == thread_local.get_zipkin_attrs()


@mock.patch('py_zipkin.thread_local._thread_local.zipkin_attrs', ['foo'])
def test_push_zipkin_attrs_adds_new_zipkin_attrs_to_list():
    assert 'foo' == thread_local.get_zipkin_attrs()
    thread_local.push_zipkin_attrs('bar')
    assert 'bar' == thread_local.get_zipkin_attrs()
