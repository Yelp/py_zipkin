import mock

from py_zipkin import storage
from py_zipkin import thread_local
from py_zipkin.storage import SpanStorage


def test_get_thread_local_zipkin_attrs_returns_back_zipkin_attrs_if_present():
    tracer = storage.get_default_tracer()
    with mock.patch.object(tracer._context_stack, '_storage', ['foo']):
        assert thread_local.get_thread_local_zipkin_attrs() == ['foo']


def test_get_thread_local_span_storage_present():
    tracer = storage.get_default_tracer()
    with mock.patch.object(tracer, '_span_storage', SpanStorage(['foo'])):
        assert thread_local.get_thread_local_span_storage() == SpanStorage(['foo'])


def test_get_zipkin_attrs_returns_the_last_of_the_list():
    tracer = storage.get_default_tracer()
    with mock.patch.object(tracer._context_stack, '_storage', ['foo']):
        assert 'foo' == thread_local.get_zipkin_attrs()


def test_pop_zipkin_attrs_removes_the_last_zipkin_attrs():
    tracer = storage.get_default_tracer()
    with mock.patch.object(tracer._context_stack, '_storage', ['foo', 'bar']):
        assert 'bar' == thread_local.pop_zipkin_attrs()
        assert 'foo' == thread_local.get_zipkin_attrs()


def test_push_zipkin_attrs_adds_new_zipkin_attrs_to_list():
    tracer = storage.get_default_tracer()
    with mock.patch.object(tracer._context_stack, '_storage', ['foo']):
        assert 'foo' == thread_local.get_zipkin_attrs()
        thread_local.push_zipkin_attrs('bar')
        assert 'bar' == thread_local.get_zipkin_attrs()
