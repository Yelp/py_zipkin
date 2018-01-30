import mock

import py_zipkin.stack


@mock.patch('py_zipkin.thread_local._thread_local.zipkin_attrs', [])
def test_get_zipkin_attrs_returns_none_if_no_zipkin_attrs():
    assert not py_zipkin.stack.ThreadLocalStack().get()
    assert not py_zipkin.stack.ThreadLocalStack().get()


def test_get_zipkin_attrs_with_context_returns_none_if_no_zipkin_attrs():
    assert not py_zipkin.stack.Stack([]).get()


@mock.patch('py_zipkin.stack.thread_local._thread_local.zipkin_attrs', ['foo'])
def test_get_zipkin_attrs_returns_the_last_of_the_list():
    assert 'foo' == py_zipkin.stack.ThreadLocalStack().get()


def test_get_zipkin_attrs_with_context_returns_the_last_of_the_list():
    assert 'foo' == py_zipkin.stack.Stack(['bar', 'foo']).get()


@mock.patch('py_zipkin.thread_local._thread_local.zipkin_attrs', [])
def test_pop_zipkin_attrs_does_nothing_if_no_requests():
    assert not py_zipkin.stack.ThreadLocalStack().pop()


def test_pop_zipkin_attrs_with_context_does_nothing_if_no_requests():
    assert not py_zipkin.stack.Stack([]).pop()


@mock.patch(
    'py_zipkin.thread_local._thread_local.zipkin_attrs', ['foo', 'bar']
)
def test_pop_zipkin_attrs_removes_the_last_zipkin_attrs():
    assert 'bar' == py_zipkin.stack.ThreadLocalStack().pop()
    assert 'foo' == py_zipkin.stack.ThreadLocalStack().get()


def test_pop_zipkin_attrs_with_context_removes_the_last_zipkin_attrs():
    context_stack = py_zipkin.stack.Stack(['foo', 'bar'])
    assert 'bar' == context_stack.pop()
    assert 'foo' == context_stack.get()


@mock.patch('py_zipkin.thread_local._thread_local.zipkin_attrs', ['foo'])
def test_push_zipkin_attrs_adds_new_zipkin_attrs_to_list():
    assert 'foo' == py_zipkin.stack.ThreadLocalStack().get()
    py_zipkin.stack.ThreadLocalStack().push('bar')
    assert 'bar' == py_zipkin.stack.ThreadLocalStack().get()


def test_push_zipkin_attrs_with_context_adds_new_zipkin_attrs_to_list():
    stack = py_zipkin.stack.Stack(['foo'])
    assert 'foo' == stack.get()
    stack.push('bar')
    assert 'bar' == stack.get()
