import mock
import pytest

import py_zipkin.storage


@pytest.fixture(autouse=True, scope='module')
def create_zipkin_attrs():
    # The following tests all expect _thread_local.zipkin_attrs to exist: if it
    # doesn't, mock.patch will fail.
    py_zipkin.storage.ThreadLocalStack().get()


@mock.patch('py_zipkin.thread_local._thread_local.zipkin_attrs', [])
def test_get_zipkin_attrs_returns_none_if_no_zipkin_attrs():
    assert not py_zipkin.storage.ThreadLocalStack().get()
    assert not py_zipkin.storage.ThreadLocalStack().get()


def test_get_zipkin_attrs_with_context_returns_none_if_no_zipkin_attrs():
    with mock.patch.object(py_zipkin.storage.log, 'warning', autospec=True) as log:
        assert not py_zipkin.storage.Stack([]).get()
        assert log.call_count == 1


def test_storage_stack_still_works_if_you_dont_pass_in_storage():
    # Let's make sure this still works if we don't pass in a custom storage.
    assert not py_zipkin.storage.Stack().get()


@mock.patch('py_zipkin.storage.thread_local._thread_local.zipkin_attrs', ['foo'])
def test_get_zipkin_attrs_returns_the_last_of_the_list():
    assert 'foo' == py_zipkin.storage.ThreadLocalStack().get()


def test_get_zipkin_attrs_with_context_returns_the_last_of_the_list():
    assert 'foo' == py_zipkin.storage.Stack(['bar', 'foo']).get()


@mock.patch('py_zipkin.thread_local._thread_local.zipkin_attrs', [])
def test_pop_zipkin_attrs_does_nothing_if_no_requests():
    assert not py_zipkin.storage.ThreadLocalStack().pop()


def test_pop_zipkin_attrs_with_context_does_nothing_if_no_requests():
    assert not py_zipkin.storage.Stack([]).pop()


@mock.patch(
    'py_zipkin.thread_local._thread_local.zipkin_attrs', ['foo', 'bar']
)
def test_pop_zipkin_attrs_removes_the_last_zipkin_attrs():
    assert 'bar' == py_zipkin.storage.ThreadLocalStack().pop()
    assert 'foo' == py_zipkin.storage.ThreadLocalStack().get()


def test_pop_zipkin_attrs_with_context_removes_the_last_zipkin_attrs():
    context_stack = py_zipkin.storage.Stack(['foo', 'bar'])
    assert 'bar' == context_stack.pop()
    assert 'foo' == context_stack.get()


@mock.patch('py_zipkin.thread_local._thread_local.zipkin_attrs', ['foo'])
def test_push_zipkin_attrs_adds_new_zipkin_attrs_to_list():
    assert 'foo' == py_zipkin.storage.ThreadLocalStack().get()
    py_zipkin.storage.ThreadLocalStack().push('bar')
    assert 'bar' == py_zipkin.storage.ThreadLocalStack().get()


def test_push_zipkin_attrs_with_context_adds_new_zipkin_attrs_to_list():
    stack = py_zipkin.storage.Stack(['foo'])
    assert 'foo' == stack.get()
    stack.push('bar')
    assert 'bar' == stack.get()
