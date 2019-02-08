import logging
from collections import deque

from py_zipkin import thread_local
from py_zipkin.thread_local import get_thread_local_span_storage
from py_zipkin.thread_local import get_thread_local_zipkin_attrs
try:  # pragma: no cover
    import contextvars
    _contextvars_tracer = contextvars.ContextVar('py_zipkin.Tracer object')
except ImportError:  # pragma: no cover
    # The contextvars module was added in python 3.7
    _contextvars_tracer = None


log = logging.getLogger('py_zipkin.storage')


def get_thread_local_tracer():
    if not hasattr(thread_local._thread_local, 'tracer'):
        thread_local._thread_local.tracer = Tracer()
    return thread_local._thread_local.tracer


def get_contextvar_tracer():  # pragma: no cover
    try:
        return _contextvars_tracer.get()
    except LookupError:
        _contextvars_tracer.set(Tracer())
        return _contextvars_tracer.get()


class Tracer(object):

    def __init__(self):
        self._is_transport_configured = False
        self._span_storage = SpanStorage()
        self._context_stack = Stack()

    def get_zipkin_attrs(self):
        return self._context_stack.get()

    def push_zipkin_attrs(self, ctx):
        self._context_stack.push(ctx)

    def pop_zipkin_attrs(self):
        return self._context_stack.pop()

    def add_span(self, span):
        self._span_storage.append(span)

    def get_spans(self):
        return self._span_storage

    def clear(self):
        self._span_storage.clear()

    def set_transport_configured(self, configured):
        self._is_transport_configured = configured

    def is_transport_configured(self):
        return self._is_transport_configured


class Stack(object):
    """
    Stack is a simple stack class.

    It offers the operations push, pop and get.
    The latter two return None if the stack is empty.
    """

    def __init__(self, storage=None):
        if storage is not None:
            log.warning('Passing a storage object to Stack is deprecated.')
            self._storage = storage
        else:
            self._storage = []

    def push(self, item):
        self._storage.append(item)

    def pop(self):
        if self._storage:
            return self._storage.pop()

    def get(self):
        if self._storage:
            return self._storage[-1]


class ThreadLocalStack(Stack):
    """
    ThreadLocalStack is variant of Stack that uses a thread local storage.

    The thread local storage is accessed lazily in every method call,
    so the thread that calls the method matters, not the thread that
    instantiated the class.
    Every instance shares the same thread local data.
    """

    def __init__(self):
        log.warning('ThreadLocalStack is deprecated. Set local_storage instead.')

    @property
    def _storage(self):
        return get_thread_local_zipkin_attrs()


class SpanStorage(deque):
    pass


def default_span_storage():
    log.warning('default_span_storage is deprecated. Set local_storage instead.')
    return get_thread_local_span_storage()


def get_default_tracer():
    if _contextvars_tracer:
        return get_contextvar_tracer()

    return get_thread_local_tracer()
