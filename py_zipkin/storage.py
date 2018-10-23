import logging
from collections import deque

from py_zipkin import thread_local


log = logging.getLogger('py_zipkin.storage')


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
        pass

    @property
    def _storage(self):
        return thread_local.get_thread_local_zipkin_attrs()


class SpanStorage(deque):
    def __init__(self):
        super(SpanStorage, self).__init__()
        self._is_transport_configured = False

    def is_transport_configured(self):
        """Helper function to check whether a transport is configured.

        We need to propagate this info to the child zipkin_spans since
        if no transport is set-up they should not generate any Span to
        avoid memory leaks.

        :returns: whether transport is configured or not
        :rtype: bool
        """
        return self._is_transport_configured

    def set_transport_configured(self, configured):
        """Set whether the transport is configured or not.

        :param configured: whether transport is configured or not
        :type configured: bool
        """
        self._is_transport_configured = configured


class ThreadLocalSpanStorage(object):
    """
    ThreadLocalSpanStorage is wrapper around SpanStorage that stores the
    SpanStorage object in thread local.

    The thread local storage is accessed lazily in every method call,
    so the thread that calls the method matters, not the thread that
    instantiated the class. This is important since for example decorators
    are created at import time and in a different thread than where the
    rest of the code runs.
    Every instance shares the same thread local data.

    To make this work I infortunately need to override every method of
    SpanStorage and call the real span_storage instance.
    """

    @property
    def _storage(self):
        return thread_local.get_thread_local_span_storage()

    def __iter__(self):
        # No need to override __next__ as __iter__ returns a pointer to
        # itself. So python will call __next__ directly on the right object.
        return self._storage.__iter__()

    def __len__(self):
        return len(self._storage)

    def append(self, el):
        return self._storage.append(el)

    def clear(self):
        return self._storage.clear()

    def is_transport_configured(self):
        return self._storage.is_transport_configured()

    def set_transport_configured(self, configured):
        self._storage.set_transport_configured(configured)
