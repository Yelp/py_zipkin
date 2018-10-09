from collections import deque

from py_zipkin import thread_local


class Stack(object):
    """
    Stack is a simple stack class.

    It offers the operations push, pop and get.
    The latter two return None if the stack is empty.
    """

    def __init__(self, storage):
        self._storage = storage

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


def default_span_storage():
    return thread_local.get_thread_local_span_storage()
