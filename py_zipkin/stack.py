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

    def __iter__(self):
        self.index = 0
        return self

    def __next__(self):
        if len(self._storage) <= self.index:
            raise StopIteration
        self.index += 1
        return self._storage[self.index - 1]

    def clear(self):
        while len(self._storage) > 0:
            self._storage.pop()


class ThreadLocalStack(Stack):
    """
    ThreadLocalStack is variant of Stack that uses a thread local storage.

    The thread local storage is accessed lazily in every method call,
    so the thread that calls the method matters, not the thread that
    instantiated the class.
    Every instance shares the same thread local data.
    """

    def __init__(self, storage_fn=None):
        self._storage_fn = storage_fn or thread_local.get_thread_local_zipkin_attrs

    @property
    def _storage(self):
        return self._storage_fn()
