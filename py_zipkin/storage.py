import logging
import threading
from typing import Any
from typing import Deque
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

from py_zipkin.encoding._helpers import Span
from py_zipkin.util import ZipkinAttrs

if TYPE_CHECKING:  # pragma: no cover
    from py_zipkin import zipkin

try:  # pragma: no cover
    # Since python 3.7 threadlocal is deprecated in favor of contextvars
    # which also work in asyncio.
    import contextvars

    _contextvars_tracer: Optional[
        contextvars.ContextVar["Tracer"]
    ] = contextvars.ContextVar("py_zipkin.Tracer object")
except ImportError:  # pragma: no cover
    # The contextvars module was added in python 3.7
    _contextvars_tracer = None

_thread_local_tracer = threading.local()


log = logging.getLogger("py_zipkin.storage")


def _get_thread_local_tracer() -> "Tracer":
    """Returns the current tracer from thread-local.

    If there's no current tracer it'll create a new one.
    :returns: current tracer.
    :rtype: Tracer
    """
    if not hasattr(_thread_local_tracer, "tracer"):
        _thread_local_tracer.tracer = Tracer()
    return _thread_local_tracer.tracer


def _set_thread_local_tracer(tracer: "Tracer") -> None:
    """Sets the current tracer in thread-local.

    :param tracer: current tracer.
    :type tracer: Tracer
    """
    _thread_local_tracer.tracer = tracer


def _get_contextvars_tracer() -> "Tracer":  # pragma: no cover
    """Returns the current tracer from contextvars.

    If there's no current tracer it'll create a new one.
    :returns: current tracer.
    :rtype: Tracer
    """
    assert _contextvars_tracer is not None
    try:
        return _contextvars_tracer.get()
    except LookupError:
        _contextvars_tracer.set(Tracer())
        return _contextvars_tracer.get()


def _set_contextvars_tracer(tracer: "Tracer") -> None:  # pragma: no cover
    """Sets the current tracer in contextvars.

    :param tracer: current tracer.
    :type tracer: Tracer
    """
    assert _contextvars_tracer is not None
    _contextvars_tracer.set(tracer)


class Tracer:
    def __init__(self) -> None:
        self._is_transport_configured = False
        self._span_storage = SpanStorage()
        self._context_stack = Stack()

    def get_zipkin_attrs(self) -> Optional[ZipkinAttrs]:
        return self._context_stack.get()

    def push_zipkin_attrs(self, ctx: ZipkinAttrs) -> None:
        self._context_stack.push(ctx)

    def pop_zipkin_attrs(self) -> Optional[ZipkinAttrs]:
        return self._context_stack.pop()

    def add_span(self, span: Span) -> None:
        self._span_storage.append(span)

    def get_spans(self) -> "SpanStorage":
        return self._span_storage

    def clear(self) -> None:
        self._span_storage.clear()

    def set_transport_configured(self, configured: bool) -> None:
        self._is_transport_configured = configured

    def is_transport_configured(self) -> bool:
        return self._is_transport_configured

    def zipkin_span(self, *argv: Any, **kwargs: Any) -> "zipkin.zipkin_span":
        from py_zipkin import zipkin

        kwargs["_tracer"] = self
        return zipkin.zipkin_span(*argv, **kwargs)

    def copy(self) -> "Tracer":
        """Return a copy of this instance, but with a deep-copied
        _context_stack.  The use-case is for passing a copy of a Tracer into
        a new thread context.
        """
        the_copy = self.__class__()
        the_copy._is_transport_configured = self._is_transport_configured
        the_copy._span_storage = self._span_storage
        the_copy._context_stack = self._context_stack.copy()
        return the_copy


class Stack:
    """
    Stack is a simple stack class.

    It offers the operations push, pop and get.
    The latter two return None if the stack is empty.

    .. deprecated::
       Use the Tracer interface which offers better multi-threading support.
       Stack will be removed in version 1.0.
    """

    def __init__(self, storage: Optional[List[ZipkinAttrs]] = None) -> None:
        if storage is not None:
            log.warning("Passing a storage object to Stack is deprecated.")
            self.__storage: List[ZipkinAttrs] = storage
        else:
            self.__storage = []

    # this pattern is currently necessary due to
    # https://github.com/python/mypy/issues/4125
    @property
    def _storage(self) -> List[ZipkinAttrs]:
        return self.__storage

    @_storage.setter
    def _storage(self, value: List[ZipkinAttrs]) -> None:  # pragma: no cover
        self.__storage = value

    @_storage.deleter
    def _storage(self) -> None:  # pragma: no cover
        del self.__storage

    def push(self, item: ZipkinAttrs) -> None:
        self._storage.append(item)

    def pop(self) -> Optional[ZipkinAttrs]:
        if self._storage:
            return self._storage.pop()
        return None

    def get(self) -> Optional[ZipkinAttrs]:
        if self._storage:
            return self._storage[-1]
        return None

    def copy(self) -> "Stack":
        # Return a new Stack() instance with a deep copy of our stack contents
        the_copy = self.__class__()
        the_copy._storage = self._storage[:]
        return the_copy


class ThreadLocalStack(Stack):
    """ThreadLocalStack is variant of Stack that uses a thread local storage.

    The thread local storage is accessed lazily in every method call,
    so the thread that calls the method matters, not the thread that
    instantiated the class.
    Every instance shares the same thread local data.

    .. deprecated::
       Use the Tracer interface which offers better multi-threading support.
       ThreadLocalStack will be removed in version 1.0.
    """

    def __init__(self) -> None:
        log.warning(
            "ThreadLocalStack is deprecated. See DEPRECATIONS.rst for"
            "details on how to migrate to using Tracer."
        )

    @property
    def _storage(self) -> List[ZipkinAttrs]:
        return get_default_tracer()._context_stack._storage

    @_storage.setter
    def _storage(self, value: List[ZipkinAttrs]) -> None:  # pragma: no cover
        get_default_tracer()._context_stack._storage = value

    @_storage.deleter
    def _storage(self) -> None:  # pragma: no cover
        del get_default_tracer()._context_stack._storage


class SpanStorage(Deque[Span]):
    """Stores the list of completed spans ready to be sent.

    .. deprecated::
       Use the Tracer interface which offers better multi-threading support.
       SpanStorage will be removed in version 1.0.
    """

    pass


def default_span_storage() -> SpanStorage:
    log.warning(
        "default_span_storage is deprecated. See DEPRECATIONS.rst for"
        "details on how to migrate to using Tracer."
    )
    return get_default_tracer()._span_storage


def has_default_tracer() -> bool:
    """Is there a default tracer created already?

    :returns: Is there a default tracer created already?
    :rtype: boolean
    """
    try:
        if _contextvars_tracer and _contextvars_tracer.get():
            return True
    except LookupError:
        pass
    return hasattr(_thread_local_tracer, "tracer")


def get_default_tracer() -> Tracer:
    """Return the current default Tracer.

    For now it'll get it from thread-local in Python 2.7 to 3.6 and from
    contextvars since Python 3.7.

    :returns: current default tracer.
    :rtype: Tracer
    """
    if _contextvars_tracer:
        return _get_contextvars_tracer()

    return _get_thread_local_tracer()


def set_default_tracer(tracer: Tracer) -> None:
    """Sets the current default Tracer.

    For now it'll get it from thread-local in Python 2.7 to 3.6 and from
    contextvars since Python 3.7.

    :returns: current default tracer.
    :rtype: Tracer
    """
    if _contextvars_tracer:
        _set_contextvars_tracer(tracer)

    _set_thread_local_tracer(tracer)
