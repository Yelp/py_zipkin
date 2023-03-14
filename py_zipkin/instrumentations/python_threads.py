import threading
from functools import partial
from typing import Any
from typing import Callable

from py_zipkin import storage

_orig_Thread_start = threading.Thread.start
_orig_Thread_run = threading.Thread.run


def _Thread_pre_start(self: Any) -> None:
    self._orig_tracer = None
    if storage.has_default_tracer():
        self._orig_tracer = storage.get_default_tracer().copy()


def _Thread_wrap_run(self: Any, actual_run_fn: Callable[[], None]) -> None:
    # This executes in the new OS thread
    if self._orig_tracer:
        # Inject our copied Tracer into our thread-local-storage
        storage.set_default_tracer(self._orig_tracer)
    try:
        actual_run_fn()
    finally:
        # I think this is probably a good idea for the same reasons the
        # parent class deletes __target, __args, and __kwargs
        if self._orig_tracer:
            del self._orig_tracer


def patch_threading() -> None:  # pragma: no cover
    """Monkey-patch threading module to work better with tracing."""

    def _new_start(self: Any) -> None:
        _Thread_pre_start(self)
        _orig_Thread_start(self)

    def _new_run(self: Any) -> None:
        _Thread_wrap_run(self, partial(_orig_Thread_run, self))

    threading.Thread.start = _new_start  # type: ignore[method-assign]
    threading.Thread.run = _new_run  # type: ignore[method-assign]


def unpatch_threading() -> None:  # pragma: no cover
    threading.Thread.start = _orig_Thread_start  # type: ignore[method-assign]
    threading.Thread.run = _orig_Thread_run  # type: ignore[method-assign]
