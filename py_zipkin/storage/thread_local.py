# -*- coding: utf-8 -*-
from py_zipkin import thread_local
from py_zipkin.storage import LocalStorage
from py_zipkin.storage import ZipkinStorage


class ThreadLocalStorage(LocalStorage):
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
    def storage(self):
        if not hasattr(thread_local._thread_local, 'zipkin_storage'):
            thread_local._thread_local.zipkin_storage = ZipkinStorage()
        return thread_local._thread_local.zipkin_storage
