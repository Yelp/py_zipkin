# -*- coding: utf-8 -*-
import logging
import threading

_thread_local = threading.local()

log = logging.getLogger('py_zipkin.thread_local')


def get_thread_local_zipkin_attrs():
    """A wrapper to return _thread_local.zipkin_attrs

    Returns a list of ZipkinAttrs objects, used for intra-process context
    propagation.

    :returns: list that may contain zipkin attribute tuples
    :rtype: list
    """
    if not hasattr(_thread_local, 'zipkin_attrs'):
        _thread_local.zipkin_attrs = []
    return _thread_local.zipkin_attrs


def get_thread_local_span_storage():
    """A wrapper to return _thread_local.span_storage

    Returns a SpanStorage object used to temporarily store all spans created in
    the current process. The transport handlers will pull from this storage when
    they emit the spans.

    :returns: SpanStore object containing all non-root spans.
    :rtype: py_zipkin.storage.SpanStore
    """
    if not hasattr(_thread_local, 'span_storage'):
        from py_zipkin.storage import SpanStorage
        _thread_local.span_storage = SpanStorage()
    return _thread_local.span_storage


def get_zipkin_attrs():
    """Get the topmost level zipkin attributes stored.

    :returns: tuple containing zipkin attrs
    :rtype: :class:`zipkin.ZipkinAttrs`
    """
    from py_zipkin.storage import ThreadLocalStack
    log.warning('get_zipkin_attrs is deprecated. '
                'Use py_zipkin.storage.ThreadLocalStack().get')
    return ThreadLocalStack().get()


def pop_zipkin_attrs():
    """Pop the topmost level zipkin attributes, if present.

    :returns: tuple containing zipkin attrs
    :rtype: :class:`zipkin.ZipkinAttrs`
    """
    from py_zipkin.storage import ThreadLocalStack
    log.warning('pop_zipkin_attrs is deprecated. '
                'Use py_zipkin.storage.ThreadLocalStack().pop')
    return ThreadLocalStack().pop()


def push_zipkin_attrs(zipkin_attr):
    """Stores the zipkin attributes to thread local.

    :param zipkin_attr: tuple containing zipkin related attrs
    :type zipkin_attr: :class:`zipkin.ZipkinAttrs`
    """
    from py_zipkin.storage import ThreadLocalStack
    log.warning('push_zipkin_attrs is deprecated. '
                'Use py_zipkin.storage.ThreadLocalStack().push')
    return ThreadLocalStack().push(zipkin_attr)
