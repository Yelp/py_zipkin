# -*- coding: utf-8 -*-
import threading
import warnings

_thread_local = threading.local()


def get_thread_local_zipkin_attrs():
    """A wrapper to return _thread_local.requests

    :returns: list that may contain zipkin attribute tuples
    :rtype: list
    """
    if not hasattr(_thread_local, 'zipkin_attrs'):
        _thread_local.zipkin_attrs = []
    return _thread_local.zipkin_attrs


def get_zipkin_attrs():
    """Get the topmost level zipkin attributes stored.

    :returns: tuple containing zipkin attrs
    :rtype: :class:`zipkin.ZipkinAttrs`
    """
    from py_zipkin.stack import ThreadLocalStack
    warnings.warn(
        'Use py_zipkin.stack.ThreadLocalStack().get',
        DeprecationWarning,
    )
    return ThreadLocalStack().get()


def pop_zipkin_attrs():
    """Pop the topmost level zipkin attributes, if present.

    :returns: tuple containing zipkin attrs
    :rtype: :class:`zipkin.ZipkinAttrs`
    """
    from py_zipkin.stack import ThreadLocalStack
    warnings.warn(
        'Use py_zipkin.stack.ThreadLocalStack().pop',
        DeprecationWarning,
    )
    return ThreadLocalStack().pop()


def push_zipkin_attrs(zipkin_attr):
    """Stores the zipkin attributes to thread local.

    :param zipkin_attr: tuple containing zipkin related attrs
    :type zipkin_attr: :class:`zipkin.ZipkinAttrs`
    """
    from py_zipkin.stack import ThreadLocalStack
    warnings.warn(
        'Use py_zipkin.stack.ThreadLocalStack().push',
        DeprecationWarning,
    )
    return ThreadLocalStack().push(zipkin_attr)
