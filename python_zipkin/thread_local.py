# -*- coding: utf-8 -*-
import threading

_thread_local = threading.local()


def get_thread_local_requests():
    """A wrapper to return _thread_local.requests

    :returns: list that may contain zipkin attribute tuples
    :rtype: list
    """
    if not hasattr(_thread_local, 'requests'):
        _thread_local.requests = []
    return _thread_local.requests


def get_zipkin_attrs():
    """Get the topmost level zipkin attributes stored.

    :returns: tuple containing zipkin attrs
    :rtype: :class:`zipkin.ZipkinAttrs`
    """
    requests = get_thread_local_requests()
    if requests:
        return requests[-1]


def pop_zipkin_attrs():
    """Pop the topmost level zipkin attributes, if present.

    :returns: tuple containing zipkin attrs
    :rtype: :class:`zipkin.ZipkinAttrs`
    """
    requests = get_thread_local_requests()
    if requests:
        return requests.pop()


def push_zipkin_attrs(zipkin_attr):
    """Stores the zipkin attributes to thread local.

    :param zipkin_attr: tuple containing zipkin related attrs
    :type zipkin_attr: :class:`zipkin.ZipkinAttrs`
    """
    get_thread_local_requests().append(zipkin_attr)
