import logging

from py_zipkin.storage import get_default_tracer

log = logging.getLogger("py_zipkin.thread_local")


def get_thread_local_zipkin_attrs():
    """A wrapper to return _thread_local.zipkin_attrs

    Returns a list of ZipkinAttrs objects, used for intra-process context
    propagation.

    .. deprecated::
       Use the Tracer interface which offers better multi-threading support.
       get_thread_local_zipkin_attrs will be removed in version 1.0.

    :returns: list that may contain zipkin attribute tuples
    :rtype: list
    """
    log.warning(
        "get_thread_local_zipkin_attrs is deprecated. See DEPRECATIONS.rst"
        " for details on how to migrate to using Tracer."
    )
    return get_default_tracer()._context_stack._storage


def get_thread_local_span_storage():
    """A wrapper to return _thread_local.span_storage

    Returns a SpanStorage object used to temporarily store all spans created in
    the current process. The transport handlers will pull from this storage when
    they emit the spans.

    .. deprecated::
       Use the Tracer interface which offers better multi-threading support.
       get_thread_local_span_storage will be removed in version 1.0.

    :returns: SpanStore object containing all non-root spans.
    :rtype: py_zipkin.storage.SpanStore
    """
    log.warning(
        "get_thread_local_span_storage is deprecated. See DEPRECATIONS.rst"
        " for details on how to migrate to using Tracer."
    )
    return get_default_tracer()._span_storage


def get_zipkin_attrs():
    """Get the topmost level zipkin attributes stored.

    .. deprecated::
       Use the Tracer interface which offers better multi-threading support.
       get_zipkin_attrs will be removed in version 1.0.

    :returns: tuple containing zipkin attrs
    :rtype: :class:`zipkin.ZipkinAttrs`
    """
    from py_zipkin.storage import ThreadLocalStack

    log.warning(
        "get_zipkin_attrs is deprecated. See DEPRECATIONS.rst for"
        "details on how to migrate to using Tracer."
    )
    return ThreadLocalStack().get()


def pop_zipkin_attrs():
    """Pop the topmost level zipkin attributes, if present.

    .. deprecated::
       Use the Tracer interface which offers better multi-threading support.
       pop_zipkin_attrs will be removed in version 1.0.

    :returns: tuple containing zipkin attrs
    :rtype: :class:`zipkin.ZipkinAttrs`
    """
    from py_zipkin.storage import ThreadLocalStack

    log.warning(
        "pop_zipkin_attrs is deprecated. See DEPRECATIONS.rst for"
        "details on how to migrate to using Tracer."
    )
    return ThreadLocalStack().pop()


def push_zipkin_attrs(zipkin_attr):
    """Stores the zipkin attributes to thread local.

    .. deprecated::
       Use the Tracer interface which offers better multi-threading support.
       push_zipkin_attrs will be removed in version 1.0.

    :param zipkin_attr: tuple containing zipkin related attrs
    :type zipkin_attr: :class:`zipkin.ZipkinAttrs`
    """
    from py_zipkin.storage import ThreadLocalStack

    log.warning(
        "push_zipkin_attrs is deprecated. See DEPRECATIONS.rst for"
        "details on how to migrate to using Tracer."
    )
    return ThreadLocalStack().push(zipkin_attr)
