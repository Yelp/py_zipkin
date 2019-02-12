Deprecations: how to migrate
============================

New Tracer interface
--------------------

The following classes and methods are deprecated and should not be used.
Please use the Tracer interface instead.
They will be removed in version 1.0.

REASON: they don't work well in multi-threaded environments and can cause
dropped spans or memory leaks.

- `py_zipkin.storage.Stack`
- `py_zipkin.storage.ThreadLocalStack`
- `py_zipkin.storage.SpanStorage`
- `py_zipkin.storage.default_span_storage`
- `py_zipkin.thread_local.get_thread_local_zipkin_attrs`
- `py_zipkin.thread_local.get_thread_local_span_storage`
- `py_zipkin.thread_local.get_zipkin_attrs`
- `py_zipkin.thread_local.pop_zipkin_attrs`
- `py_zipkin.thread_local.push_zipkin_attrs`

To access the current zipkin_attrs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from py_zipkin import get_default_tracer

    zipkin_attrs = get_default_tracer().get_zipkin_attrs()
    get_default_tracer().push_zipkin_attrs(zipkin_attrs)

To override the default tracer and provide your own in a multi-threaded env
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can instantiate your own Tracer instance and pass it to ALL your zipkin_span
context managers and decorators. You'll need to propagate the tracer around on
your own.

.. code-block:: python

    from py_zipkin import Tracer

    tracer = Tracer()
    with zipkin_span(
        service_name="homepage",
        span_name="get /home",
        transport=MockTransport(),
        sample_rate=100.0,
        get_tracer=lambda: tracer,
    ):
        # do stuff

Alternatively you can use the `get_default_tracer` and `set_default_tracer`
functions to reset the default tracer every time you switch thread or
coroutine. In this case you won't need to pass the tracer to every `zipkin_span`
but they'll be able to pull the right one automatically.

.. code-block:: python

    from py_zipkin import get_default_tracer
    from py_zipkin import set_default_tracer

    def fn(tracer):
        set_default_tracer(tracer)
        # do stuff

    def main():
        tracer = get_default_tracer()
        return await asyncio.get_event_loop().run_in_executor(None, fn, tracer)


Kind
----

`zipkin_span`'s `include` argument is deprecated. You should set `kind` instead.

REASON: Zipkin V2 data format uses kind to distinguish between client and
server spans.

.. code-block:: python

    from py_zipkin import Kind
    from py_zipkin.zipkin import zipkin_span

    # Old code, uses include
    with zipkin_span(
        service_name="homepage",
        span_name="get /home",
        include=('server',),
    ):
        pass

    # New code, uses Kind
    with zipkin_span(
        service_name="homepage",
        span_name="get /home",
        kind=Kind.SERVER,
    ):
        pass
