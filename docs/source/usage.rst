How to use py_zipkin
====================

test test tes

Log a span within the context of a zipkin trace
-----------------------------------------------

If you're already in a zipkin trace, you can use this to log a span inside. The
only required param is service_name. If you're not in a zipkin trace, this
won't do anything.

.. code-block:: python

   # As a decorator
   @zipkin_span(service_name='my_service', span_name='my_function')
   def my_function():
      do_stuff()

.. code-block:: python

   # As a context manager
   def my_function():
      with zipkin_span(service_name='my_service', span_name='do_stuff'):
         do_stuff()

Start a trace with a given sampling rate
----------------------------------------

This begins the zipkin trace and also records the root span. The required
params are service_name, transport_handler, and sample_rate.

.. code-block:: python

   # Start a trace with do_stuff() as the root span
   def some_batch_job(a, b):
      with zipkin_span(
            service_name='my_service',
            span_name='my_span_name',
            transport_handler=some_handler,
            port=22,
            sample_rate=0.05,
      ):
            do_stuff()

Trace a service call
---------------------

Client side
~~~~~~~~~~~

.. code-block:: python

   from py_zipkin.zipkin import create_http_headers_for_new_span
   from py_zipkin.zipkin import zipkin_span


   @zipkin_client_span('my_service', 'GET')
   def make_request(url, headers):
      headers.update(create_http_headers_for_new_span())
      return requests.get(url, headers=headers)

.. autofunction:: py_zipkin.zipkin.create_http_headers_for_new_span


The typical use case is instrumenting a framework like Pyramid or Django. Only
ss and sr times are recorded for the root span. Required params are
service_name, zipkin_attrs, transport_handler, and port.

Server side
~~~~~~~~~~~

.. code-block:: python

   def extract_zipkin_attrs(headers)
      return ZipkinAttrs(
         trace_id=headers.get('X-B3-TraceId')
         span_id=headers.get('X-B3-SpanId')
         parent_span_id=headers.get('X-B3-ParentSpanId')
         flags=headers.get('X-B3-Flags')
         is_sampled=headers.get('X-B3-Sampled')
      )

   # Used in a pyramid tween
   def tween(request):
      zipkin_attrs = extract_zipkin_attrs(request.headers)

      with zipkin_span(
         service_name='my_service',
         span_name='GET /user/{user_id}',
         zipkin_attrs=zipkin_attrs,
         transport_handler=SimpleHTTPHandler('localhost:9411'),
         sample_rate=1.0,  # 1% up-sampling probability
         encoding=Encoding.V2_JSON,
      ) as zipkin_span:
         return handler(request)


.. autoclass:: py_zipkin.zipkin.zipkin_span

.. autoclass:: py_zipkin.Kind

.. autoclass:: py_zipkin.zipkin.ZipkinAttrs

.. autoclass:: py_zipkin.encoding.Span
   :members:

.. autoclass:: py_zipkin.encoding._helpers.Endpoint
