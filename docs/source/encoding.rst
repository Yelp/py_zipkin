Encodings
=========

Zipkin currently supports 2 different versions of its data format. V1 spans can
be encoded in Thrift or JSON, while V2 spans can be JSON or Protobuf v3.

Supported output encodings
--------------------------

py_zipkin supports all 4 encodings. The current default is V1 thrift for backward
compatibility, but you can easily change that.

The root context manager accepts an optional `encoding` argument that you can use
to control the output encoding. Its value needs to be one of
`Encoding <#py_zipkin.encoding.Encoding>`_'s values.

.. note::
   You only need to pass the encoding to the root context manager. Passing it to
   any other `zipkin_span` will have no effect.

.. code-block:: python

   from py_zipkin.encoding import Encoding

   # Sets the encoding to V2_JSON for all spans generated in this process.
   with zipkin_server_span(
      service_name='homepage',
      span_name='GET /home',
      transport_handler=SimpleHTTPTransport(),
      sample_rate=100.0,
      encoding=Encoding.V2_JSON,
   ):
      with zipkin_span('homepage', 'inner_span'):
         # do stuff

.. autoclass:: py_zipkin.encoding.Encoding
   :members:
   :undoc-members:

Convert encoded spans between formats
-------------------------------------

py_zipkin provides some helper functions to convert between encodings. They work
on already encoded spans so they're meant to be used

.. autofunction:: py_zipkin.encoding.convert_spans

.. autofunction:: py_zipkin.encoding.detect_span_version_and_encoding
