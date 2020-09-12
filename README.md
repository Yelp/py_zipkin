[![Build Status](https://travis-ci.org/Yelp/py_zipkin.svg?branch=master)](https://travis-ci.org/Yelp/py_zipkin)
[![Coverage Status](https://img.shields.io/coveralls/Yelp/py_zipkin.svg)](https://coveralls.io/r/Yelp/py_zipkin)
[![PyPi version](https://img.shields.io/pypi/v/py_zipkin.svg)](https://pypi.python.org/pypi/py_zipkin/)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/py_zipkin.svg)](https://pypi.python.org/pypi/py_zipkin/)

py_zipkin
---------

py_zipkin provides a context manager/decorator along with some utilities to
facilitate the usage of Zipkin in Python applications.

Install
-------

```
pip install py_zipkin
```

Usage
-----

py_zipkin requires a `transport_handler` object that handles logging zipkin
messages to a central logging service such as kafka or scribe.

`py_zipkin.zipkin.zipkin_span` is the main tool for starting zipkin traces or
logging spans inside an ongoing trace. zipkin_span can be used as a context
manager or a decorator.

#### Usage #1: Start a trace with a given sampling rate

```python
from py_zipkin.zipkin import zipkin_span

def some_function(a, b):
    with zipkin_span(
        service_name='my_service',
        span_name='my_span_name',
        transport_handler=some_handler,
        port=42,
        sample_rate=0.05, # Value between 0.0 and 100.0
    ):
        do_stuff(a, b)
```

#### Usage #2: Trace a service call

The difference between this and Usage #1 is that the zipkin_attrs are calculated
separately and passed in, thus negating the need of the sample_rate param.

```python
# Define a pyramid tween
def tween(request):
    zipkin_attrs = some_zipkin_attr_creator(request)
    with zipkin_span(
        service_name='my_service',
        span_name='my_span_name',
        zipkin_attrs=zipkin_attrs,
        transport_handler=some_handler,
        port=22,
    ) as zipkin_context:
        response = handler(request)
        zipkin_context.update_binary_annotations(
            some_binary_annotations)
        return response
```

#### Usage #3: Log a span inside an ongoing trace

This can be also be used inside itself to produce continuously nested spans.

```python
@zipkin_span(service_name='my_service', span_name='some_function')
def some_function(a, b):
    return do_stuff(a, b)
```

#### Other utilities

`zipkin_span.update_binary_annotations()` can be used inside a zipkin trace
to add to the existing set of binary annotations.

```python
def some_function(a, b):
    with zipkin_span(
        service_name='my_service',
        span_name='some_function',
        transport_handler=some_handler,
        port=42,
        sample_rate=0.05,
    ) as zipkin_context:
        result = do_stuff(a, b)
        zipkin_context.update_binary_annotations({'result': result})
```

`zipkin_span.add_sa_binary_annotation()` can be used to add a binary annotation
to the current span with the key 'sa'. This function allows the user to specify the
destination address of the service being called (useful if the destination doesn't
support zipkin). See http://zipkin.io/pages/data_model.html for more information on the
'sa' binary annotation.

> NOTE: the V2 span format only support 1 "sa" endpoint (represented by remoteEndpoint)
> so `add_sa_binary_annotation` now raises `ValueError` if you try to set multiple "sa"
> annotations for the same span.

```python
def some_function():
    with zipkin_span(
        service_name='my_service',
        span_name='some_function',
        transport_handler=some_handler,
        port=42,
        sample_rate=0.05,
    ) as zipkin_context:
        make_call_to_non_instrumented_service()
        zipkin_context.add_sa_binary_annotation(
            port=123,
            service_name='non_instrumented_service',
            host='12.34.56.78',
        )
```

`create_http_headers_for_new_span()` creates a set of HTTP headers that can be forwarded
in a request to another service.

```python
headers = {}
headers.update(create_http_headers_for_new_span())
http_client.get(
    path='some_url',
    headers=headers,
)
```

Transport
---------

py_zipkin (for the moment) thrift-encodes spans. The actual transport layer is
pluggable, though.

The recommended way to implement a new transport handler is to subclass
`py_zipkin.transport.BaseTransportHandler` and implement the `send` and
`get_max_payload_bytes` methods.

`send` receives an already encoded thrift list as argument.
`get_max_payload_bytes` should return the maximum payload size supported by your
transport, or `None` if you can send arbitrarily big messages.

The simplest way to get spans to the collector is via HTTP POST. Here's an
example of a simple HTTP transport using the `requests` library. This assumes
your Zipkin collector is running at localhost:9411.

> NOTE: older versions of py_zipkin suggested implementing the transport handler
> as a function with a single argument. That's still supported and should work
> with the current py_zipkin version, but it's deprecated.

```python
import requests

from py_zipkin.transport import BaseTransportHandler


class HttpTransport(BaseTransportHandler):

    def get_max_payload_bytes(self):
        return None

    def send(self, encoded_span):
        # The collector expects a thrift-encoded list of spans.
        requests.post(
            'http://localhost:9411/api/v1/spans',
            data=encoded_span,
            headers={'Content-Type': 'application/x-thrift'},
        )
```

If you have the ability to send spans over Kafka (more like what you might do
in production), you'd do something like the following, using the
[kafka-python](https://pypi.python.org/pypi/kafka-python) package:

```python
from kafka import SimpleProducer, KafkaClient

from py_zipkin.transport import BaseTransportHandler


class KafkaTransport(BaseTransportHandler):

    def get_max_payload_bytes(self):
        # By default Kafka rejects messages bigger than 1000012 bytes.
        return 1000012

    def send(self, message):
        kafka_client = KafkaClient('{}:{}'.format('localhost', 9092))
        producer = SimpleProducer(kafka_client)
        producer.send_messages('kafka_topic_name', message)
```

Using in multithreading environments
------------------------------------

If you want to use py_zipkin in a cooperative multithreading environment,
e.g. asyncio, you need to explicitly pass an instance of `py_zipkin.storage.Stack`
as parameter `context_stack` for `zipkin_span` and `create_http_headers_for_new_span`.
By default, py_zipkin uses a thread local storage for the attributes, which is
defined in `py_zipkin.storage.ThreadLocalStack`.

Additionally, you'll also need to explicitly pass an instance of
`py_zipkin.storage.SpanStorage` as parameter `span_storage` to `zipkin_span`.

```python
from py_zipkin.zipkin import zipkin_span
from py_zipkin.storage import Stack
from py_zipkin.storage import SpanStorage


def my_function():
    context_stack = Stack()
    span_storage = SpanStorage()
    await my_function(context_stack, span_storage)

async def my_function(context_stack, span_storage):
    with zipkin_span(
        service_name='my_service',
        span_name='some_function',
        transport_handler=some_handler,
        port=42,
        sample_rate=0.05,
        context_stack=context_stack,
        span_storage=span_storage,
    ):
        result = do_stuff(a, b)
```


Firehose mode [EXPERIMENTAL]
----------------------------

"Firehose mode" records 100% of the spans, regardless of
sampling rate. This is useful if you want to treat these spans
differently, e.g. send them to a different backend that has limited
retention. It works in tandem with normal operation, however there may
be additional overhead. In order to use this, you add a
`firehose_handler` just like you add a `transport_handler`.

This feature should be considered experimental and may be removed at
any time without warning. If you do use this, be sure to send
asynchronously to avoid excess overhead for every request.


License
-------

Copyright (c) 2018, Yelp, Inc. All Rights reserved. Apache v2
