[![Travis](https://img.shields.io/travis/Yelp/py_zipkin.svg)](https://travis-ci.org/Yelp/py_zipkin?branch=master)
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

py_zipkin requires a `transport_handler` function that handles logging zipkin
messages to a central logging service such as kafka or scribe.

`py_zipkin.zipkin.zipkin_span` is the main tool for starting zipkin traces or
logging spans inside an ongoing trace. zipkin_span can be used as a context
manager or a decorator.

#### Usage #1: Start a trace with a given sampling rate

```
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

```
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
        zipkin_context.update_binary_annotations_for_root_span(
            some_binary_annotations)
        return response
```

#### Usage #3: Log a span inside an ongoing trace

This can be also be used inside itself to produce continuously nested spans.

```
@zipkin_span(service_name='my_service', span_name='some_function')
def some_function(a, b):
    return do_stuff(a, b)
```

#### Other utilities

`zipkin_span.update_binary_annotations_for_root_span()` can be used inside a zipkin trace
to add to the existing set of binary annotations for the root span.

```
def some_function(a, b):
    with zipkin_span(
        service_name='my_service',
        span_name='some_function',
        transport_handler=some_handler,
        port=42,
        sample_rate=0.05,
    ) as zipkin_context:
        result = do_stuff(a, b)
        zipkin_context.update_binary_annotations_for_root_span({'result': result})
```

`create_http_headers_for_new_span()` creates a set of HTTP headers that can be forwarded
in a request to another service.

```
headers = {}
headers.update(create_http_headers_for_new_span())
http_client.get(
    path='some_url',
    headers=headers,
)
```


License
-------

Copyright (c) 2016, Yelp, Inc. All Rights reserved. Apache v2
