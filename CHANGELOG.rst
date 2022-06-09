1.0.0 (2022-06-09)
-------------------
- Droop Python 2.7 support (minimal supported python version is 3.5)
- Recompile protobuf using version 3.19

0.21.0 (2021-03-17)
-------------------
- The default encoding is now V2 JSON. If you want to keep the old
  V1 thrift encoding you'll need to specify it.

0.20.2 (2021-03-11)
-------------------
- Don't crash when annotating exceptions that cannot be str()'d

0.20.1 (2020-10-27)
-------------------
- Support PRODUCER and CONSUMER spans

0.20.0 (2020-03-09)
-------------------
- Add create_http_headers helper

0.19.0 (2020-02-28)
-------------------
- Add zipkin_span.add_annotation() method
- Add autoinstrumentation for python Threads
- Allow creating a copy of Tracer
- Add extract_zipkin_attrs_from_headers() helper

0.18.7 (2020-01-15)
-------------------
- Expose encoding.create_endpoint helper

0.18.6 (2019-09-23)
-------------------
- Ensure tags are strings when using V2_JSON encoding

0.18.5 (2019-08-08)
-------------------
- Add testing.MockTransportHandler module

0.18.4 (2019-08-02)
-------------------
- Fix thriftpy2 import to allow cython module

0.18.3 (2019-05-15)
-------------------
- Fix unicode bug when decoding thrift tag strings

0.18.2 (2019-03-26)
-------------------
- Handled exception while emitting trace and log the error
- Ensure tracer is cleared regardless span of emit outcome

0.18.1 (2019-02-22)
-------------------
- Fix ThreadLocalStack() bug introduced in 0.18.0

0.18.0 (2019-02-13)
-------------------
- Fix multithreading issues
- Added Tracer module

0.17.1 (2019-02-05)
-------------------
- Ignore transport_handler overrides in an inner span since that causes
  spans to be dropped.

0.17.0 (2019-01-25)
-------------------
- Support python 3.7
- py-zipkin now depends on thriftpy2 rather than thriftpy. They
  can coexist in the same codebase, so it should be safe to upgrade.

0.16.1 (2018-11-16)
-------------------
- Handle null timestamps when decoding thrift traces

0.16.0 (2018-11-13)
-------------------
- py_zipkin is now able to convert V1 thrift spans to V2 JSON

0.15.1 (2018-10-31)
-------------------
- Changed DeprecationWarnings to logging.warning

0.15.0 (2018-10-22)
-------------------
- Added support for V2 JSON encoding.
- Fixed TransportHandler bug that was affecting also V1 JSON.

0.14.1 (2018-10-09)
-------------------
- Fixed memory leak introduced in 0.13.0.

0.14.0 (2018-10-01)
-------------------
- Support JSON encoding for V1 spans.
- Allow overriding the span_name after creation.

0.13.0 (2018-06-25)
-------------------
- Removed deprecated `zipkin_logger.debug()` interface.
- `py_zipkin.stack` was renamed as `py_zipkin.storage`. If you were
  importing this module, you'll need to update your code.

0.12.0 (2018-05-29)
-------------------
- Support max payload size for transport handlers.
- Transport handlers should now be implemented as classes
  extending py_zipkin.transport.BaseTransportHandler.

0.11.2 (2018-05-23)
-------------------
- Don't overwrite passed in annotations

0.11.1 (2018-05-23)
-------------------
- Add binary annotations to the span even if the request is not being
  sampled. This fixes binary annotations for firehose spans.

0.11.0 (2018-02-08)
-------------------
- Add support for "firehose mode", which logs 100% of the spans
  regardless of sample rate.

0.10.1 (2018-02-05)
-------------------
- context_stack will now default to `ThreadLocalStack()` if passed as
  `None`

0.10.0 (2018-02-05)
-------------------
- Add support for using explicit in-process context storage instead of
  using thread_local. This allows you to use py_zipkin in cooperative
  multitasking environments e.g. asyncio
- `py_zipkin.thread_local` is now deprecated. Instead use
  `py_zipkin.stack.ThreadLocalStack()`
- TraceId and SpanId generation performance improvements.
- 128-bit TraceIds now start with an epoch timestamp to support easy
  interop with AWS X-Ray

0.9.0 (2017-07-31)
------------------
- Add batch span sending. Note that spans are now sent in lists.

0.8.3 (2017-07-10)
------------------
- Be defensive about having logging handlers configured to avoid throwing
  NullHandler attribute errors

0.8.2 (2017-06-30)
------------------
- Don't log ss and sr annotations when in a client span context
- Add error binary annotation if an exception occurs

0.8.1 (2017-06-16)
------------------
- Fixed server send timing to more accurately reflect when server send
  actually occurs.
- Replaced logging_start annotation with logging_end

0.8.0 (2017-06-01)
------------------
- Added 128-bit trace id support
- Added ability to explicitly specify host for a span
- Added exception handling if host can't be determined automatically
- SERVER_ADDR ('sa') binary annotations can be added to spans
- py36 support

0.7.1 (2017-05-01)
------------------
- Fixed a bug where `update_binary_annotations` would fail for a child
  span in a trace that is not being sampled

0.7.0 (2017-03-06)
------------------
- Simplify `update_binary_annotations` for both root and non-root spans

0.6.0 (2017-02-03)
------------------
- Added support for forcing `zipkin_span` to report timestamp/duration.
  Changes API of `zipkin_span`, but defaults back to existing behavior.

0.5.0 (2017-02-01)
------------------
- Properly set timestamp/duration on server and local spans
- Updated thrift spec to include these new fields
- The `zipkin_span` entrypoint should be backwards compatible

0.4.4 (2016-11-29)
------------------
- Add optional annotation for when Zipkin logging starts

0.4.3 (2016-11-04)
------------------
- Fix bug in zipkin_span decorator

0.4.2 (2016-11-01)
------------------
- Be defensive about transport_handler when logging spans.

0.4.1 (2016-10-24)
------------------
- Add ability to override span_id when creating new ZipkinAttrs.

0.4.0 (2016-10-20)
------------------
- Added `start` and `stop` functions as friendlier versions of the
  __enter__ and __exit__ functions.

0.3.1 (2016-09-30)
------------------
- Adds new param to thrift.create_endpoint allowing creation of
  thrift Endpoint objects on a proxy machine representing another
  host.

0.2.1 (2016-09-30)
------------------
- Officially "release" v0.2.0. Accidentally pushed a v0.2.0 without
  the proper version bump, so v0.2.1 is the new real version. Please
  use this instead of v0.2.0.

0.2.0 (2016-09-30)
------------------
- Fix problem where if zipkin_attrs and sample_rate were passed, but
  zipkin_attrs.is_sampled=True, new zipkin_attrs were being generated.

0.1.2 (2016-09-29)
------------------
- Fix sampling algorithm that always sampled for rates > 50%

0.1.1 (2016-07-05)
------------------
- First py_zipkin version with context manager/decorator functionality.
