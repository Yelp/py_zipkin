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
