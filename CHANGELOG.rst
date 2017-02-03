0.5.0 (2017-02-01)
------------------
- Properly set timestamp/duration on server and local spans
- Updated thrift spec to include these new fields

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
