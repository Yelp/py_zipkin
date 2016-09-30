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
