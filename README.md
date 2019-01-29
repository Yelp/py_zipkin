[![Build Status](https://travis-ci.org/Yelp/py_zipkin.svg?branch=master)](https://travis-ci.org/Yelp/py_zipkin)
[![Coverage Status](https://img.shields.io/coveralls/Yelp/py_zipkin.svg)](https://coveralls.io/r/Yelp/py_zipkin)
[![Docs Status](https://readthedocs.org/projects/py-zipkin/badge/?version=latest)](https://py_zipkin.readthedocs.io)
[![PyPi version](https://img.shields.io/pypi/v/py_zipkin.svg)](https://pypi.python.org/pypi/py_zipkin/)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/py_zipkin.svg)](https://pypi.python.org/pypi/py_zipkin/)

# py_zipkin

py_zipkin provides a context manager/decorator along with some utilities to
facilitate the usage of Zipkin in Python applications.

## Install

```
pip install py-zipkin
```

## Documentation

Full documentation is available at https://py_zipkin.readthedocs.io.

## Developing

To run the tests after making changes run: `make test`.

To run the tests only against one python version use: `tox -e py37`.

### Docs

Docs are generated using [sphinx](https://www.sphinx-doc.org/en/master/index.html).
To regenerate them run: `make docs`. They’ll be automatically uploaded to
[readthedocs.io](https://py_zipkin.readthedocs.io) by travis once you release a new
version.

### Releasing a new version

First of all, bump the version in setup.py and update the CHANGELOG. We follow
[semver](https://semver.org/) versioning, so follow those rules in deciding the next
version.

Commit the changes and tag the commit as `vX.Y.Z`. Example:

```bash
git commit -m 'release version 1.0.0'
git tag v1.0.0
git push origin master --tags
```

Travis will automatically detect tagged commits, build the wheel and upload it to
[pypi](https://pypi.org/project/py-zipkin/).

License
-------

Copyright (c) 2018, Yelp, Inc. All Rights reserved. Apache v2
