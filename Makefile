.PHONY: all install test tests clean install-hooks

all: test

build:
	./setup.py bdist_egg

dev: clean
	./setup.py develop

install:
	pip install .

install-hooks:
	tox -e pre-commit -- install -f --install-hooks

test: install-hooks
	tox

.PHONY: tests
tests: test

.PHONY: docs
docs:
	tox -e docs

clean:
	@rm -rf .tox build dist *.egg-info
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	@rm -rf docs/build

update-protobuf:
	$(MAKE) -C py_zipkin/encoding/protobuf update-protobuf
