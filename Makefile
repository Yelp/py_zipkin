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

test:
	echo $(TOXENV)
	tox

tests: test

clean:
	@rm -rf .tox build dist *.egg-info
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

update-protobuf:
	$(MAKE) -C py_zipkin/encoding/protobuf update-protobuf

.PHONY: black
black:
	tox -e black
