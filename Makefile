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
	tox

tests: test

clean:
	@rm -rf .tox build dist *.egg-info
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

venv: setup.py requirements-dev.txt
	tox -e venv

update-protobuf:
	$(MAKE) -C py_zipkin/encoding/protobuf update-protobuf

.PHONY: black
black:
	tox -e black
