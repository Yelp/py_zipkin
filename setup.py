#!/usr/bin/python
import os

from setuptools import find_packages
from setuptools import setup

__version__ = '1.0.0'


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()


setup(
    name='py_zipkin',
    version=__version__,
    provides=["py_zipkin"],
    author='Yelp, Inc.',
    author_email='opensource+py-zipkin@yelp.com',
    license='Copyright Yelp 2019',
    url="https://github.com/Yelp/py_zipkin",
    description='Library for using Zipkin in Python.',
    long_description='\n\n'.join((read('README.md'), read('CHANGELOG.rst'))),
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=('tests*', 'testing*', 'tools*')),
    package_data={'': ['*.thrift']},
    python_requires='>=3.5',
    install_requires=[
        'thriftpy2>=0.4.0',
    ],
    extras_require={
        'protobuf': 'protobuf >= 3.12.4',
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
)
