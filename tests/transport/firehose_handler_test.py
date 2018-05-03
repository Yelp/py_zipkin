# -*- coding: utf-8 -*-
import sys

import mock

from py_zipkin.transport.firehose_handler import FirehoseHandler


@mock.patch('py_zipkin.transport.firehose_handler.staticconf', autospec=True)
def test_firehose_disabled(staticconf):
    firehose_handler = FirehoseHandler()

    firehose_handler.zipkin_namespace.read_bool.return_value = False

    # by simply saying "not firehose_handler", we are calling __bool__
    # or __nonzero__
    assert not firehose_handler
    firehose_handler.zipkin_namespace.read_bool.assert_called_once_with(
        'enable_firehose',
        default=False,
    )


@mock.patch('py_zipkin.transport.firehose_handler.staticconf', autospec=True)
def test_firehose_enabled(staticconf):
    firehose_handler = FirehoseHandler()

    firehose_handler.zipkin_namespace.read_bool.return_value = True

    assert firehose_handler
    firehose_handler.zipkin_namespace.read_bool.assert_called_once_with(
        'enable_firehose',
        default=False,
    )


@mock.patch('py_zipkin.transport.firehose_handler.socket')
@mock.patch('py_zipkin.transport.firehose_handler.staticconf', autospec=True)
def test_firehose_send(staticconf, mocket):
    firehose_handler = FirehoseHandler()

    firehose_handler('sploosh')

    sock = mocket.socket.return_value
    sock.sendto.assert_called_once_with(
        'sploosh',
        (mock.ANY, mock.ANY),
    )


@mock.patch('py_zipkin.transport.firehose_handler.clog.log_line')
@mock.patch('py_zipkin.transport.firehose_handler.staticconf', autospec=True)
def test_firehose_send_with_huge_payload(staticconf, mock_clog):
    firehose_handler = FirehoseHandler()

    # this string is longer than the UDP max packet size, so python will
    # refuse to send it and throw a OSError
    firehose_handler(('a' * 100000).encode('ascii'))

    if sys.version_info >= (3, 0):
        err = "OSError(90, 'Message too long')"
    else:
        err = "error(90, 'Message too long')"

    assert mock_clog.call_args == mock.call(
        'tmp_zipkin_error',
        'yelp_pyramid FirehoseHandler error: {}'.format(err)
    )


@mock.patch('py_zipkin.transport.firehose_handler.clog.log_line')
@mock.patch('py_zipkin.transport.firehose_handler.staticconf', autospec=True)
def test_cover_magic_methods(staticconf, mock_clog):
    """This is simply a test to ensure 100% coverage of magic methods."""
    firehose_handler = FirehoseHandler()

    assert firehose_handler.__nonzero__() == firehose_handler.is_enabled()
    assert firehose_handler.__bool__() == firehose_handler.is_enabled()
