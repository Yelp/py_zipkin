# -*- coding: utf-8 -*-
import mock

from tests.test_helpers import MockTransportHandler


class TestBaseTransportHandler(object):
    def test_call_calls_send(self):
        with mock.patch.object(MockTransportHandler, 'send', autospec=True):
            transport = MockTransportHandler()
            transport("foobar")

            assert transport.send.call_count == 1
            assert transport.send.call_args == mock.call(transport, "foobar")
