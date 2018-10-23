# -*- coding: utf-8 -*-
import json
from threading import Thread

from py_zipkin import Encoding
from py_zipkin.zipkin import zipkin_span
from tests.conftest import MockTransportHandler


@zipkin_span(service_name='service1', span_name='service1_do_stuff')
def do_stuff():
    return 'OK'


def run_inside_another_thread(transport):
    """Run this function inside a different thread.

    :param transport: transport handler. We need to pass it in since any
        assertion we do inside this thread gets silently swallowed, so we
        need a way to return the results to the main thread.
    :type transport: MockTransportHandler
    """
    with zipkin_span(
        service_name='webapp',
        span_name='index',
        transport_handler=transport,
        sample_rate=100.0,
        encoding=Encoding.V2_JSON,
    ):
        do_stuff()


def test_decorator_works_in_a_new_thread():
    """The zipkin_span decorator is instanciated in a thread and then run in
    another. Let's verify that it works and that it stores the span in the
    right thread's thread-storage.
    """
    transport = MockTransportHandler()
    thread = Thread(target=run_inside_another_thread, args=(transport,))
    thread.start()
    thread.join()

    output = transport.get_payloads()
    assert len(output) == 1

    spans = json.loads(output[0])
    assert len(spans) == 2
    assert spans[0]['name'] == 'service1_do_stuff'
    assert spans[1]['name'] == 'index'
