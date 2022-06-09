import json
import queue
import random
import threading
import time
from operator import itemgetter

from py_zipkin import Encoding
from py_zipkin.instrumentations import python_threads
from py_zipkin.zipkin import zipkin_span
from tests.test_helpers import MockTransportHandler
from tests.test_helpers import TracingThread


@zipkin_span(service_name="service1", span_name="service1_do_stuff")
def do_stuff():
    return "OK"


def run_inside_another_thread(transport):
    """Run this function inside a different thread.

    :param transport: transport handler. We need to pass it in since any
        assertion we do inside this thread gets silently swallowed, so we
        need a way to return the results to the main thread.
    :type transport: MockTransportHandler
    """
    with zipkin_span(
        service_name="webapp",
        span_name="index",
        transport_handler=transport,
        sample_rate=100.0,
        encoding=Encoding.V2_JSON,
    ):
        do_stuff()


def test_decorator_works_in_a_new_thread():
    """The zipkin_span decorator is instantiated in a thread and then run in
    another. Let's verify that it works and that it stores the span in the
    right thread's thread-storage.
    """
    transport = MockTransportHandler()
    thread = threading.Thread(target=run_inside_another_thread, args=(transport,))
    thread.start()
    thread.join()

    output = transport.get_payloads()
    assert len(output) == 1

    spans = json.loads(output[0])
    assert len(spans) == 2
    assert spans[0]["name"] == "service1_do_stuff"
    assert spans[1]["name"] == "index"


def _do_one_little_request(output_q, my_input):
    with zipkin_span(
        service_name="_one_little_request",
        span_name="do-the-thing",
        binary_annotations={"input.was": my_input},
    ) as span_ctx:
        time.sleep(random.random())
        span_ctx.add_annotation("time-" + my_input)
        output_q.put(my_input + "-output")


def _do_test_concurrent_subrequests_in_threads(thread_class):
    """What if a thread has a span context, then fires off N threads to do N
    subrequests in parallel?

    Surely the spans of the subrequests should be siblings whose parentID is
    equal to the first thread's span context's spanID.

    Furthermore, a final sub-span created in the first thread should be a child
    of that thread (no leaking of span stack from the child threads).
    """
    transport = MockTransportHandler()
    with zipkin_span(
        service_name="main_thread",
        span_name="handle_one_big_request",
        transport_handler=transport,
        sample_rate=100.0,
        encoding=Encoding.V2_JSON,
        use_128bit_trace_id=True,
    ) as span_ctx:
        assert True is span_ctx._is_local_root_span
        assert span_ctx.logging_context
        expected_trace_id = span_ctx.zipkin_attrs.trace_id
        expected_parent_id = span_ctx.zipkin_attrs.span_id

        # Now do three subrequests
        req_count = 3
        threads = []
        output_q = queue.Queue()
        for thread_idx in range(req_count):
            this_thread = thread_class(
                target=_do_one_little_request, args=(output_q, "input-%d" % thread_idx)
            )
            threads.append(this_thread)
            this_thread.start()
        outputs = set()
        for thread in threads:
            thread.join()
            outputs.add(output_q.get())
        assert {"input-0-output", "input-1-output", "input-2-output"} == outputs

    output = transport.get_payloads()
    assert len(output) == 1

    spans = sorted(json.loads(output[0]), key=itemgetter("timestamp"))
    assert len(spans) == 4
    parent_span = spans[0]
    subrequest_spans = spans[1:]
    assert len(subrequest_spans) == 3
    assert parent_span["name"] == "handle_one_big_request"
    for span in subrequest_spans:
        assert "do-the-thing" == span["name"]
        assert span["tags"]["input.was"] in ("input-0", "input-1", "input-2")
        assert expected_trace_id == span["traceId"]
        # Perhaps most importantly, all the subrequest spans should share the same
        # parentId, which is the main thread's span's 'id'
        assert expected_parent_id == span["parentId"]


def test_concurrent_subrequests_in_threads_TracingThread():
    _do_test_concurrent_subrequests_in_threads(TracingThread)


def test_concurrent_subrequests_in_threads_monkey_patched_threading():
    try:
        python_threads.patch_threading()
        _do_test_concurrent_subrequests_in_threads(python_threads.threading.Thread)
    finally:
        python_threads.unpatch_threading()
