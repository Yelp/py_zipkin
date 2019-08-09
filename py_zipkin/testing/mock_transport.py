from py_zipkin.transport import BaseTransportHandler


class MockTransportHandler(BaseTransportHandler):
    """Mock transport for use in tests.

    It doesn't emit anything and just stores the generated spans in memory.
    To check what has been emitted you can use `get_payloads` and get back
    the list of encoded spans that were emitted.
    To use it:

    .. code-block:: python

        transport = MockTransportHandler()
        with zipkin.zipkin_span(
            service_name='test_service_name',
            span_name='test_span_name',
            transport_handler=transport,
            sample_rate=100.0,
            encoding=Encoding.V2_JSON,
        ):
            do_something()

        spans = transport.get_payloads()
        assert len(spans) == 1
        decoded_spans = json.loads(spans[0])
        assert decoded_spans == [{}]
    """

    def __init__(self, max_payload_bytes=None):
        """Creates a new MockTransportHandler.

        :param max_payload_bytes: max payload size in bytes. You often don't
            need to set this in tests unless you want to test what happens
            when your spans are bigger than the maximum payload size.
        :type max_payload_bytes: int
        """
        self.max_payload_bytes = max_payload_bytes
        self.payloads = []

    def send(self, payload):
        """Overrides the real send method. Should not be called directly."""
        self.payloads.append(payload)
        return payload

    def get_max_payload_bytes(self):
        """Overrides the real method. Should not be called directly."""
        return self.max_payload_bytes

    def get_payloads(self):
        """Returns the encoded spans that were sent.

        Spans are batched before being sent, so most of the time the returned
        list will contain only one element. Each element is gonna be an encoded
        list of spans.
        """
        return self.payloads
