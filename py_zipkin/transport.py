# -*- coding: utf-8 -*-


class BaseTransportHandler(object):

    def get_max_payload_bytes(self):  # pragma: no cover
        """Returns the maximum payload size for this transport.

        Most transports have a maximum packet size that can be sent. For example,
        UDP has a 65507 bytes MTU.
        py_zipkin automatically batches collected spans for performance reasons.
        The batch size is going to be the minimum between `get_max_payload_bytes`
        and `max_span_batch_size` from `zipkin_span`.

        If you don't want to enforce a max payload size, return None.

        :returns: max payload size in bytes or None.
        """
        raise NotImplementedError('get_max_payload_bytes is not implemented')

    def send(self, payload):  # pragma: no cover
        """Sends the encoded payload over the transport.

        :argument payload: encoded list of spans.
        """
        raise NotImplementedError('send is not implemented')

    def __call__(self, payload):
        """Internal wrapper around `send`. Do not override.

        Mostly used to keep backward compatibility with older transports
        implemented as functions. However decoupling the function developers
        override and what's internally called by py_zipkin will allow us to add
        extra logic here in the future without having the users update their
        code every time.
        """
        self.send(payload)
