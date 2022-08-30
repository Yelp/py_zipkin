from typing import Optional
from typing import Tuple
from typing import Union
from urllib.request import Request
from urllib.request import urlopen

from py_zipkin.encoding import detect_span_version_and_encoding
from py_zipkin.encoding import Encoding


class BaseTransportHandler:
    def get_max_payload_bytes(self) -> Optional[int]:  # pragma: no cover
        """Returns the maximum payload size for this transport.

        Most transports have a maximum packet size that can be sent. For example,
        UDP has a 65507 bytes MTU.
        py_zipkin automatically batches collected spans for performance reasons.
        The batch size is going to be the minimum between `get_max_payload_bytes`
        and `max_span_batch_size` from `zipkin_span`.

        If you don't want to enforce a max payload size, return None.

        :returns: max payload size in bytes or None.
        """
        raise NotImplementedError("get_max_payload_bytes is not implemented")

    def send(self, payload: Union[bytes, str]) -> None:  # pragma: no cover
        """Sends the encoded payload over the transport.

        :argument payload: encoded list of spans.
        """
        raise NotImplementedError("send is not implemented")

    def __call__(self, payload: Union[bytes, str]) -> None:
        """Internal wrapper around `send`. Do not override.

        Mostly used to keep backward compatibility with older transports
        implemented as functions. However decoupling the function developers
        override and what's internally called by py_zipkin will allow us to add
        extra logic here in the future without having the users update their
        code every time.
        """
        self.send(payload)


class UnknownEncoding(Exception):
    """Exception class for when encountering an unknown Encoding"""


class SimpleHTTPTransport(BaseTransportHandler):
    def __init__(self, address: str, port: int) -> None:
        """A simple HTTP transport for zipkin.

        This is not production ready (not async, no retries) but
        it's helpful for tests or people trying out py-zipkin.

        .. code-block:: python

            with zipkin_span(
                service_name='my_service',
                span_name='home',
                sample_rate=100,
                transport_handler=SimpleHTTPTransport('localhost', 9411),
                encoding=Encoding.V2_JSON,
            ):
                pass

        :param address: zipkin server address.
        :type address: str
        :param port: zipkin server port.
        :type port: int
        """
        super().__init__()
        self.address = address
        self.port = port

    def get_max_payload_bytes(self) -> Optional[int]:
        return None

    def _get_path_content_type(self, payload: Union[str, bytes]) -> Tuple[str, str]:
        """Choose the right api path and content type depending on the encoding.

        This is not something you'd need to do generally when writing your own
        transport since in that case you'd know which encoding you're using.
        Since this is a generic transport, we need to make it compatible with
        any encoding instead.
        """
        encoded_payload = (
            payload.encode("utf-8") if isinstance(payload, str) else payload
        )
        encoding = detect_span_version_and_encoding(encoded_payload)

        if encoding == Encoding.V1_JSON:
            return "/api/v1/spans", "application/json"
        elif encoding == Encoding.V1_THRIFT:
            return "/api/v1/spans", "application/x-thrift"
        elif encoding == Encoding.V2_JSON:
            return "/api/v2/spans", "application/json"
        elif encoding == Encoding.V2_PROTO3:
            return "/api/v2/spans", "application/x-protobuf"
        else:  # pragma: nocover
            raise UnknownEncoding(f"Unknown encoding: {encoding}")

    def send(self, payload: Union[str, bytes]) -> None:
        encoded_payload = (
            payload.encode("utf-8") if isinstance(payload, str) else payload
        )
        path, content_type = self._get_path_content_type(encoded_payload)
        url = f"http://{self.address}:{self.port}{path}"

        req = Request(url, encoded_payload, {"Content-Type": content_type})
        response = urlopen(req)

        assert response.getcode() == 202
