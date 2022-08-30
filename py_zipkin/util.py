import random
import struct
import time
from typing import NamedTuple
from typing import Optional


class ZipkinAttrs(NamedTuple):
    """
    Holds the basic attributes needed to log a zipkin trace

    :param trace_id: Unique trace id
    :param span_id: Span Id of the current request span
    :param parent_span_id: Parent span Id of the current request span
    :param flags: stores flags header. Currently unused
    :param is_sampled: pre-computed bool whether the trace should be logged
    """

    trace_id: str
    span_id: Optional[str]
    parent_span_id: Optional[str]
    flags: str
    is_sampled: bool


def generate_random_64bit_string() -> str:
    """Returns a 64 bit UTF-8 encoded string. In the interests of simplicity,
    this is always cast to a `str` instead of (in py2 land) a unicode string.
    Certain clients (I'm looking at you, Twisted) don't enjoy unicode headers.

    :returns: random 16-character string
    """
    return f"{random.getrandbits(64):016x}"


def generate_random_128bit_string() -> str:
    """Returns a 128 bit UTF-8 encoded string. Follows the same conventions
    as generate_random_64bit_string().

    The upper 32 bits are the current time in epoch seconds, and the
    lower 96 bits are random. This allows for AWS X-Ray `interop
    <https://github.com/openzipkin/zipkin/issues/1754>`_

    :returns: 32-character hex string
    """
    t = int(time.time())
    lower_96 = random.getrandbits(96)
    return f"{(t << 96) | lower_96:032x}"


def unsigned_hex_to_signed_int(hex_string: str) -> int:
    """Converts a 64-bit hex string to a signed int value.

    This is due to the fact that Apache Thrift only has signed values.

    Examples:
        '17133d482ba4f605' => 1662740067609015813
        'b6dbb1c2b362bf51' => -5270423489115668655

    :param hex_string: the string representation of a zipkin ID
    :returns: signed int representation
    """
    return struct.unpack("q", struct.pack("Q", int(hex_string, 16)))[0]


def signed_int_to_unsigned_hex(signed_int: int) -> str:
    """Converts a signed int value to a 64-bit hex string.

    Examples:
        1662740067609015813  => '17133d482ba4f605'
        -5270423489115668655 => 'b6dbb1c2b362bf51'

    :param signed_int: an int to convert
    :returns: unsigned hex string
    """
    hex_string = hex(struct.unpack("Q", struct.pack("q", signed_int))[0])[2:]
    if hex_string.endswith("L"):
        return hex_string[:-1]
    return hex_string


def _should_sample(sample_rate: float) -> bool:
    if sample_rate == 0.0:
        return False  # save a die roll
    elif sample_rate == 100.0:
        return True  # ditto
    return (random.random() * 100) < sample_rate


def create_attrs_for_span(
    sample_rate: float = 100.0,
    trace_id: Optional[str] = None,
    span_id: Optional[str] = None,
    use_128bit_trace_id: bool = False,
    flags: Optional[str] = None,
) -> ZipkinAttrs:
    """Creates a set of zipkin attributes for a span.

    :param sample_rate: Float between 0.0 and 100.0 to determine sampling rate
    :type sample_rate: float
    :param trace_id: Optional 16-character hex string representing a trace_id.
                    If this is None, a random trace_id will be generated.
    :type trace_id: str
    :param span_id: Optional 16-character hex string representing a span_id.
                    If this is None, a random span_id will be generated.
    :type span_id: str
    :param use_128bit_trace_id: If true, generate 128-bit trace_ids
    :type use_128bit_trace_id: bool
    """
    # Calculate if this trace is sampled based on the sample rate
    if trace_id is None:
        if use_128bit_trace_id:
            trace_id = generate_random_128bit_string()
        else:
            trace_id = generate_random_64bit_string()
    if span_id is None:
        span_id = generate_random_64bit_string()
    is_sampled = _should_sample(sample_rate)

    return ZipkinAttrs(
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=None,
        flags=flags or "0",
        is_sampled=is_sampled,
    )
