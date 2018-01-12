# -*- coding: utf-8 -*-
import random
import struct
import time


def generate_random_64bit_string():
    """Returns a 64 bit UTF-8 encoded string. In the interests of simplicity,
    this is always cast to a `str` instead of (in py2 land) a unicode string.
    Certain clients (I'm looking at you, Twisted) don't enjoy unicode headers.

    :returns: random 16-character string
    """
    return '{:016x}'.format(random.getrandbits(64))


def generate_random_128bit_string():
    """Returns a 128 bit UTF-8 encoded string. Follows the same conventions
    as generate_random_64bit_string().

    The upper 32 bits are the current time in epoch seconds, and the
    lower 96 bits are random. This allows for AWS X-Ray `interop
    <https://github.com/openzipkin/zipkin/issues/1754>`_

    :returns: 32-character hex string
    """
    t = int(time.time())
    lower_96 = random.getrandbits(96)
    return '{:032x}'.format((t << 96) | lower_96)


def unsigned_hex_to_signed_int(hex_string):
    """Converts a 64-bit hex string to a signed int value.

    This is due to the fact that Apache Thrift only has signed values.

    Examples:
        '17133d482ba4f605' => 1662740067609015813
        'b6dbb1c2b362bf51' => -5270423489115668655

    :param hex_string: the string representation of a zipkin ID
    :returns: signed int representation
    """
    return struct.unpack('q', struct.pack('Q', int(hex_string, 16)))[0]


def signed_int_to_unsigned_hex(signed_int):
    """Converts a signed int value to a 64-bit hex string.

    Examples:
        1662740067609015813  => '17133d482ba4f605'
        -5270423489115668655 => 'b6dbb1c2b362bf51'

    :param signed_int: an int to convert
    :returns: unsigned hex string
    """
    hex_string = hex(struct.unpack('Q', struct.pack('q', signed_int))[0])[2:]
    if hex_string.endswith('L'):
        return hex_string[:-1]
    return hex_string
