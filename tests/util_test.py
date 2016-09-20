import sys

import mock

from py_zipkin import util


@mock.patch('py_zipkin.util.codecs.encode', autospec=True)
def test_generate_random_64bit_string(rand):
    rand.return_value = b'17133d482ba4f605'
    random_string = util.generate_random_64bit_string()
    assert random_string == '17133d482ba4f605'
    # This acts as a contract test of sorts. This should return a str
    # in both py2 and py3. IOW, no unicode objects.
    assert isinstance(random_string, str)


def test_unsigned_hex_to_signed_int():
    assert util.unsigned_hex_to_signed_int('17133d482ba4f605') == \
        1662740067609015813
    assert util.unsigned_hex_to_signed_int('b6dbb1c2b362bf51') == \
        -5270423489115668655


def test_signed_int_to_unsigned_hex():
    assert util.signed_int_to_unsigned_hex(1662740067609015813) == \
        '17133d482ba4f605'
    assert util.signed_int_to_unsigned_hex(-5270423489115668655) == \
        'b6dbb1c2b362bf51'

    if sys.version_info > (3,):
        with mock.patch('builtins.hex') as mock_hex:
            mock_hex.return_value = '0xb6dbb1c2b362bf51L'
            assert util.signed_int_to_unsigned_hex(-5270423489115668655) == \
                'b6dbb1c2b362bf51'
