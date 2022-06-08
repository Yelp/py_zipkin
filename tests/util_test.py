from unittest import mock

from py_zipkin import util
from py_zipkin.util import ZipkinAttrs


@mock.patch("py_zipkin.util.random.getrandbits", autospec=True)
def test_generate_random_64bit_string(rand):
    rand.return_value = 0x17133D482BA4F605
    random_string = util.generate_random_64bit_string()
    assert random_string == "17133d482ba4f605"
    # This acts as a contract test of sorts. This should return a str
    # in both py2 and py3. IOW, no unicode objects.
    assert isinstance(random_string, str)


@mock.patch("py_zipkin.util.time.time", autospec=True)
@mock.patch("py_zipkin.util.random.getrandbits", autospec=True)
def test_generate_random_128bit_string(rand, mock_time):
    rand.return_value = 0x2BA4F60517133D482BA4F605
    mock_time.return_value = float(0x17133D48)
    random_string = util.generate_random_128bit_string()
    assert random_string == "17133d482ba4f60517133d482ba4f605"
    rand.assert_called_once_with(96)  # 96 bits
    # This acts as a contract test of sorts. This should return a str
    # in both py2 and py3. IOW, no unicode objects.
    assert isinstance(random_string, str)


def test_unsigned_hex_to_signed_int():
    assert util.unsigned_hex_to_signed_int("17133d482ba4f605") == 1662740067609015813
    assert util.unsigned_hex_to_signed_int("b6dbb1c2b362bf51") == -5270423489115668655


def test_signed_int_to_unsigned_hex():
    assert util.signed_int_to_unsigned_hex(1662740067609015813) == "17133d482ba4f605"
    assert util.signed_int_to_unsigned_hex(-5270423489115668655) == "b6dbb1c2b362bf51"

    with mock.patch("builtins.hex") as mock_hex:
        mock_hex.return_value = "0xb6dbb1c2b362bf51L"
        assert (
            util.signed_int_to_unsigned_hex(-5270423489115668655) == "b6dbb1c2b362bf51"
        )


@mock.patch("py_zipkin.util.generate_random_128bit_string", autospec=True)
@mock.patch("py_zipkin.util.generate_random_64bit_string", autospec=True)
def test_create_attrs_for_span(random_64bit_mock, random_128bit_mock):
    random_64bit_mock.return_value = "0000000000000042"
    expected_attrs = ZipkinAttrs(
        trace_id="0000000000000042",
        span_id="0000000000000042",
        parent_span_id=None,
        flags="0",
        is_sampled=True,
    )
    assert expected_attrs == util.create_attrs_for_span()

    # Test overrides
    expected_attrs = ZipkinAttrs(
        trace_id="0000000000000045",
        span_id="0000000000000046",
        parent_span_id=None,
        flags="0",
        is_sampled=False,
    )
    assert expected_attrs == util.create_attrs_for_span(
        sample_rate=0.0,
        trace_id="0000000000000045",
        span_id="0000000000000046",
    )

    random_128bit_mock.return_value = "00000000000000420000000000000042"
    expected_attrs = ZipkinAttrs(
        trace_id="00000000000000420000000000000042",
        span_id="0000000000000042",
        parent_span_id=None,
        flags="0",
        is_sampled=True,
    )
    assert expected_attrs == util.create_attrs_for_span(use_128bit_trace_id=True)
