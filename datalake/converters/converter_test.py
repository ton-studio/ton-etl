import pytest
import base64
import decimal
from datalake.converters.converter import Converter


def encode_large_number(number: int) -> str:
    if number == 0:
        return base64.b64encode(b"\x00").decode()

    num_bytes = (number.bit_length() + 7) // 8 or 1
    if number < 0:
        num_bytes += 1
    encoded_bytes = number.to_bytes(num_bytes, byteorder="big", signed=True)
    return base64.b64encode(encoded_bytes).decode()


class MockConverter(Converter):
    def __init__(self):
        pass


@pytest.fixture
def converter():
    return MockConverter()


@pytest.mark.parametrize(
    "number, scale, result",
    [
        (123000, 0, decimal.Decimal("123000")),
        (123000, 3, decimal.Decimal("123")),
        (0, 0, decimal.Decimal("0")),
        (-12300045, 0, decimal.Decimal("-12300045")),
        (-1230000, 6, decimal.Decimal("-1.23")),
        (1234567890123456789012345678901234567890, 0, decimal.Decimal("1234567890123456789012345678901234567890")),
        (-1234567890123456789012345678901234567890, 0, decimal.Decimal("-1234567890123456789012345678901234567890")),
    ],
)
def test_decode_numeric_synthetic_data(converter, number, scale, result):
    numeric = {
        "value": encode_large_number(number),
        "scale": scale,
    }
    assert converter.decode_numeric(numeric) == result


@pytest.mark.parametrize(
    "numeric, result",
    [
        ({"scale": 0, "value": "BXqG5+8="}, decimal.Decimal("23530498031")),
        ({"scale": 0, "value": "AA=="}, decimal.Decimal("0")),
        ({"scale": 0, "value": "/02p+A=="}, decimal.Decimal("-11687432")),
        ({"scale": 0, "value": "ANPCG87M7aD///8="}, decimal.Decimal("999999999999999999999999")),
        ({"scale": 0, "value": "/wl7IKk8H+QndVTIH4AA"}, decimal.Decimal("-5000000000000000005000000000000000")),
        ({"scale": 0, "value": "A6DJIHXA2/O4rLxfls4/CtI="}, decimal.Decimal("1234567890123456789012345678901234567890")),
    ],
)
def test_decode_numeric_real_data(converter, numeric, result):
    assert converter.decode_numeric(numeric) == result
