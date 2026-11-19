import pytest
from pytoniq_core import Address, Cell, begin_cell

from parser.model.dedust import read_dedust_asset, write_dedust_asset
from parser.model.coffee import read_coffee_asset, write_coffee_asset


TON = Address("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c")
USDT = Address("EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs")
JURY = Address("-1:BF7E01EA11CCC2B302E99C112822F832AEE254BE7FD00EDE5046379C63CC3796")
DEDUST_JURY_ASSET = "b5ee9c720101010100240000431ffbf7e01ea11ccc2b302e99c112822f832aee254be7fd00ede5046379c63cc37968"


def test_dedust_reads_masterchain_jetton():
    asset = read_dedust_asset(Cell.one_from_boc(DEDUST_JURY_ASSET).begin_parse())
    assert asset.wc == -1
    assert asset == JURY
    assert asset.to_str(is_user_friendly=False).upper() == "-1:BF7E01EA11CCC2B302E99C112822F832AEE254BE7FD00EDE5046379C63CC3796"


def test_dedust_writes_what_the_pool_returns():
    assert write_dedust_asset(JURY).hash == Cell.one_from_boc(DEDUST_JURY_ASSET).hash


@pytest.mark.parametrize("address", [TON, USDT, JURY])
def test_dedust_round_trip(address):
    assert read_dedust_asset(write_dedust_asset(address).begin_parse()) == address


def test_coffee_reads_masterchain_jetton():
    cell = begin_cell().store_uint(1, 2).store_uint(0xFF, 8).store_bytes(JURY.hash_part).end_cell()
    asset = read_coffee_asset(cell.begin_parse())
    assert asset.wc == -1
    assert asset == JURY


@pytest.mark.parametrize("address", [TON, USDT, JURY])
def test_coffee_round_trip(address):
    assert read_coffee_asset(write_coffee_asset(address).begin_parse()) == address
