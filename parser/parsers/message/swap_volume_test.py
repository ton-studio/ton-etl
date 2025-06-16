import importlib
import pytest
from unittest.mock import Mock
from pytoniq_core import Address

from parser.parsers.message.swap_volume import estimate_tvl
from parser.model.dexpool import DexPool


TON = Address("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c")
USDT = Address("EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs")
oUSDT = Address("EQC_1YoM8RBixN95lz7odcF3Vrkc_N8Ne7gQi7Abtlet_Efi")
tsTON = Address("EQC98_qAmNEptUtPc7W6xdHh_ZHrBUFpw5Ft_IzNU20QAJav")
NOT = Address("EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT")
STORM = Address("EQBsosmcZrD6FHijA7qWGLw5wo_aH8UN435hi935jJ_STORM")

@pytest.mark.parametrize(
    "jetton_left, jetton_right, reserves_left, reserves_right, last_updated, "
    "usdt_core_price, tston_core_price, not_agg_price, storm_agg_price, "
    "res_tvl_usd, res_tvl_ton, res_is_liquid",
    [
        # TONS-STABLES pool, usual case
        (TON, USDT, 2e15, 1.1e13, 1738000000, 0.005, 1.05, 0.001, 0.005, 2.1e7, 4.2e6, True),
        # TONS-STABLES pool, no TON price found
        (TON, USDT, 2e15, 1.1e13, 1738000000, None, 1.05, 0.001, 0.005, None, None, True),
        # TONS-STABLES pool, TON price = 0
        (TON, USDT, 2e15, 1.1e13, 1738000000, 0, 1.05, 0.001, 0.005, None, None, True),
        # TONS-ORBIT_STABLES pool, time after ORBIT_HACK_TIMESTAMP
        (TON, oUSDT, 2e13, 1.1e11, 1738000000, 0.005, 1.05, 0.001, 0.005, 2e5, 4e4, True),
        # TONS-ORBIT_STABLES pool, time befor ORBIT_HACK_TIMESTAMP
        (TON, oUSDT, 2e13, 1.1e11, 1703900000, 0.005, 1.05, 0.001, 0.005, 2.1e5, 4.2e4, True),
        # LSDS-STABLES pool, usual case
        (tsTON, USDT, 2e15, 1e13, 1738000000, 0.005, 1.05, 0.001, 0.005, 2.05e7, 4.1e6, True),
        # LSDS-STABLES pool, no price for LSD
        (tsTON, USDT, 2e15, 1e13, 1738000000, 0.005, None, 0.001, 0.005, None, None, True),
        # TONS-JETTONS pool, usual case
        (TON, NOT, 1e12, 1.1e15, 1738000000, 0.005, 1.05, 0.001, 0.005, 1e4, 2e3, True),
        # JETTONS-TONS pool, usual case
        (NOT, TON, 1.1e15, 1e12, 1738000000, 0.005, 1.05, 0.001, 0.005, 1e4, 2e3, True),
        # JETTONS-JETTONS pool, usual case
        (NOT, STORM, 5e15, 1.1e15, 1738000000, 0.005, 1.05, 0.001, 0.005, None, None, False),
        # ORBIT_STABLES-JETTONS pool, time after ORBIT_HACK_TIMESTAMP
        (oUSDT, NOT, 1e10, 1.1e10, 1738000000, 0.005, 1.05, 0.001, 0.005, None, None, False),
        # ORBIT_STABLES-JETTONS pool, time befor ORBIT_HACK_TIMESTAMP
        (oUSDT, NOT, 1e10, 1.1e10, 1703900000, 0.005, 1.05, 0.001, 0.005, 2e4, 4e3, True),
    ],
)
def test_estimate_tvl(
    jetton_left,
    jetton_right,
    reserves_left,
    reserves_right,
    last_updated,
    usdt_core_price,
    tston_core_price,
    not_agg_price,
    storm_agg_price,
    res_tvl_usd,
    res_tvl_ton,
    res_is_liquid,
):
    import parser.parsers.message.swap_volume

    importlib.reload(parser.parsers.message.swap_volume)

    core_price_mapping = {
        (USDT.to_str(False).upper(), last_updated): usdt_core_price,
        (tsTON.to_str(False).upper(), last_updated): tston_core_price,
    }
    agg_price_mapping = {
        (NOT.to_str(False).upper(), last_updated): not_agg_price,
        (STORM.to_str(False).upper(), last_updated): storm_agg_price,
    }
    db = Mock()
    db.get_core_price.side_effect = lambda jetton, last_updated: core_price_mapping.get((jetton, last_updated), None)
    db.get_agg_price.side_effect = lambda jetton, last_updated: agg_price_mapping.get((jetton, last_updated), None)

    pool = DexPool(
        pool="some_pool_address",
        platform="some_platform",
        jetton_left=jetton_left,
        jetton_right=jetton_right,
        reserves_left=reserves_left,
        reserves_right=reserves_right,
        last_updated=last_updated,
        tvl_usd=None,
        tvl_ton=None,
        is_liquid=True,
    )

    estimate_tvl(pool, db)

    assert pool.tvl_usd == res_tvl_usd
    assert pool.tvl_ton == res_tvl_ton
    assert pool.is_liquid == res_is_liquid
