import copy
import time
from typing import Dict
from loguru import logger
from db import DB
from pytoniq_core import Address
from model.dexpool import DexPool
from model.dexswap import DEX_DEDUST, DEX_MEGATON, DEX_STON, DEX_STON_V2, DEX_TONCO, DEX_COFFEE, DEX_BIDASK_CLMM, DEX_BIDASK_DAMM, DEX_MOON
from model.dedust import read_dedust_asset
from model.coffee import read_coffee_asset
from parsers.message.swap_volume import estimate_tvl
from pytvm.tvm_emulator.tvm_emulator import TvmEmulator
from parsers.accounts.emulator import EmulatorException, EmulatorParser


TON = Address("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c")

"""
Listens to updates on DEX pools, exrtacts reserves and total_supply
and estimates TVL.
"""
class TVLPoolStateParser(EmulatorParser):
    def __init__(self, emulator_path, update_interval=3600):
        super().__init__(emulator_path)
        self.last_updated = int(time.time())
        # update intervals for pools
        self.update_interval = update_interval
        self.pools: Dict[str, DexPool] = {}

    def prepare(self, db: DB):
        super().prepare(db)
        self.pools = db.get_all_dex_pools()
        logger.info(f"Found {len(self.pools)} unique dex pools to handle")

    def predicate(self, obj) -> bool:
        if super().predicate(obj):
            return obj['account'] in self.pools
        return False
    
    def _do_parse(self, obj, db: DB, emulator: TvmEmulator):
        # TODO refresh pool data every update_interval
        pool = self.pools[obj['account']]
        pool.last_updated = obj['timestamp']

        # total supply is required for all cases except TONCO, Bidask DLMM
        if pool.platform not in [DEX_TONCO, DEX_BIDASK_CLMM]:
            try:
                pool.total_supply, _, _, _, _= self._execute_method(emulator, 'get_jetton_data', [], db, obj)
            except EmulatorException as e:
                """
                Ston.fi has a bug with get_jetton_data method failures when address is starting with 
                a leading zero. (details are here https://github.com/ston-fi/dex-core/pull/2/files)
                To avoid loosing data, we will retry the method call with an address without leading zero.
                """
                if pool.platform == DEX_STON and 'terminating vm with exit code 9' in e.args[0]:
                    # it is better to make a copy to avoid any issues with the original object
                    obj_fixed = copy.deepcopy(obj)
                    obj_fixed['account'] = obj['account'].replace("0:0", "0:1")
                    logger.warning(f"Retrying get_jetton_data with fixed address: {obj_fixed['account']}")
                    emulator_fixed = self._prepare_emulator(obj_fixed)
                    pool.total_supply, _, _, _, _= self._execute_method(emulator_fixed, 'get_jetton_data', [], db, obj_fixed)
                else:
                    raise e

        if pool.platform == DEX_STON or pool.platform == DEX_STON_V2:
            if pool.platform == DEX_STON:
                pool.reserves_left, pool.reserves_right, wallet0_address, wallet1_address, lp_fee, protocol_fee, ref_fee, _, _, _ = self._execute_method(emulator, 'get_pool_data', [], db, obj)
            else:
                # ston.fi V2, some pools have 13 results, some have 12
                _, _, _, pool.reserves_left, pool.reserves_right, wallet0_address, wallet1_address, lp_fee, protocol_fee, _, _, _ = \
                    self._execute_method(emulator, 'get_pool_data', [], db, obj)[0:12]
                ref_fee = None # ref fee is not a part of pool contract, it could be specified on each trade
            pool.lp_fee = lp_fee / 1e4 if lp_fee is not None else None
            pool.protocol_fee = protocol_fee / 1e4 if protocol_fee is not None else None
            pool.referral_fee = ref_fee / 1e4 if ref_fee is not None else None

            # logger.info(f"STON pool data: {pool.reserves_left}, {pool.reserves_right}")
            wallet0_address = wallet0_address.load_address() # jetton wallet address
            wallet1_address = wallet1_address.load_address()

            token0_address = db.get_wallet_master(wallet0_address)
            token1_address = db.get_wallet_master(wallet1_address)
            if token0_address is None:
                logger.warning(f"Jetton wallet {wallet0_address} not found in DB")
                return
            if token1_address is None:
                logger.warning(f"Jetton wallet {wallet1_address} not found in DB")
                return
            current_jetton_left = Address(token0_address)
            current_jetton_right = Address(token1_address)
        elif pool.platform == DEX_DEDUST:
            pool.reserves_left, pool.reserves_right = self._execute_method(emulator, 'get_reserves', [], db, obj)
            # logger.info(f"DeDust pool data: {pool.reserves_left}, {pool.reserves_right}")

            trade_fee_numerator, trade_fee_denominator = self._execute_method(emulator, 'get_trade_fee', [], db, obj)
            if trade_fee_denominator > 0 and trade_fee_numerator is not None:
                total_fee = trade_fee_numerator / trade_fee_denominator
                # https://help.dedust.io/dedust/welcome-to-dedust.io/using-dedust.io/fees
                # 80% of fee goes to LP, 20% to the protocol
                pool.lp_fee = total_fee * 0.8
                pool.protocol_fee = total_fee * 0.2

            if not pool.is_inited():
                wallet0_address, wallet1_address = self._execute_method(emulator, 'get_assets', [], db, obj)
                # TODO  - stable pools flag?
                current_jetton_left = read_dedust_asset(wallet0_address)
                current_jetton_right = read_dedust_asset(wallet1_address)
        elif pool.platform == DEX_MEGATON:
            swap_fee, _, _, jetton_a_address, _, pool.reserves_left, _, jetton_b_address, _, pool.reserves_right, _ = self._execute_method(emulator, 'get_lp_swap_data', [], db, obj)
            current_jetton_left = jetton_a_address.load_address()
            current_jetton_right = jetton_b_address.load_address()
            pool.lp_fee = swap_fee / 1e4 if swap_fee is not None else None
        elif pool.platform == DEX_TONCO:
            _router, _admin, _admin2, j0_wallet, j1_wallet, j0_master, j1_master, _, _, fee_base, fee_protocol, fee_user, _, \
                price, liq, _, _, _, _, _, pool.reserves_left, pool.reserves_right, nftv3items_active, _, _ = self._execute_method(emulator, 'getPoolStateAndConfiguration', [], db, obj)
            # total supply is not applicable for TONCO, because LP is not a jetton. But we can use number of active NFT positions as a proxy
            pool.total_supply = nftv3items_active
            current_jetton_left = j0_master.load_address()
            current_jetton_right = j1_master.load_address()
            if fee_user is not None:
                base_fee = fee_user / 1e4
                protocol_share = fee_protocol / 1e4
                pool.lp_fee = base_fee * (1 - protocol_share)
                pool.protocol_fee = base_fee * protocol_share
        elif pool.platform == DEX_COFFEE:
            ver, asset_1, asset_2, amm, amm_settings, is_active, pool.reserves_left, pool.reserves_right, total_supply, protocol_fee, lp_fee = self._execute_method(emulator, 'get_pool_data', [], db, obj)
            pool.protocol_fee = protocol_fee / 1e4 if protocol_fee is not None else None
            pool.lp_fee = lp_fee / 1e4 if lp_fee is not None else None
            if not pool.is_inited():
                current_jetton_left = read_coffee_asset(asset_1)
                current_jetton_right = read_coffee_asset(asset_2)
        elif pool.platform == DEX_BIDASK_CLMM:
            pool.reserves_left, pool.reserves_right = self._execute_method(emulator, 'get_tvl', [], db, obj)
            pool_fees = self._execute_method(emulator, 'get_fees_info', [], db, obj)
            pool_info = self._execute_method(emulator, 'get_pool_info', [], db, obj)
            
            j0_wallet, j1_wallet, bin_step, base_fee = pool_info

            lp_fee = base_fee

            if len(pool_fees) == 2:
                ref_fee, protocol_fee_reduction_factor = pool_fees
                protocol_fee = lp_fee / protocol_fee_reduction_factor
            elif len(pool_fees) == 3:
                ref_fee, protocol_fee, _ = pool_fees
            else:
                ref_fee, protocol_fee = 0, 0
            
            # Null addr for pools with native TON and jettons without master contract.
            j0_wallet_address = j0_wallet.load_address()
            if j0_wallet_address == TON:
                j0_master = TON
            else:
                j0_master = Address(db.get_wallet_master(j0_wallet_address))

            j1_wallet_address = j1_wallet.load_address()
            if j1_wallet_address == TON:
                j1_master = TON
            else:
                j1_master = Address(db.get_wallet_master(j1_wallet_address))
                
            # total supply is not applicable for Bidask CLMM
            pool.total_supply = None
            current_jetton_left = j0_master
            current_jetton_right = j1_master
            
            pool.lp_fee = lp_fee / 1e4 if lp_fee is not None else None
            pool.protocol_fee = protocol_fee / 1e4 if protocol_fee is not None else None
            pool.referral_fee = ref_fee / 1e4 if ref_fee is not None else None
        elif pool.platform == DEX_BIDASK_DAMM:
            pool.reserves_left, pool.reserves_right = self._execute_method(emulator, 'get_tvl', [], db, obj)
            pool_fees = self._execute_method(emulator, 'get_fees_info', [], db, obj)
            dynamic_fee, dynamic_fee_factor, previous_time, time_filter, time_decay, protocol_fee_reduction_factor = pool_fees
            pool_info = self._execute_method(emulator, 'get_pool_info', [], db, obj)
            
            j0_wallet, j1_wallet, base_fee = pool_info

            current_dynamic_fee = self._execute_method(emulator, 'get_dynamic_fee_by_timestamp', [obj['timestamp']], db, obj)
            lp_fee = current_dynamic_fee[0]
            protocol_fee = lp_fee / protocol_fee_reduction_factor
            # Null addr for pools with native TON and jettons without master contract.
            j0_wallet_address = j0_wallet.load_address()
            if j0_wallet_address == TON:
                j0_master = TON
            else:
                j0_master = Address(db.get_wallet_master(j0_wallet_address))

            j1_wallet_address = j1_wallet.load_address()
            if j1_wallet_address == TON:
                j1_master = TON
            else:
                j1_master = Address(db.get_wallet_master(j1_wallet_address))
                
            # total supply is not applicable for Bidask CLMM
            current_jetton_left = j0_master
            current_jetton_right = j1_master
            
            pool.lp_fee = lp_fee / 1e4 if lp_fee is not None else None
            pool.protocol_fee = protocol_fee / 1e4 if protocol_fee is not None else None
            pool.referral_fee = None
        elif pool.platform == DEX_MOON:
            asset_id1, pool.reserves_left, asset_id2, pool.reserves_right = self._execute_method(emulator, 'get_reserves', [], db, obj)
            lp_fee, protocol_fee, ref_fee = self._execute_method(emulator, 'get_fees', [], db, obj)
            current_jetton_left = asset_id1.load_address()
            if not current_jetton_left:
                current_jetton_left = TON
            current_jetton_right = asset_id2.load_address()
            if not current_jetton_right:
                current_jetton_right = TON
            pool.lp_fee = lp_fee / 1e4 if lp_fee is not None else None
            pool.protocol_fee = protocol_fee / 1e4 if protocol_fee is not None else None
            pool.referral_fee = ref_fee / 1e4 if ref_fee is not None else None
        else:
            raise Exception(f"DEX is not supported: {pool.platform}")
        
        if not pool.is_inited():
            pool.jetton_left = current_jetton_left
            pool.jetton_right = current_jetton_right
            logger.info(f"Discovered jettons for {pool.pool}: {pool.jetton_left}, {pool.jetton_right}")
            db.update_dex_pool_jettons(pool)
        estimate_tvl(pool, db)
        logger.info(pool)
        db.update_dex_pool_state(pool)

        if int(time.time()) > self.last_updated + self.update_interval:
            logger.info("Updating dex pools")
            prev_len = len(self.pools)
            self.pools.clear()
            del self.pools
            import gc
            gc.collect()
            self.pools = db.get_all_dex_pools()
            logger.info(f"Found {len(self.pools) - prev_len} new dex pools to handle")
            self.last_updated = int(time.time())
