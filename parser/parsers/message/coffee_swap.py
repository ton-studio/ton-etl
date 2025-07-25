import traceback
from model.parser import Parser, TOPIC_MESSAGES
from loguru import logger
from db import DB
from parsers.accounts.emulator import EmulatorParser
from pytoniq_core import Cell, Address, Slice
from model.dexswap import DEX_COFFEE, DexSwapParsed
from model.coffee import read_coffee_asset, write_coffee_asset
from parsers.message.swap_volume import estimate_volume


class CoffeeSwap(EmulatorParser):
    COFFEE_FACTORY_ADDRESS = Address('EQAsf2sDPfoo-0IjnRA7l_gJBB9jyo4zqfCG_1IFCCI_Qbef')

    def __init__(self, emulator_path):
        EmulatorParser.__init__(self, emulator_path)
        self.valid_pools = set()

    def prepare(self, db: DB):
        EmulatorParser.prepare(self, db)
        factory_state = Parser.get_account_state_safe(self.COFFEE_FACTORY_ADDRESS, db)
        self.factory = self._prepare_emulator(factory_state)
        
    
    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        # only external messages not blacklist
        return obj.get("opcode", None) == Parser.opcode_signed(0xc0ffee30) and \
            obj.get("direction", None) == "out" and \
            obj.get("destination", 'None') is None
    
    """
    We need to validate that the message produce by valid pool
    To check it we need to call get_pool_address on the factory address
    """
    def validate_pool(self, db: DB, asset1: Slice, asset2: Slice, amm: int, amm_settings: Cell, pool: Address): 
        if pool in self.valid_pools:
            return True
        pool_address, hash_part = self._execute_method(self.factory, 'get_pool_address', [asset1, asset2, amm, amm_settings], db, {})
        pool_address = pool_address.load_address()
        logger.info(f"Pool address: {pool_address}")
        if pool_address == pool:
            logger.info(f"Coffee pool validated: {pool}")
            self.valid_pools.add(pool_address)
            return True
        return False

    def handle_internal(self, obj, db: DB):
        cell = Parser.message_body(obj, db).begin_parse()
        op_id = cell.load_uint(32) # swap_successful_event#c0ffee30
        try:
            query_id = cell.load_uint(64)
            asset_in = read_coffee_asset(cell)
            amount_in = cell.load_coins()
            amount_out = cell.load_coins()
            reserve_in = cell.load_coins()
            reserve_out = cell.load_coins()
            protocol_fee = cell.load_coins()
            if amount_in == 0 or amount_out == 0:
                logger.info(f"Skipping zero amount swap for {obj}")
                return
            # get_pool_data
            pool_state = Parser.get_account_state_safe(Address(obj.get('source')), db)
            pool_emulator = self._prepare_emulator(pool_state)
            ver, asset_1, asset_2, amm, amm_settings, is_active, reserve_1, reserve_2, total_supply, protocol_fee, lp_fee = self._execute_method(pool_emulator, 'get_pool_data', [], db, {})

            if not self.validate_pool(db, asset_1, asset_2, amm, amm_settings, Address(obj.get('source', None))):
                logger.warning(f"Skipping invalid pool {obj.get('source', None)}")
                return

            parent_body = db.get_parent_message_body(obj.get('msg_hash'))
            if not parent_body:
                logger.warning(f"Unable to find parent message for {obj.get('msg_hash')}")
                return
            cell = Cell.one_from_boc(parent_body).begin_parse()
            op_id = cell.load_uint(32) # swap_internal#c0ffee20
            assert op_id == 0xc0ffee20, f"Parent message for Coffee swap_successful_event is {op_id}"
            parent_query_id = cell.load_uint(64)
            previous_amount = cell.load_coins()
            if cell.load_bit():
                asset_type = cell.load_uint(2)
                if asset_type == 1:
                    wc = cell.load_uint(8)
                    hash_part = cell.load_uint(256)
                if asset_type == 2:
                    asset_id = cell.load_uint(32)
            min_output_amount = cell.load_coins()
            next_body = cell.load_maybe_ref()
            swap_params = cell.load_ref().begin_parse()
            deadline = swap_params.load_uint(32)
            recipient = swap_params.load_address()
            referral = swap_params.load_address()

            asset_1_address = read_coffee_asset(asset_1)
            asset_2_address = read_coffee_asset(asset_2)
            if asset_in == asset_1_address:
                asset_out = asset_2_address
            elif asset_in == asset_2_address:
                asset_out = asset_1_address
            else:
                logger.warning(f"Asset in swap_successful_event message id={obj.get('msg_hash')} does not match the pool {obj.get('source')}")
                return
                
        except Exception as e:
            logger.warning(f"Failed to parse Coffee DEX swap (tx_hash = {obj.get('tx_hash')}): {e} {traceback.format_exc()}")
            return

        swap = DexSwapParsed(
            tx_hash=Parser.require(obj.get('tx_hash', None)),
            msg_hash=Parser.require(obj.get('msg_hash', None)),
            trace_id=Parser.require(obj.get('trace_id', None)),
            platform=DEX_COFFEE,
            swap_utime=Parser.require(obj.get('created_at', None)),
            swap_user=recipient,
            swap_pool=Parser.require(obj.get('source', None)),
            swap_src_token=asset_in,
            swap_dst_token=asset_out,
            swap_src_amount=amount_in,
            swap_dst_amount=amount_out,
            referral_address=referral,
            reserve0=reserve_1,
            reserve1=reserve_2,
            query_id=query_id,
            min_out=min_output_amount
        )
        estimate_volume(swap, db)
        db.serialize(swap)
        db.discover_dex_pool(swap)
