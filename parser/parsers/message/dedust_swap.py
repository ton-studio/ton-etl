from model.parser import Parser, TOPIC_MESSAGES
from loguru import logger
from db import DB
from parsers.accounts.emulator import EmulatorParser
from pytoniq_core import Cell, Address
from model.dexswap import DEX_DEDUST, DexSwapParsed
from model.dedust import read_dedust_asset, write_dedust_asset
from parsers.message.swap_volume import estimate_volume
from pytvm.tvm_emulator.tvm_emulator import TvmEmulator


# twin non-stable pools to avoid wrong prices estimation
# TODO - move to a special table?
BLACKLIST = set([
    Parser.uf2raw('EQA0a6c40n_Kejx_Wj0vowdeYCFYG9XnLdLMRHihXc27cng5'), 
    Parser.uf2raw('EQDpuDAY31FH2jM9PysFsmJ3aXMMReGYb_P65aDOXVYDcCJX')
])

DEDUST_FACTORY_ADDRESS = Address('EQBfBWT7X2BHg9tXAxzhz2aKiNTU1tpt5NsiK0uSDW_YAJ67')
    
class DedustSwap(EmulatorParser):

    def __init__(self, emulator_path):
        EmulatorParser.__init__(self, emulator_path)
        self.valid_pools = set()

    def prepare(self, db: DB):
        EmulatorParser.prepare(self, db)
        factory_state = Parser.get_account_state_safe(DEDUST_FACTORY_ADDRESS, db)
        self.factory = self._prepare_emulator(factory_state)
        
    
    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        # only external messages not blacklist
        return obj.get("opcode", None) == Parser.opcode_signed(0x9c610de3) and \
            obj.get("direction", None) == "out" and \
            obj.get("destination", 'None') is None and \
            not obj.get("source", None) in BLACKLIST
    
    """
    We need to validate that the message produce by valid pool
    To check it we need to call get_pool_address on the factory address
    see documentation for more details: https://docs.dedust.io/reference/factory
    """
    def validate_pool(self, db: DB, asset0:Address, asset1:Address, pool:str): 
        if pool in self.valid_pools:
            return True
        for pool_type in [0, 1]:
            pool_address, = self._execute_method(self.factory, 'get_pool_address', [pool_type, write_dedust_asset(asset0).begin_parse(), write_dedust_asset(asset1).begin_parse()], db, {})
            pool_address = pool_address.load_address()
            logger.info(f"Pool address for type {pool_type}: {pool_address}")
            if pool_address == pool:
                logger.info(f"DeDust pool validated: {pool}")
                self.valid_pools.add(pool_address)
                return True
        return False

    # do not need emulator for the current state 
    def handle_internal(self, obj, db: DB):
        cell = Parser.message_body(obj, db).begin_parse()
        op_id = cell.load_uint(32) # swap#9c610de3
        try:
            asset_in = read_dedust_asset(cell)
            asset_out = read_dedust_asset(cell)
            amount_in = cell.load_coins()
            amount_out = cell.load_coins()
            if amount_in == 0 or amount_out == 0:
                logger.info(f"Skipping zero amount swap for {obj}")
                return
            if not self.validate_pool(db, asset_in, asset_out, Address(obj.get('source', None))):
                logger.warning(f"Skipping invalid pool {obj.get('source', None)}")
                return
        except Exception as e:
            logger.warning(f"Failed to parse DeDust ext message: {e}")
            return
        payload = cell.load_ref().begin_parse()
        sender_addr = payload.load_address()
        referral_addr = payload.load_address()
        reserve0 = payload.load_coins()
        reserve1 = payload.load_coins()

        swap = DexSwapParsed(
            tx_hash=Parser.require(obj.get('tx_hash', None)),
            msg_hash=Parser.require(obj.get('msg_hash', None)),
            trace_id=Parser.require(obj.get('trace_id', None)),
            platform=DEX_DEDUST,
            swap_utime=Parser.require(obj.get('created_at', None)),
            swap_user=sender_addr,
            swap_pool=Parser.require(obj.get('source', None)),
            swap_src_token=asset_in,
            swap_dst_token=asset_out,
            swap_src_amount=amount_in,
            swap_dst_amount=amount_out,
            referral_address=referral_addr,
            reserve0=reserve0,
            reserve1=reserve1
        )
        estimate_volume(swap, db)
        db.serialize(swap)
        db.discover_dex_pool(swap)
