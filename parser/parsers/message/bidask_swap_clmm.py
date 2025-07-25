from model.parser import Parser, TOPIC_MESSAGES
from loguru import logger
from db import DB
from parsers.accounts.emulator import EmulatorParser
from pytoniq_core import Cell, Address
from model.dexswap import DEX_BIDASK_CLMM, DexSwapParsed
from model.dedust import read_dedust_asset, write_dedust_asset
from parsers.message.swap_volume import estimate_volume
from pytvm.tvm_emulator.tvm_emulator import TvmEmulator


class BidaskClmmSwap(EmulatorParser):
    BIDASK_FACTORY_ADDRESS = Address('EQAuBZGak9BdkxuCC9gWUsY4Em3jog94BI4eRzX-3_Bidask')

    def __init__(self, emulator_path):
        EmulatorParser.__init__(self, emulator_path)
        self.valid_pools = set()
    
    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        # only internal messages sent to the router
        return obj.get("opcode", None) == Parser.opcode_signed(0x520e4831) and \
            obj.get("direction", None) == "in"
    
    def validate_pool(self, db: DB, pool_addr: Address): 
        first_pool_tx = db.get_first_transaction(pool_addr.to_str(is_user_friendly=False))

        return Address(first_pool_tx.get('source', None)) == self.BIDASK_FACTORY_ADDRESS

    def prepare(self, db: DB):
        EmulatorParser.prepare(self, db)

    # do not need emulator for the current state 
    def handle_internal(self, obj, db: DB):
        tx_hash = Parser.require(obj.get('tx_hash', None))
        tx = Parser.require(db.is_tx_successful(tx_hash))
        if not tx:
            logger.info(f"Skipping failed tx for {tx_hash}")
            return
        if not self.validate_pool(db, Address(db.get('destination'), None)):
            logger.warning(f"Skipping invalid pool {obj.get('source', None)}")
            return

        pool_state = Parser.get_account_state_safe(obj.get("destination"), db)
        pool_emulator = self._prepare_emulator(pool_state)
        
        cell = Parser.message_body(obj, db).begin_parse()
        cell.load_uint(32) # 0x520e4831
        query_id = cell.load_uint(64)
        new_current_bin_number = cell.load_uint(32)
        new_sqrt_p = cell.load_uint(32)
        from_account = cell.load_bit()
        amount_x = cell.load_coins()
        amount_y = cell.load_coins()
        is_x = cell.load_bit()
        receiver_address = cell.load_address()

        logger.info(f"query_id: {query_id}, from_account: {from_account}, amount_x: {amount_x}, amount_y: {amount_y}, is_x: {is_x}, receiver_address: {receiver_address}, from_address: {from_address}")

        range_tx = obj
        for i in range(5): # max 5 reentrancies for swap
            try:
                range_swap = Cell.one_from_boc(db.get_parent_message_body(range_tx.get('msg_hash'))).begin_parse()
            except Exception as e:
                logger.info("parrent range swap not found", e)

            swap_op = range_swap.load_uint(32)
            if swap_op != 0x66210c65:
                logger.warning(f"Parent message for bidask clmm swap is {swap_op}, expected 0x66210c65 {tx_hash}")
                range_tx = db.get_parent_message_with_body(range_tx.get('msg_hash'))
                continue
            else:
                break

        input_query_id = range_swap.load_uint(64)
        input_user_address = range_swap.load_address()
        input_is_account = range_swap.load_bit()
        input_is_x = range_swap.load_bit()
        input_amount = range_swap.load_coins()
        input_out = range_swap.load_coins()

        logger.info(f"input_query_id: {input_query_id}, input_is_account: {input_is_account}, input_is_x: {input_is_x}, input_amount: {input_amount}, is_x: {is_x}, input_out: {input_out}")

        token_x, token_y, = self._execute_method(pool_emulator, 'get_pool_tokens', [], db, {})
        token_x_address = token_x.load_address()
        token_y_address = token_y.load_address()

        src_token = [token_x_address, token_y_address][is_x]
        dst_token = [token_y_address, token_x_address][is_x]
        output_amount = [amount_x, amount_y][is_x]

        src_token_master = db.get_wallet_master(src_token)
        dst_token_master = db.get_wallet_master(dst_token)

        reserve0, reserve1 = self._execute_method(pool_emulator, 'get_tvl', [], db, {})

        swap = DexSwapParsed(
            tx_hash=Parser.require(obj.get('tx_hash', None)),
            msg_hash=Parser.require(obj.get('msg_hash', None)),
            trace_id=Parser.require(obj.get('trace_id', None)),
            platform=DEX_BIDASK_CLMM,
            swap_utime=Parser.require(obj.get('created_at', None)),
            swap_user=input_user_address,
            swap_pool=Parser.require(obj.get('destination', None)),
            swap_src_token=src_token_master,
            swap_dst_token=dst_token_master,
            swap_src_amount=input_amount,
            swap_dst_amount=output_amount,
            referral_address=None,
            reserve0=reserve0,
            reserve1=reserve1
        )
        estimate_volume(swap, db)
        db.serialize(swap)
        db.discover_dex_pool(swap)

