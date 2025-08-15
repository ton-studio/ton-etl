from model.parser import Parser, TOPIC_MESSAGES
from loguru import logger
from db import DB
from parsers.accounts.emulator import EmulatorParser
from pytoniq_core import Cell, Address
from model.dexswap import DEX_BIDASK_CLMM, DexSwapParsed
from model.dedust import read_dedust_asset, write_dedust_asset
from parsers.message.swap_volume import estimate_volume
from pytvm.tvm_emulator.tvm_emulator import TvmEmulator
import base64

class BidaskClmmSwap(EmulatorParser):
    BIDASK_POOLS_CODE_HASHES = [
        "v/KqDp40glzQHQgUaETayPSq7EEnLPwS5CykwUgShcE="
        ]

    def __init__(self, emulator_path):
        EmulatorParser.__init__(self, emulator_path)
        self.valid_pools = set()
    
    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        # only internal messages sent to the router
        return obj.get("opcode", None) in [Parser.opcode_signed(0x520e4831), Parser.opcode_signed(0xd3a25890)] and \
            obj.get("direction", None) == "in"
    
    def validate_pool(self, db: DB, pool_state): 
        try:
            code_boc_str = pool_state["code_boc"]
            code_cell_hash = base64.b64encode(Cell.one_from_boc(code_boc_str)._hash).decode('utf-8')
            return code_cell_hash in self.BIDASK_POOLS_CODE_HASHES
        except Exception as e:
            logger.info("bidask pool state not found", e)

        return False

    def prepare(self, db: DB):
        EmulatorParser.prepare(self, db)

    # do not need emulator for the current state 
    def handle_internal(self, obj, db: DB):
        tx_hash = Parser.require(obj.get('tx_hash', None))
        tx = Parser.require(db.is_tx_successful(tx_hash))
        if not tx:
            logger.info(f"Skipping failed tx for {tx_hash}")
            return
        
        pool_state = Parser.get_account_state_safe(Address(obj.get("destination")), db)

        if not self.validate_pool(db, pool_state):
            logger.warning(f"Skipping invalid pool {obj.get('destination', None)}")
            return

        pool_emulator = self._prepare_emulator(pool_state)

        cell = Parser.message_body(obj, db).begin_parse()
        op = cell.load_uint(32) # 0x520e4831 pool:swap_success_callback

        if op == 0x520e4831: # pool:swap_success_callback
            query_id = cell.load_uint(64)
            new_current_bin_number = cell.load_uint(32)
            new_sqrt_p = cell.load_uint(256)
            from_account = cell.load_bit()
            amount_x = cell.load_coins()
            amount_y = cell.load_coins()
            is_x = cell.load_bit()
            receiver_address = cell.load_address()
            additional_data_cell = cell.load_maybe_ref()
            from_address = None
            ref_addr = None
            if additional_data_cell is not None:
                data_slice = additional_data_cell.begin_parse()
                from_address = data_slice.load_address()
                try:
                    ref_addr = data_slice.load_address()
                except Exception as e:
                    pass

            logger.info(f"query_id: {query_id}, from_account: {from_account}, amount_x: {amount_x}, amount_y: {amount_y}, is_x: {is_x}, receiver_address: {receiver_address}, from_address: {from_address}")

            range_tx = obj
            for i in range(5): # max 5 reentrancies for swap
                try:
                    range_swap = Cell.one_from_boc(db.get_parent_message_body(range_tx.get('msg_hash'))).begin_parse()
                except Exception as e:
                    logger.info("parrent range swap not found", e)

                swap_op = range_swap.load_uint(32)
                if swap_op != 0x66210c65: # range:swap
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

            src_token = [token_y_address, token_x_address][is_x]
            dst_token = [token_x_address, token_y_address][is_x]
            output_amount = [amount_x, amount_y][is_x]
            refund_amount = [amount_y, amount_x][is_x]

            input_amount -= refund_amount

            if input_amount == 0 or output_amount == 0:
                logger.info(f"Skipping zero amount swap for {obj}")
                return

            src_token_master = None
            dst_token_master = None

            if src_token == Address("0:0000000000000000000000000000000000000000000000000000000000000000"):
                src_token_master = "0:0000000000000000000000000000000000000000000000000000000000000000"
            else:
                src_token_master = db.get_wallet_master(src_token)

            if dst_token == Address("0:0000000000000000000000000000000000000000000000000000000000000000"):
                dst_token_master = "0:0000000000000000000000000000000000000000000000000000000000000000"
            else:
                dst_token_master = db.get_wallet_master(dst_token)

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
                referral_address=ref_addr,
                query_id=input_query_id
            )

            estimate_volume(swap, db)
            db.serialize(swap)
            db.discover_dex_pool(swap)
        elif op == 0xd3a25890: # pool:swap_success_callback_v2
            query_id = cell.load_uint(64)
            new_current_bin_number = cell.load_uint(32)
            new_sqrt_p = cell.load_uint(256)
            from_account = cell.load_bit()
            amount_x = cell.load_coins()
            amount_y = cell.load_coins()
            is_x = cell.load_bit()
            receiver_address = cell.load_address()
            ref_cell = cell.load_maybe_ref()
            ref_addr = None
            if ref_cell is not None:
                ref_addr = ref_addr.begin_parse().load_address()


            logger.info(f"query_id: {query_id}, from_account: {from_account}, amount_x: {amount_x}, amount_y: {amount_y}, is_x: {is_x}, receiver_address: {receiver_address}, from_address: {from_address}")

            range_tx = obj
            for i in range(5): # max 5 reentrancies for swap
                try:
                    range_swap = Cell.one_from_boc(db.get_parent_message_body(range_tx.get('msg_hash'))).begin_parse()
                except Exception as e:
                    logger.info("parrent range swap not found", e)

                swap_op = range_swap.load_uint(32)
                if swap_op != 0x87d36990: # range:swap_v2
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

            src_token = [token_y_address, token_x_address][is_x]
            dst_token = [token_x_address, token_y_address][is_x]
            output_amount = [amount_x, amount_y][is_x]
            refund_amount = [amount_y, amount_x][is_x]

            input_amount -= refund_amount

            if input_amount == 0 or output_amount == 0:
                logger.info(f"Skipping zero amount swap for {obj}")
                return

            src_token_master = None
            dst_token_master = None

            if src_token == Address("0:0000000000000000000000000000000000000000000000000000000000000000"):
                src_token_master = "0:0000000000000000000000000000000000000000000000000000000000000000"
            else:
                src_token_master = db.get_wallet_master(src_token)

            if dst_token == Address("0:0000000000000000000000000000000000000000000000000000000000000000"):
                dst_token_master = "0:0000000000000000000000000000000000000000000000000000000000000000"
            else:
                dst_token_master = db.get_wallet_master(dst_token)

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
                referral_address=ref_addr,
                query_id=input_query_id  
            )

            estimate_volume(swap, db)
            db.serialize(swap)
            db.discover_dex_pool(swap)
        

