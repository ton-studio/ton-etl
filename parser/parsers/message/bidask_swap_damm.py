from model.parser import Parser, TOPIC_MESSAGES
from loguru import logger
from db import DB
from parsers.accounts.emulator import EmulatorParser
from pytoniq_core import Cell, Address
from model.dexswap import DEX_BIDASK_DAMM, DexSwapParsed
from parsers.message.swap_volume import estimate_volume
import base64

class BidaskDammSwap(EmulatorParser):
    BIDASK_POOLS_CODE_HASHES = [
        "oTI7bKi3ig68sMDj7FmV0Sk1VoCmLKLp8JMAvSoUBsY=",
        "soSSUgw7A7zd0cfq4QtcxolhyDQHawC1OfoIA8w9pqw="
        ]

    def __init__(self, emulator_path):
        EmulatorParser.__init__(self, emulator_path)
        self.valid_pools = set()
    
    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        # only internal messages sent to the router
        return obj.get("opcode", None) in [Parser.opcode_signed(0xf8a7ea5), Parser.opcode_signed(0x6edd65f0), Parser.opcode_signed(0x30a3f9bc)] and \
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
        
        pool_state = Parser.get_account_state_safe(Address(obj.get("source")), db)

        if not self.validate_pool(db, pool_state):
            logger.warning(f"Skipping invalid pool {obj.get('source', None)}")
            return
        
        pool_swap = Cell.one_from_boc(db.get_parent_message_body(obj.get('msg_hash')))

        pool_emulator = self._prepare_emulator(pool_state)
        input_user_address = None
        src_token_master = None
        dst_token_master = None
        input_amount = None
        output_amount = None
        ref_addr = None
        input_query_id = None
        from_address = None
        slippage = None

        body = pool_swap.begin_parse()
        op = body.load_uint(32)

        if op == 0xdd79732c: # cpmm:pool:swap (ton->jetton swap)
            input_query_id = body.load_uint(64)
            input_amount = body.load_coins()
            input_user_address = body.load_address()
            slippage = body.load_coins()
            from_user_address = body.load_address()
            exact_out = body.load_coins()
            ref_cell = body.load_maybe_ref()
            if ref_cell is not None:
                ref_addr = ref_cell.begin_parse().load_address()

            src_token_master = "0:0000000000000000000000000000000000000000000000000000000000000000"

            out_message = Parser.message_body(obj, db).begin_parse()

            op = out_message.load_uint(32)

            if op == 0xf8a7ea5: # jetton transfer
                query_id = out_message.load_uint(64)
                output_amount  = out_message.load_coins()
                dst = out_message.load_address()
                dst_token_master = db.get_wallet_master(Address(obj.get("destination")))
            elif op == 0x6edd65f0: # native_transfer_notification
                query_id = out_message.load_uint(64)
                output_amount  = out_message.load_coins()
                dst_token_master = "0:0000000000000000000000000000000000000000000000000000000000000000"
            else:
                return
            
            logger.info(f"query_id: {input_query_id}, input_amount: {input_amount}, output_amount: {output_amount}, src_token_master: {src_token_master}, dst_token_master: {dst_token_master}, input_user_address: {input_user_address}")
            
        elif op == 0x7362d09c: # transfer_notification (jetton -> ton potential swap)
            input_query_id = body.load_uint(64)
            input_amount = body.load_coins()
            forward_payload = body.load_maybe_ref()
            fp_body = forward_payload.begin_parse()
            fp_op = fp_body.load_uint(32)

            if fp_op == 0xdd79732c: # (jetton -> ton swap)
                input_user_address = fp_body.load_address()
                slippage = fp_body.load_coins()
                from_user_address = fp_body.load_address()
                exact_out = fp_body.load_coins()
                ref_cell = fp_body.load_maybe_ref()
                if ref_cell is not None:
                    ref_addr = ref_cell.begin_parse().load_address()

                src_token_master = db.get_wallet_master(Address(db.get_parent_message_with_body(obj.get("msg_hash")).get("source")))

                out_message = Parser.message_body(obj, db).begin_parse()

                op = out_message.load_uint(32)

                if op == 0xf8a7ea5: # jetton transfer
                    query_id = out_message.load_uint(64)
                    output_amount  = out_message.load_coins()
                    dst = out_message.load_address()
                    dst_token_master = db.get_wallet_master(Address(obj.get("destination")))
                elif op == 0x6edd65f0: # native_transfer_notification
                    query_id = out_message.load_uint(64)
                    output_amount  = out_message.load_coins()
                    dst_token_master = "0:0000000000000000000000000000000000000000000000000000000000000000"
                else:
                    return
                
            else:
                return

            logger.info(f"query_id: {input_query_id}, input_amount: {input_amount}, output_amount: {output_amount}, src_token_master: {src_token_master}, dst_token_master: {dst_token_master}, input_user_address: {input_user_address}")
            
        elif op == 0xf4a31e27: # cpmm:pool:swap_from_account
            amount_x = 0
            amount_y = 0
            input_query_id = body.load_uint(64)
            input_user_address = body.load_address() # trade account owner
            seed = body.load_ref()
            input_amount = body.load_coins()
            is_x = body.load_bit()
            slippage = body.load_coins()
            exact_out = body.load_coins()

            out_message = Parser.message_body(obj, db).begin_parse()

            op = out_message.load_uint(32)

            if op == 0x30a3f9bc: # cpmm:trade_account:deposit
                query_id = out_message.load_uint(64)
                _, _ = out_message.load_maybe_ref(), out_message.load_maybe_ref()
                amount_x = out_message.load_coins()
                amount_y = out_message.load_coins()
            else:
                return

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

            if src_token == Address("0:0000000000000000000000000000000000000000000000000000000000000000"):
                src_token_master = "0:0000000000000000000000000000000000000000000000000000000000000000"
            else:
                src_token_master = db.get_wallet_master(src_token)

            if dst_token == Address("0:0000000000000000000000000000000000000000000000000000000000000000"):
                dst_token_master = "0:0000000000000000000000000000000000000000000000000000000000000000"
            else:
                dst_token_master = db.get_wallet_master(dst_token)
        else:
            return
        
        swap = DexSwapParsed(
                tx_hash=Parser.require(obj.get('tx_hash', None)),
                msg_hash=Parser.require(obj.get('msg_hash', None)),
                trace_id=Parser.require(obj.get('trace_id', None)),
                platform=DEX_BIDASK_DAMM,
                swap_utime=Parser.require(obj.get('created_at', None)),
                swap_user=input_user_address,
                swap_pool=Parser.require(obj.get('source', None)),
                min_out=slippage,
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

