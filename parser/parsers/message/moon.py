import base64
import traceback
from model.parser import Parser, TOPIC_MESSAGES
from loguru import logger
from db import DB
from pytoniq_core import Cell, Address
from model.dexswap import DEX_MOON, DexSwapParsed
from parsers.message.swap_volume import estimate_volume


TON = Address("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c")

class Moon(Parser):
    POOL_CODE_HASHES = [
        'KAgWUlMoah5P5j76ubo1SBE3qsCxoPntzLj2RC3eRsQ=',
    ]

    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        # only external messages not blacklist
        return obj.get("opcode") == Parser.opcode_signed(0xcb7f38d6) and obj.get("direction") == "out"
    
    def validate_pool(self, db: DB, pool_state): 
        try:
            code_boc_str = pool_state["code_boc"]
            code_cell_hash = base64.b64encode(Cell.one_from_boc(code_boc_str)._hash).decode('utf-8')
            return code_cell_hash in self.POOL_CODE_HASHES
        except Exception as e:
            logger.warning(f"Moon pool state not found: {e} {traceback.format_exc()}")

        return False

    def handle_internal(self, obj, db: DB):
        try:
            pool_state = Parser.get_account_state_safe(Address(obj.get("source")), db)
            if not self.validate_pool(db, pool_state):
                logger.warning(f"Skipping invalid Moon pool {obj.get('source')}")
                return

            in_jetton_transfer = db.get_parent_jetton_transfer(Parser.require(obj.get('trace_id')), Parser.require(obj.get('tx_hash')))
            recipient = in_jetton_transfer.get('source')

            fp = Cell.one_from_boc(in_jetton_transfer.get('forward_payload')).begin_parse()
            op_id = fp.load_uint(32)  # moon_swap_succeed#cb7f38d6
            min_out = fp.load_coins()
            deadline = fp.load_uint(64)
            excess = fp.load_address()
            referral = fp.load_address()

            next_fulfill_cell = fp.load_maybe_ref()
            if next_fulfill_cell:
                nf = next_fulfill_cell.begin_parse()
                recipient = nf.load_address()
                
            parent_message = db.get_parent_message_with_body(obj.get('msg_hash'))
            tx = db.get_transaction(obj.get('tx_hash'))

            swap = DexSwapParsed(
                tx_hash=Parser.require(obj.get('tx_hash')),
                msg_hash=Parser.require(obj.get('msg_hash')),
                trace_id=Parser.require(obj.get('trace_id')),
                platform=DEX_MOON,
                swap_utime=Parser.require(obj.get('created_at')),
                swap_user=recipient,
                swap_pool=Parser.require(obj.get('source')),
                swap_src_token=Parser.require(in_jetton_transfer.get('jetton_master_address')),
                swap_dst_token=TON,
                swap_src_amount=Parser.require(in_jetton_transfer.get('amount')),
                swap_dst_amount=obj.get('value') - parent_message.get('value') + obj.get('fwd_fee') + tx.get('total_fees'),
                referral_address=referral,
                min_out=min_out
            )
            estimate_volume(swap, db)
            db.serialize(swap)
            db.discover_dex_pool(swap)

        except Exception as e:
            logger.warning(f"Failed to parse Moon swap (tx_hash = {obj.get('tx_hash')}): {e} {traceback.format_exc()}")
            return
