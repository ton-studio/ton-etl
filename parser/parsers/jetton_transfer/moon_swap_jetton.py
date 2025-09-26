import traceback
from model.parser import Parser, TOPIC_JETTON_TRANSFERS
from loguru import logger
from db import DB
from pytoniq_core import Cell, Address
from model.dexswap import DEX_MOON, DexSwapParsed
from parsers.message.moon_swap_ton import MoonSwapTON
from parsers.message.swap_volume import estimate_volume


class MoonSwapJetton(MoonSwapTON):
    def topics(self):
        return [TOPIC_JETTON_TRANSFERS]

    def predicate(self, obj) -> bool:
        return not obj.get("tx_aborted", True)
    
    def handle_internal(self, obj: dict, db: DB):
        try:
            fp = Cell.one_from_boc(obj.get('forward_payload')).begin_parse()
            op_id = fp.load_uint(32)
            if op_id != 0xcb7f38d6:  # moon_swap_succeed#cb7f38d6
                return
        except Exception:
            return

        try:
            pool_state = Parser.get_account_state_safe(Address(obj.get("source")), db)
            if not self.validate_pool(db, pool_state):
                logger.warning(f"Skipping invalid Moon pool {obj.get('source')}")
                return

            tx = db.get_parent_transaction(obj.get('trace_id'), obj.get('tx_hash'))
            msg_hash = db.get_out_msg_hashes(tx.get('trace_id'), tx.get('hash'))[0]

            in_jetton_transfer = db.get_parent_jetton_transfer(Parser.require(obj.get('trace_id')), tx.get('hash'))
            if in_jetton_transfer:
                recipient = in_jetton_transfer.get('source')

                fp = Cell.one_from_boc(in_jetton_transfer.get('forward_payload')).begin_parse()
                op_id = fp.load_uint(32)
                min_out = fp.load_coins()
                deadline = fp.load_uint(64)
                excess = fp.load_address()
                referral = fp.load_address()

                next_fulfill_cell = fp.load_maybe_ref()
                if next_fulfill_cell:
                    nf = next_fulfill_cell.begin_parse()
                    recipient = nf.load_address()

                swap_src_token = Parser.require(in_jetton_transfer.get('jetton_master_address'))
                swap_src_amount = int(Parser.require(in_jetton_transfer.get('amount')))
                query_id = in_jetton_transfer.get('query_id')

            else:
                parent_message = db.get_parent_message_with_body(msg_hash)
                recipient = parent_message.get('source')
                body = Cell.one_from_boc(parent_message.get('body')).begin_parse()
                op_id = body.load_uint(32)
                query_id = body.load_uint(64)
                swap_src_amount = body.load_coins()
                min_out = fp.load_coins()
                deadline = fp.load_uint(64)
                excess = fp.load_address()
                referral = fp.load_address()

                next_fulfill_cell = fp.load_maybe_ref()
                if next_fulfill_cell:
                    nf = next_fulfill_cell.begin_parse()
                    recipient = nf.load_address()

                swap_src_token = self.TON


            swap_dst_amount = int(Parser.require(obj.get('amount')))
            if swap_src_amount == 0 or swap_dst_amount == 0:
                logger.warning(f"Skipping zero amount swap for Moon DEX {obj}")
                return

            swap = DexSwapParsed(
                tx_hash=Parser.require(tx.get('hash')),
                msg_hash=msg_hash,
                trace_id=Parser.require(tx.get('trace_id')),
                platform=DEX_MOON,
                swap_utime=Parser.require(tx.get('now')),
                swap_user=recipient,
                swap_pool=Parser.require(tx.get('acoount')),
                swap_src_token=swap_src_token,
                swap_dst_token=Parser.require(obj.get('jetton_master_address')),
                swap_src_amount=swap_src_amount,
                swap_dst_amount=swap_dst_amount,
                referral_address=referral,
                query_id=query_id,
                min_out=min_out
            )
            estimate_volume(swap, db)

            logger.info(f"Moon swap parsed: {swap}")
            db.serialize(swap)
            db.discover_dex_pool(swap)

        except Exception as e:
            logger.warning(f"Failed to parse Moon swap (tx_hash = {obj.get('tx_hash')}): {e} {traceback.format_exc()}")
            return
