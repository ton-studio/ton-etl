import base64
import time
import traceback
from typing import List
from loguru import logger
from pytoniq_core import Cell, Address
from db import DB
from model.dexswap import DEX_MOON, DexSwapParsed
from model.parser import Parser, TOPIC_MESSAGES
from parsers.message.swap_volume import estimate_volume


class MoonSwapTON(Parser):
    TON = Address("EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c")

    POOL_CODE_HASHES = [
        'KAgWUlMoah5P5j76ubo1SBE3qsCxoPntzLj2RC3eRsQ=',
    ]

    SWAP_SUCCEED_OPCODE = Parser.opcode_signed(0xcb7f38d6)  # moon_swap_succeed#cb7f38d6
    SWAP_OPCODE = Parser.opcode_signed(0xb37a900b)  # moon_swap#b37a900b
    NON_SWAP_OPCODES = map(
        Parser.opcode_signed,
        [
            0xc47c1f57,  # swap_failed#c47c1f57
            0x90d3b4ad,  # withdraw_liquidity_payout#90d3b4ad
            0xff86f067,  # withdraw_liquidity_notify#ff86f067
            0x16b463b4,  # cancel_deposit_payout#16b463b4
            0xd53276db,  # excess#d53276db
            0xbaaa2c1b,  # deposit_record#baaa2c1b
            0x040beadd,  # collect_fees#040beadd
            0x737b4eb6,  # provide_liquidity_succeed#737b4eb6
            0x7daf7060,  # provide_liquidity_failed#7daf7060
        ],
    )

    def __init__(self, update_interval=3600):
        super().__init__()
        self.last_updated = int(time.time())
        self.update_interval = update_interval
        self.pools: List[str] = []

    def prepare(self, db: DB):
        super().prepare(db)
        self.pools = db.get_dex_pool_addresses(DEX_MOON)

    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        self.update_pools()
        return obj.get('direction') == "out" and (
            obj.get('opcode') == self.SWAP_SUCCEED_OPCODE
            or obj.get('source') in self.pools
            and obj.get('opcode') not in self.NON_SWAP_OPCODES
        )

    def validate_pool(self, db: DB, pool_state):
        try:
            code_boc_str = pool_state["code_boc"]
            code_cell_hash = base64.b64encode(Cell.one_from_boc(code_boc_str)._hash).decode('utf-8')
            return code_cell_hash in self.POOL_CODE_HASHES
        except Exception as e:
            logger.warning(f"Moon.cx pool state not found: {e} {traceback.format_exc()}")

        return False

    def update_pools(self, db: DB):
        if int(time.time()) > self.last_updated + self.update_interval:
            logger.info("Updating Moon.cx pools")
            self.pools = db.get_dex_pool_addresses(DEX_MOON)
            self.last_updated = int(time.time())

    def handle_internal(self, obj, db: DB):
        try:
            pool_state = Parser.get_account_state_safe(Address(obj.get("source")), db)
            if not self.validate_pool(db, pool_state):
                logger.warning(f"Skipping invalid Moon.cx pool {obj.get('source')}")
                return

            in_jetton_transfer = db.get_parent_jetton_transfer(
                Parser.require(obj.get('trace_id')), Parser.require(obj.get('tx_hash'))
            )
            recipient = in_jetton_transfer.get('source')

            fp = Cell.one_from_boc(in_jetton_transfer.get('forward_payload')).begin_parse()
            op_id = fp.load_uint(32)
            if op_id != self.SWAP_OPCODE:
                logger.warning(f"Skipping Moon.cx non swap tx {obj.get('tx_hash')}")
                return

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

            swap_src_amount = int(Parser.require(in_jetton_transfer.get('amount')))
            # The pool sends swap_dst_amount TON plus the original value from the transfer notification.
            # To cover gas fees the pool deducts a constant fee of 0.007TON.
            # This resulting message is being sent using send_mode = 0,
            # i.e. fwd_fee and action_fees are deducted from the message value
            swap_dst_amount = int(
                obj.get('value')
                - parent_message.get('value')
                + obj.get('fwd_fee')
                + tx.get('action_total_action_fees')
                + 7000000
            )
            if swap_src_amount == 0 or swap_dst_amount <= 0:
                logger.warning(f"Skipping zero/negative amount swap for Moon.cx DEX {obj}")
                return

            swap = DexSwapParsed(
                tx_hash=Parser.require(obj.get('tx_hash')),
                msg_hash=Parser.require(obj.get('msg_hash')),
                trace_id=Parser.require(obj.get('trace_id')),
                platform=DEX_MOON,
                swap_utime=Parser.require(obj.get('created_at')),
                swap_user=recipient,
                swap_pool=Parser.require(obj.get('source')),
                swap_src_token=Parser.require(in_jetton_transfer.get('jetton_master_address')),
                swap_dst_token=self.TON,
                swap_src_amount=swap_src_amount,
                swap_dst_amount=swap_dst_amount,
                referral_address=referral,
                query_id=in_jetton_transfer.get('query_id'),
                min_out=min_out,
            )
            estimate_volume(swap, db)

            logger.info(f"Moon.cx swap parsed: {swap}")
            db.serialize(swap)
            db.discover_dex_pool(swap)

        except Exception as e:
            logger.warning(f"Failed to parse Moon.cx swap (tx_hash = {obj.get('tx_hash')}): {e} {traceback.format_exc()}")
            return
