import base64
from typing import Optional

from db import DB
from loguru import logger
from model.dexswap import DEX_DEDUST_CPMM_V3, DexSwapParsed
from model.parser import TOPIC_MESSAGES, Parser
from parsers.accounts.emulator import EmulatorParser
from parsers.message.swap_volume import estimate_volume
from pytoniq_core import Address, Cell

TON_NATIVE_ADDRESS = (
    "0:0000000000000000000000000000000000000000000000000000000000000000"
)


def _addr_or_zero(addr: Optional[Address]) -> str:
    return addr.to_str(is_user_friendly=False).upper() if addr else TON_NATIVE_ADDRESS

class CPMMV3Swap(EmulatorParser):
    """Parses swaps emitted by CPMM v3 pools (default & uranus-linked)."""

    POOL_CODE_HASHES = {
        "OZelwe6JI+k886apjqPvlILGTdiuorqDZWieFNmcdg0=",
    }

    SWAP_EVENT_OPCODE = Parser.opcode_signed(0x78E79BA4)

    def __init__(self, emulator_path: str):
        super().__init__(emulator_path)
        self.valid_pools: set[str] = set()

    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        return (
            obj.get("opcode") == self.SWAP_EVENT_OPCODE
            and obj.get("direction") == "out"
            and obj.get("destination") is None
        )

    def prepare(self, db: DB):
        super().prepare(db)

    def _is_valid_pool(self, pool_address: Address, pool_state: dict) -> bool:
        address_key = pool_address.to_str(is_user_friendly=False)
        if address_key in self.valid_pools:
            return True

        if not pool_state or not pool_state.get("code_boc"):
            return False

        code_hash = base64.b64encode(
            Cell.one_from_boc(pool_state["code_boc"])._hash
        ).decode("utf-8")

        if code_hash in self.POOL_CODE_HASHES:
            self.valid_pools.add(address_key)
            return True

        logger.warning(f"Invalid CPMM v3 pool code hash {code_hash} for {address_key}")
        return False

    def _parse_tokens(self, emulator, db: DB, pool_state: dict):
        # get_pool_data returns tuple [status, deposit_active, swap_active, assetX, assetY, ...]
        _, _, _, asset_x_cell, asset_y_cell, *_ = self._execute_method(
            emulator, "get_pool_data", [], db, pool_state
        )

        def parse_asset(cell: Cell):
            slice_ = cell.begin_parse() if hasattr(cell, "begin_parse") else cell
            try:
                return slice_.load_address(), True
            except ValueError as exc:
                logger.warning(f"Failed to parse pool asset cell: {exc}")
                return None, False

        asset_x, asset_x_ok = parse_asset(asset_x_cell)
        asset_y, asset_y_ok = parse_asset(asset_y_cell)
        if not asset_x_ok or not asset_y_ok:
            return None, None, False

        return asset_x, asset_y, True

    def handle_internal(self, obj, db: DB):
        tx_hash = Parser.require(obj.get("tx_hash"))
        if not Parser.require(db.is_tx_successful(tx_hash)):
            logger.info(f"Skipping failed tx for {tx_hash}")
            return

        pool_address = Address(Parser.require(obj.get("source")))
        pool_state = Parser.get_account_state_safe(pool_address, db)
        if not pool_state or not pool_state.get("code_boc"):
            logger.warning(
                f"Account state missing for {pool_address.to_str(is_user_friendly=False)}"
            )
            return
        if not self._is_valid_pool(pool_address, pool_state):
            return

        cell = Parser.message_body(obj, db).begin_parse()
        opcode_prefix = Parser.opcode_signed(cell.load_uint(32))
        if opcode_prefix != self.SWAP_EVENT_OPCODE:
            logger.debug(f"Unexpected opcode {opcode_prefix} for CPMM swap")
            return

        x_to_y = cell.load_bit()
        amount_in = cell.load_coins()
        amount_out = cell.load_coins()
        if amount_in == 0 or amount_out == 0:
            logger.info(f"Skipping zero amount swap for {tx_hash}")
            return

        initiator = cell.load_address()
        cell.load_address()
        cell.load_ref()
        fees = cell.load_ref().begin_parse()
        fees.load_bit()
        lp_fee = fees.load_coins()
        creator_fee = fees.load_coins()
        protocol_fee = fees.load_coins()
        partner_fee = fees.load_coins()
        referrer_fee = fees.load_coins()

        pool_emulator = self._prepare_emulator(pool_state)
        asset_x, asset_y, assets_ok = self._parse_tokens(pool_emulator, db, pool_state)
        if not assets_ok:
            logger.warning(
                f"Missing pool assets for {pool_address.to_str(is_user_friendly=False)}"
            )
            return
        src_token = asset_x if x_to_y else asset_y
        dst_token = asset_y if x_to_y else asset_x

        swap = DexSwapParsed(
            tx_hash=tx_hash,
            msg_hash=Parser.require(obj.get("msg_hash")),
            trace_id=Parser.require(obj.get("trace_id")),
            platform=DEX_DEDUST_CPMM_V3,
            swap_utime=Parser.require(obj.get("created_at")),
            swap_user=initiator.to_str(is_user_friendly=False).upper()
            if initiator
            else None,
            swap_pool=pool_address.to_str(is_user_friendly=False).upper(),
            swap_src_token=_addr_or_zero(src_token),
            swap_dst_token=_addr_or_zero(dst_token),
            swap_src_amount=amount_in,
            swap_dst_amount=amount_out,
            referral_address=None,
            query_id=None,
        )

        estimate_volume(swap, db)
        db.serialize(swap)
        db.discover_dex_pool(swap)
