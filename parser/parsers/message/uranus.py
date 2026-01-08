import base64
import traceback

from db import DB
from loguru import logger
from model.parser import TOPIC_MESSAGES, NonCriticalParserError, Parser
from model.uranus import UranusTradeEvent
from parsers.message.swap_volume import USDT
from pytoniq_core import Address, Cell

"""Uranus bonding curve parser."""

URANUS_MASTER_CODE_HASHES = {
    "ci03vlGO4NS2cUB3cn7WByS/N56PFHnCILhT0a888A0=",
}

BUY_EVENT_OPCODE = Parser.opcode_signed(0xA0AA6BC2)
SELL_EVENT_OPCODE = Parser.opcode_signed(0x3AB0FCCC)
EVENTS = [BUY_EVENT_OPCODE, SELL_EVENT_OPCODE]
EVENT_TYPE_BY_OPCODE = {
    BUY_EVENT_OPCODE: "BuyEvent",
    SELL_EVENT_OPCODE: "SellEvent",
}


class UranusTrade(Parser):
    def __init__(self):
        self._graduated_seen: set[str] = set()

    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        return (
            obj.get("opcode", None) in EVENTS
            and obj.get("direction", None) == "out"
            and obj.get("destination") is None
        )

    def handle_internal(self, obj, db: DB):
        try:
            tx_hash = Parser.require(obj.get("tx_hash", None))
            trace_id = Parser.require(obj.get("trace_id", None))
            msg_hash = Parser.require(obj.get("msg_hash", None))
            created_at = Parser.require(obj.get("created_at", None))
            source_addr_raw = Parser.require(obj.get("source", None))
            opcode = Parser.require(obj.get("opcode", None))

            event_type = EVENT_TYPE_BY_OPCODE.get(opcode)
            if event_type is None:
                logger.warning(f"Unknown uranus event opcode: {opcode}")
                return
            is_buy_event = opcode == BUY_EVENT_OPCODE

            source_addr = Address(source_addr_raw)
            account_state = Parser.get_account_state_safe(source_addr, db)
            if not account_state or not account_state.get("code_boc"):
                logger.warning(
                    f"Account state for {source_addr_raw} missing, skip uranus trade"
                )
                return

            code_hash_b64 = base64.b64encode(
                Cell.one_from_boc(account_state["code_boc"])._hash
            ).decode("utf-8")
            if code_hash_b64 not in URANUS_MASTER_CODE_HASHES:
                logger.warning(
                    f"Code hash {code_hash_b64} for {source_addr_raw} not in uranus master whitelist"
                )
                return
            meme_master_raw = source_addr.to_str(is_user_friendly=False).upper()

            cell = Parser.message_body(obj, db).begin_parse()
            opcode_raw = cell.load_uint(32)
            body_opcode = Parser.opcode_signed(opcode_raw)
            if body_opcode != opcode:
                logger.warning(
                    f"Uranus opcode mismatch: header {opcode} vs body {body_opcode}"
                )
                return

            trader_address = cell.load_address()
            amount_in = cell.load_coins()
            amount_out = cell.load_coins()

            if amount_in == 0 or amount_out == 0:
                logger.debug(f"Skipping zero amount uranus trade for {tx_hash}")
                return

            creator_fee = cell.load_coins()
            protocol_fee = cell.load_coins()
            partner_fee = cell.load_coins()
            referrer_fee = cell.load_coins()

            current_supply = cell.load_coins()
            raised_funds = cell.load_coins()

            is_graduated = None
            if is_buy_event:
                is_graduated = cell.load_bit()
                if is_graduated and not self._is_first_graduation(meme_master_raw):
                    is_graduated = False

            ton_price = db.get_core_price(USDT, created_at)
            if ton_price is None:
                logger.warning(f"No TON price found for {created_at}")
                ton_price = 0

            ton_amount = amount_in if is_buy_event else amount_out
            volume_usd = ton_amount * ton_price / 1e6

            event = UranusTradeEvent(
                tx_hash=tx_hash,
                trace_id=trace_id,
                msg_hash=msg_hash,
                event_time=created_at,
                meme_master=meme_master_raw,
                event_type=event_type,
                trader_address=trader_address.to_str(is_user_friendly=False).upper()
                if trader_address
                else None,
                amount_in=amount_in,
                amount_out=amount_out,
                creator_fee=creator_fee,
                protocol_fee=protocol_fee,
                partner_fee=partner_fee,
                referrer_fee=referrer_fee,
                current_supply=current_supply,
                raised_funds=raised_funds,
                is_graduated=is_graduated,
                volume_usd=volume_usd,
            )

            db.serialize(event)
        except Exception as e:
            logger.error(
                f"Failed to parse uranus trade event: {e} {traceback.format_exc()}"
            )
            raise NonCriticalParserError(
                f"Failed to parse uranus trade event: {e} {traceback.format_exc()}"
            ) from e

    def _is_first_graduation(self, meme_master_raw: str) -> bool:
        if meme_master_raw in self._graduated_seen:
            return False

        self._graduated_seen.add(meme_master_raw)
        return True
