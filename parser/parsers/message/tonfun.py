from typing import Optional
from functools import partial
from operator import eq
from decimal import Decimal
from loguru import logger
from pytoniq_core import Cell, Slice
from model.parser import Parser, TOPIC_MESSAGES
from model.tonfun import TonFunTradeEvent
from db import DB

"""
Tonfun parser implementation based on public SDK https://github.com/ton-fun-tech/ton-bcl-sdk
"""

EVENT_TYPES = {
    Parser.opcode_signed(0xcd78325d): "buy_log",
    Parser.opcode_signed(0x5e97d116): "sell_log",
    Parser.opcode_signed(0x0f6ab54f): "send_liq_log"
}

def parse_referral(cs: Slice) -> dict:
    opcode = cs.load_uint(32) # crc32(ref_v1)
    if opcode != 0xf7ecea4c:
        logger.warning(f"Unknown referral opcode: {opcode}")
        return {}
    return {
        "partner_address": cs.load_address(),
        "platform_tag": cs.load_address() if cs.remaining_bits else None,
        "extra_tag": cs.load_address() if cs.remaining_bits else None,
    }

def parse_event(cs: Cell) -> Optional[dict]:
    event_id = cs.load_uint(32)
    return {
        "buy_log": lambda: {"type": "Buy", **parse_trade_data(cs)},
        "sell_log": lambda: {"type": "Sell", **parse_trade_data(cs)},
        "send_liq_log": lambda: {"type": "SendLiq", **parse_send_liq_data(cs)}
    }.get(EVENT_TYPES.get(event_id, "unknown"), lambda: None)()

def parse_send_liq_data(cs: Cell) -> dict:
    ton_liq = cs.load_coins()
    jetton_liq = cs.load_coins()
    return {
        "trader": None,
        "ton_amount": 0,
        "bcl_amount": 0,
        "current_supply": jetton_liq,
        "ton_liq_collected": ton_liq,
        "partner_address": None,
        "platform_tag": None,
        "extra_tag": None
    }

def parse_trade_data(cs: Cell) -> dict:
    return {
        "trader": cs.load_address(),
        "ton_amount": cs.load_coins(),
        "bcl_amount": cs.load_coins(),
        "current_supply": cs.load_coins(),
        "ton_liq_collected": cs.load_coins(),
        **(parse_referral(cs.load_ref().begin_parse()) if cs.load_bit() else
           {"partner_address": None, "platform_tag": None, "extra_tag": None})
    }

def make_event(obj: dict, trade_data: dict) -> TonFunTradeEvent:
    return TonFunTradeEvent(
        tx_hash=obj["tx_hash"],
        trace_id=obj["trace_id"],
        event_time=obj["created_at"],
        bcl_master=obj["source"],
        event_type=trade_data["type"],
        trader_address=trade_data["trader"],
        ton_amount=int(trade_data["ton_amount"]),
        bcl_amount=int(trade_data["bcl_amount"]),
        partner_address=trade_data["partner_address"],
        platform_tag=trade_data["platform_tag"],
        extra_tag=trade_data["extra_tag"]
    )

class TonFunTrade(Parser):
    topics = lambda _: [TOPIC_MESSAGES]

    # ext out with specific opcodes
    predicate = lambda _, obj: (
        obj.get("opcode") in EVENT_TYPES and
        obj.get("direction") == "out" and 
        obj.get("destination", 'None') is None
    )

    def handle_internal(self, obj: dict, db: DB) -> None:
        maybe_trade_data = parse_event(Parser.message_body(obj, db).begin_parse())
        if maybe_trade_data:
            event = make_event(obj, maybe_trade_data)
            logger.info(f"Parsed tonfun event: {event}")
            db.serialize(event)
