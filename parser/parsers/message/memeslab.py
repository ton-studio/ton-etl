import traceback
from typing import Optional
from loguru import logger
from pytoniq_core import Cell, Address
from model.parser import NonCriticalParserError, Parser, TOPIC_MESSAGES
from model.memeslab import MemesLabEvent
from db import DB
from parsers.message.swap_volume import USDT

# Opcode mapping for event types
EVENT_TYPES = {
    Parser.opcode_signed(0xace8e777),  # Buy
    Parser.opcode_signed(0xace8e778),  # Sell
    Parser.opcode_signed(0xace8e779),  # ListToken
}

JETTON_WALLET_CODE_HASH_WHITELIST = [
    "ipKseGXVM3nzsWGuSptUyN721dwpyvwHemFB6YK/Kis=",
]

def parse_memeslab_event(opcode: int, cs: Cell) -> Optional[dict]:
    try:
        raw_opcode = cs.load_uint(32)  # opcode

        if raw_opcode == 0xace8e777:  # Buy
            event_type = "Buy"
        elif raw_opcode == 0xace8e778:  # Sell
            event_type = "Sell"

        elif raw_opcode == 0xace8e779:  # ListToken

            event_type = "ListToken"
            query_id = cs.load_uint(64)
            pool_complete = cs.load_uint(4)
            if cs.remaining_bits >= 128:
                total_ton_collected = cs.load_uint(64)
                total_supply = cs.load_uint(64)
            else:
                total_ton_collected = int(1000 * 1e9)
                total_supply = int(1000000000 * 1e9)
            return {
                "type": event_type,
                "trader": None,
                "ton_amount": 0,
                "jetton_amount": 0,
                "total_ton_collected": total_ton_collected,
                "current_supply": total_supply,
            }
        else:
            event_type = "Unknown"  # In case the opcode doesn't match Buy/Sell

        BASE_TOTAL_SUPPLY = int(1000000000 * 1e9)  # 1B in Jettons
        cs.load_uint(64)  # query_id
        real_base_reserve = cs.load_uint(64)  # real_base_reserves
        total_ton_collected = cs.load_uint(64)  # real_quote_reserves
        current_supply = BASE_TOTAL_SUPPLY - real_base_reserve
        ton_amount = cs.load_uint(64)  # TON in
        token_amount = cs.load_uint(64)  # Jetton in/out
        trader = cs.load_address()  # sender
        cs.load_uint(4)  # reserved (0 or 1)
        return {
            "type": event_type,
            "trader": trader,
            "ton_amount": ton_amount,
            "jetton_amount": token_amount,
            "current_supply": current_supply,
            "total_ton_collected": total_ton_collected,
        }
    except Exception as e:
        logger.error(f"Error parsing event: {e}")
        return None


def make_event(obj: dict, trade_data: dict, price_usd: float) -> MemesLabEvent:

    return MemesLabEvent(
        tx_hash=obj["tx_hash"],
        trace_id=obj["trace_id"],
        event_time=obj["created_at"],
        jetton_master=obj["source"],
        event_type=trade_data["type"],
        trader_address=trade_data["trader"],
        ton_amount=int(trade_data["ton_amount"]),
        jetton_amount=int(trade_data["jetton_amount"]),
        current_supply=int(trade_data["current_supply"]),
        total_ton_collected=int(trade_data["total_ton_collected"]),
        volume_usd=int(trade_data["ton_amount"]) * price_usd / 1e6,
    )


class MemesLabTrade(Parser):
    topics = lambda _: [TOPIC_MESSAGES]
    predicate = lambda _, obj: (
        obj.get("opcode") in EVENT_TYPES and obj.get("direction") == "out"
    )

    def handle_internal(self, obj: dict, db: DB) -> None:
        jetton_master_address = Address(Parser.require(obj.get('source', None)))
        jetton_master = db.get_jetton_master(jetton_master_address)
        if not jetton_master:
            raise Exception(f"Unable to get jetton_master from DB for {jetton_master_address}")
        code_hash = jetton_master['jetton_wallet_code_hash']
        if code_hash not in JETTON_WALLET_CODE_HASH_WHITELIST:
            logger.warning("Jetton wallet code hash {} for {} not in whitelist", code_hash, obj.get('source', None))
            return

        try:
            cs = Parser.message_body(obj, db).begin_parse()
            trade_data = parse_memeslab_event(obj.get("opcode"), cs)
            if trade_data:
                ton_price = db.get_core_price(
                    USDT, Parser.require(obj.get("created_at"))
                )
                ton_price = ton_price or 0
                event = make_event(obj, trade_data, ton_price)
                db.serialize(event)
        except Exception as e:
            logger.error(f"MemesLab parse error: {e}\n{traceback.format_exc()}")
            raise NonCriticalParserError(f"MemesLab parse error: {e}") from e
