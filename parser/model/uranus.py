import decimal
from dataclasses import dataclass
from typing import Optional


@dataclass
class UranusTradeEvent:
    __tablename__ = "uranus_trade"

    tx_hash: str
    trace_id: str
    msg_hash: str
    event_time: int
    meme_master: str
    event_type: str  # BuyEvent, SellEvent
    trader_address: str

    # from BuyEvent & SellEvent
    amount_in: decimal.Decimal
    amount_out: decimal.Decimal

    # from TradeFees
    creator_fee: decimal.Decimal
    protocol_fee: decimal.Decimal
    partner_fee: decimal.Decimal
    referrer_fee: decimal.Decimal

    # from BuyEvent & SellEvent
    current_supply: decimal.Decimal
    raised_funds: decimal.Decimal

    # from BuyEvent
    is_graduated: Optional[bool]

    # extra info
    volume_usd: Optional[decimal.Decimal]
