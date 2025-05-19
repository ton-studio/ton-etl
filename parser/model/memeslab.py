from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass
class MemesLabEvent:
    __tablename__ = "memeslab_trade_event"
    tx_hash: str
    trace_id: str
    event_time: int
    jetton_master: str
    event_type: str  # Buy, Sell, List
    trader_address: Optional[str]
    ton_amount: Optional[Decimal]
    jetton_amount: Optional[Decimal]
    volume_usd: Optional[Decimal]
    current_supply: Decimal  
    total_ton_collected: Decimal 