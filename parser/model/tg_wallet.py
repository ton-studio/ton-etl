from dataclasses import dataclass
from typing import Optional


@dataclass
class TgWalletRequest:
    __tablename__ = "tg_wallet_request"

    tx_hash: str
    msg_hash: str
    trace_id: str
    event_time: int
    tx_lt: int
    successful: bool
    wallet: str
    source: Optional[str]  # sender of internal request (gas relayer), None for external
    gasless: bool  # internal request (I-opcode), gas paid by the sender
    request_type: str  # send_one, send_bulk, change_key
    subwallet_id: int
    valid_until: int
    seqno: int
    messages_count: int
    new_public_key: Optional[str]  # hex, only for change_key
