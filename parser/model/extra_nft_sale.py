from dataclasses import dataclass
from typing import Union
from pytoniq_core import Address


@dataclass
class ExtraNFTSale:
    __tablename__ = "extra_nft_sales"
    __schema__ = "parsed"

    address: Address
    is_complete: bool
    created_at: int
    marketplace_address: Address
    nft_address: Address
    nft_owner_address: Address
    full_price: int
    asset: Union[Address, str]
    marketplace_fee_address: Address
    marketplace_fee: int
    royalty_address: Address
    royalty_amount: int
    last_transaction_lt: int
    last_tx_now: int
    code_hash: str
    data_hash: str
