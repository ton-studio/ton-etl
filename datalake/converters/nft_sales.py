import base64
import decimal
from typing import List
from topics import TOPIC_NFT_SALES, TOPIC_NFT_AUCTIONS, TOPIC_EXTRA_NFT_SALES
from loguru import logger
from pytoniq_core import Cell
from converters.converter import Converter

"""
Converts nft sale and auctions contracts states
"""
class NFTSalesConverter(Converter):
    def __init__(self):
        super().__init__("schemas/nft_sales.avsc", ignored_fields=[], updates_enabled=True)

    def timestamp(self, obj):
        return obj['last_tx_now']
    
    def topics(self) -> List[str]:
        return [TOPIC_NFT_SALES, TOPIC_NFT_AUCTIONS, TOPIC_EXTRA_NFT_SALES]
    
    def convert(self, obj, table_name=None):
        if table_name == "getgems_nft_sales":
            price = self.decode_numeric(obj['full_price'])
            return {
                "address": obj['address'],
                "type": "sale",
                "nft_address": obj['nft_address'],
                "nft_owner_address": obj['nft_owner_address'],
                "created_at": obj['created_at'],
                "is_complete": obj['is_complete'],
                "is_canceled": False,
                "end_time": None,
                "marketplace_address": obj['marketplace_address'],
                "marketplace_fee_address": obj['marketplace_fee_address'],
                "marketplace_fee": self.decode_numeric(obj['marketplace_fee']),
                "price": price, # full price
                "asset": "TON", # default
                "royalty_address": obj['royalty_address'],
                "royalty_amount": self.decode_numeric(obj['royalty_amount']),
                "timestamp": obj['last_tx_now'],
                "lt": obj['last_transaction_lt']
            }
        elif table_name == "getgems_nft_auctions":
            price = self.decode_numeric(obj['last_bid'])
            return {
                "address": obj['address'],
                "type": "auction",
                "nft_address": obj['nft_addr'],
                "nft_owner_address": obj['nft_owner'],
                "created_at": obj['created_at'],
                "is_complete": obj['end_flag'],
                "is_canceled": obj['is_canceled'],
                "end_time": min(0x7fffffff, obj['end_time']),
                "marketplace_address": obj['mp_addr'],
                "marketplace_fee_address": obj['mp_fee_addr'],
                "marketplace_fee": decimal.Decimal(round(1.0 * obj['mp_fee_factor'] / obj['mp_fee_base'] * int(price)) if obj['mp_fee_base'] > 0 else 0),
                "price": price, # last_bid
                "asset": "TON", # default
                "royalty_address": obj['royalty_fee_addr'],
                "royalty_amount": decimal.Decimal(round(1.0 * obj['royalty_fee_factor'] / obj['royalty_fee_base'] * int(price)) if obj['royalty_fee_base'] > 0 else 0),
                "max_bid": self.decode_numeric(obj['max_bid']),
                "min_bid": self.decode_numeric(obj['min_bid']),
                "min_step": decimal.Decimal(obj['min_step']),
                "last_bid_at": obj['last_bid_at'],
                "last_member": obj['last_member'],
                "timestamp": obj['last_tx_now'],
                "lt": obj['last_transaction_lt']
            }
        elif table_name == "extra_nft_sales":
            price = self.decode_numeric(obj['full_price'])
            return {
                "address": obj['address'],
                "type": "sale",
                "nft_address": obj['nft_address'],
                "nft_owner_address": obj['nft_owner_address'],
                "created_at": obj['created_at'],
                "is_complete": obj['is_complete'],
                "is_canceled": False,
                "end_time": None,
                "marketplace_address": obj['marketplace_address'],
                "marketplace_fee_address": obj['marketplace_fee_address'],
                "marketplace_fee": self.decode_numeric(obj['marketplace_fee']),
                "price": price, # full price
                "asset": obj['asset'],
                "royalty_address": obj['royalty_address'],
                "royalty_amount": self.decode_numeric(obj['royalty_amount']),
                "timestamp": obj['last_tx_now'],
                "lt": obj['last_transaction_lt']
            }
