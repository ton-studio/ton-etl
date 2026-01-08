import base64
from dataclasses import dataclass, asdict
import decimal
from typing import List
from topics import (
    TOPIC_DEX_SWAPS,
    TOPIC_GASPUMP_EVENTS,
    TOPIC_TONFUN,
    TOPIC_MEMESLAB,
    TOPIC_URANUS,
)
from loguru import logger
from converters.converter import Converter

"""
Common data model for dex swaps and memepad trades
"""

PLATFORM_TYPE_DEX = "dex"
PLATFORM_TYPE_LAUNCHPAD = "launchpad"

PROJECT_TONFUN = "ton.fun"
PROJECT_GASPUMP = "gaspump"
PROJECT_MEMESLAB = "memeslab"
PROJECT_URANUS = "uranus"

EVENT_TYPE_TRADE = "trade"
EVENT_TYPE_LAUNCH = "launch" # launch from bonding curve

# allows to override internal DEX name
DEX_MAPPING = {
    "ston.fi_v2": "ston.fi"
}

DEX_VERSION_MAPPING = {
    "ston.fi_v2": 2,
    "dedust": 2 # only Protocol 2.0
}

TON_NATIVE_ADDRESS = "0:0000000000000000000000000000000000000000000000000000000000000000"

@dataclass
class Trade:
    tx_hash: str
    trace_id: str
    project_type: str # dex or launchpad
    project: str
    version: int
    event_time: int
    event_type: str # trade or launch
    trader_address: str
    pool_address: str
    router_address: str
    query_id: str # if supported
    token_sold_address: str # token address
    token_bought_address: str
    amount_sold_raw: int # raw amount of token, without decimals
    amount_bought_raw: int
    volume_ton: float
    volume_usd: float
    referral_address: str
    platform_tag: str


class DexTradesConverter(Converter):
    def __init__(self):
        super().__init__("schemas/dex_trades.avsc", ignored_fields=["created", "updated", "id"])

    def timestamp(self, obj):
        if obj['__table'] == "dex_swap_parsed":
            return obj['swap_utime']
        else:
            return obj['event_time']
        
    def topics(self) -> List[str]:
        return [
            TOPIC_DEX_SWAPS,
            TOPIC_GASPUMP_EVENTS,
            TOPIC_TONFUN,
            TOPIC_MEMESLAB,
            TOPIC_URANUS,
        ]

    def convert(self, obj, table_name=None):
        trades = []
        if table_name == "dex_swap_parsed":
            # dex trades
            trades.append(Trade(
                tx_hash=obj['tx_hash'],
                trace_id=obj['trace_id'],
                project_type=PLATFORM_TYPE_DEX,
                project=DEX_MAPPING.get(obj['platform'], obj['platform']),
                version=DEX_VERSION_MAPPING.get(obj['platform'], 1), # default version - 1
                event_time=obj['swap_utime'],
                event_type=EVENT_TYPE_TRADE,
                trader_address=obj['swap_user'],
                pool_address=obj['swap_pool'],
                router_address=obj['router'],
                query_id=self.decode_numeric(obj['query_id']),
                token_sold_address=obj['swap_src_token'],
                token_bought_address=obj['swap_dst_token'],
                amount_sold_raw=self.decode_numeric(obj['swap_src_amount']),
                amount_bought_raw=self.decode_numeric(obj['swap_dst_amount']),
                volume_ton=self.decode_numeric(obj['volume_ton']),
                volume_usd=self.decode_numeric(obj['volume_usd']),
                referral_address=obj['referral_address'],
                platform_tag=None
            ))
        elif table_name == "tonfun_bcl_trade":
            # ton fun trades
            common = {
                'tx_hash': obj['tx_hash'],
                'trace_id': obj['trace_id'],
                'project_type': PLATFORM_TYPE_LAUNCHPAD,
                'project': obj.get('project') or "ton.fun",
                'version': 1,
                'event_time': obj['event_time'],
                'pool_address': obj['bcl_master'],
                'router_address': None, # no router in ton.fun
                'query_id': None, # no query id in ton.fun
                'referral_address': obj['partner_address'],
                'platform_tag': obj['platform_tag']
            }
            if obj['event_type'] == "SendLiq":
                trades.append(Trade(
                    **common,
                    event_type=EVENT_TYPE_LAUNCH,
                    trader_address=None, # doesn't include
                    token_sold_address=None, # N/A
                    token_bought_address=None, # N/A
                    amount_sold_raw=None, # N/A
                    amount_bought_raw=None, # N/A
                    volume_ton=None, # N/A
                    volume_usd=None, # N/A
                ))
            else:
                is_buy = obj['event_type'] == 'Buy'
                trades.append(Trade(
                    **common,
                    event_type=EVENT_TYPE_TRADE,
                    trader_address=obj['trader_address'],
                    token_sold_address=TON_NATIVE_ADDRESS if is_buy else obj['bcl_master'],
                    token_bought_address=obj['bcl_master'] if is_buy else TON_NATIVE_ADDRESS,
                    amount_sold_raw=self.decode_numeric(obj['ton_amount'] if is_buy else obj['bcl_amount']),
                    amount_bought_raw=self.decode_numeric(obj['bcl_amount'] if is_buy else obj['ton_amount']),
                    volume_ton=int(self.decode_numeric(obj['ton_amount'])) / 1e9,
                    volume_usd=self.decode_numeric(obj['volume_usd']),
                ))
        elif table_name == "gaspump_trade":
            # gaspump trades
            is_buy = obj['event_type'] == 'DeployAndBuyEmitEvent' or obj['event_type'] == 'BuyEmitEvent'
            common = {
                'tx_hash': obj['tx_hash'],
                'trace_id': obj['trace_id'],
                'project_type': PLATFORM_TYPE_LAUNCHPAD,
                'project': "gaspump",
                'version': 1,
                'event_time': obj['event_time'],
                'pool_address': obj['jetton_master'],
                'trader_address': obj['trader_address'],
                'router_address': None,
                'query_id': None,
                'referral_address': None,
                'platform_tag': None
            }
            trades.append(Trade(
                **common,
                event_type=EVENT_TYPE_TRADE,
                token_sold_address=TON_NATIVE_ADDRESS if is_buy else obj['jetton_master'],
                token_bought_address=obj['jetton_master'] if is_buy else TON_NATIVE_ADDRESS,
                amount_sold_raw=self.decode_numeric(obj['ton_amount'] if is_buy else obj['jetton_amount']),
                amount_bought_raw=self.decode_numeric(obj['jetton_amount'] if is_buy else obj['ton_amount']),
                volume_ton=int(self.decode_numeric(obj['ton_amount'])) / 1e9,
                volume_usd=self.decode_numeric(obj['volume_usd']),
            ))
            if obj['bonding_curve_overflow']:
                trades.append(Trade(
                    **common,
                    event_type=EVENT_TYPE_LAUNCH,
                    token_sold_address=None, # N/A
                    token_bought_address=None, # N/A
                    amount_sold_raw=None, # N/A
                    amount_bought_raw=None, # N/A
                    volume_ton=None, # N/A
                    volume_usd=None, # N/A
                ))
        elif table_name == "memeslab_trade_event":
            # MemesLab trades
            common = {
                'tx_hash': obj['tx_hash'],
                'trace_id': obj['trace_id'],
                'project_type': PLATFORM_TYPE_LAUNCHPAD,
                'project': PROJECT_MEMESLAB,
                'version': 1,
                'event_time': obj['event_time'],
                'pool_address': obj['jetton_master'],
                'router_address': None, # no router in MemesLab
                'query_id': None, # no query id in MemesLab
                'referral_address': None, # no referral address in MemesLab
                'platform_tag': None, # no platform tag in MemesLab
            }
            if obj['event_type'] == "ListToken":
                trades.append(Trade(
                    **common,
                    event_type=EVENT_TYPE_LAUNCH,
                    trader_address=None, # doesn't include
                    token_sold_address=None, # N/A
                    token_bought_address=None, # N/A
                    amount_sold_raw=None, # N/A
                    amount_bought_raw=None, # N/A
                    volume_ton=None, # N/A
                    volume_usd=None, # N/A
                ))
            else:
                is_buy = obj['event_type'] == 'Buy'
                trades.append(Trade(
                    **common,
                    event_type=EVENT_TYPE_TRADE,
                    trader_address=obj['trader_address'],
                    token_sold_address=TON_NATIVE_ADDRESS if is_buy else obj['jetton_master'],
                    token_bought_address=obj['jetton_master'] if is_buy else TON_NATIVE_ADDRESS,
                    amount_sold_raw=self.decode_numeric(obj['ton_amount'] if is_buy else obj['jetton_amount']),
                    amount_bought_raw=self.decode_numeric(obj['jetton_amount'] if is_buy else obj['ton_amount']),
                    volume_ton=int(self.decode_numeric(obj['ton_amount'])) / 1e9,
                    volume_usd=self.decode_numeric(obj['volume_usd']),
                ))
        elif table_name == "uranus_trade":
            # Uranus memepad trades
            is_buy = obj['event_type'] == 'BuyEvent'
            ton_amount_raw = obj['amount_in'] if is_buy else obj['amount_out']
            common = {
                'tx_hash': obj['tx_hash'],
                'trace_id': obj['trace_id'],
                'project_type': PLATFORM_TYPE_LAUNCHPAD,
                'project': PROJECT_URANUS,
                'version': 1,
                'event_time': obj['event_time'],
                'pool_address': obj['meme_master'],
                'router_address': None,
                'query_id': None,
                'referral_address': None,
                'platform_tag': None,
            }
            trades.append(Trade(
                **common,
                event_type=EVENT_TYPE_TRADE,
                trader_address=obj['trader_address'],
                token_sold_address=TON_NATIVE_ADDRESS if is_buy else obj['meme_master'],
                token_bought_address=obj['meme_master'] if is_buy else TON_NATIVE_ADDRESS,
                amount_sold_raw=self.decode_numeric(obj['amount_in'] if is_buy else obj['amount_out']),
                amount_bought_raw=self.decode_numeric(obj['amount_out'] if is_buy else obj['amount_in']),
                volume_ton=int(self.decode_numeric(ton_amount_raw)) / 1e9,
                volume_usd=self.decode_numeric(obj['volume_usd']),
            ))
            if is_buy:
                if obj.get('is_graduated') is True:
                    trades.append(Trade(
                        **common,
                        event_type=EVENT_TYPE_LAUNCH,
                        trader_address=None,
                        token_sold_address=None,
                        token_bought_address=None,
                        amount_sold_raw=None,
                        amount_bought_raw=None,
                        volume_ton=None,
                        volume_usd=None,
                    ))

        for trade in trades:
            if trade.volume_ton is not None:
                trade.volume_ton = round(decimal.Decimal(trade.volume_ton), 9)
            if trade.volume_usd is not None:
                trade.volume_usd = round(decimal.Decimal(trade.volume_usd), 6)

        return list(map(asdict, trades))
