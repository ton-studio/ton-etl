from dataclasses import dataclass
import decimal

"""
CREATE TABLE parsed.dex_swap_parsed (
	tx_hash bpchar(44) NULL,
	msg_hash bpchar(44) NOT NULL primary key,
	trace_id bpchar(44) NULL,
	platform public.dex_name NULL,
	swap_utime int8 NULL,
	swap_user varchar NULL,
	swap_pool varchar NULL,
	swap_src_token varchar NULL,
	swap_dst_token varchar NULL,
	swap_src_amount numeric NULL,
	swap_dst_amount numeric NULL,
	referral_address varchar NULL,
	reserve0 numeric NULL,
	reserve1 numeric NULL,
	query_id numeric NULL,
	min_out numeric NULL,
    volume_usd numeric NULL,
    volume_ton numeric NULL,
	created timestamp NULL,
	updated timestamp NULL
);
"""

DEX_DEDUST = "dedust"
DEX_STON = "ston.fi"
DEX_STON_V2 = "ston.fi_v2"
DEX_MEGATON = "megaton"
DEX_TONCO = "tonco"
DEX_COFFEE = "coffee"
DEX_BIDASK = "bidask"

@dataclass
class DexSwapParsed:
    __tablename__ = 'dex_swap_parsed'

    tx_hash: str
    msg_hash: str
    trace_id: str
    platform: str
    swap_utime: int
    swap_user: str
    swap_pool: str
    swap_src_token: str
    swap_dst_token: str
    swap_src_amount: decimal.Decimal
    swap_dst_amount: decimal.Decimal
    referral_address: str
    reserve0: decimal.Decimal = None
    reserve1: decimal.Decimal = None
    query_id: decimal.Decimal = None
    min_out: decimal.Decimal = None
    volume_usd: decimal.Decimal = None
    volume_ton: decimal.Decimal = None
    router: str = None
