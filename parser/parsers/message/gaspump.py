from model.parser import Parser, TOPIC_MESSAGES
from loguru import logger
from db import DB
from pytoniq_core import Cell, Address
from model.tradoor import TradoorOptionOrderEvent, TradoorPerpOrderEvent
from model.gaspump import GaspumpEvent
from parsers.message.swap_volume import USDT, estimate_volume


"""
Gaspump jettons emit events for buy and sell according to the following TL-B schema:

;;
;; Type: DeployAndBuyEmitEvent
;; Header: 0x67617301
;; TLB: deploy_and_buy_emit_event#67617301 from:address inputTonAmount:coins tonAmount:coins 
;; jettonAmount:coins feeTonAmount:coins bondingCurveOverflow:bool = DeployAndBuyEmitEvent
;;
;;
;; Type: BuyEmitEvent
;; Header: 0x67617302
;; TLB: buy_emit_event#67617302 from:address inputTonAmount:coins tonAmount:coins 
;; jettonAmount:coins feeTonAmount:coins bondingCurveOverflow:bool = BuyEmitEvent
;;
;; Type: SellEmitEvent
;; Header: 0x67617303
;; TLB: sell_emit_event#67617303 from:address tonAmount:coins jettonAmount:coins 
;; feeTonAmount:coins = SellEmitEvent

To control authenticity of the events we are checking code_hash
of the sender smart-contract using the whitelist below
"""

GASPUMP_CODE_HASH_WHITELIST = set([
    '6ox2FGgwSndqH0lxBiss6BMBM2reNTNNKPu5yjNVTRc=',
    'zHJFV4ogLC+2/uN1jCQZBs9IA3xEaUEF5kqRdWjDOlw=',
    'epVAPFfHhSCA8fFWbj5rWnMUNwUah+hGTl0yl3+VaVg=',
    's8xEFRVKnzs0e0+nHdDo1N9iOmaGHMnQexA8lf1elkU=',
    'r5eJZ7VurJARfQResj0zCr6kG6QH7tauAs0F2SGuG6U=',
    'xw8j8tT8ut2DfkUFvG62n14weRqpu/SwiITp5nBt4U4='
])

DEPLOY_AND_BUY_EVENT = Parser.opcode_signed(0x67617301)
BUY_EVENT = Parser.opcode_signed(0x67617302)
SELL_EVENT = Parser.opcode_signed(0x67617303)
EVENTS = [DEPLOY_AND_BUY_EVENT, BUY_EVENT, SELL_EVENT]

class GasPumpTrade(Parser):
    
    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        # only external messages with specific opcodes
        return obj.get("opcode", None) in EVENTS and \
            obj.get("direction", None) == "out" and \
            obj.get("destination", 'None') is None

    def handle_internal(self, obj, db: DB):
        code_hash = Parser.get_account_state_safe(Address(Parser.require(obj.get('source', None))), db)['code_hash']
        if code_hash not in GASPUMP_CODE_HASH_WHITELIST:
            logger.warning("Code hash {} for {} not in whitelist", code_hash, obj.get('source', None))
            return

        cell = Parser.message_body(obj, db).begin_parse()
        opcode = cell.load_uint(32) # 0xad8e31ef
        trader_address = cell.load_address()
        if opcode == DEPLOY_AND_BUY_EVENT or opcode == BUY_EVENT:
            event_type = "DeployAndBuyEmitEvent" if opcode == DEPLOY_AND_BUY_EVENT else "BuyEmitEvent"
            input_ton_amount = cell.load_coins()
            ton_amount = cell.load_coins()
            jetton_amount = cell.load_coins()
            fee_ton_amount = cell.load_coins()
            bonding_curve_overflow = cell.load_uint(1) == 1
        elif opcode == SELL_EVENT:
            event_type = "SellEmitEvent"
            ton_amount = cell.load_coins()
            jetton_amount = cell.load_coins()
            fee_ton_amount = cell.load_coins()
            bonding_curve_overflow = None
            input_ton_amount = None
        else:
            raise Exception(f"Unsupported opcode: {opcode}")
        
        
        ton_price = db.get_core_price(USDT, Parser.require(obj.get('created_at', None)))
        if ton_price is None:
            logger.warning(f"No TON price found for {Parser.require(obj.get('created_at', None))}")
            ton_price = 0
        
        event = GaspumpEvent(
            tx_hash=Parser.require(obj.get('tx_hash', None)),
            trace_id=Parser.require(obj.get('trace_id', None)),
            event_time=Parser.require(obj.get('created_at', None)),
            jetton_master=Parser.require(obj.get('source', None)),
            event_type=event_type,
            trader_address=trader_address,
            input_ton_amount=input_ton_amount,
            ton_amount=ton_amount,
            jetton_amount=jetton_amount,
            fee_ton_amount=fee_ton_amount,
            bonding_curve_overflow=bonding_curve_overflow,
            volume_usd=ton_amount * ton_price / 1e6
        )

        db.serialize(event)
