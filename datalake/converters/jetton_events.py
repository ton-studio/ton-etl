import base64
from typing import List
from topics import TOPIC_JETTON_BURN, TOPIC_JETTON_MINT, TOPIC_JETTON_TRANSFERS
from loguru import logger
from pytoniq_core import Cell
from converters.converter import Converter, NumericField


"""
The converter is designed to handle events from jetton_mint, jetton_burn and jetton_transfers tables.
"""
class JettonEventsConverter(Converter):
    def __init__(self):
        super().__init__("schemas/jetton_events.avsc", numeric_fields=[
            "query_id",
            "forward_ton_amount",
            "amount"
            ], ignored_fields=['updated', 'created', 'msg_hash', 'from_address'])
        
    def timestamp(self, obj):
        if obj['__table'] == "jetton_mint":
            return obj['utime']
        else:
            return obj['tx_now']
        
    def topics(self) -> List[str]:
        return [TOPIC_JETTON_MINT, TOPIC_JETTON_BURN, TOPIC_JETTON_TRANSFERS]

    def convert(self, obj, table_name=None):
        if table_name == "jetton_mint":
            obj['type'] = "mint"
            obj['tx_aborted'] = not obj['successful']
            del obj['successful']

            obj['jetton_wallet'] = obj['wallet']
            del obj['wallet']
            del obj['minter']
            obj['custom_payload'] = None
            # We have from_address inside minter address but it is better to use null for source field
            # to make it possible consider all events in the same way as balance transfers
            obj['source'] = None # obj['from_address']
            obj['destination'] = obj['owner']
            del obj['owner']
        elif table_name == "jetton_burns":
            obj['type'] = "burn"
            obj['utime'] = obj['tx_now']
            del obj['tx_now']
            obj['source'] = obj['owner']
            del obj['owner']
            obj['destination'] = None
            obj['jetton_wallet'] = obj['jetton_wallet_address']
            del obj['jetton_wallet_address']
            obj['destination'] = None
            obj['forward_ton_amount'] = None
            obj['forward_payload'] = None
            obj['comment'] = None
        elif table_name == "jetton_transfers":
            obj['type'] = "transfer"
            obj['utime'] = obj['tx_now']
            del obj['tx_now']
            obj['jetton_wallet'] = obj['jetton_wallet_address']
            del obj['jetton_wallet_address']

        obj['jetton_master'] = obj['jetton_master_address']
        del obj['jetton_master_address']

        if obj['jetton_master'] is None:
            logger.warning(f"Zero jetton master found in {obj}, ignoring")
            return None

        forward_payload = obj.get('forward_payload', None)
        if forward_payload:
            obj['forward_payload'] = base64.b64decode(forward_payload)
            cell = Cell.one_from_boc(base64.b64decode(forward_payload)).begin_parse()
            try:
                obj['comment'] = cell.load_snake_string().replace('\x00', '')
            except Exception as e:
                pass
        # Convert base64 custom_payload into binary
        if obj.get('custom_payload', None):
            obj['custom_payload'] = base64.b64decode(obj['custom_payload'])
        if obj['custom_payload'] == '':
            obj['custom_payload'] = None
        return super().convert(obj, table_name)