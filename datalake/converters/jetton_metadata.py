import base64
from typing import List
from topics import TOPIC_JETTON_METADATA
from loguru import logger
from pytoniq_core import Cell
from converters.converter import Converter, NumericField


"""
The converter is supposed to process metadata stream without partitioning by timestamp,
rather it is recommended to use adding_at partitioning key.
"""
class JettonMetadataConverter(Converter):
    def __init__(self):
        super().__init__("schemas/jetton_metadata.avsc", updates_enabled=True)
        
    def timestamp(self, obj):
        # if case of partitioning by timestamp, we should use max of update_time_metadata and update_time_onchain
        return max(obj['update_time_metadata'], obj['update_time_onchain'])
    
    def topics(self) -> List[str]:
        return [TOPIC_JETTON_METADATA]

    def convert(self, obj, table_name=None):
        sources_raw = obj['sources'].split(",") if obj['sources'] else [None, None, None, None, None, None]
        sources = {
            "symbol": sources_raw[0],
            "name": sources_raw[1],
            "description": sources_raw[2],
            "image": sources_raw[3],
            "image_data": sources_raw[4],
            "decimals": sources_raw[5]
        }
        obj['sources'] = sources
        return super().convert(obj, table_name)