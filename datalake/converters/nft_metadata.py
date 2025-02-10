import base64
from typing import List
from topics import TOPIC_NFT_COLLECTIONS_METADATA, TOPIC_NFT_ITEMS_METADATA
from loguru import logger
from pytoniq_core import Cell
from converters.converter import Converter, NumericField


"""
Converts metadata fro both NFT collections and NFT items.
"""
class NFTMetadataConverter(Converter):
    def __init__(self):
        super().__init__("schemas/nft_metadata.avsc", updates_enabled=True)
        
    def timestamp(self, obj):
        # if case of partitioning by timestamp, we should use max of update_time_metadata and update_time_onchain
        return max(obj['update_time_metadata'], obj['update_time_onchain'])
    
    def topics(self) -> List[str]:
        return [TOPIC_NFT_ITEMS_METADATA, TOPIC_NFT_COLLECTIONS_METADATA]

    def convert(self, obj, table_name=None):
        sources_raw = obj['sources'].split(",") if obj['sources'] else [None, None, None, None, None]

        if table_name == "nft_collection_metadata":
            return {
                "type": "collection",
                "address": obj['address'],
                "update_time_onchain": obj['update_time_onchain'],
                "update_time_metadata": obj['update_time_metadata'],
                "parent_address": obj['owner_address'], # collection owner
                "content_onchain": obj['content'],
                "metadata_status": obj['metadata_status'],
                "name": obj['name'],
                "description": obj['description'],
                "image": obj['image'],
                "image_data": obj['image_data'],
                "attributes": None,
                "sources": {
                    "name": sources_raw[0],
                    "description": sources_raw[1],
                    "image": sources_raw[2],
                    "image_data": sources_raw[3],
                    "attributes": None
                },
                "tonapi_image_url": obj['tonapi_image_url']
            }
        elif table_name == "nft_item_metadata":
            return {
                "type": "item",
                "address": obj['address'],
                "update_time_onchain": obj['update_time_onchain'],
                "update_time_metadata": obj['update_time_metadata'],
                "parent_address": obj.get('collection_address', None), # collection address
                "content_onchain": obj['content'],
                "metadata_status": obj['metadata_status'],
                "name": obj['name'],
                "description": obj['description'],
                "image": obj['image'],
                "image_data": obj['image_data'],
                "attributes": obj['attributes'],
                "sources": {
                    "name": sources_raw[0],
                    "description": sources_raw[1],
                    "image": sources_raw[3],
                    "image_data": sources_raw[4],
                    "attributes": sources_raw[2]
                },
                "tonapi_image_url": obj['tonapi_image_url']
            }
