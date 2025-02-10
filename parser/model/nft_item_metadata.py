from dataclasses import dataclass


@dataclass
class NFTItemMetadata:
    __tablename__ = "nft_item_metadata"
    __schema__ = "parsed"

    address: str
    update_time_onchain: int
    collection_address: str
    content: str
    metadata_status: int = 0
    name: str = None
    description: str = None
    attributes: str = None
    image: str = None
    image_data: str = None
    sources: str = None
    tonapi_image_url: str = None
    update_time_metadata: int = None
