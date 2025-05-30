import json
import os
import time
from loguru import logger
from urllib.parse import urlparse
import requests

from db import DB
from model.parser import TOPIC_NFT_ITEMS, Parser
from model.nft_item_metadata import NFTItemMetadata

OFFCHAIN_UPDATE_TIME_INTERVAL = 86400
OFFCHAIN_UPDATE_STATUS_ERROR = -1
OFFCHAIN_UPDATE_STATUS_OK = 1
OFFCHAIN_UPDATE_STATUS_NO = 0

IPFS_GATEWAY = "https://w3s.link/ipfs/"
DATALAKE_USER_AGENT = "TON DataLake/1.0"

METADATA_ONCHAIN = "onchain"
METADATA_OFFCHAIN = "offchain"
METADATA_TONAPI = "tonapi"

"""
The parser extracts on-chain and off-chain metadata from NFT items and stores it in the database.
"""
class NFTItemMetadataParser(Parser):
    TONAPI_BASE_URL = "https://tonapi.io/v2/nfts"

    def __init__(self, timeout: int = 10, max_attempts: int = 3, tonapi_only_mode: bool = False):
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.tonapi_only_mode = tonapi_only_mode

    def topics(self):
        return [TOPIC_NFT_ITEMS]

    def predicate(self, obj) -> bool:
        return True
    
    def fetch_url(self, url: str):
        parsed_url = urlparse(url)
        retry = 0
        while retry < self.max_attempts:
            try:
                if parsed_url.scheme == 'ipfs':
                    response = requests.get(IPFS_GATEWAY + parsed_url.netloc + parsed_url.path, timeout=self.timeout, 
                                            headers={"User-Agent": DATALAKE_USER_AGENT})
                    if response.status_code != 200:
                        raise Exception(f"Response status_code = {response.status_code}")
                    return response.text
                elif parsed_url.scheme is None or len(parsed_url.scheme) == 0:
                    logger.error(f"No schema for URL: {url}")
                    return None
                else:
                    if parsed_url.netloc == 'localhost':
                        logger.warning(f"Skipping {url}")
                        return None
                    response = requests.get(url, timeout=self.timeout, headers={"User-Agent": DATALAKE_USER_AGENT})
                    if response.status_code != 200:
                        raise Exception(f"Response status_code = {response.status_code}")
                    return response.text
            except Exception as e:
                if "NameResolutionError" in str(e):
                    logger.error(f"NameResolutionError for {url}: {e}")
                    return None
                else:
                    logger.error(f"Unable to fetch data from {url}: {e}")
                    time.sleep(1)
            retry += 1
        return None

    def handle_internal(self, obj: dict, db: DB):
        address = Parser.require(obj.get("address", None))
        metadata = db.get_nft_item_metadata(address)
        prev_ts_onchain = metadata.update_time_onchain if metadata else 0
        prev_ts_offchain = metadata.update_time_metadata if metadata else 0
        created = False
        onchain_updated = False
        offchain_updated = False
        def normalize_json(s):
            try:
                if s and type(s) == str:
                    return json.dumps(json.loads(s))
                if s and type(s) == dict:
                    return json.dumps(s)
            except json.JSONDecodeError:
                pass
            return None

        if metadata:
            logger.info(f"NFT item metadata for {address} already exists")
            if metadata.collection_address != obj.get("collection_address", None):
                onchain_updated = True
                logger.info(f"Collection address has been changed for {address}: {metadata.collection_address} -> {obj.get('collection_address', None)}")
                metadata.collection_address = obj.get("collection_address", None)
            if normalize_json(metadata.content) != normalize_json(obj.get("content", None)):
                onchain_updated = True
                logger.info(f"NFT item content has been changed for {address}: {metadata.content} -> {obj.get('content', None)}")
                metadata.content = obj.get("content", None)
        else:
            logger.info(f"NFT item metadata for {address} does not exist, creating")
            metadata = NFTItemMetadata(
                address=address,
                update_time_onchain=obj.get("last_tx_now", None),
                collection_address=obj.get("collection_address", None),
                content=obj.get("content", None),
            )
            created = True

        if (
            onchain_updated
            or created
            or not metadata.update_time_metadata 
            or metadata.update_time_metadata < time.time() - OFFCHAIN_UPDATE_TIME_INTERVAL
            or metadata.metadata_status == OFFCHAIN_UPDATE_STATUS_ERROR
            or not metadata.tonapi_image_url
        ):
            try:
                content = json.loads(obj.get('content', None))
            except json.JSONDecodeError:
                content = None

            if not content or type(content) is not dict:
                logger.warning(f"NFT item content is not set for {address}")
                metadata.metadata_status = OFFCHAIN_UPDATE_STATUS_NO
            else:
                if "attributes" in content and type(content["attributes"]) is str:
                    try:
                        content["attributes"] = json.loads(content["attributes"].replace("'", '"'))
                    except Exception:
                        content["attributes"] = None
                # track sources of metadata
                sources = []
                def append_onchain_metadata(key):
                    value = content.get(key, None)
                    if value:
                        sources.append(METADATA_ONCHAIN)
                    else:
                        sources.append("")
                    return value

                name = append_onchain_metadata('name')
                description = append_onchain_metadata('description')
                attributes = append_onchain_metadata('attributes')
                image = append_onchain_metadata('image')
                image_data = append_onchain_metadata('image_data')
                
                uri = content.get('uri', None)

                def update_metadata(index, obj, key, prev, source=METADATA_OFFCHAIN):
                    if sources[index] == "" and obj.get(key, None):
                        value = obj.get(key, None)
                        try:
                            if isinstance(value, str):
                                value = value.replace("\x00", "")
                            elif isinstance(value, (dict, list)):
                                value = json.dumps(value)
                            else:
                                value = str(value)
                        except Exception:
                            value = None
                        logger.info(f"Using {source} {key}: {value}")
                        sources[index] = source
                        return value
                    else:
                        return prev
                if uri:
                    if not self.tonapi_only_mode:
                        logger.info(f"Updating offchain metadata for {address}: {uri}")
                        try:
                            offchain_metadata = json.loads(self.fetch_url(uri))
                            logger.info(f"Offchain metadata for {address}: {offchain_metadata}")
                            name = update_metadata(0, offchain_metadata, "name", name, METADATA_OFFCHAIN)
                            description = update_metadata(1, offchain_metadata, "description", description, METADATA_OFFCHAIN)
                            attributes = update_metadata(2, offchain_metadata, "attributes", attributes, METADATA_OFFCHAIN)
                            image = update_metadata(3, offchain_metadata, "image", image, METADATA_OFFCHAIN)
                            image_data = update_metadata(4, offchain_metadata, "image_data", image_data, METADATA_OFFCHAIN)
                            metadata.metadata_status = OFFCHAIN_UPDATE_STATUS_OK
                        except Exception as e:
                            logger.error(f"Error updating offchain metadata for {address}: {e}")
                            metadata.metadata_status = OFFCHAIN_UPDATE_STATUS_ERROR
                    if self.tonapi_only_mode or metadata.metadata_status == OFFCHAIN_UPDATE_STATUS_ERROR:
                        try:
                            retry_delay = 0.1
                            timeout = self.timeout
                            while True:
                                logger.info(f"Trying to get metadata from TonAPI for {address}")
                                try:
                                    tonapi_response = requests.get(f"{self.TONAPI_BASE_URL}/{address}", timeout=timeout, headers={
                                        "User-Agent": DATALAKE_USER_AGENT,
                                        "Authorization": 'Bearer %s' % os.getenv("TONAPI_API_KEY")
                                        })
                                except Exception as e:
                                    logger.warning(f"Error getting metadata from TonAPI for {address}: {e}. Retrying after {retry_delay}s!")
                                    time.sleep(retry_delay)
                                    retry_delay *= 2
                                    timeout = int(timeout * 1.5)
                                    continue
                                if tonapi_response.status_code == 429:
                                    logger.warning(f"TonAPI response status_code = 429 (Too Many Requests) for {address}. Retrying after {retry_delay}s!")
                                    time.sleep(retry_delay)
                                    retry_delay *= 2
                                    continue
                                if tonapi_response.status_code != 200:
                                    raise Exception(f"Response status_code = {tonapi_response.status_code}")
                                logger.info(f"TonAPI response for {address}: {tonapi_response.json()}")
                                tonapi_metadata = tonapi_response.json().get("metadata", None)
                                name = update_metadata(0, tonapi_metadata, "name", name, METADATA_TONAPI)
                                description = update_metadata(1, tonapi_metadata, "description", description, METADATA_TONAPI)
                                attributes = update_metadata(2, tonapi_metadata, "attributes", attributes, METADATA_TONAPI)
                                image = update_metadata(3, tonapi_metadata, "image", image, METADATA_TONAPI)
                                previews = tonapi_response.json().get("previews", [])
                                for preview in previews:
                                    if preview.get("resolution") == "500x500":
                                        metadata.tonapi_image_url = preview.get("url")
                                        break
                                metadata.metadata_status = OFFCHAIN_UPDATE_STATUS_OK
                                logger.info(f"TonAPI image url for {address}: {metadata.tonapi_image_url}")
                                break
                        except Exception as e:
                            logger.error(f"Error getting metadata from TonAPI for {address}: {e}")
                            metadata.metadata_status = OFFCHAIN_UPDATE_STATUS_ERROR
                else:
                    logger.warning(f"URI is not set for {address}")
                    metadata.metadata_status = OFFCHAIN_UPDATE_STATUS_NO
                if metadata.tonapi_image_url is None:
                    logger.info(f"Updating tonapi image url for {address}")
                    try:
                        tonapi_response = requests.get(f"{self.TONAPI_BASE_URL}/{address}", timeout=self.timeout, headers={
                                    "User-Agent": DATALAKE_USER_AGENT,
                                    "Authorization": 'Bearer %s' % os.getenv("TONAPI_API_KEY")
                                    })
                        if tonapi_response.status_code != 200:
                            raise Exception(f"Response status_code = {tonapi_response.status_code}")
                        previews = tonapi_response.json().get("previews", [])
                        for preview in previews:
                            if preview.get("resolution") == "500x500":
                                metadata.tonapi_image_url = preview.get("url")
                                break
                        logger.info(f"TonAPI image url for {address}: {metadata.tonapi_image_url}")
                    except Exception as e:
                        logger.error(f"Error getting tonapi image url for {address}: {e}")
                if name:
                    metadata.name = name
                if description:
                    metadata.description = description
                if attributes:
                    metadata.attributes = attributes
                if image:
                    metadata.image = image
                if image_data:
                    metadata.image_data = image_data
                metadata.sources = ",".join(sources)

            metadata.update_time_metadata = time.time()
            offchain_updated = True

        if onchain_updated:
            metadata.update_time_onchain=obj.get("last_tx_now", None)

        if onchain_updated or offchain_updated:
            logger.info(f"Upserting NFT item metadata for {address}")
            db.upsert_nft_item_metadata(metadata, prev_ts_onchain, prev_ts_offchain)


class TestnetNFTItemMetadataParser(NFTItemMetadataParser):
    TONAPI_BASE_URL = "https://testnet.tonapi.io/v2/nfts"
