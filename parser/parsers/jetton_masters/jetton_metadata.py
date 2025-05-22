import json
import os
import time
from loguru import logger
from urllib.parse import urlparse
import requests

from db import DB
from model.parser import TOPIC_JETTON_MASTERS, Parser, TOPIC_JETTON_WALLETS
from model.jetton_metadata import JettonMetadata

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
The parser extracts on-chain and off-chainmetadata from jetton masters and stores it in the database.
"""
class JettonMastersMetadataParser(Parser):
    TONAPI_BASE_URL = "https://tonapi.io/v2/jettons"

    def __init__(self, timeout: int = 10, max_attempts: int = 3):
        self.timeout = timeout
        self.max_attempts = max_attempts

    def topics(self):
        return [TOPIC_JETTON_MASTERS]

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
                logger.error(f"Unable to fetch data from {url}: {e}")
                time.sleep(1)
            retry += 1
        return None

    def handle_internal(self, obj: dict, db: DB):
        address = Parser.require(obj.get("address", None))
        metadata = db.get_jetton_metadata(address)
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
            logger.info(f"Jetton metadata for {address} already exists")
            if metadata.mintable != obj.get("mintable", None):
                onchain_updated = True
                logger.info(f"Mintable flag has been changed for {address}: {metadata.mintable} -> {obj.get('mintable', None)}")
                metadata.mintable = obj.get("mintable", None)
            if metadata.admin_address != obj.get("admin_address", None):
                onchain_updated = True
                logger.info(f"Admin address has been changed for {address}: {metadata.admin_address} -> {obj.get('admin_address', None)}")
                metadata.admin_address = obj.get("admin_address", None)
            if normalize_json(metadata.jetton_content_onchain) != normalize_json(obj.get("jetton_content", None)):
                onchain_updated = True
                logger.info(f"Jetton content has been changed for {address}: {metadata.jetton_content_onchain} -> {obj.get('jetton_content', None)}")
                metadata.jetton_content_onchain = obj.get("jetton_content", None)
            if metadata.jetton_wallet_code_hash != obj.get("jetton_wallet_code_hash", None):
                onchain_updated = True
                logger.info(f"Jetton wallet code hash has been changed for {address}: {metadata.jetton_wallet_code_hash} -> {obj.get('jetton_wallet_code_hash', None)}")
                metadata.jetton_wallet_code_hash = obj.get("jetton_wallet_code_hash", None)
            if metadata.code_hash != obj.get("code_hash", None):
                onchain_updated = True
                logger.info(f"Code hash has been changed for {address}: {metadata.code_hash} -> {obj.get('code_hash', None)}")
                metadata.code_hash = obj.get("code_hash", None)
        else:
            logger.info(f"Jetton metadata for {address} does not exist, creating")
            metadata = JettonMetadata(
                address=address,
                update_time_onchain=obj.get("last_tx_now", None),
                mintable=obj.get("mintable", None),
                admin_address=obj.get("admin_address", None),
                jetton_content_onchain=obj.get("jetton_content", None),
                jetton_wallet_code_hash=obj.get("jetton_wallet_code_hash", None),
                code_hash=obj.get("code_hash", None)
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
                jetton_content = json.loads(obj.get('jetton_content', None))
            except json.JSONDecodeError:
                jetton_content = None

            if not jetton_content or type(jetton_content) is not dict:
                logger.warning(f"Jetton content is not set for {address}")
                metadata.metadata_status = OFFCHAIN_UPDATE_STATUS_NO
            else:
                # track sources of metadata
                sources = []
                def append_onchain_metadata(key):
                    value = jetton_content.get(key, None)
                    if value:
                        sources.append(METADATA_ONCHAIN)
                    else:
                        sources.append("")
                    return value

                symbol = append_onchain_metadata('symbol')
                name = append_onchain_metadata('name')
                description = append_onchain_metadata('description')
                image = append_onchain_metadata('image')
                image_data = append_onchain_metadata('image_data')
                decimals = append_onchain_metadata('decimals')
                
                uri = jetton_content.get('uri', None)

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
                    logger.info(f"Updating offchain metadata for {address}: {uri}")
                    try:
                        offchain_metadata = json.loads(self.fetch_url(uri))
                        logger.info(f"Offchain metadata for {address}: {offchain_metadata}")
                        symbol = update_metadata(0, offchain_metadata, "symbol", symbol, METADATA_OFFCHAIN)
                        name = update_metadata(1, offchain_metadata, "name", name, METADATA_OFFCHAIN)
                        description = update_metadata(2, offchain_metadata, "description", description, METADATA_OFFCHAIN)
                        image = update_metadata(3, offchain_metadata, "image", image, METADATA_OFFCHAIN)
                        image_data = update_metadata(4, offchain_metadata, "image_data", image_data, METADATA_OFFCHAIN)
                        decimals = update_metadata(5, offchain_metadata, "decimals", decimals, METADATA_OFFCHAIN)
                        metadata.metadata_status = OFFCHAIN_UPDATE_STATUS_OK
                    except Exception as e:
                        logger.error(f"Error updating offchain metadata for {address}: {e}")
                        try:
                            logger.info(f"Trying to get metadata from TonAPI for {address}")
                            tonapi_response = requests.get(f"{self.TONAPI_BASE_URL}/{address}", timeout=self.timeout, headers={
                                "User-Agent": DATALAKE_USER_AGENT,
                                "Authorization": 'Bearer %s' % os.getenv("TONAPI_API_KEY")
                                })
                            if tonapi_response.status_code != 200:
                                raise Exception(f"Response status_code = {tonapi_response.status_code}")
                            logger.info(f"TonAPI response for {address}: {tonapi_response.json()}")
                            tonapi_metadata = tonapi_response.json().get("metadata", None)
                            symbol = update_metadata(0, tonapi_metadata, "symbol", symbol, METADATA_TONAPI)
                            name = update_metadata(1, tonapi_metadata, "name", name, METADATA_TONAPI)
                            description = update_metadata(2, tonapi_metadata, "description", description, METADATA_TONAPI)
                            image = update_metadata(3, tonapi_metadata, "image", image, METADATA_TONAPI)
                            decimals = update_metadata(5, tonapi_metadata, "decimals", decimals, METADATA_TONAPI)
                            metadata.metadata_status = OFFCHAIN_UPDATE_STATUS_OK
                        except Exception as e2:
                            logger.error(f"Error getting metadata from TonAPI for {address}: {e2}")
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
                        metadata.tonapi_image_url = tonapi_response.json().get("preview", None)
                        logger.info(f"TonAPI image url for {address}: {metadata.tonapi_image_url}")
                    except Exception as e:
                        logger.error(f"Error getting tonapi image url for {address}: {e}")
                if symbol:
                    metadata.symbol = symbol
                if name:
                    metadata.name = name
                if description:
                    metadata.description = description
                if image:
                    metadata.image = image
                if image_data:
                    metadata.image_data = image_data
                if decimals:
                    try:
                        decimals = int(decimals)
                        if decimals >= 0 and decimals < 255:
                            metadata.decimals = decimals
                        else:
                            logger.error(f"Decimals are out of range for {address}: {decimals}")
                    except Exception as e:
                        logger.error(f"Error parsing decimals for {address}: {e}")
                metadata.sources = ",".join(sources)

            metadata.update_time_metadata = time.time()
            offchain_updated = True

        if onchain_updated:
            metadata.update_time_onchain=obj.get("last_tx_now", None)

        if onchain_updated or offchain_updated:
            logger.info(f"Upserting jetton metadata for {address}")
            db.upsert_jetton_metadata(metadata, prev_ts_onchain, prev_ts_offchain)


class TestnetJettonMastersMetadataParser(JettonMastersMetadataParser):
    TONAPI_BASE_URL = "https://testnet.tonapi.io/v2/jettons"
