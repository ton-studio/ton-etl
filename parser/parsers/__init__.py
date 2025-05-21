from typing import Dict, List, Set
from parsers.accounts.nfts_parser import NFTItemsParser
from parsers.accounts.staking_pools import StakingPoolsParser
from parsers.message.tonco import TONCOSwap
from parsers.jetton_transfer.megaton import MegatonDexSwap
from parsers.message.tonfun import TonFunTrade
from parsers.jetton_masters.jetton_metadata import JettonMastersMetadataParser
from parsers.message.stonfi_swap_v2 import StonfiSwapV2
from parsers.message.gaspump import GasPumpTrade
from parsers.accounts.tvl import TVLPoolStateParser
from parsers.accounts.jetton_wallets_recover import JettonWalletsRecover
from parsers.accounts.nfts_recover import NFTsRecover
from parsers.message_contents.decode_comment import CommentsDecoder
from parsers.accounts.core_prices import CorePricesHipoTON, CorePricesLSDstTON, CorePricesLSDtsTON, CorePricesStormTrade, CorePricesUSDT
from parsers.message.dedust_swap import DedustSwap
from parsers.message.stonfi_swap import StonfiSwap, TestnetStonfiSwap
from parsers.message.jetton_mint import JettonMintParser, HipoTokensMinted
from parsers.nft_transfer.nft_history import NftHistoryParser
from parsers.nft_items.nft_item_metadata import NFTItemMetadataParser
from parsers.nft_collections.nft_collection_metadata import NFTCollectionMetadataParser
from parsers.message.memeslab import MemesLabTrade
from model.parser import Parser
from loguru import logger
import os

EMULATOR_PATH = os.environ.get("EMULATOR_LIBRARY")
METADATA_FETCH_TIMEOUT = int(os.environ.get("METADATA_FETCH_TIMEOUT", "10"))
METADATA_FETCH_MAX_ATTEMPTS = int(os.environ.get("METADATA_FETCH_MAX_ATTEMPTS", "3"))
TONAPI_ONLY_MODE = os.environ.get("TONAPI_ONLY_MODE", "0").lower() in ('true', '1')
TESTNET_MODE = int(os.environ.get("TESTNET_MODE", "0"))

_mainnet_parsers = [
    NftHistoryParser(),

    # DEX trades
    DedustSwap(EMULATOR_PATH), 
    StonfiSwap(),
    StonfiSwapV2(),
    MegatonDexSwap(),
    TonFunTrade(),
    GasPumpTrade(),
    TONCOSwap(),
    MemesLabTrade(),
    JettonMintParser(),
    HipoTokensMinted(),

    CorePricesUSDT(),
    CorePricesLSDstTON(),
    CorePricesLSDtsTON(),
    CorePricesHipoTON(EMULATOR_PATH),
    # TON Vault
    CorePricesStormTrade(EMULATOR_PATH, Parser.uf2raw('EQDpJnZP89Jyxz3euDaXXFUhwCWtaOeRmiUJTi3jGYgF8fnj'),
                          Parser.uf2raw('EQCNY2AQ3ZDYwJAqx_nzl9i9Xhd_Ex7izKJM6JTxXRnO6n1F')),
    # NOT Vault
    CorePricesStormTrade(EMULATOR_PATH, Parser.uf2raw('EQAG8_BzwlWkmqb9zImr9RJjjgZZCLMOQXP9PR0B1PYHvfSS'),
                         Parser.uf2raw('EQAqtjTPy9vjuXUDaR4HgMPQfJBpIQorJLmRuk-ZQbRT-eiY')),
    # USDT Vault
    CorePricesStormTrade(EMULATOR_PATH, Parser.uf2raw('EQAz6ehNfL7_8NI7OVh1Qg46HsuC4kFpK-icfqK9J3Frd6CJ'),
                         Parser.uf2raw('EQCup4xxCulCcNwmOocM9HtDYPU8xe0449tQLp6a-5BLEegW')),
    TVLPoolStateParser(EMULATOR_PATH),
    StakingPoolsParser(EMULATOR_PATH),

    NFTsRecover(EMULATOR_PATH),
    JettonWalletsRecover(EMULATOR_PATH),
    NFTItemsParser(EMULATOR_PATH),
    
    CommentsDecoder(),

    JettonMastersMetadataParser(METADATA_FETCH_TIMEOUT, METADATA_FETCH_MAX_ATTEMPTS),

    NFTItemMetadataParser(METADATA_FETCH_TIMEOUT, METADATA_FETCH_MAX_ATTEMPTS, TONAPI_ONLY_MODE),
    NFTCollectionMetadataParser(METADATA_FETCH_TIMEOUT, METADATA_FETCH_MAX_ATTEMPTS, TONAPI_ONLY_MODE)
]

_testnet_parsers = [
    TestnetStonfiSwap()
]

_parsers = _testnet_parsers if TESTNET_MODE else _mainnet_parsers

"""
dict of parsers, where key is the topic name
"""
def generate_parsers(names: Set)-> Dict[str, List[Parser]]: 
    out: Dict[str, List[Parser]] = {}

    for parser in _parsers:
        if names is not None:
            if type(parser).__name__ not in names:
                logger.info(f"Skipping parser {parser}, it is not in supported parsers list")
                continue
            else:
                logger.info(f"Adding parser {parser}: {type(parser).__name__}, {names}")
        for topic in parser.topics():
            if topic not in out:
                out[topic] = []
            out[topic].append(parser)
    return out
