
#!/usr/bin/env python

import os
from typing import Any, Dict
from pytoniq_core import Address, Cell
from db import DB
import requests
from loguru import logger

TOPIC_MESSAGES = "ton.public.messages"
TOPIC_MESSAGE_CONTENTS = "ton.public.message_contents"
TOPIC_ACCOUNT_STATES = "ton.public.latest_account_states"
TOPIC_JETTON_TRANSFERS = "ton.public.jetton_transfers"
TOPIC_NFT_TRANSFERS = "ton.public.nft_transfers"
TOPIC_DEX_SWAPS = "ton.parsed.dex_swap_parsed"
TOPIC_JETTON_WALLETS = "ton.public.jetton_wallets"
TOPIC_JETTON_MASTERS = "ton.public.jetton_masters"
TOPIC_NFT_ITEMS = "ton.parsed.nft_items"
TOPIC_NFT_COLLECTIONS = "ton.public.nft_collections"

"""
Base class for any kind of errors during parsing that are not critical
and meant to be ignored. For example data format is broken and we aware of 
it and not going to stop parsing.
"""
class NonCriticalParserError(Exception):
    pass

ACCOUNT_STATE_CACHE: Dict[str, Dict[str, Any]] = {}

"""
Base class for parser
"""
class Parser:

    """
    Original ton-index-worker writes all bodies into message_contents table.
    In datalake mode we don't use it and all message bodies are stored in the
    same table with messages.
    """
    USE_MESSAGE_CONTENT = int(os.environ.get("USE_MESSAGE_CONTENT", '0')) == 1

    """
    To be invoked before starting parser with the DB instance
    """
    def prepare(self, db: DB):
        pass
    """
    Returns list of the topics this parser is able to handle data from
    """
    def topics(self):
        raise Exception("Not implemented")
    
    """
    Check is this object could be processed by the parser
    """
    def predicate(self, obj) -> bool:
        raise Exception("Not implemented")
    
    """
    Handles the object that passed predicate check
    """
    def handle_internal(self, obj, db: DB):
        raise Exception("Not implemented")
    
    def handle(self, obj, db: DB):
        if self.predicate(obj):
            try:
                self.handle_internal(obj, db)
                return True
            except NonCriticalParserError as e:
                print(f"Non critical error during handling object {obj}: {e}")
                return False
        return False
    
    """
    Helper method to convert uint values to int
    """
    @classmethod
    def opcode_signed(clz, opcode):
        return opcode if opcode < 0x80000000 else -1 * (0x100000000 - opcode)
    
    """
    Converts user friendly address to raw format
    """
    @classmethod
    def uf2raw(clz, addr):
        return  Address(addr).to_str(is_user_friendly=False).upper()
    
    """
    Returns non-null values or raises exception otherwise
    """
    @classmethod
    def require(clz, value, msg="Value is null"):
        if value is None:
            raise Exception(msg)
        return value
    
    """
    Extract message body from DB and return parsed cell
    """
    @classmethod
    def message_body(clz, obj, db: DB) -> Cell:
        body = db.get_message_body(obj.get('body_hash')) if Parser.USE_MESSAGE_CONTENT else obj.get('body_boc')
        return Cell.one_from_boc(Parser.require(body))
    
    """
    If we are indexing not from scratch, we can encounter some missing account required for the parser.
    For example, we can have NFT item to parse but missing the account state for the collection.
    To overcome this we can use additional step to fetch states using RPC (toncenter) and cache it to minimize RPC calls.
    """
    @classmethod    
    def get_account_state_safe(clz, address: Address, db: DB):
        res = db.get_latest_account_state(address)
        if res:
            return res

        if address in ACCOUNT_STATE_CACHE:
            return ACCOUNT_STATE_CACHE[address]
        
        logger.info(f"Fetching account state from toncenter RPC for {address}")
        
        res = requests.get(f"https://toncenter.com/api/v3/accountStates?address={address.to_str(is_user_friendly=False)}")
        if res.status_code != 200:
            raise Exception(f"Failed to fetch account state from toncenter RPC for {address}")
        
        result = res.json()
        for account in result['accounts']:
            if Address(account['address']) == address:
                logger.info(f"Found account state for {address} in toncenter RPC")
                account_state = {
                    'account': address.to_str(is_user_friendly=False).upper(),
                    'code_boc': account['code_boc'],
                    'data_boc': account['data_boc'],
                }
                ACCOUNT_STATE_CACHE[address] = account_state
                return account_state
        
        logger.warning(f"No account state found for {address} in toncenter RPC")
        return None
