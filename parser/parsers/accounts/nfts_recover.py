from model.parser import Parser, TOPIC_ACCOUNT_STATES
from loguru import logger
from db import DB
from pytoniq_core import Cell, Address, begin_cell
from pytvm.tvm_emulator.tvm_emulator import TvmEmulator
from parsers.accounts.emulator import EmulatorParser


"""
Goes through all NFTs in states and push it to the nft_items table.
This is special parser that is required if you indexed chain not from the first block
but used ton-smc-scanner to extract all states for some mc seqno and you need to populate
all NFTs as well
"""
class NFTsRecover(EmulatorParser):
    def __init__(self, emulator_path):
        super().__init__(emulator_path)

    # For optimization reason we will parse only NFTs with codes that we already have in the DB
    def prepare(self, db: DB):
        super().prepare(db)
        self.uniq_codes = db.get_uniq_nft_item_codes()
        logger.info(f"Found {len(self.uniq_codes)} unique NFT item codes")

    def predicate(self, obj) -> bool:
        if super().predicate(obj):
            return obj['code_hash'] in self.uniq_codes
        return False
    
    def _do_parse(self, obj, db: DB, emulator: TvmEmulator):
        nft_address = Address(obj['account'])

        init, index, collection_address, \
            owner_address, individual_content = self._execute_method(emulator, 'get_nft_data', [], db, obj)
        
        try:
            collection_address = collection_address.load_address()
        except Exception as e:
            logger.warning(f"Failed to load collection address: {e}")
            return
        if owner_address is not None:
            try:
                owner_address = owner_address.load_address()
            except Exception as e:
                logger.warning(f"Failed to load owner address: {e}")
                return
        # Check that NFT address is not faked, i.e. collection returns the same address for its index
        if collection_address is not None:
            collection_state = Parser.get_account_state_safe(collection_address, db)
            if collection_state is None:
                logger.warning(f"No state for collection {collection_address}")
                return
            if collection_state['data_boc'] is None:
                logger.warning(f"No data boc for collection {collection_address}")
                return
            collection_emulator = self._prepare_emulator(collection_state)
            
            original_address, = self._execute_method(collection_emulator, 'get_nft_address_by_index', [index], db, obj)
            original_address = original_address.load_address()
            if original_address != nft_address:
                logger.warning(f"NFT address mismatch: {original_address} != {nft_address}")
                return

        logger.info(f"New NFT discovered: {nft_address}: {index} {collection_address} {owner_address} {obj['last_trans_lt']}")
        # Ignore contnet at this method, because it requires one more get method invocation
        # db.insert_nft_item(nft_address, index, collection_address, owner_address, obj['last_trans_lt'],
        #                    obj['code_hash'], obj['data_hash'])
