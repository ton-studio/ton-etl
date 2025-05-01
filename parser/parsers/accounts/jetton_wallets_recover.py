from model.parser import Parser, TOPIC_ACCOUNT_STATES
from loguru import logger
from db import DB
from pytoniq_core import Cell, Address, begin_cell
from pytvm.tvm_emulator.tvm_emulator import TvmEmulator
from parsers.accounts.emulator import EmulatorParser


"""
Goes through all jetton wallets in states and push it to the jetton_wallet table.
This is special parser that is required if you indexed chain not from the first block
but used ton-smc-scanner to extract all states for some mc seqno and you need to populate
all jetton wallets as well
"""
class JettonWalletsRecover(EmulatorParser):
    def __init__(self, emulator_path):
        super().__init__(emulator_path)

    # For optimization reason we will parse only jetton wallets with codes that we already have in the DB
    def prepare(self, db: DB):
        super().prepare(db)
        self.uniq_codes = db.get_uniq_jetton_wallets_codes()
        logger.info(f"Found {len(self.uniq_codes)} unique jetton wallets codes")

    def predicate(self, obj) -> bool:
        if super().predicate(obj):
            return obj['code_hash'] in self.uniq_codes
        return False
    
    def _do_parse(self, obj, db: DB, emulator: TvmEmulator):
        wallet_address = Address(obj['account'])

        balance, owner, jetton, _ = self._execute_method(emulator, 'get_wallet_data', [], db, obj)
        
        jetton = jetton.load_address()
        if jetton is None:
            logger.warning(f"Jetton address is None for {wallet_address}")
            return
        # Check that jetton wallets is not faked, i.e. master returns the same address for its owner
        master_state = Parser.get_account_state_safe(jetton, db)
        if master_state is None:
            logger.warning(f"No state for jetton master {jetton}")
            return
        if master_state['data_boc'] is None:
            logger.warning(f"No data boc for jetton master {jetton}")
            return
        master_emulator = self._prepare_emulator(master_state)
        
        original_address, = self._execute_method(master_emulator, 'get_wallet_address', [owner], db, obj)
        if original_address is None:
            logger.warning(f"No original address for jetton master {jetton}")
            return
        original_address = original_address.load_address()
        if original_address != wallet_address:
            logger.warning(f"Jetton wallet address mismatch: {original_address} != {wallet_address}")
            return

        logger.info(f"New jetton_wallet discovered: {wallet_address}")
        db.insert_jetton_wallet(wallet_address, balance, owner.load_address(), jetton, obj['last_trans_lt'],
                           obj['code_hash'], obj['data_hash'])
