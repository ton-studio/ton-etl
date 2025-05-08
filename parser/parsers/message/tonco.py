from model.parser import NonCriticalParserError, Parser, TOPIC_MESSAGES
from loguru import logger
from db import DB
from pytoniq_core import Cell, Address
from model.dexswap import DEX_STON, DEX_STON_V2, DEX_TONCO, DexSwapParsed
from parsers.message.swap_volume import estimate_volume

"""
TONCO CLMM DEX swap parsing based on https://docs.tonco.io/technical-reference/contracts/scenarios
The parser processes ROUTERV3_PAY_TO messages from the pool to the router. If the messages
processed successfully it means the swap was successful.
"""
ROUTER = Parser.uf2raw('EQC_-t0nCnOFMdp7E7qPxAOCbCWGFz-e3pwxb6tTvFmshjt5')

class TONCOSwap(Parser):
    
    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        # only internal messages sent to the router
        return obj.get("opcode", None) == Parser.opcode_signed(0xa1daa96d) and \
            obj.get("direction", None) == "in" and \
            obj.get("destination", None) == ROUTER
    

    def handle_internal(self, obj, db: DB):
        tx_hash = Parser.require(obj.get('tx_hash', None))
        tx = Parser.require(db.is_tx_successful(tx_hash))
        if not tx:
            logger.info(f"Skipping failed tx for {tx_hash}")
            return
        # raise
        cell = Parser.message_body(obj, db).begin_parse()
        cell.load_uint(32) # 0xa1daa96d
        query_id = cell.load_uint(64)
        owner0 = cell.load_address()
        owner1 = cell.load_address() # should be the same as owner0
        exit_code = cell.load_uint(32)
        if exit_code != 200:
            logger.warning(f"Ignoring tonco message with exit code: {exit_code} {tx_hash}")
            return
        seqno = cell.load_uint(64)
        coinsinfo_cell = cell.load_maybe_ref()
        logger.info(f"query_id: {query_id}, owner0: {owner0}, owner1: {owner1}, exit_code: {exit_code}, seqno: {seqno}")
        if coinsinfo_cell:
            coinsinfo_cell = coinsinfo_cell.begin_parse()
            amount0 = coinsinfo_cell.load_coins()
            jetton0_address = coinsinfo_cell.load_address()
            amount1 = coinsinfo_cell.load_coins()
            jetton1_address = coinsinfo_cell.load_address()
            logger.info(f"amount0: {amount0}, jetton0_address: {jetton0_address}, amount1: {amount1}, jetton1_address: {jetton1_address}")
        else:
            logger.info(f"No coinsinfo_cell for {tx_hash}")
            return
        if amount0 != 0 and amount1 != 0:
            logger.info(f"Ignoring swap with non-zero amounts {amount0} {amount1} {tx_hash}")
            return
        
        jetton0_master = db.get_wallet_master(jetton0_address)
        jetton1_master = db.get_wallet_master(jetton1_address)
        if not jetton0_master:
            logger.warning(f"Wallet not found for {jetton0_address} {tx_hash}")
            return
        if not jetton1_master:
            logger.warning(f"Wallet not found for {jetton1_address} {tx_hash}")
            return
        
        try:
            pool_swap = Cell.one_from_boc(db.get_parent_message_body(obj.get('msg_hash'))).begin_parse()
        except TypeError as e:
            if self.IGNORE_MISSING_PARENT_MESSAGE_BODY:
                raise NonCriticalParserError(f"Parent message's body was not found for msg_hash={obj.get('msg_hash')}")
            raise e
        swap_op = pool_swap.load_uint(32)
        if swap_op != 0xa7fb58f8:
            logger.warning(f"Parent message for tonco swap is {swap_op}, expected 0xa7fb58f8 {tx_hash}")
            return
        swap_query_id = pool_swap.load_uint(64)
        swap_owner_address = pool_swap.load_address()
        swap_source_wallet = pool_swap.load_address()
        swap_ref1 = pool_swap.load_ref().begin_parse()
        swap_input_amount = swap_ref1.load_coins()
        swap_ref1.load_uint(160) # sqrtPriceLimitX96
        min_out = swap_ref1.load_coins()
        
        logger.info(f"swap_query_id: {swap_query_id}, swap_owner_address: {swap_owner_address}, swap_source_wallet: {swap_source_wallet}, swap_input_amount: {swap_input_amount}")
        if swap_query_id != query_id:
            logger.warning(f"Query id mismatch: {query_id} {swap_query_id} {tx_hash}")
            return
        
        src_amount = swap_input_amount
        if swap_source_wallet == jetton0_address:
            src_master = jetton0_master
            dst_master = jetton1_master
            dst_amount = amount1
        elif swap_source_wallet == jetton1_address:
            src_master = jetton1_master
            dst_master = jetton0_master
            dst_amount = amount0

        swap = DexSwapParsed(
            tx_hash=Parser.require(obj.get('tx_hash', None)),
            msg_hash=Parser.require(obj.get('msg_hash', None)),
            trace_id=Parser.require(obj.get('trace_id', None)),
            platform=DEX_TONCO,
            swap_utime=Parser.require(obj.get('created_at', None)),
            swap_user=owner0,
            swap_pool=Parser.require(obj.get('source', None)),
            swap_src_token=src_master,
            swap_dst_token=dst_master,
            swap_src_amount=src_amount,
            swap_dst_amount=dst_amount,
            referral_address=None,
            query_id=query_id,
            min_out=min_out,
            router=Parser.require(obj.get("destination", None))
        )
        estimate_volume(swap, db)
        logger.info(swap)
        db.serialize(swap)
        db.discover_dex_pool(swap)
