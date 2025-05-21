import traceback
from model.parser import Parser, TOPIC_MESSAGES
from loguru import logger
from db import DB
from pytoniq_core import Cell, Address
from model.dexswap import DEX_STON, DexSwapParsed
from parsers.message.swap_volume import estimate_volume


class StonfiSwap(Parser):
    STONFI_ROUTER = Parser.uf2raw('EQB3ncyBUTjZUA5EnFKR5_EnOMI9V1tTEAAPaiU71gc4TiUt')
    
    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        # only internal messages processed by the router
        return obj.get("opcode", None) == Parser.opcode_signed(0xf93bb43f) and \
            obj.get("direction", None) == "in" and \
            obj.get("destination", None) == self.STONFI_ROUTER
    

    def handle_internal(self, obj, db: DB):
        cell = Parser.message_body(obj, db).begin_parse()
        cell.load_uint(32) # 0xf93bb43f
        query_id = cell.load_uint(64)
        # fix for rare case of wrong owner address
        try:
            owner = cell.load_address()
        except Exception as e:
            logger.error(f"Failed to load owner address: {e} {traceback.format_exc()}")
            return

        exit_code = cell.load_uint(32)
        params = cell.load_ref().begin_parse()
        token0_amount = params.load_coins()
        wallet0_address = params.load_address()
        token1_amount = params.load_coins()
        wallet1_address = params.load_address()
        logger.info(f"{owner} {exit_code} {token0_amount} {wallet0_address} {token1_amount} {wallet1_address}")

        if exit_code != 3326308581:
            logger.debug(f"Message is not a payment to user, exit code {exit_code}")
            return


        # if token0_amount == 0 or token1_amount == 0:
        #     logger.info(f"Skipping zero amount swap for {obj}")
        #     return

        # TODO Handle missing tx? is it ever possible?
        tx = Parser.require(db.is_tx_successful(Parser.require(obj.get('tx_hash', None))))
        if not tx:
            logger.info(f"Skipping failed tx for {obj.get('tx_hash', None)}")
            return

        parent_body = db.get_parent_message_body(obj.get('msg_hash'))
        if not parent_body:
            logger.warning(f"Unable to find parent message for {obj.get('msg_hash')}")
            return
        cell = Cell.one_from_boc(parent_body).begin_parse()
        
        op_id = cell.load_uint(32) # 0x25938561
        assert op_id == 0x25938561, f"Parent message for ston.fi swap is {op_id}"
        parent_query_id = cell.load_uint(64)

        to_address = cell.load_address()
        token_wallet = cell.load_address()
        token_amount = cell.load_coins()
        min_out = cell.load_coins()
        has_ref_address = cell.load_uint(1)
        addresses = cell.load_ref().begin_parse()
        from_user = addresses.load_address()
        referral_address = None
        if has_ref_address:
            referral_address = addresses.load_address()
        
        if token_wallet == wallet0_address:
            src_wallet_address = wallet0_address
            src_amount = token_amount - token0_amount
            dst_wallet_address = wallet1_address
            dst_amount = token1_amount
        elif token_wallet == wallet1_address:
            src_wallet_address = wallet1_address
            src_amount = token_amount - token1_amount
            dst_wallet_address = wallet0_address
            dst_amount = token0_amount
        else:
            logger.warning(f"Wallet addresses in swap message id={obj.get('msg_hash')} and payment message  don't match")
        
        src_master = db.get_wallet_master(src_wallet_address)
        dst_master = db.get_wallet_master(dst_wallet_address)
        if not src_master:
            logger.warning(f"Wallet not found for {src_wallet_address}")
            return
        if not dst_master:
            logger.warning(f"Wallet not found for {dst_wallet_address}")
            return

        swap = DexSwapParsed(
            tx_hash=Parser.require(obj.get('tx_hash', None)),
            msg_hash=Parser.require(obj.get('msg_hash', None)),
            trace_id=Parser.require(obj.get('trace_id', None)),
            platform=DEX_STON,
            swap_utime=Parser.require(obj.get('created_at', None)),
            swap_user=from_user,
            swap_pool=Parser.require(obj.get('source', None)),
            swap_src_token=src_master,
            swap_dst_token=dst_master,
            swap_src_amount=src_amount,
            swap_dst_amount=dst_amount,
            referral_address=referral_address,
            query_id=query_id,
            min_out=min_out
        )
        estimate_volume(swap, db)
        db.serialize(swap)
        db.discover_dex_pool(swap)


class TestnetStonfiSwap(StonfiSwap):
    STONFI_ROUTER = Parser.uf2raw('kQB3ncyBUTjZUA5EnFKR5_EnOMI9V1tTEAAPaiU71gc4Tp6n')
