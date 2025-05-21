from model.parser import Parser, TOPIC_MESSAGES
from loguru import logger
from db import DB
from pytoniq_core import Cell, Address
from model.dexswap import DEX_STON, DEX_STON_V2, DexSwapParsed
from parsers.message.swap_volume import estimate_volume

"""
Implementation of parser for V2 ston.fi swap - https://docs.ston.fi/docs/developer-section/api-reference-v2
V2 version allows to use multiple routers (below). TODO: sync list with ston.fi API https://api.ston.fi/v1/pools?dex_v2=true
"""
class StonfiSwapV2(Parser):
    ROUTERS = set(map(Parser.uf2raw, [
        'EQABT9GCyDI60CbC4c6uS33HFDwaqd6MddiwIIw7CXTgNR3A',
        'EQATvO_BXfkFocOXhlve01EZfsiyFjoV-0k9CLmpgwtzVtcN',
        'EQAgERF5tvrNn0AM2Rrrvk-MutGP60ZL70bJPuqvCTGY-17_',
        'EQAyY2lBQ6RsVe88CKTmeH3BWWsUCWu7ugQNaf5kwLDYAoKt',
        'EQBCl1JANkTpMpJ9N3lZktPMpp2btRe2vVwHon0la8ibRied',
        'EQBQErJi0DHgKYseIHtrQk4N5CQLCr3XYwkQIEw0HNs470OG',
        'EQBZj7nhXNhB4O9rRCn4qGS82DZaPUPlyM2k6ZrbvQ1j3Ge7',
        'EQBjM7B2PKa82IPKrUFbMFaKeQDFGTMRnrvY1TmptC7Kxz7B',
        'EQBzkqAN4ViYdS24lD2fFPe8odHn2rUkfMYbEJ88EBKBAS1b',
        'EQCCdNmj4QbNjrg_PM-JJE-B9f_czXLkYmrO7P9UkA6tt95m',
        'EQCRgwuFbPRR7TGodkJwbjiBtNtb0hfzJIliV-5kY6lKr_18',
        'EQChoROpuUM4cpN6IRzqNTrkP9iVZHYoHgxMABDVU28vlUiG',
        'EQCpuYtq55nhkwYDmL4OWjsrdYy83gj5_49nNRQ5CrPOze49',
        'EQCxkYVQcfXKw9uJ-MMtutvR2Cu0DVCZFfLNBp6NwXgO8vQY',
        'EQDBYUj5KEPUQrbj7da742UYJIeT9QU5C2dKsi12SdQ3yh9a',
        'EQDTb1w1TCohFqnNcyPrrbbBJQdAwwPn8DbCoaSUd0S5T4fB',
        'EQDi1eWU3HWWst8owY8OMq2Dz9nJJEHUROza8R-_wEGb8yu6',
        'EQDwyjgjnTXJVPjXji3OPtUilcCjceGVQOLGwr9_sRLjImfG',
        # new routers v2.2
        'EQDAPye7HAPAAl4WXpz5jOCdhf2H9h9QkkzRQ-6K5usiuQeC',
        'EQByADL5Ra2dldrMSBctgfSm2X2W1P61NVW2RYDb8eJNJGx6',
        'EQDQ6j53q21HuZtw6oclm7z4LU2cG6S2OKvpSSMH548d7kJT',
        'EQDx--jUU9PUtHltPYZX7wdzIi0SPY3KZ8nvOs0iZvQJd6Ql',
        'EQBigMnbY4NU1uwdvzertV5mv_yI7282R-ffW7XZFWPEVRDG',
        'EQCx0HDJ_DxLxDSQyfsEqHI8Rs65nygvdmeD9Ra7rY15OWN8',
        'EQAyD7O8CvVdR8AEJcr96fHI1ifFq21S8QMt1czi5IfJPyfA',
        'EQCS4UEa5UaJLzOyyKieqQOQ2P9M-7kXpkO5HnP3Bv250cN3',
        'EQCiypoBWNIEPlarBp04UePyEj5zH0ZDHxuRNqJ1WQx3FCY-',
        'EQAQYbnb1EGK0Wb8mk3vEW4vbHTyv7cOcfJlPWQ87_6_qfzR',
        'EQDh5oHPvfRwPu2bORBGCoLEO4WQZKL4fk5DD1gydeNG9oEH',
        'EQCDT9dCT52pdfsLNW0e6qP5T3cgq7M4Ug72zkGYgP17tsWD',
        'EQAiv3IuxYA6ZGEunOgZSTuMBzbpjwRbWw09-WsE-iqKKMrK',
        'EQADEFMTMnC-gu5v2U0ZY8AYaGhAOk9TcECg1TOquAW3r-IE',
        'EQBQ_UBQvR9ryUjKDwijtoiyyga2Wl-yJm6Y8gl0k-HDh_5x',
        'EQBCtlN7Zy96qx-3yH0Yi4V0SNtQ-8RbhYaNs65MC4Hwfq31',
        # memecoin router?
        'EQAJG5pyZPWEiQiMVJdf7bDRgRLzg6QR57qKeRsOrMO-ncZN',
        # stable coin router for AquaUSDT
        'EQDkncuJ267Py3EmL2XAN7YsSNQMUu8u-GHsW9jVljcH8fr5',
        # 2024-12-16
        'EQCiz74FCV2lYlvFPEYhL3Jql8WwIO7QvbvYT-LQH0SmtCgI',
        # 2025-02-11
        'EQBwpBGEAb-NgjUxpmARAgVl8C4F_5GsXxZ3dpsA1qzQerNl',
        # 2025-04-17
        "EQBjK_kjY5R_DoyTRff109VzFrSlKFCC_gOOWIMtyEvCcv2J",
        # 2025-04-27
        "EQDgebEMA6yriI7SMffE65DIVA9rzSRmfGV_gy3ylIhLicY8",
        # 2025-05-09
        "EQACn16m9OrZ-mw186M4NlIpVP8Tb3q6SV9aX8NjSgVfJTo9",
        "EQAGV9vw11tKW2QOCYCXEmIdyufM3p5CfcgHcY9NiiBLfZGH",
        "EQBSNX_5mSikBVttWhIaIb0f8jJU7fL6kvyyFVppd7dWRO6M"
        # 2025-05-21
        "EQAiLV677BgHNXEUuDJ3Cw8K5WOiJSO86xh8YQq2LthJEoED",
        # 2025-05-27
        "EQAz1D0ZUiG_9XCyjrJ1-xTx-CnmnQ3J3LMKQ7sZTr-XlNZP",
        # 2025-05-29
        "EQBqgCTdrtSod76UrcOeALSiLCp3WuNIFQBQvyjjlQMvwLkc",
    ]))
    
    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        # only internal messages processed by the router
        return obj.get("opcode", None) == Parser.opcode_signed(0x657b54f5) and \
            obj.get("direction", None) == "in" and \
            obj.get("destination", None) in self.ROUTERS
    

    def handle_internal(self, obj, db: DB):
        cell = Parser.message_body(obj, db).begin_parse()
        cell.load_uint(32) # 0x657b54f5
        query_id = cell.load_uint(64)
        owner = cell.load_address()
        excesses_address = cell.load_address()
        excesses_address_2 = cell.load_address()
        exit_code = cell.load_uint(32)

        if exit_code != 0xc64370e5: # swap_ok
            logger.debug(f"Message is not a payment to user, exit code {exit_code}")
            return

        custom_payload = cell.load_maybe_ref()
    
        additional_info = cell.load_ref().begin_parse()
        fwd_ton_amount = additional_info.load_coins()
        amount0_out = additional_info.load_coins()
        token0_address = additional_info.load_address()
        amount1_out = additional_info.load_coins()
        token1_address = additional_info.load_address()
        logger.info(f"{owner} {exit_code} {fwd_ton_amount} {amount0_out} {token0_address} {amount1_out} {token1_address}")


        tx = Parser.require(db.is_tx_successful(Parser.require(obj.get('tx_hash', None))))
        if not tx:
            logger.info(f"Skipping failed tx for {obj.get('tx_hash', None)}")
            return

        parent_body = db.get_parent_message_body(obj.get('msg_hash'))
        if not parent_body:
            logger.warning(f"Unable to find parent message for {obj.get('msg_hash')}")
            return
        cell = Cell.one_from_boc(parent_body).begin_parse()
        
        op_id = cell.load_uint(32) # 0x6664de2a
        assert op_id == 0x6664de2a, f"Parent message for ston.fi swap is {op_id}"
        parent_query_id = cell.load_uint(64)

        from_user = cell.load_address()
        left_amount = cell.load_coins()
        right_amount = cell.load_coins()

        dex_payload = cell.load_ref().begin_parse()
        transferred_op = dex_payload.load_uint(32)
        token_wallet1 = dex_payload.load_address()
        swap_body = dex_payload.load_ref().begin_parse()
        min_out = swap_body.load_coins()
        receiver = swap_body.load_address()
        fwd_gas = swap_body.load_coins()
        custom_payload = swap_body.load_maybe_ref()
        refund_fwd_gas = swap_body.load_coins()
        refund_payload = swap_body.load_maybe_ref()
        ref_fee = swap_body.load_uint(16)
        ref_address = swap_body.load_address()

        logger.info(f"token_wallet1: {token_wallet1}, left_amount: {left_amount}, right_amount: {right_amount}, ref_address: {ref_address}")
        
        if token_wallet1 == token1_address:
            src_wallet_address = token0_address
            src_amount = left_amount - amount0_out
            dst_wallet_address = token1_address
            dst_amount = amount1_out
        elif token_wallet1 == token0_address:
            src_wallet_address = token1_address
            src_amount = right_amount - amount1_out
            dst_wallet_address = token0_address
            dst_amount = amount0_out
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
            platform=DEX_STON_V2,
            swap_utime=Parser.require(obj.get('created_at', None)),
            swap_user=from_user,
            swap_pool=Parser.require(obj.get('source', None)),
            swap_src_token=src_master,
            swap_dst_token=dst_master,
            swap_src_amount=src_amount,
            swap_dst_amount=dst_amount,
            referral_address=ref_address,
            query_id=query_id,
            min_out=min_out,
            router=Parser.require(obj.get("destination", None))
        )
        estimate_volume(swap, db)
        logger.info(swap)
        db.serialize(swap)
        db.discover_dex_pool(swap)


class TestnetStonfiSwapV2(StonfiSwapV2):
    ROUTERS = set(map(Parser.uf2raw, [
        'kQALh-JBBIKK7gr0o4AVf9JZnEsFndqO0qTCyT-D-yBsWk0v',
    ]))
