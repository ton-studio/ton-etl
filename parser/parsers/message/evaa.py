from loguru import logger
from pytoniq_core import Cell, Address

from db import DB
from model.parser import Parser, TOPIC_MESSAGES
from model.evaa import EvaaSupply, EvaaWithdraw, EvaaLiquidation


EVAA_POOLS = [
    Parser.uf2raw("EQC8rUZqR_pWV1BylWUlPNBzyiTYVoBEmQkMIQDZXICfnuRr"),  # EVAA main pool
    Parser.uf2raw("EQBIlZX2URWkXCSg3QF2MJZU-wC5XkBoLww-hdWk2G37Jc6N"),  # EVAA LP pool
    Parser.uf2raw("EQANURVS3fhBO9bivig34iyJQi97FhMbpivo1aUEAS2GYSu-"),  # EVAA Alts pool
    Parser.uf2raw("EQBozwKVDya9IL3Kw4mR5AQph4yo15EuMdyX8nLljeaUxrpM"),  # Coffin pool
]


def evaa_asset_to_address(asset_id: int):
    return Address((0, asset_id.to_bytes(32, "big")))


# v4 update was made with this tx: XUL1eBYPbMd2fHydBGgojhqYEHsNi1bmSDS1xGGrwPo=
def is_v4_contract(utime: int) -> bool:
    return Parser.require(utime) > 1716051631


class EvaaSupplyParser(Parser):
    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        return (
            obj.get("opcode", None) == Parser.opcode_signed(0x11a)
            and obj.get("direction", None) == "in"
            and obj.get("destination", None) in EVAA_POOLS
        )

    def handle_internal(self, obj, db: DB):
        logger.info(f"Parsing EVAA supply message {Parser.require(obj.get('msg_hash', None))}")
        cell = Parser.message_body(obj, db).begin_parse()
        cell.load_uint(32)  # 0x11a

        supply = EvaaSupply(
            tx_hash=Parser.require(obj.get("tx_hash", None)),
            msg_hash=Parser.require(obj.get("msg_hash", None)),
            trace_id=Parser.require(obj.get("trace_id", None)),
            utime=Parser.require(obj.get("created_at", None)),
            successful=Parser.require(db.is_tx_successful(Parser.require(obj.get("tx_hash", None)))),
            query_id=cell.load_uint(64),
            owner_address=cell.load_address(),
            asset_id=evaa_asset_to_address(cell.load_uint(256)),
            amount=cell.load_uint(64),
            user_new_principal=cell.load_int(64) if is_v4_contract(obj.get("created_at")) else None,
            repay_amount_principal=cell.load_int(64),
            supply_amount_principal=cell.load_int(64),
            pool_address=obj.get("destination", None),
        )
        logger.info(f"Adding EVAA supply {supply}")
        db.serialize(supply)


class EvaaWithdrawAndLiquidationParser(Parser):
    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj) -> bool:
        return obj.get("direction", None) == "in" and obj.get("source", None) in EVAA_POOLS

    def handle_internal(self, obj, db: DB):
        cell = Parser.message_body(obj, db).begin_parse()
        try:
            cell.load_coins()  # version
            cell.load_uint(3)  # flags
            op = cell.load_uint(32)
            query_id = cell.load_uint(64)
        except:
            logger.info("Not an EVAA message")
            return

        if op == 0x211a or op == 0x311a:
            approved = True
        elif op == 0x211f or op == 0x311f:
            approved = False
        else:
            logger.info(f"Skipping message with opcode {op}")
            return

        logger.info(f"Parsing possible EVAA withdraw/liquidation approve message {Parser.require(obj.get('msg_hash', None))}")

        parent_message = db.get_parent_message_with_body(obj.get("msg_hash"))
        if not parent_message:
            raise Exception(f"Unable to find parent message for {obj.get('msg_hash')}")

        logger.info(
            f"Parsing EVAA withdraw_collateralized/liquidate_satisfied message {Parser.require(parent_message.get('msg_hash', None))}"
        )

        cell = Cell.one_from_boc(Parser.require(parent_message.get("body"))).begin_parse()
        parent_op = cell.load_uint(32)  # 0x211 / 0x311

        if parent_op == 0x211:
            recipient_address = None
            if cell.refs:
                ref = cell.load_ref().begin_parse()
                recipient_address = ref.load_address()
            withdraw = EvaaWithdraw(
                tx_hash=Parser.require(parent_message.get("tx_hash", None)),
                msg_hash=Parser.require(parent_message.get("msg_hash", None)),
                trace_id=Parser.require(parent_message.get("trace_id", None)),
                utime=Parser.require(parent_message.get("created_at", None)),
                successful=Parser.require(db.is_tx_successful(Parser.require(obj.get("tx_hash", None)))),
                query_id=cell.load_uint(64),
                owner_address=cell.load_address(),
                asset_id=evaa_asset_to_address(cell.load_uint(256)),
                amount=cell.load_uint(64),
                user_new_principal=cell.load_int(64) if is_v4_contract(parent_message.get("created_at")) else None,
                borrow_amount_principal=cell.load_int(64),
                reclaim_amount_principal=cell.load_int(64),
                recipient_address=recipient_address,
                pool_address=obj.get("source", None),
                approved=approved,
            )
            logger.info(f"Adding EVAA withdraw {withdraw}")
            db.serialize(withdraw)

        elif parent_op == 0x311:
            ref = cell.load_ref().begin_parse()
            liqudation = EvaaLiquidation(
                tx_hash=Parser.require(parent_message.get("tx_hash", None)),
                msg_hash=Parser.require(parent_message.get("msg_hash", None)),
                trace_id=Parser.require(parent_message.get("trace_id", None)),
                utime=Parser.require(parent_message.get("created_at", None)),
                successful=Parser.require(db.is_tx_successful(Parser.require(obj.get("tx_hash", None)))),
                query_id=cell.load_uint(64),
                owner_address=cell.load_address(),
                liquidator_address=cell.load_address(),
                transferred_asset_id=evaa_asset_to_address(cell.load_uint(256)),
                delta_loan_principal=ref.load_int(64),
                amount=ref.load_uint(64),
                protocol_gift=ref.load_uint(64),
                new_user_loan_principal=ref.load_int(64) if is_v4_contract(parent_message.get("created_at")) else None,
                collateral_asset_id=evaa_asset_to_address(ref.load_uint(256)),
                delta_collateral_principal=ref.load_int(64),
                collateral_reward=ref.load_uint(64),
                min_collateral_amount=ref.load_uint(64),
                new_user_collateral_principal=ref.load_int(64) if is_v4_contract(parent_message.get("created_at")) else None,
                pool_address=obj.get("source", None),
                approved=approved,
            )
            logger.info(f"Adding EVAA liquidation {liqudation}")
            db.serialize(liqudation)
