import traceback
from loguru import logger

from db import DB
from model.parser import NonCriticalParserError, Parser, TOPIC_MESSAGES
from model.jetton_mint import JettonMint
from pytoniq_core import Address


# Mint is not covered by TEP-74 and we should not be strict on the format
class WrongMintFormat(NonCriticalParserError):
    pass

"""
TEP-74 jetton standard does not specify mint format but it has recommended form of internal_transfer message:

internal_transfer  query_id:uint64 amount:(VarUInteger 16) from:MsgAddress
                     response_address:MsgAddress
                     forward_ton_amount:(VarUInteger 16)
                     forward_payload:(Either Cell ^Cell)
                     = InternalMsgBody;

So mint is an internal_transfer without preceding transfer message
"""
class JettonMintParser(Parser):
    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj: dict) -> bool:
        return obj.get("opcode") == Parser.opcode_signed(0x178d4519) and obj.get("direction") == "in"

    def handle_internal(self, obj: dict, db: DB):
        # ensure we have no transfer operation before
        prev_message = db.get_parent_message_with_body(obj.get("msg_hash"))
        if prev_message and prev_message.get("opcode") == Parser.opcode_signed(0x0f8a7ea5):
            # skip ordinary chain transfer => internal_transfer
            return

        logger.info(f"Parsing jetton mint message {Parser.require(obj.get('msg_hash'))}")
        cell = Parser.message_body(obj, db).begin_parse()
        cell.load_uint(32) # internal_transfer#0x178d4519
        try:
            query_id = cell.load_uint(64)
            amount = cell.load_coins()
            from_address = cell.load_address()
            response_destination = cell.load_address()
            forward_ton_amount = cell.load_coins()

            forward_payload = None
            try:
                if cell.load_uint(1):
                    forward_payload = cell.load_ref()
                else:
                    # in-place, read the rest of the cell slice and refs
                    forward_payload = cell.to_cell()
            except Exception as e:
                logger.error(f"Unable to parse forward payload {e}")

            wallet = db.get_jetton_wallet(Address(Parser.require(obj.get("destination"))))

            tx = Parser.require(db.get_transaction(Parser.require(obj.get("tx_hash"))))

            mint = JettonMint(
                tx_hash=Parser.require(obj.get("tx_hash")),
                msg_hash=Parser.require(obj.get("msg_hash")),
                trace_id=Parser.require(obj.get("trace_id")),
                utime=Parser.require(obj.get("created_at")),
                tx_lt=tx['lt'],
                successful=tx['compute_exit_code'] == 0 and tx['action_result_code'] == 0,
                query_id=query_id,
                amount=amount,
                minter=Parser.require(obj.get("source")),
                wallet=Parser.require(obj.get("destination")),
                owner=wallet['owner'] if wallet else None,
                jetton_master_address=wallet['jetton'] if wallet else None,
                from_address=from_address,
                response_destination=response_destination,
                forward_ton_amount=forward_ton_amount,
                forward_payload=forward_payload.to_boc(True, True) if forward_payload is not None else None,
            )
            logger.info(f"Adding jetton mint {mint}")
            db.serialize(mint)
        except Exception as e:
            logger.error(f"Unable to parse jetton mint {e}: {traceback.format_exc()}")
            raise WrongMintFormat(f"Unable to parse jetton mint {e}")


"""
Hipo project implemented alternative op-code for mint activity, see
https://github.com/HipoFinance/contract/blob/main/contracts/schema.tlb

tokens_minted#5445efee
    query_id:uint64
    tokens:Coins
    coins:Coins
    owner:MsgAddress
    round_since:uint32
        = InternalMsgBody;
"""
class HipoTokensMinted(Parser):
    HTON_MASTER = Parser.uf2raw("EQDPdq8xjAhytYqfGSX8KcFWIReCufsB9Wdg0pLlYSO_h76w")

    def topics(self):
        return [TOPIC_MESSAGES]

    def predicate(self, obj: dict) -> bool:
        return (
            obj.get("opcode") == Parser.opcode_signed(0x5445efee)
            and obj.get("direction") == "in"
            and obj.get("source") == self.HTON_MASTER
        )

    def handle_internal(self, obj: dict, db: DB):
        logger.info(f"Parsing Hipo tokens mint message {Parser.require(obj.get('msg_hash'))}")
        cell = Parser.message_body(obj, db).begin_parse()
        cell.load_uint(32)  # tokens_minted#5445efee
        query_id = cell.load_uint(64)  # query_id:uint64
        amount = cell.load_coins()  # tokens:Coins

        wallet = db.get_jetton_wallet(Address(Parser.require(obj.get("destination"))))
        assert wallet['jetton'] == self.HTON_MASTER

        tx = Parser.require(db.get_transaction(Parser.require(obj.get("tx_hash"))))

        mint = JettonMint(
            tx_hash=Parser.require(obj.get("tx_hash")),
            msg_hash=Parser.require(obj.get("msg_hash")),
            trace_id=Parser.require(obj.get("trace_id")),
            utime=Parser.require(obj.get("created_at")),
            tx_lt=tx['lt'],
            successful=tx['compute_exit_code'] == 0 and tx['action_result_code'] == 0,
            query_id=query_id,
            amount=amount,
            minter=Parser.require(obj.get("source")),
            wallet=Parser.require(obj.get("destination")),
            owner=wallet['owner'] if wallet else None,
            jetton_master_address=wallet['jetton'],
            from_address=None,
            response_destination=None,
            forward_ton_amount=None,
            forward_payload=None,
        )
        logger.info(f"Adding Hipo tokens_minted event {mint}")
        db.serialize(mint)


class TestnetHipoTokensMinted(HipoTokensMinted):
    HTON_MASTER = Parser.uf2raw("kQB519C3IXFgCr4qKj6QrtaB9Pm3Sawr-Gonlo3O0cKL_I03")
