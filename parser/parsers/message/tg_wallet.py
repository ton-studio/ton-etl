from loguru import logger

from db import DB
from model.parser import NonCriticalParserError, Parser, TOPIC_MESSAGES
from model.tg_wallet import TgWalletRequest
from pytoniq_core import Cell

"""
Telegram native wallet (WalletTg), https://github.com/ton-blockchain/tg-wallet-contract

Every account runs the same immutable trampoline (JMP to config param -123), so all wallets share
one code hash. Incoming body is a signed request with the signature in front:

signed_request$_ signature:bits512 request:Request = SignedRequest;
seqno_header$_ subwallet_id:uint32 valid_until:uint32 seqno:uint32 = SeqnoHeader;
send_one#63896e74 / #63896e75 header:SeqnoHeader send_mode:uint8 message:^Cell
send_bulk#73896e74 / #73896e75 header:SeqnoHeader len:uint8 head:(Maybe ^Cell)
change_key#fbba99c7 / #fbba99c8 header:SeqnoHeader new_public_key:uint256
    rotation_signature:^Cell encrypted_old_private_key:^Cell

...74/...c7 opcodes are accepted only in internal messages (gasless, sent by gas relayers),
...75/...c8 only in external messages.
"""

TG_WALLET_CODE_HASH = "kUmuUcHkaJcQzr94MCl7Fqz7rbNjqSClN4k+f/7sp2g="

REQUESTS = {
    0x63896e74: ("send_one", True),
    0x63896e75: ("send_one", False),
    0x73896e74: ("send_bulk", True),
    0x73896e75: ("send_bulk", False),
    0xfbba99c7: ("change_key", True),
    0xfbba99c8: ("change_key", False),
}


class TgWalletRequestParser(Parser):
    def topics(self):
        return [TOPIC_MESSAGES]

    """
    obj.opcode holds signature bits, so the request opcode is read right after the signature.
    Requires body_boc in the message (USE_MESSAGE_CONTENT=0), no DB lookups for every inbound message.
    """
    def predicate(self, obj) -> bool:
        if obj.get("direction") != "in" or obj.get("opcode") is None or not obj.get("body_boc"):
            return False
        return TgWalletRequestParser.request_opcode(Cell.one_from_boc(obj.get("body_boc"))) in REQUESTS

    @classmethod
    def request_opcode(clz, body: Cell):
        cell = body.begin_parse()
        if cell.remaining_bits < 512 + 32:
            return None
        cell.skip_bits(512)
        return cell.load_uint(32)

    @classmethod
    def decode(clz, body: Cell) -> dict:
        cell = body.begin_parse()
        cell.skip_bits(512)  # signature
        request_type, internal = REQUESTS[cell.load_uint(32)]
        res = {
            "request_type": request_type,
            "gasless": internal,
            "subwallet_id": cell.load_uint(32),
            "valid_until": cell.load_uint(32),
            "seqno": cell.load_uint(32),
            "new_public_key": None,
        }
        if request_type == "send_one":
            res["messages_count"] = 1
        elif request_type == "send_bulk":
            res["messages_count"] = cell.load_uint(8)
        else:
            res["messages_count"] = 0
            res["new_public_key"] = cell.load_bytes(32).hex()
        return res

    def handle_internal(self, obj, db: DB):
        tx = Parser.require(db.get_transaction(Parser.require(obj.get("tx_hash"))))
        if TG_WALLET_CODE_HASH not in (tx.get("account_state_code_hash_after"), tx.get("account_state_code_hash_before")):
            return

        try:
            decoded = TgWalletRequestParser.decode(Parser.message_body(obj, db))
        except Exception as e:
            raise NonCriticalParserError(f"Unable to parse tg wallet request {obj.get('msg_hash')}: {e}") from e

        # external request comes without source, internal with mismatched opcode is rejected by the contract
        if decoded["gasless"] != (obj.get("source") is not None):
            logger.warning(f"Unexpected tg wallet request channel for {obj.get('msg_hash')}, skipping")
            return

        request = TgWalletRequest(
            tx_hash=Parser.require(obj.get("tx_hash")),
            msg_hash=Parser.require(obj.get("msg_hash")),
            trace_id=Parser.require(obj.get("trace_id")),
            event_time=Parser.require(obj.get("tx_now")),
            tx_lt=Parser.require(obj.get("tx_lt")),
            # note: internal request with invalid signature is ignored by the contract with exit code 0
            successful=tx["compute_exit_code"] == 0 and tx["action_result_code"] == 0,
            wallet=Parser.require(obj.get("destination")),
            source=obj.get("source"),
            **decoded,
        )
        logger.info(f"Adding tg wallet request {request}")
        db.serialize(request)
