import pytest
from model.tg_wallet import TgWalletRequest
from parsers.message.tg_wallet import TG_WALLET_CODE_HASH, TgWalletRequestParser


class FakeDB:
    def __init__(self, code_hash=TG_WALLET_CODE_HASH):
        self.code_hash = code_hash
        self.serialized = []

    def get_transaction(self, tx_hash):
        return {
            "hash": tx_hash,
            "compute_exit_code": 0,
            "action_result_code": 0,
            "account_state_code_hash_before": self.code_hash,
            "account_state_code_hash_after": self.code_hash,
        }

    def serialize(self, obj):
        self.serialized.append(obj)


# real mainnet messages, fetched from toncenter v3 API

# text comment top-up longer than 512 bits, not a signed request
COMMENT_TOPUP = {
    "tx_hash": "T/TU2R0YFJWGbMqcxwkEEnfQItUHH5mqxe2ASzi7oFY=",
    "tx_lt": 109531619000009,
    "tx_now": 1791829850,
    "msg_hash": "w0xBI1niuhZGOE6l39aRRIrw3jwI+U4P2Uk6aM4pBQQ=",
    "trace_id": "jsmmmJ1pfQlF6hehvhqu+U+nPRI+C9/7A2HG2wu8WIc=",
    "direction": "in",
    "source": "0:7FF3E64A88CFB9A15333CBE2AFDBADFCFF4CF5329D53DE789E1922042D004E18",
    "destination": "0:5B6A81AFF80B7B8036488BFABF05343A3A8C975470EDDD96251C10985AE26CBA",
    "opcode": 0,
    "body_boc": "te6cckEBAgEA0AAB/gAAAABkZXAudGcg4oCUINC/0L7Qu9GD0YfQuCBORlQt0L/QvtC00LDRgNC+0Log0LHQtdGB0L/Qu9Cw0YLQvdC+ISDwn46BINCf0YDQvtGB0YLQviDQvtGC0LrRgNC+0LkgZGVwLnRnINCyINCx0YDQsNGD0LfQtdGA0LUsINABAJi/0LXRgNC10LnQtNC4INCyIEdvcmlsbGEgQ2FzZSDQuCDQvtGC0LrRgNC+0Lkg0LHQtdGB0L/Qu9Cw0YLQvdGL0Lkg0LrQtdC50YEuTMB/cg==",
}

# send_one via external message, user pays gas
SEND_ONE_EXTERNAL = {
    "tx_hash": "KCP3tLOWYTYddYwMNAuabBIKlST8Za9c9y2QlNnI7Zk=",
    "tx_lt": 108863989000025,
    "tx_now": 1791562882,
    "msg_hash": "LBSqQvJjH6Zju9YnTHmZuJ651TYic6K4AKJMfS768dk=",
    "trace_id": "KCP3tLOWYTYddYwMNAuabBIKlST8Za9c9y2QlNnI7Zk=",
    "direction": "in",
    "source": None,
    "destination": "0:5B6A81AFF80B7B8036488BFABF05343A3A8C975470EDDD96251C10985AE26CBA",
    "opcode": 1542725357,
    "body_boc": "te6cckEBAwEAjQABolv0Hu0EWsM6x93VXtTd8hSMOUEBtPIw1I87ovKF3BRNR7hpAArzqNocMf3u8rftX99UH2u6wJwh/k25Vxb+xgxjiW51f/9/EWrJFaoAAAAKAwEBaEIAXOCHWZMmyKd48lAKBj01zptK3587xSGLaqqYaiEJkWcgL68IAAAAAAAAAAAAAAAAAAECAAC6FUZI",
}

# send_one via internal message from a Telegram gas relayer
SEND_ONE_GASLESS = {
    "tx_hash": "PI9rCm2w/Baizvzz11f8rqUtppTuvEpQnOz88mXuHDA=",
    "tx_lt": 108829266000025,
    "tx_now": 1791548997,
    "msg_hash": "TiAk4fvzLqIyzEDtmucozpCzSxAJMcB5ShGGhSOcrqQ=",
    "trace_id": "/F0BkreHzMzXg539naP/F+F0KUTERVzJK+Osyvzd2hw=",
    "direction": "in",
    "source": "0:580B3ADEB9A6AAC1FD9DE8349B6CAFAD1DD692DCBF87014925080FCFA274A766",
    "destination": "0:5B6A81AFF80B7B8036488BFABF05343A3A8C975470EDDD96251C10985AE26CBA",
    "opcode": -188595664,
    "body_boc": "te6cckEBAwEAjQABovTCQjBtIIGVj+y/SIWWG9ZL2GbMCFYk4+nyWVRkRNFiLj7O8vitUPiU5V4u9oyK0RYFLmwpzpIMAnvD7qmuqgpjiW50f/9/EWrI328AAAAEAwEBaEIAWBrblu8W1vIwaLGjF/mc9tsUjgYUZf4rKjp4frX0bd+gL68IAAAAAAAAAAAAAAAAAAECAACSYM0m",
}

# send_bulk with 5 messages via external message
SEND_BULK_EXTERNAL = {
    "tx_hash": "ZuXmmPtuPFGiJgJut+j0s/s4cBOpjCpseMDwC3HJE3E=",
    "tx_lt": 109066318000001,
    "tx_now": 1791643792,
    "msg_hash": "LAKxIdfsSUSmC6jco23o19qlWNyecbNs+U3oeyOzTWc=",
    "trace_id": "ZuXmmPtuPFGiJgJut+j0s/s4cBOpjCpseMDwC3HJE3E=",
    "direction": "in",
    "source": None,
    "destination": "0:5918A2782E1684A0FF927A11F1D7986D497CE0F179645F02F77F6E074218886F",
    "opcode": -528595838,
    "body_boc": "te6cckECCQEAAXsAAaPgfkSCvUywghjvXSPFoLYI/uMVhbNmz48pX3Xp3TlHSQn2jav9lbt1fSvC9uKxTkAOIAqDiARKdyu+W93Yt2AOc4ludX//fxFqylLiAAAAAQXAAQMFgYHAAgMEAwcBgYHABQYHAWhCAFdRIT9uDDmPYCZuFXltYmnXPA5zapPRvPFpGfiot0STIC+veMgAAAAAAAAAAAAAAAABCAFoQgA+KwcEkKPeubjQtONoHCudpXqpoud7hJp0PEaf+iiQIaAvr3jIAAAAAAAAAAAAAAAAAQgBaEIALz5F636d8XFdfKp6sJkkpdQCHuhryjs+1mxABslMvOmgL694yAAAAAAAAAAAAAAAAAEIAWhCABYv3XUmgOIiAPPIZO/JTZNZK0GwyMCcjPeu5TF1WdVwIC+veMgAAAAAAAAAAAAAAAABCAFoQgAAJq2AbN729QB4N/2q5Hobj7PjsFDd3foSXdYIX5sKDCAvr3jIAAAAAAAAAAAAAAAAAQgAAD3/SlE=",
}

# change_key via external message
CHANGE_KEY_EXTERNAL = {
    "tx_hash": "8hKXG1wSKmFN5sPQd7AD7+/6h27FH33Ei8fiCzCw7FI=",
    "tx_lt": 105741823000003,
    "tx_now": 1790314375,
    "msg_hash": "qsz5X0HNgVKcSqCat3o1kmYKEgUjxr3smF0Wwyr95Vs=",
    "trace_id": "8hKXG1wSKmFN5sPQd7AD7+/6h27FH33Ei8fiCzCw7FI=",
    "direction": "in",
    "source": None,
    "destination": "0:58C6102CB06733DCFA74F13DFB126EC9C94DDA042DB8E1FDAD0C23781BA34A78",
    "opcode": 62041400,
    "body_boc": "te6cckEBAgEAtQAB4AOyrTgNt4GIrzWbZoJNcObkbo7rD71OdXFSwhGaOPKr5ffmezh78zoNzh7tjYg7IUn0ClIHy8kUporWxdL6ZwP7upnIf/9/EWq2CFEAAAA6ihkTCprLBFbfR5qsWU1Y9Fhf3MtkZLKyp8IorHEv4/gBAIAPyBf1bXdtO4qhx/bbHtc2Zl74Yfg+caV7+sSywIQ5VWM4zvsjuqskvzuh0O1aKaIBcgOyHby2mH0V6VDRiBAPyHe5wQ==",
}

# change_key via internal message from a Telegram gas relayer
CHANGE_KEY_GASLESS = {
    "tx_hash": "IZ6Qny+ezMWHGaMnNyysdVbc0HpdWgRVemK61l71hlE=",
    "tx_lt": 109036396000019,
    "tx_now": 1791631822,
    "msg_hash": "5D+9thlI0BfpGxwR9V38Tw72DVA8LRYzchd75CtwKNA=",
    "trace_id": "YHN+kPlVHVKN7SEr+aJGfBhR6vBRNs/Q1v7JvVQ/uAQ=",
    "direction": "in",
    "source": "0:C113A94DFAB12034CEF0FFB51CE49135606AFEF15A6D57D5876643AE680F8012",
    "destination": "0:588BF67782D8F247AA3D09B8C2F0F2C3CC32F22EB5F938DB683358AEECC42FFD",
    "opcode": -1322868695,
    "body_boc": "te6cckEBAwEA2AAC4LEmoCnQmCTic2asQrFDnNABOpujh3f1Nnhw3lpthA2mgAUcjUoOi2yfrHcUveFipGGrR6Eh/4cH7q/VDYgpHwD7upnHf/9/EWrKIrwAAAAATIm2sSu/l2yYDffbtt5ji7LW9zbPr/XVY6QkMop/igIBAgCAz/wfsFgNiJNaiUdRALgG/pIY2VmKAT1BpgT9phW+FxAVDKw64RTercNXpI+NhHgYFsIdhr2tm+qYZ9zP15FpCABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAlNPP4",
}


@pytest.fixture
def parser():
    return TgWalletRequestParser()


@pytest.mark.parametrize(
    "msg, request_type, gasless, valid_until, seqno, messages_count, new_public_key",
    [
        (SEND_ONE_EXTERNAL, "send_one", False, 1791563178, 10, 1, None),
        (SEND_ONE_GASLESS, "send_one", True, 1791549295, 4, 1, None),
        (SEND_BULK_EXTERNAL, "send_bulk", False, 1791644386, 1, 5, None),
        (CHANGE_KEY_EXTERNAL, "change_key", False, 1790314577, 58, 0,
         "8a19130a9acb0456df479aac594d58f4585fdccb6464b2b2a7c228ac712fe3f8"),
        (CHANGE_KEY_GASLESS, "change_key", True, 1791632060, 0, 0,
         "4c89b6b12bbf976c980df7dbb6de638bb2d6f736cfaff5d563a424328a7f8a02"),
    ],
)
def test_signed_requests(parser, msg, request_type, gasless, valid_until, seqno, messages_count, new_public_key):
    db = FakeDB()
    assert parser.handle(msg, db)
    assert db.serialized == [TgWalletRequest(
        tx_hash=msg["tx_hash"],
        msg_hash=msg["msg_hash"],
        trace_id=msg["trace_id"],
        event_time=msg["tx_now"],
        tx_lt=msg["tx_lt"],
        successful=True,
        wallet=msg["destination"],
        source=msg["source"],
        gasless=gasless,
        request_type=request_type,
        subwallet_id=0x7FFF7F11,
        valid_until=valid_until,
        seqno=seqno,
        messages_count=messages_count,
        new_public_key=new_public_key,
    )]


def test_skip_non_request(parser):
    assert not parser.predicate(COMMENT_TOPUP)
    assert not parser.predicate({**SEND_ONE_EXTERNAL, "direction": "out"})


def test_skip_other_code_hash(parser):
    db = FakeDB(code_hash="other")
    parser.handle(SEND_ONE_GASLESS, db)
    assert db.serialized == []
