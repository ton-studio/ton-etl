import base64
from model.parser import TOPIC_JETTON_TRANSFERS, Parser
from loguru import logger
from db import DB
from parsers.utils import decode_decimal
from pytoniq_core import Cell, Address
from model.dexswap import DEX_MEGATON, DEX_STON, DEX_STON_V2, DexSwapParsed
from parsers.message.swap_volume import estimate_volume

"""
Megaton swaps contain jetton transfer chain with the following pattern:
1. User -> router
2. router -> LP master
3. LP master -> router
4. router -> User
"""
ROUTERS = set(map(Parser.uf2raw, [
    'EQAWcJ0nO3WtNlSmUjKcqv4735YCviRqu7LMNJoPXsdHVLC9',
    'EQAxC3GzQBgjlvW6CJAwgaHvarTfVxo8p7Be_6RMSjsPki6s',
    'EQBtVR30-8FAT73cCxPsMidzhiMnUKmpZy_F7aaDQRYxWurb'
    ]))

class MegatonDexSwap(Parser):
    
    def topics(self):
        return [TOPIC_JETTON_TRANSFERS]

    def predicate(self, obj) -> bool:
        # trying to get the last transfer in the chain
        return obj.get("tx_aborted", True) == False and obj.get("source", None) in ROUTERS
    

    def handle_internal(self, obj, db: DB):
        current_router = obj.get("source", None)
        transfers = db.get_jetton_transfers_by_trace_id(Parser.require(obj.get('trace_id', None)))
        if len(transfers) == 0:
            logger.warning(f"No transfers found for {obj.get('trace_id', None)}")
            return
        obj["query_id"] = int.from_bytes(base64.b64decode(obj["query_id"]["value"]), "big")
        obj["amount"] = decode_decimal(obj.get("amount", 0))
        # filter out aborted transfers and transfers with lt greater than current transfer
        transfers = list(filter(lambda x: x.get("tx_aborted", True) == False and \
                                x.get("tx_lt") <= obj.get("tx_lt") and x.get("query_id") == obj.get("query_id", 0),
                                transfers))
        # sort by lt in descending order
        transfers = sorted(transfers, key=lambda x: x.get("tx_lt"), reverse=False)
        if len(transfers) < 4:
            logger.warning(f"Not enough transfers found for {obj.get('trace_id', None)}: {len(transfers)}")
            return
        
        router_user = transfers[-1]
        lp_router = transfers[-2]
        router_lp = transfers[-3]
        user_router = transfers[-4]

        # last transfer has to be our message
        assert router_user.get("tx_hash") == obj.get("tx_hash")
        if lp_router.get("destination") != current_router:
            logger.warning(f"LP router {lp_router.get('destination')} is not the current router {current_router}")
            return
        if router_lp.get("source") != current_router:
            logger.warning(f"Router {router_lp.get('source')} is not the current router {current_router}")
            return
        if user_router.get("destination") != current_router:
            logger.warning(f"Router {router_user.get('destination')} is not the current router {current_router}")
            return
        if lp_router.get("source") != router_lp.get("destination"):
            logger.warning(f"LP {lp_router.get('source')} is not the same in trace_id {obj.get('trace_id', None)}")
            return
        
        if router_user.get("jetton_master_address") != lp_router.get("jetton_master_address"):
            logger.warning(f"Jetton address {router_user.get('jetton_master_address')} and {lp_router.get('jetton_master_address')} is not the same in trace_id {obj.get('trace_id', None)}")
            return
        
        if router_lp.get("jetton_master_address") != user_router.get("jetton_master_address"):
            logger.warning(f"Jetton address {router_lp.get('jetton_master_address')} and {user_router.get('jetton_master_address')} is not the same in trace_id {obj.get('trace_id', None)}")
            return
        
        if router_user.get("jetton_master_address") == user_router.get("jetton_master_address"):
            logger.warning(f"Jetton address {router_user.get('jetton_master_address')} and {user_router.get('jetton_master_address')} is the same in trace_id {obj.get('trace_id', None)}")
            return
        
        if router_lp.get("amount") != user_router.get("amount"):
            logger.warning(f"Jetton amount {router_lp.get('amount')} and {user_router.get('amount')} is not the same in trace_id {obj.get('trace_id', None)}")
            return
        
        if router_user.get("amount") == user_router.get("amount"):
            logger.warning(f"Jetton address {router_user.get('amount')} and {user_router.get('amount')} is the same in trace_id {obj.get('trace_id', None)}")
            return
        
        if router_user.get("destination") != user_router.get("source"):
            logger.warning(f"User {router_user.get('destination')} is not the same as user {user_router.get('source')} in trace_id {obj.get('trace_id', None)}")
            return
        
        # initial design of DexSwapParsed implied that msg_hash is unique key
        # but in case of megaton we don't have msg_hash in place so we use tx_hash instead
        swap = DexSwapParsed(
            tx_hash=Parser.require(obj.get('tx_hash', None)),
            msg_hash=Parser.require(obj.get('tx_hash', None)),
            trace_id=Parser.require(obj.get('trace_id', None)),
            platform=DEX_MEGATON,
            swap_utime=Parser.require(obj.get('tx_now', None)),
            swap_user=router_user.get("destination"),
            swap_pool=lp_router.get("source"),
            swap_src_token=user_router.get("jetton_master_address"),
            swap_dst_token=router_user.get("jetton_master_address"),
            swap_src_amount=int(user_router.get("amount")),
            swap_dst_amount=int(router_user.get("amount")),
            referral_address=None,
            query_id=obj.get("query_id", 0),
            min_out=None,
            router=current_router
        )
        estimate_volume(swap, db)
        logger.info(swap)
        db.serialize(swap)
        db.discover_dex_pool(swap)

        
