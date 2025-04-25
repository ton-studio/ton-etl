import base64
import decimal
from typing import Dict, List, Set, Union
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from pytoniq_core import Address, ExternalAddress
from dataclasses import asdict
from loguru import logger
import json
from dataclasses import dataclass

from model.dexswap import DexSwapParsed
from model.dexpool import DexPool
from model.jetton_metadata import JettonMetadata
from model.nft_item_metadata import NFTItemMetadata
from model.nft_collection_metadata import NFTCollectionMetadata
from model.parser import NonCriticalParserError

@dataclass
class FakeRecord:
    value: any
    topic: str
    timestamp: any = None

def serialize_addr(addr: Union[Address, ExternalAddress, None]) -> str:
    if isinstance(addr, Address):
        return addr.to_str(is_user_friendly=False).upper()
    if isinstance(addr, ExternalAddress): # extrernal addresses are not supported
        return None
    return None
    
class DB():
    def __init__(
            self,
            use_message_content: bool,
            dex_pool_history: bool = False,
            run_migrations: bool = False,
            ignore_missing_parent_message_body: bool = False,
        ):
        self.use_message_content = use_message_content
        self.dex_pool_history = dex_pool_history
        self.ignore_missing_parent_message_body = ignore_missing_parent_message_body
        self.pool = pool.SimpleConnectionPool(1, 3)
        if not self.pool:
            raise Exception("Unable to init connection")
        # Stores the number of rows with update to control commit frequency
        self.updated = 0
        self.conn = None
        self.dex_pools_cache = set()

        if run_migrations:
            logger.info("Running migration")
            conn = self.pool.getconn()
            conn.autocommit = True
            with open("createdb.sql", "r") as f:
                buffer = []
                block = False
                for line in f.read().split("\n"):
                    buffer.append(line)
                    if line.startswith("DO"):
                        block = True
                    if not block and line.strip().endswith(";") or block and line.startswith("END $$"):
                        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                            sql = "\n".join(buffer)
                            logger.info(f"Running statement: {sql}")
                            cursor.execute(sql)
                        block = False
                        buffer = []
            self.pool.putconn(conn)
        
    """
    Acquires connection from the pool. After the end of the session caller has to release it
    """
    def acquire(self):
        assert self.conn is None, "Connection was not released"
        self.conn = self.pool.getconn()

    def release(self):
        assert self.conn is not None, "Unable to release connection, was not acquired"
        self.conn.commit()
        self.pool.putconn(self.conn)
        self.conn = None
        self.updated = 0

    """
    Returns message body by body hash
    """
    def get_message_body(self, body_hash) -> str:
        assert self.use_message_content, "get_message_body is not supported in datalake mode"
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("select body from message_contents mc  where hash = %s", (body_hash, ))
            res = cursor.fetchone()
            if not res:
                return None
            return res['body']
    
    """
    Returns jetton master
    """
    def get_wallet_master(self, jetton_wallet: Address) -> str:
        assert self.conn is not None
        assert type(jetton_wallet) == Address
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("select jetton from jetton_wallets jw where address = %s",
                           (serialize_addr(jetton_wallet), ))
            res = cursor.fetchone()
            if not res:
                return None
            return res['jetton']
        
    """
    Returns jetton wallet
    """
    def get_jetton_wallet(self, jetton_wallet: Address) -> str:
        assert self.conn is not None
        assert type(jetton_wallet) == Address
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("select * from jetton_wallets jw where address = %s",
                           (serialize_addr(jetton_wallet), ))
            return cursor.fetchone()
        
    """
    Returns message body for the parent message
    """
    def get_parent_message_body(self, msg_hash) -> str:
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            if self.use_message_content:
                cursor.execute("""
                            select mc.body from trace_edges te
                            join messages m on m.tx_hash = te.left_tx and m.direction ='in'
                            join message_contents mc on mc.hash = m.body_hash 
                            where te.msg_hash = %s
                            """, (msg_hash, ))
                res = cursor.fetchone()
                if not res:
                    return None
                return res['body']
            else:
                cursor.execute("""
                            select m.body_boc from trace_edges te
                            join messages m on m.tx_hash = te.left_tx and m.direction ='in'
                            where te.msg_hash = %s
                            """, (msg_hash, ))
                res = cursor.fetchone()
                if not res:
                    if self.ignore_missing_parent_message_body:
                        raise NonCriticalParserError(f"Parent message's body was not found for msg_hash={msg_hash}")
                    else:
                        return None
                return res['body_boc']

    """
    Returns parent message with message body
    """
    def get_parent_message_with_body(self, msg_hash) -> dict:
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            if self.use_message_content:
                cursor.execute(
                    """
                    select m.*, mc.body from trace_edges te
                    join messages m on m.tx_hash = te.left_tx and m.direction ='in'
                    join message_contents mc on mc.hash = m.body_hash 
                    where te.msg_hash = %s
                    """, 
                    (msg_hash, ),
                )
                return cursor.fetchone()
            else:
                cursor.execute(
                    """
                    select m.* from trace_edges te
                    join messages m on m.tx_hash = te.left_tx and m.direction ='in'
                    where te.msg_hash = %s
                    """, 
                    (msg_hash, ),
                ) 
                res = cursor.fetchone()
                if res:
                    res['body'] = res['body_boc']
                return res

    def get_nft_sale(self, address: str) -> dict:
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                select address, marketplace_address as marketplace, nft_owner_address as owner, 
                full_price as price, false as is_auction, code_hash
                from getgems_nft_sales where address = %s
                union
                select address, mp_addr as marketplace, nft_owner as owner, 
                last_bid as price, true as is_auction, code_hash
                from getgems_nft_auctions where address = %s
                """, 
                (address, address),
            )
            res = cursor.fetchone()
            return res
        
    def get_transaction(self, tx_hash: str):
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                select * from transactions where hash = %s
                """, 
                (tx_hash,),
            )
            return cursor.fetchone()
    
    def is_tx_successful(self, tx_hash: str) -> bool:
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                select compute_exit_code, action_result_code from transactions where hash = %s
                """, 
                (tx_hash,),
            )
            res = cursor.fetchone()
            if not res:
                return None
            return res['compute_exit_code'] == 0 and res['action_result_code'] == 0

    def serialize(self, obj):
        table = obj.__tablename__
        schema = getattr(obj, '__schema__', 'parsed')
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            names = []
            values = []
            placeholders = []
            for k, v in asdict(obj).items():
                if k.startswith("_"):
                    continue
                names.append(k)
                if type(v) == Address:
                    v = v.to_str(is_user_friendly=False).upper()
                values.append(v)
                placeholders.append('%s')
            # TODO add support for upsert
            names.append('created')
            names.append('updated')
            placeholders.append('now()')
            placeholders.append('now()')
            cursor.execute(f"""
                insert into {schema}.{table}({",".join(names)}) values ({",".join(placeholders)})
                on conflict do nothing
                            """, tuple(values))
            self.updated += 1

    def insert_message_comment(self, msg_hash, comment):
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
                insert into parsed.message_comments(hash, comment)
                           values (%s, %s)
                on conflict do nothing
                            """, (msg_hash, comment))
            self.updated += 1

    def insert_nft_item(self, address, index, collection_address, owner_address, last_trans_lt, code_hash, data_hash):
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
                insert into public.nft_items(address, index, collection_address, owner_address, 
                           last_transaction_lt, code_hash, data_hash, init)
                           values (%s, %s, %s, %s, %s, %s, %s, true)
                on conflict do nothing
                            """, (address.to_str(is_user_friendly=False).upper(), index,
                                  serialize_addr(collection_address),
                                  serialize_addr(owner_address), last_trans_lt,
                                    code_hash, data_hash))
            self.updated += 1


    def insert_nft_item_v2(self, address, index, collection_address, owner_address, last_trans_lt, last_tx_now, init, content):
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
                insert into parsed.nft_items(address, "index", collection_address, owner_address, 
                           last_transaction_lt, last_tx_now, init, content)
                           values (%s, %s, %s, %s, %s, %s, %s, %s)
                on conflict(address) do update set
                           init = EXCLUDED.init,
                           index = EXCLUDED.index,
                           collection_address = EXCLUDED.collection_address,
                           owner_address = EXCLUDED.owner_address,
                           content = EXCLUDED.content,
                           last_transaction_lt = EXCLUDED.last_transaction_lt,
                           last_tx_now = EXCLUDED.last_tx_now
                           WHERE nft_items.last_transaction_lt < EXCLUDED.last_transaction_lt
                            """, (address.to_str(is_user_friendly=False).upper(), index,
                                  serialize_addr(collection_address),
                                  serialize_addr(owner_address), last_trans_lt,
                                   last_tx_now, init, json.dumps(content)))
            self.updated += 1


    def insert_jetton_wallet(self, address, balance, owner, jetton, last_trans_lt, code_hash, data_hash):
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
                insert into public.jetton_wallets(address, balance, owner, jetton, 
                           last_transaction_lt, code_hash, data_hash)
                           values (%s, %s, %s, %s, %s, %s, %s)
                on conflict do nothing
                            """, (address.to_str(is_user_friendly=False).upper(), balance,
                                  serialize_addr(owner),
                                  serialize_addr(jetton), last_trans_lt,
                                    code_hash, data_hash))
            self.updated += 1

    def insert_mc_library(self, boc):
        assert self.conn is not None
        logger.info(f"Insert boc: {boc}")
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
                insert into parsed.mc_libraries(boc)
                           values(%s)
                on conflict do nothing
                            """, (boc, ))
            self.updated += 1


    def insert_staking_position(self, address: Address, pool: str, utime: int, lt: int, balance: float, pending: float):
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
                insert into parsed.staking_pools_nominators(address, pool, utime, lt, balance, pending)
                           values(%s, %s, %s, %s, %s, %s)
                on conflict(address, pool) do update
                           set balance = EXCLUDED.balance,
                           pending = EXCLUDED.pending,
                           utime = EXCLUDED.utime,
                           lt = EXCLUDED.lt
                           where staking_pools_nominators.balance != EXCLUDED.balance or staking_pools_nominators.pending != EXCLUDED.pending
                            """, (address.to_str(is_user_friendly=False).upper(), pool, utime, lt, balance, pending))
            self.updated += 1


    def insert_core_price(self, asset, price, obj):
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
                insert into prices.core(tx_hash, lt, asset, price, price_ts, created, updated)
                           values(%s, %s, %s, %s, %s, now(), now())
                on conflict (tx_hash) do update 
                           set tx_hash = EXCLUDED.tx_hash,
                           lt = EXCLUDED.lt,
                           asset = EXCLUDED.asset,
                           price = EXCLUDED.price,
                           price_ts = EXCLUDED.price_ts,
                           updated = now()
                            """, (obj.get('last_trans_hash'), obj.get('last_trans_lt'), asset,
                                  price, obj.get('timestamp')))
            self.updated += 1

    def get_core_price(self, asset: str, timestamp: int) -> float:
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                select price from prices.core where asset = %s
                and price_ts < %s order by price_ts desc limit 1
                """, 
                (asset, timestamp),
            )
            res = cursor.fetchone()
            if not res:
                return None
            return float(res['price'])
        
    def get_agg_price(self, asset: str, timestamp: int) -> float:
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                select price_ton from prices.agg_prices where base = %s
                and price_time < %s order by price_time desc limit 1
                """, 
                (asset, timestamp),
            )
            res = cursor.fetchone()
            if not res:
                return None
            return float(res['price_ton'])
        
    def get_uniq_nft_item_codes(self) -> Set[str]:
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("select distinct code_hash as h from nft_items ni")
            return set(map(lambda x: x['h'], cursor.fetchall()))
        
    def get_uniq_jetton_wallets_codes(self) -> Set[str]:
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("select distinct code_hash as h from jetton_wallets ni")
            return set(map(lambda x: x['h'], cursor.fetchall()))
        
    # Returns the latest account state
    def get_latest_account_state(self, address: Address):
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                select * from latest_account_states where account = %s
                """, 
                (address.to_str(is_user_friendly=False).upper(), )
            )
            res = cursor.fetchone()
            return res
        
    # for debugging purposese
    def get_messages_for_processing(self, tx_hash):

        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                select * from messages where tx_hash = %s
                """, 
                (tx_hash,),
            )
            return list(map(lambda x: FakeRecord(value=json.dumps(dict(x)).encode('utf-8'), topic="ton.public.messages"),
                            cursor.fetchall()))

    def get_jetton_transfers_for_processing(self, trace_id):

        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                select * from jetton_transfers where trace_id = %s
                """, 
                (trace_id,),
            )
            class DecimalEncoder(json.JSONEncoder):
                def default(self, o):
                    if isinstance(o, decimal.Decimal):
                        return {
                            'value': base64.b64encode(int(o).to_bytes(length=128, byteorder='big')).decode('utf-8'),
                            'scale': 0
                        }
                    return super().default(o)
            return list(map(lambda x: FakeRecord(value=json.dumps(dict(x),  cls=DecimalEncoder).encode('utf-8'), topic="ton.public.jetton_transfers"),
                            cursor.fetchall()))
        
    # for debugging purposese
    def get_account_state_for_processing(self, address):

        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                select * from latest_account_states where account = %s
                """, 
                (address,),
            )
            return list(map(lambda x: FakeRecord(value=json.dumps(dict(x)).encode('utf-8'), topic="ton.public.latest_account_states"),
                            cursor.fetchall()))
        
    def get_mc_libraries(self) -> []:
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("select boc from parsed.mc_libraries")
            return [x['boc'] for x in cursor.fetchall()]
        
    """
    Calculates weighted average price for the base asset using latest prices for all pools and
    updates the prices.agg_prices table. Trades for the last {average_window} seconds are used for
    the calculation. Every price has weight that is equal to its volume multipleid by time_lag,
    which is ranged from 1 to 0 in exponential order. The more recent the price the higher its weight.
    """
    def update_agg_prices(self, base, price_time, average_window=1800):
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
            insert into prices.agg_prices(base, price_time, price_ton, price_usd, created, updated)
            with latest_prices as (
                select price_ton, price_usd,
                volume_usd, volume_ton,
                pow(2, -1. * (%s - swap_utime) / %s) as time_lag
                from prices.dex_trade where base = %s
                and swap_utime <= %s and swap_utime > %s - %s 
            )
            select %s as base,
            %s as price_time,
            sum(price_ton * time_lag * volume_ton) / sum(volume_ton * time_lag) as price_ton,
            sum(price_usd * time_lag * volume_usd) / sum(volume_usd * time_lag) as price_usd,
            now() as created, now() as updated
            from latest_prices
            on conflict (base, price_time) do update
            set price_ton = EXCLUDED.price_ton,
            price_usd = EXCLUDED.price_usd,
            updated = now()
                            """, (price_time, average_window, base, price_time, price_time, average_window, base, price_time))
            self.updated += 1

    def discover_dex_pool(self, swap: DexSwapParsed):
        if swap.swap_pool in self.dex_pools_cache:
            return
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
                insert into prices.dex_pool(pool, platform, discovered_at)
                           values (%s, %s, %s)
                on conflict do nothing
                            """, (swap.swap_pool, swap.platform, swap.swap_utime))
            self.dex_pools_cache.add(swap.swap_pool)
            
    """
    Returns all dex pools as DexPool objects with jetton addresses filled only
    """
    def get_all_dex_pools(self) -> Dict[str, DexPool]:
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("select pool, platform, jetton_left, jetton_right from prices.dex_pool")
            output = {}
            for row in cursor.fetchall():
                pool = DexPool(
                    pool=row['pool'],
                    platform=row['platform'],
                    jetton_left=row['jetton_left'],
                    jetton_right=row['jetton_right']
                )
                if pool.jetton_left:
                    pool.jetton_left = Address(pool.jetton_left)
                if pool.jetton_right:
                    pool.jetton_right = Address(pool.jetton_right)
                output[row['pool']] = pool
            return output
        

    def update_dex_pool_jettons(self, pool: DexPool):
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
                update prices.dex_pool
                           set jetton_left = %s, jetton_right = %s
                           where pool = %s
                            """, (serialize_addr(pool.jetton_left), serialize_addr(pool.jetton_right), pool.pool))
            for jetton in [pool.jetton_left, pool.jetton_right]:
                cursor.execute("""
                    insert into prices.dex_pool_link(jetton, pool)
                            values (%s, %s)
                    on conflict do nothing
                                """, (serialize_addr(jetton), pool.pool))
            self.updated += 1

    def update_dex_pool_state(self, pool: DexPool):
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(f"""
                update prices.dex_pool
                           set reserves_left = %s, reserves_right = %s, total_supply = %s,
                           tvl_usd = %s, tvl_ton = %s, last_updated = %s, is_liquid = %s,
                           lp_fee = %s, protocol_fee = %s, referral_fee = %s
                           where pool = %s and (last_updated < %s or last_updated is null)
                            """, (pool.reserves_left, pool.reserves_right, pool.total_supply,
                                  pool.tvl_usd, pool.tvl_ton, pool.last_updated, pool.is_liquid, pool.lp_fee, pool.protocol_fee, pool.referral_fee, pool.pool, pool.last_updated))
            
            if self.dex_pool_history:
                cursor.execute(f"""
                    insert into prices.dex_pool_history (pool, timestamp, reserves_left, reserves_right, total_supply, tvl_usd, tvl_ton)
                            values (%s, %s, %s, %s, %s, %s, %s)
                            on conflict do nothing
                                """, (pool.pool, pool.last_updated, pool.reserves_left, pool.reserves_right, 
                                    pool.total_supply, pool.tvl_usd, pool.tvl_ton))
            self.updated += 1

    """
    Returns jetton metadata
    """
    def get_jetton_metadata(self, address: str) -> JettonMetadata:
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("select * from parsed.jetton_metadata where address = %s", (address,))
            row = cursor.fetchone()
            if not row:
                return None
            return JettonMetadata(
                address=row['address'],
                update_time_onchain=row['update_time_onchain'],
                update_time_metadata=row['update_time_metadata'],
                mintable=row['mintable'],
                admin_address=row['admin_address'],
                jetton_content_onchain=row['jetton_content_onchain'],
                jetton_wallet_code_hash=row['jetton_wallet_code_hash'],
                code_hash=row['code_hash'],
                metadata_status=row['metadata_status'],
                symbol=row['symbol'],
                name=row['name'],
                description=row['description'],
                image=row['image'],
                image_data=row['image_data'],
                decimals=row['decimals'],
                sources=row['sources'],
                tonapi_image_url=row['tonapi_image_url']
            )
        
    """
    Returns NFT item metadata
    """
    def get_nft_item_metadata(self, address: str) -> NFTItemMetadata:
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("select * from parsed.nft_item_metadata where address = %s", (address,))
            row = cursor.fetchone()
            if not row:
                return None
            return NFTItemMetadata(
                address=row['address'],
                update_time_onchain=row['update_time_onchain'],
                update_time_metadata=row['update_time_metadata'],
                collection_address=row['collection_address'],
                content=row['content'],
                metadata_status=row['metadata_status'],
                name=row['name'],
                description=row['description'],
                attributes=row['attributes'],
                image=row['image'],
                image_data=row['image_data'],
                sources=row['sources'],
                tonapi_image_url=row['tonapi_image_url']
            )
        
    """
    Returns NFT collection metadata
    """
    def get_nft_collection_metadata(self, address: str) -> NFTCollectionMetadata:
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("select * from parsed.nft_collection_metadata where address = %s", (address,))
            row = cursor.fetchone()
            if not row:
                return None
            return NFTCollectionMetadata(
                address=row['address'],
                update_time_onchain=row['update_time_onchain'],
                update_time_metadata=row['update_time_metadata'],
                owner_address=row['owner_address'],
                content=row['content'],
                metadata_status=row['metadata_status'],
                name=row['name'],
                description=row['description'],
                image=row['image'],
                image_data=row['image_data'],
                sources=row['sources'],
                tonapi_image_url=row['tonapi_image_url']
            )
        
    """
    Upsert jetton metadata
    """
    def upsert_jetton_metadata(self, metadata: JettonMetadata, prev_ts_onchain: int, prev_ts_offchain: int):
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            jetton_content = metadata.jetton_content_onchain
            if jetton_content and type(jetton_content) == dict:
                jetton_content = json.dumps(jetton_content)
            cursor.execute("""
            insert into parsed.jetton_metadata(address, update_time_onchain, update_time_metadata, mintable, admin_address, 
                           jetton_content_onchain, jetton_wallet_code_hash, code_hash, metadata_status, symbol, name, description,
                            image, image_data, decimals, sources, tonapi_image_url)
                           values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                           on conflict (address) do update
                           set update_time_onchain = EXCLUDED.update_time_onchain,
                           update_time_metadata = EXCLUDED.update_time_metadata,
                           mintable = EXCLUDED.mintable,
                           admin_address = EXCLUDED.admin_address,
                           jetton_content_onchain = EXCLUDED.jetton_content_onchain,
                           jetton_wallet_code_hash = EXCLUDED.jetton_wallet_code_hash,
                           code_hash = EXCLUDED.code_hash,
                           metadata_status = EXCLUDED.metadata_status,
                           symbol = EXCLUDED.symbol,
                           name = EXCLUDED.name,
                           description = EXCLUDED.description,
                           image = EXCLUDED.image,
                           image_data = EXCLUDED.image_data,
                           decimals = EXCLUDED.decimals,
                           sources = EXCLUDED.sources,
                           tonapi_image_url = EXCLUDED.tonapi_image_url
                           -- where jetton_metadata.update_time_onchain = %s and jetton_metadata.update_time_metadata = %s
            """, (metadata.address, metadata.update_time_onchain, metadata.update_time_metadata, metadata.mintable, metadata.admin_address,
                  jetton_content, metadata.jetton_wallet_code_hash, metadata.code_hash, metadata.metadata_status,
                  metadata.symbol, metadata.name, metadata.description, metadata.image, metadata.image_data, metadata.decimals, metadata.sources,
                  metadata.tonapi_image_url, prev_ts_onchain, prev_ts_offchain))
            self.updated += 1

    """
    Upsert NFT item metadata
    """
    def upsert_nft_item_metadata(self, metadata: NFTItemMetadata, prev_ts_onchain: int, prev_ts_offchain: int):
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            content = metadata.content
            if content and type(content) == dict:
                content = json.dumps(content)
            attributes = metadata.attributes
            if attributes and type(attributes) in (dict, list):
                attributes = json.dumps(attributes)
            cursor.execute("""
            insert into parsed.nft_item_metadata(address, update_time_onchain, update_time_metadata, 
                           collection_address, content, metadata_status, name, description, attributes,
                           image, image_data, sources, tonapi_image_url)
                           values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                           on conflict (address) do update
                           set update_time_onchain = EXCLUDED.update_time_onchain,
                           update_time_metadata = EXCLUDED.update_time_metadata,
                           collection_address = EXCLUDED.collection_address,
                           content = EXCLUDED.content,
                           metadata_status = EXCLUDED.metadata_status,
                           name = EXCLUDED.name,
                           description = EXCLUDED.description,
                           attributes = EXCLUDED.attributes,
                           image = EXCLUDED.image,
                           image_data = EXCLUDED.image_data,
                           sources = EXCLUDED.sources,
                           tonapi_image_url = EXCLUDED.tonapi_image_url
                           -- where jetton_metadata.update_time_onchain = %s and jetton_metadata.update_time_metadata = %s
            """, (metadata.address, metadata.update_time_onchain, metadata.update_time_metadata, metadata.collection_address, content,
                  metadata.metadata_status, metadata.name, metadata.description, attributes, metadata.image, metadata.image_data,
                  metadata.sources, metadata.tonapi_image_url, prev_ts_onchain, prev_ts_offchain))
            self.updated += 1

    """
    Upsert NFT collection metadata
    """
    def upsert_nft_collection_metadata(self, metadata: NFTCollectionMetadata, prev_ts_onchain: int, prev_ts_offchain: int):
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            content = metadata.content
            if content and type(content) == dict:
                content = json.dumps(content)
            cursor.execute("""
            insert into parsed.nft_collection_metadata(address, update_time_onchain, update_time_metadata, 
                           owner_address, content, metadata_status, name, description,
                           image, image_data, sources, tonapi_image_url)
                           values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                           on conflict (address) do update
                           set update_time_onchain = EXCLUDED.update_time_onchain,
                           update_time_metadata = EXCLUDED.update_time_metadata,
                           content = EXCLUDED.content,
                           owner_address = EXCLUDED.owner_address,
                           metadata_status = EXCLUDED.metadata_status,
                           name = EXCLUDED.name,
                           description = EXCLUDED.description,
                           image = EXCLUDED.image,
                           image_data = EXCLUDED.image_data,
                           sources = EXCLUDED.sources,
                           tonapi_image_url = EXCLUDED.tonapi_image_url
                           -- where jetton_metadata.update_time_onchain = %s and jetton_metadata.update_time_metadata = %s
            """, (metadata.address, metadata.update_time_onchain, metadata.update_time_metadata, metadata.owner_address, content, 
                  metadata.metadata_status, metadata.name, metadata.description, metadata.image, metadata.image_data, metadata.sources,
                  metadata.tonapi_image_url, prev_ts_onchain, prev_ts_offchain))
            self.updated += 1

    def get_jetton_transfers_by_trace_id(self, trace_id: str) -> List[dict]:
        assert self.conn is not None
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("select * from public.jetton_transfers where trace_id = %s", (trace_id,))
            return cursor.fetchall()
