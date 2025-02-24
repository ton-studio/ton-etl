-- SQL queries to build initial NFT events table based on full history
-- NFT mints
create table datalake.nft_events_initial_mints 
with (
      external_location = 's3://ton-blockchain-public-datalake-test/nft_events/nft_events_initial_mints'
)
as
with nft_items_states_ranks as (
  select ni.*, row_number() over (partition by ni.address order by ni.lt asc ) as rank
  from datalake.nft_items ni where block_date <= '20250212'
), nft_items_latest_state as (
  select * from nft_items_states_ranks where rank = 1
),
raw_min_tx as (
  SELECT ni.*, t.hash as tx_hash, t.trace_id
  FROM datalake.transactions t 
  join nft_items_latest_state ni on t.account = ni.address and t.block_date = ni.block_date -- must be in the same partition
  and end_status ='active' and orig_status != 'active' -- new account deployment
  where t.block_date <= '20250212'
), mints as (
  select block_date, address as nft_item_address, is_init, index as nft_item_index,
  collection_address, owner_address, content_onchain, timestamp, lt, tx_hash, trace_id,
  (select account from transactions t 
    where t.trace_id = raw_min_tx.trace_id order by lt asc limit 1) as deployer
  from raw_min_tx
)
select * from mints

-- NFT put_on_sale

create table datalake.nft_events_initial_put_on_sale
with (
      external_location = 's3://ton-blockchain-public-datalake-test/nft_events/nft_events_initial_put_on_sale'
)
as
with transfers_to_sale_contracts as (
  select row_number() over(partition by ns.address order by ns.lt asc) as rank, t.*,
  ns.address as nft_sale_contract, type as sale_type, ns.nft_owner_address, end_time,
  marketplace_address, marketplace_fee_address,
  marketplace_fee, price, asset, royalty_address, royalty_amount, max_bid, min_bid, min_step
  from datalake.nft_transfers t
  join nft_sales ns on ns.address = t.new_owner and t.nft_item_address = ns.nft_address
  where t.block_date <= '20250212' and ns.block_date <= '20250212'
  and not t.tx_aborted
), put_on_sale as (
    select t.block_date, nft_item_address, nft_item_index, nft_collection_address as collection_address,
    nft_owner_address as owner_address,
    is_init, content_onchain, row_number() over(partition by ni.address order by ni.lt desc) as nft_state_rank, -- from nft_items
    tx_now as timestamp, tx_lt as lt, tx_hash, trace_id, forward_amount, forward_payload, comment,
    nft_sale_contract, sale_type, end_time as sale_end_time, marketplace_address, marketplace_fee_address,
    marketplace_fee, price, asset, royalty_address, royalty_amount, max_bid, min_bid, min_step, query_id, custom_payload
    from transfers_to_sale_contracts t
    join datalake.nft_items ni on ni.address = nft_item_address and ni.lt <= tx_lt
    where rank = 1 -- first state of the sale contract
)
select * from put_on_sale where nft_state_rank = 1

-- NFT cancel_sale
create table datalake.nft_events_initial_cancel_sale
with (
      external_location = 's3://ton-blockchain-public-datalake-test/nft_events/nft_events_initial_cancel_sale'
)
as
with transfers_from_sale_contracts_to_owner as (
  select row_number() over(partition by ns.address order by ns.lt asc) as rank,  t.*,
  ns.address as nft_sale_contract, type as sale_type, ns.nft_owner_address, end_time, marketplace_address, marketplace_fee_address,
  marketplace_fee, price, asset, royalty_address, royalty_amount, max_bid, min_bid, min_step 
  from datalake.nft_transfers t
  join nft_sales ns on ns.address = t.old_owner and t.nft_item_address = ns.nft_address and ns.nft_owner_address = t.new_owner
  and (ns.is_complete or ns.is_canceled)
  where t.block_date <= '20250212' and ns.block_date <= '20250212'
  and not t.tx_aborted
), cancel_sale as (
  select t.block_date, nft_item_address, nft_item_index, nft_collection_address as collection_address,
  nft_owner_address as owner_address, 
  is_init, content_onchain, row_number() over(partition by ni.address order by ni.lt desc) as nft_state_rank, -- from nft_items
  tx_now as timestamp, tx_lt as lt, tx_hash, trace_id, forward_amount, forward_payload, comment,
  nft_sale_contract, sale_type, end_time as sale_end_time, marketplace_address, marketplace_fee_address,
  marketplace_fee, price, asset, royalty_address, royalty_amount, max_bid, min_bid, min_step, query_id, custom_payload
  from transfers_from_sale_contracts_to_owner t
  join datalake.nft_items ni on ni.address = nft_item_address and ni.lt <= tx_lt
  where rank = 1 -- first state after sale completion
)
select * from cancel_sale where nft_state_rank = 1

-- NFT sale

create table datalake.nft_events_initial_sale
with (
      external_location = 's3://ton-blockchain-public-datalake-test/nft_events/nft_events_initial_sale'
)
as
with transfers_from_sale_contracts_to_buyer as (
  select row_number() over(partition by ns.address order by ns.lt asc) as rank,  t.*,
  ns.address as nft_sale_contract, type as sale_type, ns.nft_owner_address as seller, t.new_owner as buyer, end_time, marketplace_address, marketplace_fee_address,
  marketplace_fee, price, asset, royalty_address, royalty_amount, max_bid, min_bid, min_step 
  from datalake.nft_transfers t
  join nft_sales ns on ns.address = t.old_owner and t.nft_item_address = ns.nft_address and ns.nft_owner_address != t.new_owner
  and (ns.is_complete or ns.is_canceled)
  where t.block_date <= '20250212' and ns.block_date <= '20250212'
  and not t.tx_aborted
), sales as (
  select t.block_date, nft_item_address, nft_item_index, nft_collection_address as collection_address,
  seller, buyer,
  is_init, content_onchain, row_number() over(partition by ni.address order by ni.lt desc) as nft_state_rank, -- from nft_items
  tx_now as timestamp, tx_lt as lt, tx_hash, trace_id, forward_amount, forward_payload, comment,
  nft_sale_contract, sale_type, end_time as sale_end_time, marketplace_address, marketplace_fee_address,
  marketplace_fee, price, asset, royalty_address, royalty_amount, max_bid, min_bid, min_step, query_id, custom_payload
  from transfers_from_sale_contracts_to_buyer t
  join datalake.nft_items ni on ni.address = nft_item_address and ni.lt <= tx_lt
  where rank = 1 -- the first state after the sale
)
select * from sales where nft_state_rank = 1


-- NFT transfers

create table datalake.nft_events_initial_ordinary_transfers
with (
      external_location = 's3://ton-blockchain-public-datalake-test/nft_events/nft_events_initial_ordinary_transfers'
)
as
with sales_related as (
  select tx_hash from datalake.nft_events_initial_put_on_sale
  union 
  select tx_hash from datalake.nft_events_initial_cancel_sale
  union 
  select tx_hash from datalake.nft_events_initial_sale
)
select t.*, 
(select is_init from datalake.nft_items ni where ni.address = nft_item_address order by ni.lt desc limit 1) as is_init,
(select content_onchain from datalake.nft_items ni where ni.address = nft_item_address order by ni.lt desc limit 1) as content_onchain
from datalake.nft_transfers t
left join sales_related s on s.tx_hash = t.tx_hash
where t.block_date <= '20250212'
and s.tx_hash is null
and not t.tx_aborted

-- TON DNS bid

create table datalake.nft_events_initial_tondns_bid
with (
      external_location = 's3://ton-blockchain-public-datalake-test/nft_events/nft_events_initial_tondns_bid'
)
as
with history as (
select *, lag(content_onchain, 1) over (partition by address order by lt asc) as prev_content  from datalake.nft_items 
where collection_address ='0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF'
and block_date <= '20250212'
)
select address, is_init, index, collection_address, owner_address, content_onchain, timestamp, lt, block_date, json_extract_scalar(content_onchain, '$.max_bid_address') as bidder,
cast(json_extract_scalar(content_onchain, '$.max_bid_amount') as bigint) as price
from history where
  json_extract_scalar(content_onchain, '$.max_bid_address') is not null and 
  (
  json_extract_scalar(content_onchain, '$.max_bid_address') !=
  json_extract_scalar(prev_content, '$.max_bid_address') or 
  
  json_extract_scalar(content_onchain, '$.max_bid_amount') !=
  json_extract_scalar(prev_content, '$.max_bid_amount')
  
  or prev_content is null
  )
  

  -- TON DNS release

create table datalake.nft_events_initial_tondns_release
with (
  external_location = 's3://ton-blockchain-public-datalake-test/nft_events/nft_events_initial_tondns_release'
)
as
with history as (
select *, lag(owner_address, 1) over (partition by address order by lt asc) as prev_owner  from datalake.nft_items 
where collection_address ='0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF'
and block_date <= '20250212'
)
select address, is_init, index, collection_address, owner_address, content_onchain, timestamp, lt, block_date,
  prev_owner
from  history
where owner_address is null and prev_owner is not null

-- TON DNS auction implicit finish
create table datalake.nft_events_initial_tondns_auction_implicit_finish
with (
  external_location = 's3://ton-blockchain-public-datalake-test/nft_events/nft_events_initial_tondns_auction_implicit_finish'
)
as
with history as (
  select *, lag(timestamp, 1) over (partition by address order by lt desc) as next_event, lag(owner_address, 1) over (partition by address order by lt desc) as next_owner
  from datalake.nft_items 
  where collection_address ='0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF'
  and block_date <= '20250212'
), winners as (
  select distinct address, is_init, index, collection_address,
  json_extract_scalar(content_onchain, '$.max_bid_address')  as owner_address,
  content_onchain, 
  cast(json_extract_scalar(content_onchain, '$.auction_end_time') as bigint) as timestamp, 0 as lt,
  date_format(from_unixtime(cast(json_extract_scalar(content_onchain, '$.auction_end_time') as bigint)), '%Y%m%d') as block_date,
  cast(json_extract_scalar(content_onchain, '$.max_bid_amount') as bigint) as price-- , block_date --, lt as orig_lt, timestamp as orig_timestamp
  from  history
  where next_event is not null and next_event > cast(json_extract_scalar(content_onchain, '$.auction_end_time') as bigint)
  and json_extract_scalar(content_onchain, '$.max_bid_address')  is not null
), duplicates_ranks as (
  --  due to batch commits we can have some wrong entries, let's exlude rows if time between buy events of the same address is less then one day
  select *, lag(timestamp, 1) over (partition by address order by timestamp desc) as adjustent_winner_ts from winners
)
 select address, is_init, index, collection_address, owner_address, content_onchain, timestamp, lt, block_date, price from duplicates_ranks
 where adjustent_winner_ts is null -- last win event
 or adjustent_winner_ts - timestamp > 86400

 -- Telemint auction

create table datalake.nft_events_initial_telemint_bid
with (
      external_location = 's3://ton-blockchain-public-datalake-test/nft_events/nft_events_initial_telemint_bid'
)
as
with telemint_collections as (
select distinct collection_address from datalake.nft_items 
where json_extract_scalar(content_onchain, '$.bidder_address') is not null
), history as (
select *, lag(content_onchain, 1) over (partition by address order by lt asc) as prev_content  from datalake.nft_items 
where collection_address in (select * from telemint_collections)
and block_date <= '20250212'
)
select distinct address, is_init, index, collection_address, owner_address, content_onchain, timestamp, lt, block_date,
json_extract_scalar(content_onchain, '$.bidder_address') as bidder,
cast(json_extract_scalar(content_onchain, '$.bid') as bigint) as price
from history where
  json_extract_scalar(content_onchain, '$.bidder_address') is not null and 
  (
  json_extract_scalar(content_onchain, '$.bidder_address') !=
  json_extract_scalar(prev_content, '$.bidder_address') or 
  
  json_extract_scalar(content_onchain, '$.bid') !=
  json_extract_scalar(prev_content, '$.bid')
  or prev_content is null
  )


-- Telemin put on sale

create table datalake.nft_events_initial_telemint_put_on_sale
with (
      external_location = 's3://ton-blockchain-public-datalake-test/nft_events/nft_events_initial_telemint_put_on_sale'
)
as
with telemint_collections as (
select distinct collection_address from datalake.nft_items 
where json_extract_scalar(content_onchain, '$.bidder_address') is not null
), history as (
select *, lag(content_onchain, 1) over (partition by address order by lt asc) as prev_content  from datalake.nft_items 
where collection_address in (select * from telemint_collections)
and block_date <= '20250212'
)
select distinct address, is_init, index, collection_address, owner_address, content_onchain, timestamp, lt, block_date, 
try(cast(json_extract_scalar(content_onchain, '$.initial_min_bid') as bigint)) as price,
try(cast(json_extract_scalar(content_onchain, '$.max_bid') as bigint)) as max_bid,
try(cast(json_extract_scalar(content_onchain, '$.min_bid') as bigint)) as min_bid,
try(cast(json_extract_scalar(content_onchain, '$.min_bid_step') as bigint)) as min_bid_step,
try(cast(json_extract_scalar(content_onchain, '$.end_time') as bigint)) as sale_end_time
from history where
  json_extract_scalar(content_onchain, '$.duration') is not null and 
  json_extract_scalar(prev_content, '$.duration') is null
  and owner_address is not null
  and try(json_extract_scalar(content_onchain, '$.initial_min_bid')) is not null

-- Telemin cancel sale

create table datalake.nft_events_initial_telemint_cancel_sale
with (
      external_location = 's3://ton-blockchain-public-datalake-test/nft_events/nft_events_initial_telemint_cancel_sale'
)
as
with telemint_collections as (
select distinct collection_address from datalake.nft_items 
where json_extract_scalar(content_onchain, '$.bidder_address') is not null
), history as (
select *, lag(content_onchain, 1) over (partition by address order by lt asc) as prev_content,
lag(owner_address, 1) over (partition by address order by lt asc) as prev_owner from datalake.nft_items 
where collection_address in (select * from telemint_collections)
and block_date <= '20250212'
)
select distinct address, is_init, index, collection_address, owner_address, content_onchain, timestamp, lt, block_date, 
greatest(coalesce(try(cast(json_extract_scalar(prev_content, '$.bid') as bigint)), 0),
coalesce(try(cast(json_extract_scalar(prev_content, '$.initial_min_bid') as bigint)), 0)
) as price,
try(cast(json_extract_scalar(prev_content, '$.max_bid') as bigint)) as max_bid,
try(cast(json_extract_scalar(prev_content, '$.min_bid') as bigint)) as min_bid,
try(cast(json_extract_scalar(prev_content, '$.min_bid_step') as bigint)) as min_bid_step,
try(cast(json_extract_scalar(prev_content, '$.end_time') as bigint)) as sale_end_time
from history where
  json_extract_scalar(prev_content, '$.duration') is not null and 
  json_extract_scalar(content_onchain, '$.duration') is null
  and owner_address is not null and owner_address = prev_owner


-- Telemin sale

create table datalake.nft_events_initial_telemint_sales
with (
      external_location = 's3://ton-blockchain-public-datalake-test/nft_events/nft_events_initial_telemint_sales'
)
as
with telemint_collections as (
select distinct collection_address from datalake.nft_items 
where json_extract_scalar(content_onchain, '$.bidder_address') is not null
), history as (
select *, lag(content_onchain, 1) over (partition by address order by lt asc) as prev_content,
lag(owner_address, 1) over (partition by address order by lt asc) as prev_owner from datalake.nft_items 
where collection_address in (select * from telemint_collections)
and block_date <= '20250212'
)
select distinct address, is_init, index, collection_address, owner_address as buyer, prev_owner as seller, content_onchain, timestamp, lt, block_date, 
coalesce(cast(json_extract_scalar(prev_content, '$.bid') as bigint), 0) as price,
try(cast(json_extract_scalar(prev_content, '$.max_bid') as bigint)) as max_bid,
try(cast(json_extract_scalar(prev_content, '$.min_bid') as bigint)) as min_bid,
try(cast(json_extract_scalar(prev_content, '$.min_bid_step') as bigint)) as min_bid_step,
try(cast(json_extract_scalar(prev_content, '$.end_time') as bigint)) as sale_end_time,
prev_owner
from history where
  json_extract_scalar(prev_content, '$.duration') is not null and 
  json_extract_scalar(content_onchain, '$.duration') is null
  and owner_address is not null and prev_owner is not null and owner_address != prev_owner


-- final join

create table "datalake".nft_events
with (
            format = 'AVRO', 
            write_compression = 'SNAPPY',
            external_location = 's3://ton-blockchain-public-datalake-test/nft_events/joined',
            bucketed_by = ARRAY['nft_item_address'],
            bucket_count = 1,
            partitioned_by = ARRAY['block_date']
)
as
with mint as (
    select 'mint' as type, nft_item_address, is_init, nft_item_index, collection_address,
    owner_address, content_onchain, timestamp, lt, tx_hash, trace_id,
    deployer as prev_owner,
    cast (null as decimal(20,0)) as query_id,
    cast (null as decimal(38, 0)) as forward_amount, cast(null as varbinary) as forward_payload,
    cast(null as varchar) as comment, cast(null as varbinary) as custom_payload, cast(null as varchar) as sale_contract,
    cast(null as varchar) as sale_type,
    cast(null as bigint) as sale_end_time, cast(null as varchar) as marketplace_address,
    cast(null as varchar) as marketplace_fee_address, cast(null as bigint) as marketplace_fee,
    cast(null as bigint) as sale_price, cast(null as varchar) as payment_asset,
    cast(null as varchar) as royalty_address, cast(null as bigint) as royalty_amount,
    cast(null as bigint) as auction_max_bid, cast(null as bigint) as auction_min_bid,
    cast(null as bigint) as auction_min_step, block_date from datalake.nft_events_initial_mints 
), put_on_sale as (
    select 'put_on_sale' as type, nft_item_address, is_init, nft_item_index, collection_address,
    owner_address, content_onchain, timestamp, lt, tx_hash, trace_id,
    cast(null as varchar) as prev_owner, query_id,
    forward_amount, forward_payload,
    comment, custom_payload,nft_sale_contract as sale_contract, sale_type,
    sale_end_time, marketplace_address,
    marketplace_fee_address, marketplace_fee,
    price as sale_price, asset as payment_asset,
    royalty_address, royalty_amount,
    max_bid as auction_max_bid, min_bid as auction_min_bid,
    min_step as auction_min_step, block_date from datalake.nft_events_initial_put_on_sale
), cancel_sale as (
    select 'cancel_sale' as type, nft_item_address, is_init, nft_item_index, collection_address,
    owner_address, content_onchain, timestamp, lt, tx_hash, trace_id,
    cast(null as varchar) as prev_owner, query_id,
    forward_amount, forward_payload,
    comment, custom_payload,nft_sale_contract as sale_contract, sale_type,
    sale_end_time, marketplace_address,
    marketplace_fee_address, marketplace_fee,
    price as sale_price, asset as payment_asset,
    royalty_address, royalty_amount,
    max_bid as auction_max_bid, min_bid as auction_min_bid,
    min_step as auction_min_step, block_date from datalake.nft_events_initial_cancel_sale
), sale as (
    select 'sale' as type, nft_item_address, is_init, nft_item_index, collection_address,
    buyer as owner_address, content_onchain, timestamp, lt, tx_hash, trace_id,
    seller as prev_owner, query_id,
    forward_amount, forward_payload,
    comment, custom_payload,nft_sale_contract as sale_contract, sale_type,
    sale_end_time, marketplace_address,
    marketplace_fee_address, marketplace_fee,
    price as sale_price, asset as payment_asset,
    royalty_address, royalty_amount,
    max_bid as auction_max_bid, min_bid as auction_min_bid,
    min_step as auction_min_step, block_date from datalake.nft_events_initial_sale
), transfers as (
    select 'transfer' as type, nft_item_address, is_init, nft_item_index, 
    nft_collection_address as collection_address,
    new_owner as owner_address, content_onchain, tx_now as timestamp,
    tx_lt as lt, tx_hash, trace_id,
    old_owner as prev_owner, query_id,
    forward_amount, forward_payload,
    comment, custom_payload, cast(null as varchar) as sale_contract,
    cast(null as varchar) as sale_type,
    cast(null as bigint) as sale_end_time, cast(null as varchar) as marketplace_address,
    cast(null as varchar) as marketplace_fee_address, cast(null as bigint) as marketplace_fee,
    cast(null as bigint) as sale_price, cast(null as varchar) as payment_asset,
    cast(null as varchar) as royalty_address, cast(null as bigint) as royalty_amount,
    cast(null as bigint) as auction_max_bid, cast(null as bigint) as auction_min_bid,
    cast(null as bigint) as auction_min_step, block_date from datalake.nft_events_initial_ordinary_transfers
), tondns_bids as (
    select 'bid' as type, address as nft_item_address, is_init, index as nft_item_index, 
    collection_address,
    owner_address, content_onchain, timestamp,
    lt, cast(null as varchar) as tx_hash, cast(null as varchar) as  trace_id,
    bidder as prev_owner, cast(null as decimal(20, 0)) as query_id,
    cast(null as decimal(38, 0)) as forward_amount, cast(null as varbinary)  as forward_payload,
    cast(null as varchar)  as comment, cast(null as varbinary) as custom_payload,
    cast(null as varchar) as sale_contract,
    'auction' as sale_type,
    cast(null as bigint) as sale_end_time, collection_address as marketplace_address,
    collection_address as marketplace_fee_address, price as marketplace_fee,
    price as sale_price, 'TON' as payment_asset,
    cast(null as varchar) as royalty_address, cast(null as bigint) as royalty_amount,
    cast(null as bigint) as auction_max_bid, cast(null as bigint) as auction_min_bid,
    cast(null as bigint) as auction_min_step, block_date from datalake.nft_events_initial_tondns_bid
), tondns_release as (
    select 'transfer' as type, address as nft_item_address, is_init, index as nft_item_index, 
    collection_address,
    owner_address, content_onchain, timestamp,
    lt, cast(null as varchar) as tx_hash, cast(null as varchar) as  trace_id,
    prev_owner, cast(null as decimal(20, 0)) as query_id,
    cast(null as decimal(38, 0)) as forward_amount, cast(null as varbinary)  as forward_payload,
    cast(null as varchar)  as comment, cast(null as varbinary) as custom_payload,
    cast(null as varchar) as sale_contract,
    cast(null as varchar) as sale_type,
    cast(null as bigint) as sale_end_time, cast(null as varchar) as marketplace_address,
    cast(null as varchar) as marketplace_fee_address, cast(null as bigint) as marketplace_fee,
    cast(null as bigint)  as sale_price, cast(null as varchar) as payment_asset,
    cast(null as varchar) as royalty_address, cast(null as bigint) as royalty_amount,
    cast(null as bigint) as auction_max_bid, cast(null as bigint) as auction_min_bid,
    cast(null as bigint) as auction_min_step, block_date from datalake.nft_events_initial_tondns_release
), tondns_finish as (
    select 'sale' as type, address as nft_item_address, is_init, index as nft_item_index, 
    collection_address,
    owner_address, content_onchain, timestamp,
    lt, cast(null as varchar) as tx_hash, cast(null as varchar) as  trace_id,
    cast(null as varchar) as prev_owner, cast(null as decimal(20, 0)) as query_id,
    cast(null as decimal(38, 0)) as forward_amount, cast(null as varbinary)  as forward_payload,
    cast(null as varchar)  as comment, cast(null as varbinary) as custom_payload,
    cast(null as varchar) as sale_contract,
    'auction' as sale_type,
    timestamp , collection_address as marketplace_address,
    collection_address as marketplace_fee_address, price as marketplace_fee,
    price as sale_price, 'TON' as payment_asset,
    cast(null as varchar) as royalty_address, cast(null as bigint) as royalty_amount,
    price as auction_max_bid, cast(null as bigint) as auction_min_bid,
    cast(null as bigint) as auction_min_step, block_date from datalake.nft_events_initial_tondns_auction_implicit_finish
), telemint_bids as (
    select 'bid' as type, address as nft_item_address, is_init, index as nft_item_index, 
    collection_address,
    owner_address, content_onchain, timestamp,
    lt, cast(null as varchar) as tx_hash, cast(null as varchar) as  trace_id,
    bidder as prev_owner, cast(null as decimal(20, 0)) as query_id,
    cast(null as decimal(38, 0)) as forward_amount, cast(null as varbinary)  as forward_payload,
    cast(null as varchar)  as comment, cast(null as varbinary) as custom_payload,
    cast(null as varchar) as sale_contract,
    'auction' as sale_type,
    cast(null as bigint) as sale_end_time,
    '0:408DA3B28B6C065A593E10391269BAAA9C5F8CAEBC0C69D9F0AABBAB2A99256B' as marketplace_address, -- Fragment
    '0:408DA3B28B6C065A593E10391269BAAA9C5F8CAEBC0C69D9F0AABBAB2A99256B' as marketplace_fee_address,
    try(price * 5 / 100) as marketplace_fee, -- constant 5%
    price as sale_price, 'TON' as payment_asset,
    cast(null as varchar) as royalty_address, cast(null as bigint) as royalty_amount,
    cast(null as bigint) as auction_max_bid, cast(null as bigint) as auction_min_bid,
    cast(null as bigint) as auction_min_step, block_date from datalake.nft_events_initial_telemint_bid
), telemint_put_on_sale as (
    select 'put_on_sale' as type, address as nft_item_address, is_init, index as nft_item_index, 
    collection_address,
    owner_address, content_onchain, timestamp,
    lt, cast(null as varchar) as tx_hash, cast(null as varchar) as  trace_id,
    cast(null as varchar) as prev_owner, cast(null as decimal(20, 0)) as query_id,
    cast(null as decimal(38, 0)) as forward_amount, cast(null as varbinary)  as forward_payload,
    cast(null as varchar)  as comment, cast(null as varbinary) as custom_payload,
    cast(null as varchar) as sale_contract,
    'auction' as sale_type,
    sale_end_time as sale_end_time,
    '0:408DA3B28B6C065A593E10391269BAAA9C5F8CAEBC0C69D9F0AABBAB2A99256B' as marketplace_address, -- Fragment
    '0:408DA3B28B6C065A593E10391269BAAA9C5F8CAEBC0C69D9F0AABBAB2A99256B' as marketplace_fee_address,
    try(price * 5 / 100) as marketplace_fee, -- constant 5%
    price as sale_price, 'TON' as payment_asset,
    cast(null as varchar) as royalty_address, cast(null as bigint) as royalty_amount,
    max_bid as auction_max_bid, min_bid as auction_min_bid,
    min_bid_step as auction_min_step, block_date from datalake.nft_events_initial_telemint_put_on_sale
), telemint_cancel_sale as (
    select 'cancel_sale' as type, address as nft_item_address, is_init, index as nft_item_index, 
    collection_address,
    owner_address, content_onchain, timestamp,
    lt, cast(null as varchar) as tx_hash, cast(null as varchar) as  trace_id,
    cast(null as varchar) as prev_owner, cast(null as decimal(20, 0)) as query_id,
    cast(null as decimal(38, 0)) as forward_amount, cast(null as varbinary)  as forward_payload,
    cast(null as varchar)  as comment, cast(null as varbinary) as custom_payload,
    cast(null as varchar) as sale_contract,
    'auction' as sale_type,
    sale_end_time as sale_end_time,
    '0:408DA3B28B6C065A593E10391269BAAA9C5F8CAEBC0C69D9F0AABBAB2A99256B' as marketplace_address, -- Fragment
    '0:408DA3B28B6C065A593E10391269BAAA9C5F8CAEBC0C69D9F0AABBAB2A99256B' as marketplace_fee_address,
    try(price * 5 / 100) as marketplace_fee, -- constant 5%
    price as sale_price, 'TON' as payment_asset,
    cast(null as varchar) as royalty_address, cast(null as bigint) as royalty_amount,
    max_bid as auction_max_bid, min_bid as auction_min_bid,
    min_bid_step as auction_min_step, block_date from  datalake.nft_events_initial_telemint_cancel_sale
), telemint_sale as (
    select 'sale' as type, address as nft_item_address, is_init, index as nft_item_index, 
    collection_address,
    buyer as owner_address, content_onchain, timestamp,
    lt, cast(null as varchar) as tx_hash, cast(null as varchar) as  trace_id,
    prev_owner as prev_owner, cast(null as decimal(20, 0)) as query_id,
    cast(null as decimal(38, 0)) as forward_amount, cast(null as varbinary)  as forward_payload,
    cast(null as varchar)  as comment, cast(null as varbinary) as custom_payload,
    cast(null as varchar) as sale_contract,
    'auction' as sale_type,
    sale_end_time as sale_end_time,
    '0:408DA3B28B6C065A593E10391269BAAA9C5F8CAEBC0C69D9F0AABBAB2A99256B' as marketplace_address, -- Fragment
    '0:408DA3B28B6C065A593E10391269BAAA9C5F8CAEBC0C69D9F0AABBAB2A99256B' as marketplace_fee_address,
    price * 5 / 100 as marketplace_fee, -- constant 5%
    price as sale_price, 'TON' as payment_asset,
    cast(null as varchar) as royalty_address, cast(null as bigint) as royalty_amount,
    max_bid as auction_max_bid, min_bid as auction_min_bid,
    min_bid_step as auction_min_step, block_date from datalake.nft_events_initial_telemint_sales
),
output as (
    select * from mint
    union all
    select * from put_on_sale
    union all
    select * from cancel_sale
    union all
    select * from sale
    union all
    select * from transfers
    union all
    select * from tondns_bids
    union all
    select * from tondns_release
    union all
    select * from tondns_finish
    union all
    select * from telemint_bids
    union all
    select * from telemint_put_on_sale
    union all
    select * from telemint_cancel_sale
    union all
    select * from telemint_sale
)
select distinct * from output