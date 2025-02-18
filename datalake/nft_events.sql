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
  