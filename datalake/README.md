# Datalake exporters

TON-ETL consist of multiple data processing layers and the final one is exporters. The main goal for exporters is to
prepare data for external usage (normalize it, fit into the same model and send to the final destination).
Currently two main destinations are supported:
* AWS S3 Data Lake 
* Near real-time data streaming via public Kafka topics


## AWS S3 Data Lake

Datalake endpoints:
* Mainnet: s3://ton-blockchain-public-datalake/v1/ (eu-central-1 region)

All data tables are stored in separate folders and named by data type. Data is partitioned by block date. Block date
is extracted from specific field for each data type and converted into string in __YYYYMMDD__ format.
Initially data is partitioned by adding date, but at the end of the day it is re-partitioned using [Airflow DAG](./airflow/dags/datalake_daily_sync.py) (see below).

SNS notifications are enabled for the bucket, SNS ARN is ``arn:aws:sns:eu-central-1:180088995794:TONPublicDataLakeNotifications``.

## Near real-time  data streaming via pulic Kafka topics

AWS S3 Data Lake is suitable for batch processing but it doesn't support real-time  data processing.
Pulic Kafka topics are introduced to address this limitation. Data updates from TON-ETL are converted using the
same schema converters as S3 Data Lake and sent to the public Kafka topics. 
Kafka topics endpoints:
* Mainnet: kafka.redoubt.online:9094

Connection params:
* Protocol: SASL_PLAINTEXT, SCRAM-SHA-512
* Data format: JSON
* Data retention: 7 days
* GroupId: mandatory
* Username and password: provided by TON-ETL team

List of topics supported:
* indexer.streaming_account_states
* indexer.streaming_blocks
* indexer.streaming_dex_trades
* indexer.streaming_jetton_events
* indexer.streaming_jetton_metadata
* indexer.streaming_messages (with raw bodies)
* indexer.streaming_transactions
* indexer.streaming_dex_pools

Public Kafka topics are available for free, but to provide better observability and performance we would like to ask you to
contact us for connection credentials linked to your organisation using [the following form](https://docs.google.com/forms/d/e/1FAIpQLSc4OhA1pe6OzyaG_gb8plAG8XlJpOkcAw7vo8fSeDeBBGFmCA/viewform?usp=sf_link).

Note that the data stream is near real-time with with estimated delay in range 10-30 seconds after the block is processed. 
This delay originates from the multiple reasons:
* Blocks propagation 
* RocksDB indexing
* Decoding layer
* External Kafka broker replication latency


# Data types

All target destinations share the same data model (but underlying data format may be different). This section describes
the data model for supported data types.

## Blocks

[AVRO schema](./schemas/blocks_export.avsc)

Partition field: __gen_utime__
URL: **s3://ton-blockchain-public-datalake/v1/blocks/**

Contains information about blocks (masterchain and workchains).

## Transactions

[AVRO schema](./schemas/transactions_export.avsc).

Partition field: __now__
URL: **s3://ton-blockchain-public-datalake/v1/transactions/**

Additionaly we are adding account_state_code_hash_after and account_state_balance_after fields.

## Messages

[AVRO schema](./schemas/messages_export.avsc)

Partition field: __tx_now__
URL: **s3://ton-blockchain-public-datalake/v1/messages/**

Contains messages from transactions. Internal messages are included twice with different direction:
* in - message that initiated transaction
* out - message that was result of transaction


## Messages with raw bodies

[AVRO schema](./schemas/messages_with_data_export.avsc)

Partition field: __tx_now__
URL: **s3://ton-blockchain-public-datalake/v1/messages_with_body/**

Contains the same data as ``messages`` table with two more fields:
* body_boc - raw body of the message body
* init_state_boc - raw init state (if present) from the message

## Account states

[AVRO schema](./schemas/account_states.avsc)

Partition field: __timestamp__
URL: **s3://ton-blockchain-public-datalake/v1/account_states/**

Contains raw account states with raw data and code.


## Jetton events

[AVRO schema](./schemas/jetton_events_export.avsc)

Partition field: __utime__
URL: **s3://ton-blockchain-public-datalake/v1/jetton_events/**

Contains jetton events, event type is defined in ``type`` field:
* transfer - TEP-74 transfer event
* burn - TEP-74 burn event
* mint - TEP-74 jetton standard does not specify mint format but it has recommended form of internal_transfer message. 
So we are using it as mint event. Also there are some jetton-specific mint implementations, 
the current implementation supports HIPO hTON mints.

All jetton events include tx_aborted field, pay attention that if it is ``false`` then the event should be discarded.
Aborted events are stored becase it could be useful for some types of analysis.

``source`` field is set to ``null`` for mint events and ``destination`` is set to ``null`` for burn events.

Note that fields ``query_id``, ``forward_ton_amount``, ``amount`` are stored as  decimal values with scale equals to 0. Since the data mart
doesn't support off-chain metadata and stores only raw data the amount is stored as raw value without dividing by 10^decimals.

## Jetton metadata

[AVRO schema](./schemas/jetton_metadata.avsc)

Partition field: __adding_at__
URL: **s3://ton-blockchain-public-datalake/v1/jetton_metadata/**

According to [TEP-64](https://github.com/ton-blockchain/TEPs/blob/master/text/0064-token-data-standard.md)
standard Jetton metadata could be stored off-chain or on-chain (or even both). Having in mind that the data could be
stored off-chain and may not be available at the time of processing we are using the following approach:
* Get on-chain metadata
* If off-chain metadata is available and the field is present, store value from off-chain metadata
* If off-chain metadata is not available, fallback to [tonapi API V2](https://tonapi.io/api-v2#operations-Jettons-getJettonInfo)
to extract jetton metadata cached by tonapi.

Also for all jettons cached image from tonapi is requested. It is a mitigation for the case when image is not available 
any longer.

Fields description:
* address - jetton master address
* update_time_onchain - time of on-chain update (for example in case of admin address transfer)
* update_time_metadata - time of off-chain update
* mintable - mintable flag (on-chain value)
* admin_address - admin address (on-chain value)
* jetton_content_onchain - JSON serialized into string with on-chain jetton content
* jetton_wallet_code_hash - jetton wallet code_hash (on-chain value)
* code_hash - jetton code hash (on-chain value)
* metadata_status - off-chain metadata update status (0 - no off-chain metadata, 1 - success, -1 - error)
* symbol - TEP-64 jetton symbol (on-chain or off-chain value, see sources field for details)
* name - TEP-64 jetton name (on-chain or off-chain value, see sources field for details)
* description - TEP-64 jetton description (on-chain or off-chain value, see sources field for details)
* image - TEP-64 jetton image url (on-chain or off-chain value, see sources field for details)
* image_data - TEP-64 jetton image data (on-chain or off-chain value, see sources field for details)
* decimals - TEP-64 jetton decimals (on-chain or off-chain value, see sources field for details). Note that if decimals fields is not present it means that the jetton uses default value of 9.
* sources - recored with sources of jetton metadata fields (6 fields for symbol, name, description, image, image_data, decimals). Possible values are:
    * "" - field is not set
    * "onchain" - field is based on on-chain value
    * "offchain" - field is based on off-chain value
    * "tonapi" - tonapi was used to get the value (fallback)
* tonapi_image_url - tonapi cached image url
* adding_date - partition field, date when the output file was created

## Jetton metadata snapshots

``jetton_metadata`` table allows to get the full history of jetton metadata changes but for most cases it is more suitable to have
the latest snapshot of jetton metadata. To simplify usage in this case daily snapshots with the latest metadata are created.

Partition field: __snapshot_date__
URL: **s3://ton-blockchain-public-datalake/v1/jetton_metadata_snapshots/**

This table contains the same data as ``jetton_metadata`` table but only for the latest snapshot for each jetton.

Note: for performance reasons daily snapshots are splited into 10 files.

It is recommended to use ``jetton_metadata_latest`` view to get the latest snapshot of jetton metadata (see [athena_ddl.sql](./athena_ddl.sql)).


## DEX Trades

[AVRO schema](./schemas/dex_trades.avsc)

Partition field: __event_time__
URL: **s3://ton-blockchain-public-datalake/v1/dex_trades/**

Contains dex and launchpad trades data. 

Fields description:
* tx_hash, trace_id - transaction hash and trace id
* project_type - project type, possible values: ``dex`` for classical AMM DEXs and ``launchpad`` for bonding curve launcpads
* project - project name, see the list below
* version - project version
* event_time - timestamp of the event
* event_type - ``trade`` or ``launch``. ``launch`` is used for the event when liquidity is collected from the bonding curve and sent to DEX.
* trader_address - address of the trader
* pool_address - address of the pool, ``null`` if the pool is not known
* router_address - address of the router, ``null`` if the router is not used by the project (see table below)
* token_sold_address, token_bought_address - address of the token sold/bought. See below the list of special wrapped TON aliases.
* amount_sold_raw, amount_bought_raw - amount of the token sold/bought as raw value without dividing by 10^decimals. To get decimals use ``jetton_metadata`` table.
* referral_address - referral address, ``null`` if the referral is not specified or not supported by the project (see table below)
* platform_tag - platform address, ``null`` if the platform is not specified or not supported by the project (see table below)
* query_id - query id, ``null`` if the query id is not supported by the project (see table below)
* volume_ton - volume in TON
* volume_usd - volume in USD

Volume estimation is based on the amount of tokens sold/bought in the current trade and it is calculated only if the trade involves one of the following assets:
* TON (or wrapped TON)
* USDT (or wrapped USDT or USDC)
* LSD (stTON, tsTON, hTON)

Supported projects:
| Project Type | Project Name | Description | Features |
|--------------|--------------|-------------|----------|
| dex | [ston.fi](https://app.ston.fi/swap) | Decentralized exchange with AMM pools. Supported [version 1](https://docs.ston.fi/docs/developer-section/api-reference-v1) and [version 2](https://docs.ston.fi/docs/developer-section/api-reference-v2). | referral_address, router_address (v2 only), query_id |
| dex | [dedust.io](https://app.dedust.io/) | Only [Protocol 2.0](https://docs.dedust.io/docs/introduction) is supported | referral_address |
| dex | [megaton.fi](https://megaton.fi/) | Decentralized exchange with AMM pools | router_address |
| dex | [tonco](https://app.tonco.io/) | Decentralized exchange with CLMM AMM pools | router_address, query_id |
| launchpad | [ton.fun](https://tonfun-1.gitbook.io/tonfun) | Launchpad SDK adopted by multiple projects ([Blum](https://blum.io/), [BigPump](https://docs.pocketfi.org/features/big.pump), etc) | referral_address, platform_tag |
| launchpad | [gaspump](https://gaspump.tg/) | Bonding curve launchpad for memecoins ([docs](https://github.com/gas111-bot/gaspump-sdk)) | - |

TON Aliases and wrapped TONs used by the projects:
* 0:0000000000000000000000000000000000000000000000000000000000000000 - native TON (dedust, ton.fun, gaspump)
* 0:8CDC1D7640AD5EE326527FC1AD0514F468B30DC84B0173F0E155F451B4E11F7C - pTON (ston.fi)
* 0:671963027F7F85659AB55B821671688601CDCF1EE674FC7FBBB1A776A18D34A3 - pTONv2 (ston.fi)
* 0:D0A1CE4CDC187C79615EA618BD6C29617AF7A56D966F5A192A768F345EE63FD2 - WTON (ston.fi)
* 0:9A8DA514D575D20234C3FB1395EE9138F5F1AD838ABC905DC42C2389B46BD015 - WTON (megaton.fi)

## DEX Pools (TVL)

[AVRO schema](./schemas/dex_pools.avsc)

Partition field: __last_updated__
URL: **s3://ton-blockchain-public-datalake/v1/dex_pools/**

Contains history of DEX pools states. Each state includes static information (jettons in the pools), slowly changing fields (fees), reserves and TVL estimated in USD and TON.

Fields:
* pool - pool address
* project - project name (the same as in ``dex_trades`` table)
* version  - project version (the same as in ``dex_trades`` table)
* discovered_at - timestamp of the pool discovery, i.e. first swap in the pool
* jetton_left, jetton_right - addresses of the jettons in the pool (fixed for each pool address, could not be changed)
* reserves_left, reserves_right - raw amount of the jettons in the pool
* total_supply - total supply of the pool LP-jetton. In case of TONCO - number of active NFT positions.
* is_liquid - flag if the pool is liquid. Pool is considered liquid if it has at TON, LSD or stable coin. Full list of supported assets see in [swap_volume.py](../parser/parsers/message/swap_volume.py). 
* tvl_usd, tvl_ton - TVL of the pool in USD and TON. null for pools with is_liquid=false.
* last_updated - timestamp of the pool update (swap or pool LP-jetton mint/burn)
* lp_fee, protocol_fee, referral_fee - fees for the pool. Total fee is equal to lp_fee + protocol_fee + referral_fee (only if referral address is present during a swap). 

Note that for ston.fi v2 referral_fee is always null, it is specified in each swap but it is not
parsed just right now.

## Balances history

[AVRO schema](./schemas/balances_history.avsc)

Partition field: __timestamp__
URL: **s3://ton-blockchain-public-datalake/v1/balances_history/**

Contains balances history for native TON balances and Jetton balances. Fields:
* address - address of the asset owner
* asset - asset type, ``TON`` for native TON or jetton address for Jetton balance
* amount - balance amount
* mintless_claimed - boolean flag if the mintless jetton was claimed (only for mintless jetton wallets)
* timestamp - timestamp of the balance update
* lt - logical time of the balance update

### Balances history snapshots

For convenience daily snapshots of the balances history are created. It allows to get the latest balance for each address and asset without
full scan of the ``balances_history`` table. Partitioned by ``block_date`` field (i.e. date of the latest block in the snapshot). 

URL: **s3://ton-blockchain-public-datalake/v1/balances_snapshot/**

Contains the same fields as ``balances_history`` table. Note that in future old snapshots will be removed.
To get the latest snapshot one can use the following query (to get top-100 TON holders):

```sql
with latest_balances as (
select * from "balances_snapshot"
where block_date = (SELECT max(block_date) FROM "balances_snapshot$partitions")
)
select * from latest_balances
where asset = 'TON' order by amount desc
limit 100
```

## NFT items

[AVRO schema](./schemas/nft_items.avsc)

Partition field: __timestamp__
URL: **s3://ton-blockchain-public-datalake/v1/nft_items/**

Contains [TEP-62](https://github.com/ton-blockchain/TEPs/blob/master/text/0062-nft-standard.md) NFT full history of NFT items states. Includes on-chain metadata.

Fields:
* address - NFT address
* is_init - true if the NFT is initialized
* index - NFT index
* collection_address - NFT collection address (may be null)
* owner_address - NFT owner address
* content_onchain - NFT metadata extracted from the on-chain data
* timestamp - timestamp of the NFT state update
* lt - logical time of the NFT state update

## NFT transfers

[AVRO schema](./schemas/nft_transfers.avsc)

Partition field: __tx_now__
URL: **s3://ton-blockchain-public-datalake/v1/nft_transfers/**

Contains history of NFT transfers. Includes the following fields:
* tx_hash - transaction hash
* tx_lt - transaction logical time
* tx_now - transaction block timestamp
* tx_aborted - transaction aborted flag (aborted=true means that transfer was not successful)
* query_id - query id
* nft_item_address - NFT item address
* nft_item_index - NFT item index
* nft_collection_address - NFT collection address (may be null)
* old_owner - old owner address
* new_owner - new owner address
* response_destination, custom_payload, forward_amount, forward_payload - see [TEP-62](https://github.com/ton-blockchain/TEPs/blob/master/text/0062-nft-standard.md)
* comment - text comment from forward_payload
* trace_id - trace id from the transaction


## NFT sales contracts history

[AVRO schema](./schemas/nft_sales.avsc)

Partition field: __timestamp__
URL: **s3://ton-blockchain-public-datalake/v1/nft_sales/**

Contains history of NFT sales contracts. Includes the following fields:
* address - NFT sales contract address
* type - sale/auction. sale means fixed price sale, auction means auction with bids.
* nft_address - NFT item address
* nft_owner_address - NFT owner address
* created_at - timestamp of the NFT sales contract creation
* is_complete - true if the sale is complete
* is_canceled - true if the sale is canceled (only for auction)
* end_time - time of expiration of the sale
* marketplace_address - address of the marketplace
* marketplace_fee_address - address of the marketplace fee
* marketplace_fee - amount of the marketplace fee
* price - price of the NFT (current bid for auction)
* asset - asset type, ``TON`` for native TON
* royalty_address - address of the royalty
* royalty_amount - amount of the royalty
* max_bid, min_bid, min_step - max bid, min bid and min bit step for auction
* last_bit_at, last_member - information about the last bid
* timestamp, lt - state timestamp and logical time

## NFT metadata

[AVRO schema](./schemas/nft_metadata.avsc)

Partition field: __adding_at__
URL: **s3://ton-blockchain-public-datalake/v1/nft_metadata/**

This data source is quite similar to [jetton_metadata](#jetton-metadata). It contains NFT metadata for both items and collections.

Fields description:
* type - possible values: ``item`` or ``collection``
* address - NFT item/collection address
* update_time_onchain - time of on-chain update (for example in case of admin address transfer)
* update_time_metadata - time of off-chain update
* parent_address - collection owner address if type is ``collection``, otherwise collection address for the item
* content_onchain - JSON serialized into string with on-chain metadata content
* metadata_status - off-chain metadata update status (0 - no off-chain metadata, 1 - success, -1 - error)
* name - TEP-64 NFT name (on-chain or off-chain value, see sources field for details)
* description - TEP-64 NFT description (on-chain or off-chain value, see sources field for details)
* image - TEP-64 NFT image url (on-chain or off-chain value, see sources field for details)
* image_data - TEP-64 NFT image data (on-chain or off-chain value, see sources field for details)
* attributes - TEP-64 NFT attributes (on-chain or off-chain value, see sources field for details). Only for items.
* sources - recored with sources of NFT metadata fields (5 fields for name, description, image, image_data, attributes). Possible values are:
    * "" - field is not set
    * "onchain" - field is based on on-chain value
    * "offchain" - field is based on off-chain value
    * "tonapi" - tonapi was used to get the value (fallback)
* tonapi_image_url - tonapi cached image url
* adding_date - partition field, date when the output file was created

In most cases we don't need to use the full history of NFT metadata, so we recommend to use ``nft_metadata_latest`` view to get the latest snapshot of NFT metadata (see [athena_ddl.sql](./athena_ddl.sql)).

## NFT Events

[AVRO schema](./schemas/nft_events.avsc)

Partition field: __timestamp__
URL: **s3://ton-blockchain-public-datalake/v1/nft_events/**

Comprehensive data mart build on top of ``nft_items``, ``nft_transfers``, ``messages`` and ``nft_sales`` tables. Includes
all NFT related events including mint, transfer, sales, auctions, etc.
Fields:
* type - event type (see below)
* nft_item_address - NFT item address
* is_init - true if the NFT is initialized
* nft_item_index - NFT index
* collection_address - NFT collection address (may be null)
* owner_address - NFT owner address
* content_onchain - NFT metadata extracted from the on-chain data
* timestamp - timestamp of the NFT state update or action
* lt - logical time of the NFT state update or action
* tx_hash - transaction hash (if applicable)
* trace_id - trace id (if applicable)
* prev_owner - previous owner address (if applicable)
* query_id - query id (if exists)
* forward_amount - amount of the forward message from transfer message (if related to the event)
* forward_payload - payload of the forward message from transfer message (if related to the event)
* comment - text comment from forward_payload
* custom_payload - custom payload from the transfer message (if related to the event)
* sale_contract - address of the sale contract (if related to the event)
* sale_type - type of the sale (``sale`` or ``auction``)
* sale_end_time - end time of the sale (if applicable)
* marketplace_address - address of the marketplace
* marketplace_fee_address - address of the marketplace fee
* marketplace_fee - amount of the marketplace fee
* sale_price - price of the NFT
* payment_asset - asset type of the payment (only ``TON`` is supported)
* royalty_address - address of the royalty
* royalty_amount - amount of the royalty
* auction_max_bid, auction_min_bid, auction_min_step - max bid, min bid and min bit step for auction

Supported event types:

| Type | prev_owner | Comments |
|------|------------|----------|
| mint | deployer address | Tx related to the NFT deployment |
| put_on_sale | null | NFT is put on sale via sale contract |
| cancel_sale | null | Sale is cancelled and NFT is returned to the owner |
| sale | seller | NFT is sold via sale contract, see sale related fields for details |
| transfer | previous owner | Direct NFT transfer between addresses. Also includes automatic transfers of TON DNS in case of expiration. |
| bid | bidder | New bid for an auction |

NFT events are generated using the following script: [nft_events.sql](./nft_events.sql), daily sync DAG is available [here](./airflow/dags/datalake_daily_sync.py). In terms of sales
it the following implementations are supported:
* [GetGems compatible](https://github.com/getgems-io/nft-contracts) sales contracts
* [TON DNS NFTs](https://tonviewer.com/EQC3dNlesgVD8YbAazcauIrXBPfiVhMMr5YYk2in0Mtsz0Bz)
* [Telemint NFTs](https://github.com/TelegramMessenger/telemint)

# Data corrections

This section describes the list of data corrections that were applied to the data lake and should be removed or fixed. 
Data corrections are required in case of incorrect data was stored in the data lake and should be removed or fixed.
In such cases keys of the rows to be excluded are stored in the **excluded_rows** that is stored in **s3://ton-blockchain-public-datalake/v1/excluded_rows** in CSV format with two fields:
* table - name of the table
* key - key of the row in the table

Data corrections are applied to the data lake by partitions, i.e. full partition is regenerated so there are two options
to the consumers of the data lake:
1. Re-import the partitions
2. Apply data corrections manually


**1. DeDust trades data correction**

Date: 2024-12-11

Table: dex_trades

Key: tx_hash

Partitions: 20241201, 20241203, 20241204

Number of rows: 12

Full list: s3://ton-blockchain-public-datalake/v1/excluded_rows/dedust_20241211.csv

Description: DeDust swap parses used ext-out messages to detect swaps and before the fix
it was possible to create a smart contract that would send ext-out messages with arbitrary payload
resulting in incorrect trades data parsing. Some users utilised this bug and created three smart contracts
and sent 12 ext-outs. The fix was applied in [#65](https://github.com/re-doubt/ton-etl/pull/65) to mitigate the issue.

# Known issues

* __messages__, __account_states__ and other tables contains wrong values for [anycast addresses](https://docs.ton.org/v3/documentation/data-formats/tlb/msg-tlb#addr_std10) for messages before 02/02/2025 21:30 UTC.
* [Meridian NFT collection](https://tonviewer.com/EQAVGhk_3rUA3ypZAZ1SkVGZIaDt7UdvwA4jsSGRKRo-MRDN?section=overview) has a lack of onchain metadata for items before 2025-01-23 (out of gas issue for get methods).
* [DIGGER GAME PASS NFT collection](https://tonviewer.com/EQAQQD4LjKX7vOut9VZDnwDdXZVH4dCJ9s-_cqznLT9dCo1v) is not indexed due to frozen collection address.
* pTON ("wrapped" TON) transfers have incorrect amount field and have to be ignored for the following jettons: ``8CDC1D7640AD5EE326527FC1AD0514F468B30DC84B0173F0E155F451B4E11F7C`` and ``671963027F7F85659AB55B821671688601CDCF1EE674FC7FBBB1A776A18D34A3``


# Integration with Athena

As far the data is stored in S3 in Avro format Athena can directly read it. To start working with the data you need to create table with the DDL
provided in [athena_ddl.sql](./athena_ddl.sql) file and load partitions using ``MSCK REPAIR TABLE`` command.

## Query examples

Get most active jettons from the last 30 days:

```sql
with top_jettons as (
select jetton_master, approx_distinct(trace_id) operations from jetton_events
where block_date >= date_format(date_add('day', -30, current_date), '%Y%m%d')
group by 1
order by operations desc limit 10
)
select symbol, jetton_master, operations from top_jettons 
join jetton_metadata_latest on jetton_master = address
order by operations desc
```

Get actual owners for Telegram NFTs and TON DNS based on the ``nft_events`` table:

```sql
select 
case
    when collection_address = '0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF' 
    then 'TON DNS'
    when collection_address = '0:0E41DC1DC3C9067ED24248580E12B3359818D83DEE0304FABCF80845EAFAFDB2' 
    then 'TG Anonymous number'
    when collection_address = '0:80D78A35F955A14B679FAA887FF4CD5BFC0F43B4A4EEA2A7E6927F3701B273C2' 
    then 'TG Username'
end as asset_type,
case
    when collection_address = '0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF' 
    then json_extract_scalar(content_onchain, '$.domain')
    when collection_address = '0:0E41DC1DC3C9067ED24248580E12B3359818D83DEE0304FABCF80845EAFAFDB2' 
    then replace(replace(json_extract_scalar(content_onchain, '$.uri'), 'https://nft.fragment.com/number/', ''), '.json', '')
    when collection_address = '0:80D78A35F955A14B679FAA887FF4CD5BFC0F43B4A4EEA2A7E6927F3701B273C2' 
    then replace(replace(json_extract_scalar(content_onchain, '$.uri'), 'https://nft.fragment.com/username/', ''), '.json', '')
end as name,
owner_address, nft_item_address, content_onchain
from datalake.nft_items_latest_state 
where collection_address = '0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF'
 or collection_address = '0:0E41DC1DC3C9067ED24248580E12B3359818D83DEE0304FABCF80845EAFAFDB2'
 or collection_address = '0:80D78A35F955A14B679FAA887FF4CD5BFC0F43B4A4EEA2A7E6927F3701B273C2'
```

# Airflow DAGs

Daily increments are prepared using the following DAG: [datalake_daily_sync.py](./airflow/dags/datalake_daily_sync.py).
It is tested on Airflow 2.3.4. Required variables:
* DATALAKE_KAFKA_BROKER - Kafka broker endpoint
* DATALAKE_SOURCE_DATABASE - source database name for the tables from exporters
* DATALAKE_TARGET_DATABASE - target database name for the tables with daily increments
* DATALAKE_TMP_LOCATION - temporary location for Athena
* DATALAKE_ATHENA_DATALAKE_OUTPUT_BUCKET - S3 bucket for the datalake output
* DATALAKE_ATHENA_WORKGROUP - Athena workgroup name
