create schema if not exists parsed;
create schema if not exists prices;

-- initial script

CREATE TABLE IF NOT EXISTS parsed.mc_libraries (
	boc varchar NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS mc_libraries_md5_idx ON parsed.mc_libraries USING btree (md5((boc)::text));

-- core prices

CREATE TABLE IF NOT EXISTS prices.core (
	tx_hash bpchar(44) NOT NULL,
	lt int8 NULL,
	asset varchar NOT NULL,
	price numeric NOT NULL,
	price_ts int8 NULL,
	created timestamp NULL,
	updated timestamp NULL,
	CONSTRAINT core_pkey PRIMARY KEY (tx_hash)
);
CREATE INDEX IF NOT EXISTS core_asset_idx ON prices.core USING btree (asset, price_ts DESC);


-- GasPump events

DO $$ BEGIN
    create type parsed.gaspump_event as enum('DeployAndBuyEmitEvent', 'BuyEmitEvent', 'SellEmitEvent');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS parsed.gaspump_trade (
    tx_hash bpchar(44) NULL primary key,
    trace_id bpchar(44) NULL,
    event_time int4 NULL,
    jetton_master varchar NULL,
    event_type parsed.gaspump_event  NULL,
    trader_address varchar null,
    ton_amount numeric NULL,
    jetton_amount numeric NULL,
    fee_ton_amount numeric NULL,
    input_ton_amount numeric NULL,
    bonding_curve_overflow bool NULL,
    created timestamp NULL,
    updated timestamp NULL
);


DO $$ BEGIN
-- DEX Swaps
CREATE TYPE public."dex_name" AS ENUM (
	'dedust',
	'ston.fi');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS parsed.dex_swap_parsed (
	tx_hash bpchar(44) NULL,
	msg_hash bpchar(44) NOT NULL,
	trace_id bpchar(44) NULL,
	platform public."dex_name" NULL,
	swap_utime int8 NULL,
	swap_user varchar NULL,
	swap_pool varchar NULL,
	swap_src_token varchar NULL,
	swap_dst_token varchar NULL,
	swap_src_amount numeric NULL,
	swap_dst_amount numeric NULL,
	referral_address varchar NULL,
	reserve0 numeric NULL,
	reserve1 numeric NULL,
	query_id numeric NULL,
	min_out numeric NULL,
	volume_usd numeric NULL,
	volume_ton numeric NULL,
	created timestamp NULL,
	updated timestamp NULL,
	CONSTRAINT dex_swap_parsed_pkey PRIMARY KEY (msg_hash)
);
CREATE INDEX IF NOT EXISTS dex_swap_parsed_swap_utime_idx ON parsed.dex_swap_parsed USING btree (swap_utime);
CREATE INDEX IF NOT EXISTS dex_swap_parsed_tx_hash_idx ON parsed.dex_swap_parsed USING btree (tx_hash);

-- ston.fi V2 support
DO $$ BEGIN
    ALTER TYPE public.dex_name ADD VALUE 'ston.fi_v2' AFTER 'ston.fi';
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

ALTER TABLE parsed.dex_swap_parsed ADD column if not exists router varchar NULL;


CREATE TABLE IF NOT EXISTS parsed.jetton_mint (
    tx_hash bpchar(44) NULL primary key,
    msg_hash bpchar(44) NULL,
    trace_id bpchar(44) NULL,
    utime int4 NULL,
    successful boolean NULL,
    query_id numeric NULL,
    amount numeric NULL,
    minter varchar NULL,
    from_address varchar NULL,
    wallet varchar NULL,
    response_destination varchar NULL,
    forward_ton_amount numeric NULL,
    forward_payload bytea NULL,
    created timestamp NULL,
    updated timestamp NULL
);

ALTER TABLE parsed.jetton_mint ADD column if not exists "owner" varchar NULL;
ALTER TABLE parsed.jetton_mint ADD column if not exists "jetton_master_address" varchar NULL;

--  required for fast lookup of parent message by msg_hash
CREATE INDEX IF NOT EXISTS trace_edges_msg_hash_idx ON public.trace_edges (msg_hash);

-- to be aligned with jetton_transfers and jetton_burn
ALTER TABLE parsed.jetton_mint ADD column if not exists "tx_lt" int8 NULL;

CREATE TABLE IF NOT EXISTS parsed.jetton_metadata (
    address public."tonaddr" not NULL primary key,
    update_time_onchain int4 null, -- onchain
    update_time_metadata int4 null, -- metadata update time
    mintable bool NULL, -- from onchain
  	admin_address public."tonaddr" NULL, -- from onchain
    jetton_content_onchain jsonb NULL, -- onchain
  	jetton_wallet_code_hash public."tonhash" NULL, -- onchain
	code_hash public."tonhash" null, -- onchain
	metadata_status int, -- 1 - ok, -1 error, 0 on chain only
	symbol varchar null, -- on/offchain
	name varchar null, -- on/offchain
	description varchar null, -- on/offchain
	image varchar null, -- on/offchain
	image_data varchar null, -- on/offchain
	decimals smallint null, -- on/offchain
	sources varchar null, -- [on|off] 6 times for symbol, name, description, image, image_data, decimals
	tonapi_image_url varchar null -- tonapi image url
);

-- megaton dex support
DO $$ BEGIN
    ALTER TYPE public.dex_name ADD VALUE 'megaton' AFTER 'ston.fi_v2';
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- required for megaton parser
CREATE INDEX IF NOT EXISTS jetton_transfers_trace_id_idx ON public.jetton_transfers (trace_id);

-- TonFun
CREATE TABLE IF NOT EXISTS parsed.tonfun_bcl_trade (
    tx_hash bpchar(44) NULL primary key,
    trace_id bpchar(44) NULL,
    event_time int4 NULL,
    bcl_master varchar NULL,
    event_type varchar NULL,
    trader_address varchar null,
    ton_amount numeric NULL,
    bcl_amount numeric NULL,
    referral_ver int8 NULL,
    partner_address varchar NULL,
    platform_tag varchar NULL,
    extra_tag varchar NULL,
    created timestamp NULL,
    updated timestamp NULL
);

--MemesLab
CREATE TABLE IF NOT EXISTS parsed.memeslab_trade_event (
    tx_hash bpchar(44) NULL PRIMARY KEY,
    trace_id bpchar(44) NULL,
    event_time int4 NULL,
    jetton_master varchar NULL,
    event_type varchar NULL,
    trader_address varchar NULL,
    ton_amount numeric NULL,
    jetton_amount numeric NULL,
    current_supply numeric NULL,
    total_ton_collected numeric NULL,
    volume_usd numeric NULL,
    created timestamp NULL,
    updated timestamp NULL
);
-- add memeslab_trade_event primary key
BEGIN;
ALTER TABLE parsed.memeslab_trade_event DROP CONSTRAINT memeslab_trade_event_pkey;
ALTER TABLE parsed.memeslab_trade_event ADD PRIMARY KEY (tx_hash, event_type);
COMMIT;

-- Uranus memepad
CREATE TABLE IF NOT EXISTS parsed.uranus_trade (
    tx_hash bpchar(44) NULL,
    trace_id bpchar(44) NULL,
    msg_hash bpchar(44) NULL,
    event_time int4 NULL,
    meme_master varchar NULL,
    event_type varchar NULL,
    trader_address varchar NULL,
    amount_in numeric NULL,
    amount_out numeric NULL,
    creator_fee numeric NULL,
    protocol_fee numeric NULL,
    partner_fee numeric NULL,
    referrer_fee numeric NULL,
    current_supply numeric NULL,
    raised_funds numeric NULL,
    is_graduated bool NULL,
    volume_usd numeric NULL,
    created timestamp NULL,
    updated timestamp NULL
);
BEGIN;
ALTER TABLE parsed.uranus_trade ADD PRIMARY KEY (tx_hash, event_type);
COMMIT;


-- Adding usd volume for memepads
ALTER TABLE parsed.gaspump_trade ADD column if not exists "volume_usd" numeric NULL;
ALTER TABLE parsed.tonfun_bcl_trade ADD column if not exists "volume_usd" numeric NULL;

-- Fixing tonfun_bcl_trade primary key
BEGIN;
ALTER TABLE parsed.tonfun_bcl_trade DROP CONSTRAINT tonfun_bcl_trade_pkey;
ALTER TABLE parsed.tonfun_bcl_trade ADD PRIMARY KEY (tx_hash, event_type);
COMMIT;

-- Adding project name
ALTER TABLE parsed.tonfun_bcl_trade ADD column if not exists "project" varchar NULL;

-- tonco DEC support
DO $$ BEGIN
    ALTER TYPE public.dex_name ADD VALUE 'tonco' AFTER 'megaton';
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Coffee DEX support
DO $$ BEGIN
    ALTER TYPE public.dex_name ADD VALUE 'coffee' AFTER 'tonco';
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Add fees info for dex swaps

CREATE TABLE IF NOT EXISTS prices.dex_pool (
	pool varchar NOT null primary KEY,
	platform public.dex_name NULL,
	discovered_at int4 null,
	jetton_left varchar null,
	jetton_right varchar null,
	reserves_left numeric null,
	reserves_right numeric null,
    total_supply numeric null,
    tvl_usd numeric NULL,
    tvl_ton numeric NULL,
    last_updated int4 null,
    is_liquid boolean null
);

ALTER TABLE prices.dex_pool ADD column if not exists "lp_fee" numeric NULL;
ALTER TABLE prices.dex_pool ADD column if not exists "protocol_fee" numeric NULL;
ALTER TABLE prices.dex_pool ADD column if not exists "referral_fee" numeric NULL;

CREATE TABLE IF NOT EXISTS prices.dex_pool_link (
    id serial primary key,
    jetton varchar null,
    pool varchar null references prices.dex_pool(pool)
);

-- Bidask CLMM DEX support
DO $$ BEGIN
    ALTER TYPE public.dex_name ADD VALUE 'bidask_clmm' AFTER 'tonco';
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Moon DEX support
DO $$ BEGIN
    ALTER TYPE public.dex_name ADD VALUE 'moon.cx' AFTER 'bidask_clmm';
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Bidask DAMM DEX support
DO $$ BEGIN
    ALTER TYPE public.dex_name ADD VALUE 'bidask_damm' AFTER 'moon.cx';
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- CPMM v3 DEX support
DO $$ BEGIN
    ALTER TYPE public.dex_name ADD VALUE 'cpmm_pool_v3' AFTER 'bidask_damm';
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Staking pools

CREATE TABLE IF NOT EXISTS parsed.staking_pools_nominators (
    pool varchar NULL,
    address varchar NULL,
    utime int4 NULL,
    lt int8 NULL,
    balance numeric NULL,
    pending numeric NULL,
    CONSTRAINT staking_pools_nominators_pkey PRIMARY KEY (pool, address)
);

CREATE INDEX IF NOT EXISTS staking_pools_nominators_idx ON parsed.staking_pools_nominators (address);

-- NFT items

CREATE TABLE IF NOT EXISTS parsed.nft_items (
	address public."tonaddr" NOT NULL,
	init bool NULL,
	"index" numeric NULL,
	collection_address public."tonaddr" NULL,
	owner_address public."tonaddr" NULL,
	"content" jsonb NULL,
	last_transaction_lt int8 NULL,
	last_tx_now int4 NULL,
	CONSTRAINT nft_items_parsed_pkey PRIMARY KEY (address)
);

-- NFT item metadata

CREATE TABLE IF NOT EXISTS parsed.nft_item_metadata (
	address public.tonaddr NOT NULL PRIMARY KEY,
	update_time_onchain int4 NULL,
	update_time_metadata int4 NULL,
	"content" jsonb NULL,
	metadata_status int4 NULL,
	"name" varchar NULL,
	description varchar NULL,
	"attributes" jsonb NULL,
	image varchar NULL,
	image_data varchar NULL,
	sources varchar NULL,
	tonapi_image_url varchar NULL
);

ALTER TABLE parsed.nft_item_metadata ADD column if not exists "collection_address" public."tonaddr" NULL;

-- NFT collection metadata

CREATE TABLE IF NOT EXISTS parsed.nft_collection_metadata (
	address public.tonaddr NOT NULL PRIMARY KEY,
	update_time_onchain int4 NULL,
	update_time_metadata int4 NULL,
  	owner_address public."tonaddr" NULL,
	"content" jsonb NULL,
	metadata_status int4 NULL,
	"name" varchar NULL,
	description varchar NULL,
	image varchar NULL,
	image_data varchar NULL,
	sources varchar NULL,
	tonapi_image_url varchar NULL
);

-- NFT sales that are not supported by TON indexer

CREATE TABLE IF NOT EXISTS parsed.extra_nft_sales
(
    id serial PRIMARY KEY,
    address varchar NULL,
    is_complete boolean NULL,
    created_at bigint NULL,
    marketplace_address varchar NULL,
    nft_address varchar NULL,
    nft_owner_address varchar NULL,
    full_price numeric NULL,
    asset varchar NULL,
    marketplace_fee_address varchar NULL,
    marketplace_fee numeric NULL,
    royalty_address varchar NULL,
    royalty_amount numeric NULL,
    last_transaction_lt bigint NULL,
    last_tx_now integer NULL,
    code_hash varchar NULL,
    data_hash varchar NULL,
    created timestamp NULL,
    updated timestamp NULL
);
