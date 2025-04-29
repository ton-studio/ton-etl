CREATE EXTERNAL TABLE `excluded_rows`(
  `table` string, 
  `key` string)
COMMENT 'Contains data corrections'
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/excluded_rows'
TBLPROPERTIES (
  'classification'='csv', 
  'transient_lastDdlTime'='1733881198')
  
CREATE EXTERNAL TABLE `account_states`(
  `account` string COMMENT 'from deserializer', 
  `hash` string COMMENT 'from deserializer', 
  `balance` bigint COMMENT 'from deserializer', 
  `account_status` string COMMENT 'from deserializer', 
  `timestamp` int COMMENT 'from deserializer', 
  `last_trans_hash` string COMMENT 'from deserializer', 
  `last_trans_lt` bigint COMMENT 'from deserializer', 
  `frozen_hash` string COMMENT 'from deserializer', 
  `data_hash` string COMMENT 'from deserializer', 
  `code_hash` string COMMENT 'from deserializer', 
  `data_boc` binary COMMENT 'from deserializer', 
  `code_boc` binary COMMENT 'from deserializer')
PARTITIONED BY ( 
  `block_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"latest_account_states\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"account\",\"type\":\"string\"},{\"name\":\"hash\",\"type\":\"string\"},{\"name\":\"balance\",\"type\":[\"long\",\"null\"]},{\"name\":\"account_status\",\"type\":[\"string\",\"null\"]},{\"name\":\"timestamp\",\"type\":[\"int\",\"null\"]},{\"name\":\"last_trans_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"last_trans_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"frozen_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"data_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"code_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"data_boc\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"code_boc\",\"type\":[\"bytes\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/account_states'
TBLPROPERTIES (
)

CREATE EXTERNAL TABLE `blocks`(
  `workchain` int COMMENT 'from deserializer', 
  `shard` bigint COMMENT 'from deserializer', 
  `seqno` int COMMENT 'from deserializer', 
  `root_hash` string COMMENT 'from deserializer', 
  `file_hash` string COMMENT 'from deserializer', 
  `mc_block_workchain` int COMMENT 'from deserializer', 
  `mc_block_shard` bigint COMMENT 'from deserializer', 
  `mc_block_seqno` int COMMENT 'from deserializer', 
  `global_id` int COMMENT 'from deserializer', 
  `version` int COMMENT 'from deserializer', 
  `after_merge` boolean COMMENT 'from deserializer', 
  `before_split` boolean COMMENT 'from deserializer', 
  `after_split` boolean COMMENT 'from deserializer', 
  `want_merge` boolean COMMENT 'from deserializer', 
  `want_split` boolean COMMENT 'from deserializer', 
  `key_block` boolean COMMENT 'from deserializer', 
  `vert_seqno_incr` boolean COMMENT 'from deserializer', 
  `flags` int COMMENT 'from deserializer', 
  `gen_utime` bigint COMMENT 'from deserializer', 
  `start_lt` bigint COMMENT 'from deserializer', 
  `end_lt` bigint COMMENT 'from deserializer', 
  `validator_list_hash_short` int COMMENT 'from deserializer', 
  `gen_catchain_seqno` int COMMENT 'from deserializer', 
  `min_ref_mc_seqno` int COMMENT 'from deserializer', 
  `prev_key_block_seqno` int COMMENT 'from deserializer', 
  `vert_seqno` int COMMENT 'from deserializer', 
  `master_ref_seqno` int COMMENT 'from deserializer', 
  `rand_seed` string COMMENT 'from deserializer', 
  `created_by` string COMMENT 'from deserializer', 
  `tx_count` int COMMENT 'from deserializer')
PARTITIONED BY ( 
  `block_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"blocks\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"workchain\",\"type\":\"int\"},{\"name\":\"shard\",\"type\":\"long\"},{\"name\":\"seqno\",\"type\":\"int\"},{\"name\":\"root_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"file_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"mc_block_workchain\",\"type\":[\"int\",\"null\"]},{\"name\":\"mc_block_shard\",\"type\":[\"long\",\"null\"]},{\"name\":\"mc_block_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"global_id\",\"type\":[\"int\",\"null\"]},{\"name\":\"version\",\"type\":[\"int\",\"null\"]},{\"name\":\"after_merge\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"before_split\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"after_split\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"want_merge\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"want_split\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"key_block\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"vert_seqno_incr\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"flags\",\"type\":[\"int\",\"null\"]},{\"name\":\"gen_utime\",\"type\":[\"long\",\"null\"]},{\"name\":\"start_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"end_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"validator_list_hash_short\",\"type\":[\"int\",\"null\"]},{\"name\":\"gen_catchain_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"min_ref_mc_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"prev_key_block_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"vert_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"master_ref_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"rand_seed\",\"type\":[\"string\",\"null\"]},{\"name\":\"created_by\",\"type\":[\"string\",\"null\"]},{\"name\":\"tx_count\",\"type\":[\"int\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/blocks'
TBLPROPERTIES (
)


CREATE EXTERNAL TABLE `jetton_events`(
  `type` string COMMENT 'from deserializer', 
  `tx_hash` string COMMENT 'from deserializer', 
  `tx_lt` bigint COMMENT 'from deserializer', 
  `utime` int COMMENT 'from deserializer', 
  `trace_id` string COMMENT 'from deserializer', 
  `tx_aborted` boolean COMMENT 'from deserializer', 
  `query_id` decimal(20,0) COMMENT 'from deserializer', 
  `amount` decimal(38,0) COMMENT 'from deserializer', 
  `source` string COMMENT 'from deserializer', 
  `destination` string COMMENT 'from deserializer', 
  `jetton_wallet` string COMMENT 'from deserializer', 
  `jetton_master` string COMMENT 'from deserializer', 
  `response_destination` string COMMENT 'from deserializer', 
  `custom_payload` binary COMMENT 'from deserializer', 
  `forward_ton_amount` decimal(38,0) COMMENT 'from deserializer', 
  `forward_payload` binary COMMENT 'from deserializer', 
  `comment` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `block_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"jetton_events\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"tx_lt\",\"type\":\"long\"},{\"name\":\"utime\",\"type\":\"int\"},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"tx_aborted\",\"type\":\"boolean\"},{\"name\":\"query_id\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":0},\"null\"]},{\"name\":\"amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"source\",\"type\":[\"string\",\"null\"]},{\"name\":\"destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"jetton_wallet\",\"type\":[\"string\",\"null\"]},{\"name\":\"jetton_master\",\"type\":[\"string\",\"null\"]},{\"name\":\"response_destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"custom_payload\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"forward_ton_amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"forward_payload\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"comment\",\"type\":[\"string\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/jetton_events'
TBLPROPERTIES (
)

CREATE EXTERNAL TABLE `messages`(
  `tx_hash` string COMMENT 'from deserializer', 
  `tx_lt` bigint COMMENT 'from deserializer', 
  `tx_now` int COMMENT 'from deserializer', 
  `msg_hash` string COMMENT 'from deserializer', 
  `direction` string COMMENT 'from deserializer', 
  `trace_id` string COMMENT 'from deserializer', 
  `source` string COMMENT 'from deserializer', 
  `destination` string COMMENT 'from deserializer', 
  `value` bigint COMMENT 'from deserializer', 
  `fwd_fee` bigint COMMENT 'from deserializer', 
  `ihr_fee` bigint COMMENT 'from deserializer', 
  `created_lt` bigint COMMENT 'from deserializer', 
  `created_at` bigint COMMENT 'from deserializer', 
  `opcode` int COMMENT 'from deserializer', 
  `ihr_disabled` boolean COMMENT 'from deserializer', 
  `bounce` boolean COMMENT 'from deserializer', 
  `bounced` boolean COMMENT 'from deserializer', 
  `import_fee` bigint COMMENT 'from deserializer', 
  `body_hash` string COMMENT 'from deserializer', 
  `comment` string COMMENT 'from deserializer', 
  `init_state_hash` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `block_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"messages\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"tx_lt\",\"type\":\"long\"},{\"name\":\"tx_now\",\"type\":\"int\"},{\"name\":\"msg_hash\",\"type\":\"string\"},{\"name\":\"direction\",\"type\":\"string\"},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"source\",\"type\":[\"string\",\"null\"]},{\"name\":\"destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"value\",\"type\":[\"long\",\"null\"]},{\"name\":\"fwd_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"ihr_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"created_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"created_at\",\"type\":[\"long\",\"null\"]},{\"name\":\"opcode\",\"type\":[\"int\",\"null\"]},{\"name\":\"ihr_disabled\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"bounce\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"bounced\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"import_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"body_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"comment\",\"type\":[\"string\",\"null\"]},{\"name\":\"init_state_hash\",\"type\":[\"string\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/messages'
TBLPROPERTIES (
)

CREATE EXTERNAL TABLE `messages_with_data`(
  `tx_hash` string COMMENT 'from deserializer', 
  `tx_lt` bigint COMMENT 'from deserializer', 
  `tx_now` int COMMENT 'from deserializer', 
  `msg_hash` string COMMENT 'from deserializer', 
  `direction` string COMMENT 'from deserializer', 
  `trace_id` string COMMENT 'from deserializer', 
  `source` string COMMENT 'from deserializer', 
  `destination` string COMMENT 'from deserializer', 
  `value` bigint COMMENT 'from deserializer', 
  `fwd_fee` bigint COMMENT 'from deserializer', 
  `ihr_fee` bigint COMMENT 'from deserializer', 
  `created_lt` bigint COMMENT 'from deserializer', 
  `created_at` bigint COMMENT 'from deserializer', 
  `opcode` int COMMENT 'from deserializer', 
  `ihr_disabled` boolean COMMENT 'from deserializer', 
  `bounce` boolean COMMENT 'from deserializer', 
  `bounced` boolean COMMENT 'from deserializer', 
  `import_fee` bigint COMMENT 'from deserializer', 
  `body_hash` string COMMENT 'from deserializer', 
  `body_boc` binary COMMENT 'from deserializer', 
  `comment` string COMMENT 'from deserializer', 
  `init_state_hash` string COMMENT 'from deserializer', 
  `init_state_boc` binary COMMENT 'from deserializer')
PARTITIONED BY ( 
  `block_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"messages_with_data\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"tx_lt\",\"type\":\"long\"},{\"name\":\"tx_now\",\"type\":\"int\"},{\"name\":\"msg_hash\",\"type\":\"string\"},{\"name\":\"direction\",\"type\":\"string\"},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"source\",\"type\":[\"string\",\"null\"]},{\"name\":\"destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"value\",\"type\":[\"long\",\"null\"]},{\"name\":\"fwd_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"ihr_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"created_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"created_at\",\"type\":[\"long\",\"null\"]},{\"name\":\"opcode\",\"type\":[\"int\",\"null\"]},{\"name\":\"ihr_disabled\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"bounce\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"bounced\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"import_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"body_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"body_boc\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"comment\",\"type\":[\"string\",\"null\"]},{\"name\":\"init_state_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"init_state_boc\",\"type\":[\"bytes\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/messages_with_data'
TBLPROPERTIES (
)

CREATE EXTERNAL TABLE `transactions`(
  `account` string COMMENT 'from deserializer', 
  `hash` string COMMENT 'from deserializer', 
  `lt` bigint COMMENT 'from deserializer', 
  `block_workchain` int COMMENT 'from deserializer', 
  `block_shard` bigint COMMENT 'from deserializer', 
  `block_seqno` int COMMENT 'from deserializer', 
  `mc_block_seqno` int COMMENT 'from deserializer', 
  `trace_id` string COMMENT 'from deserializer', 
  `prev_trans_hash` string COMMENT 'from deserializer', 
  `prev_trans_lt` bigint COMMENT 'from deserializer', 
  `now` int COMMENT 'from deserializer', 
  `orig_status` string COMMENT 'from deserializer', 
  `end_status` string COMMENT 'from deserializer', 
  `total_fees` bigint COMMENT 'from deserializer', 
  `account_state_hash_before` string COMMENT 'from deserializer', 
  `account_state_hash_after` string COMMENT 'from deserializer', 
  `account_state_code_hash_before` string COMMENT 'from deserializer', 
  `account_state_code_hash_after` string COMMENT 'from deserializer', 
  `account_state_balance_before` bigint COMMENT 'from deserializer', 
  `account_state_balance_after` bigint COMMENT 'from deserializer', 
  `descr` string COMMENT 'from deserializer', 
  `aborted` boolean COMMENT 'from deserializer', 
  `destroyed` boolean COMMENT 'from deserializer', 
  `credit_first` boolean COMMENT 'from deserializer', 
  `is_tock` boolean COMMENT 'from deserializer', 
  `installed` boolean COMMENT 'from deserializer', 
  `storage_fees_collected` bigint COMMENT 'from deserializer', 
  `storage_fees_due` bigint COMMENT 'from deserializer', 
  `storage_status_change` string COMMENT 'from deserializer', 
  `credit_due_fees_collected` bigint COMMENT 'from deserializer', 
  `credit` bigint COMMENT 'from deserializer', 
  `compute_skipped` boolean COMMENT 'from deserializer', 
  `skipped_reason` string COMMENT 'from deserializer', 
  `compute_success` boolean COMMENT 'from deserializer', 
  `compute_msg_state_used` boolean COMMENT 'from deserializer', 
  `compute_account_activated` boolean COMMENT 'from deserializer', 
  `compute_gas_fees` bigint COMMENT 'from deserializer', 
  `compute_gas_used` bigint COMMENT 'from deserializer', 
  `compute_gas_limit` bigint COMMENT 'from deserializer', 
  `compute_gas_credit` bigint COMMENT 'from deserializer', 
  `compute_mode` int COMMENT 'from deserializer', 
  `compute_exit_code` int COMMENT 'from deserializer', 
  `compute_exit_arg` int COMMENT 'from deserializer', 
  `compute_vm_steps` bigint COMMENT 'from deserializer', 
  `compute_vm_init_state_hash` string COMMENT 'from deserializer', 
  `compute_vm_final_state_hash` string COMMENT 'from deserializer', 
  `action_success` boolean COMMENT 'from deserializer', 
  `action_valid` boolean COMMENT 'from deserializer', 
  `action_no_funds` boolean COMMENT 'from deserializer', 
  `action_status_change` string COMMENT 'from deserializer', 
  `action_total_fwd_fees` bigint COMMENT 'from deserializer', 
  `action_total_action_fees` bigint COMMENT 'from deserializer', 
  `action_result_code` int COMMENT 'from deserializer', 
  `action_result_arg` int COMMENT 'from deserializer', 
  `action_tot_actions` int COMMENT 'from deserializer', 
  `action_spec_actions` int COMMENT 'from deserializer', 
  `action_skipped_actions` int COMMENT 'from deserializer', 
  `action_msgs_created` int COMMENT 'from deserializer', 
  `action_action_list_hash` string COMMENT 'from deserializer', 
  `action_tot_msg_size_cells` bigint COMMENT 'from deserializer', 
  `action_tot_msg_size_bits` bigint COMMENT 'from deserializer', 
  `bounce` string COMMENT 'from deserializer', 
  `bounce_msg_size_cells` bigint COMMENT 'from deserializer', 
  `bounce_msg_size_bits` bigint COMMENT 'from deserializer', 
  `bounce_req_fwd_fees` bigint COMMENT 'from deserializer', 
  `bounce_msg_fees` bigint COMMENT 'from deserializer', 
  `bounce_fwd_fees` bigint COMMENT 'from deserializer', 
  `split_info_cur_shard_pfx_len` int COMMENT 'from deserializer', 
  `split_info_acc_split_depth` int COMMENT 'from deserializer', 
  `split_info_this_addr` string COMMENT 'from deserializer', 
  `split_info_sibling_addr` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `block_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"transactions\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"account\",\"type\":[\"null\",\"string\"]},{\"name\":\"hash\",\"type\":[\"null\",\"string\"]},{\"name\":\"lt\",\"type\":[\"null\",\"long\"]},{\"name\":\"block_workchain\",\"type\":[\"int\",\"null\"]},{\"name\":\"block_shard\",\"type\":[\"long\",\"null\"]},{\"name\":\"block_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"mc_block_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"prev_trans_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"prev_trans_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"now\",\"type\":[\"int\",\"null\"]},{\"name\":\"orig_status\",\"type\":[\"string\",\"null\"]},{\"name\":\"end_status\",\"type\":[\"string\",\"null\"]},{\"name\":\"total_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"account_state_hash_before\",\"type\":[\"string\",\"null\"]},{\"name\":\"account_state_hash_after\",\"type\":[\"string\",\"null\"]},{\"name\":\"account_state_code_hash_before\",\"type\":[\"string\",\"null\"]},{\"name\":\"account_state_code_hash_after\",\"type\":[\"string\",\"null\"]},{\"name\":\"account_state_balance_before\",\"type\":[\"long\",\"null\"]},{\"name\":\"account_state_balance_after\",\"type\":[\"long\",\"null\"]},{\"name\":\"descr\",\"type\":[\"string\",\"null\"]},{\"name\":\"aborted\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"destroyed\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"credit_first\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"is_tock\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"installed\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"storage_fees_collected\",\"type\":[\"long\",\"null\"]},{\"name\":\"storage_fees_due\",\"type\":[\"long\",\"null\"]},{\"name\":\"storage_status_change\",\"type\":[\"string\",\"null\"]},{\"name\":\"credit_due_fees_collected\",\"type\":[\"long\",\"null\"]},{\"name\":\"credit\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_skipped\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"skipped_reason\",\"type\":[\"string\",\"null\"]},{\"name\":\"compute_success\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"compute_msg_state_used\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"compute_account_activated\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"compute_gas_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_gas_used\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_gas_limit\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_gas_credit\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_mode\",\"type\":[\"int\",\"null\"]},{\"name\":\"compute_exit_code\",\"type\":[\"int\",\"null\"]},{\"name\":\"compute_exit_arg\",\"type\":[\"int\",\"null\"]},{\"name\":\"compute_vm_steps\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_vm_init_state_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"compute_vm_final_state_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"action_success\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"action_valid\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"action_no_funds\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"action_status_change\",\"type\":[\"string\",\"null\"]},{\"name\":\"action_total_fwd_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"action_total_action_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"action_result_code\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_result_arg\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_tot_actions\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_spec_actions\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_skipped_actions\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_msgs_created\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_action_list_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"action_tot_msg_size_cells\",\"type\":[\"long\",\"null\"]},{\"name\":\"action_tot_msg_size_bits\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce\",\"type\":[\"string\",\"null\"]},{\"name\":\"bounce_msg_size_cells\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce_msg_size_bits\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce_req_fwd_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce_msg_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce_fwd_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"split_info_cur_shard_pfx_len\",\"type\":[\"int\",\"null\"]},{\"name\":\"split_info_acc_split_depth\",\"type\":[\"int\",\"null\"]},{\"name\":\"split_info_this_addr\",\"type\":[\"string\",\"null\"]},{\"name\":\"split_info_sibling_addr\",\"type\":[\"string\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/transactions'
TBLPROPERTIES (
)


CREATE EXTERNAL TABLE `jetton_metadata`(
  `address` string COMMENT 'from deserializer', 
  `update_time_onchain` int COMMENT 'from deserializer', 
  `update_time_metadata` int COMMENT 'from deserializer', 
  `mintable` boolean COMMENT 'from deserializer', 
  `admin_address` string COMMENT 'from deserializer', 
  `jetton_content_onchain` string COMMENT 'from deserializer', 
  `jetton_wallet_code_hash` string COMMENT 'from deserializer', 
  `code_hash` string COMMENT 'from deserializer', 
  `metadata_status` int COMMENT 'from deserializer', 
  `symbol` string COMMENT 'from deserializer', 
  `name` string COMMENT 'from deserializer', 
  `description` string COMMENT 'from deserializer', 
  `image` string COMMENT 'from deserializer', 
  `image_data` string COMMENT 'from deserializer', 
  `decimals` int COMMENT 'from deserializer', 
  `sources` struct<symbol:string,name:string,description:string,image:string,image_data:string,decimals:string> COMMENT 'from deserializer', 
  `tonapi_image_url` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `adding_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"jetton_metadata\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"address\",\"type\":[\"string\",\"null\"]},{\"name\":\"update_time_onchain\",\"type\":[\"int\",\"null\"]},{\"name\":\"update_time_metadata\",\"type\":[\"int\",\"null\"]},{\"name\":\"mintable\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"admin_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"jetton_content_onchain\",\"type\":[\"string\",\"null\"]},{\"name\":\"jetton_wallet_code_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"code_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"metadata_status\",\"type\":[\"int\",\"null\"]},{\"name\":\"symbol\",\"type\":[\"string\",\"null\"]},{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"image\",\"type\":[\"string\",\"null\"]},{\"name\":\"image_data\",\"type\":[\"string\",\"null\"]},{\"name\":\"decimals\",\"type\":[\"int\",\"null\"]},{\"name\":\"sources\",\"type\":[{\"type\":\"record\",\"name\":\"sources\",\"fields\":[{\"name\":\"symbol\",\"type\":[\"string\",\"null\"]},{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"image\",\"type\":[\"string\",\"null\"]},{\"name\":\"image_data\",\"type\":[\"string\",\"null\"]},{\"name\":\"decimals\",\"type\":[\"string\",\"null\"]}]},\"null\"]},{\"name\":\"tonapi_image_url\",\"type\":[\"string\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/jetton_metadata'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='archival_added_at', 
  'averageRecordSize'='1333', 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"jetton_metadata\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"address\",\"type\":[\"string\",\"null\"]},{\"name\":\"update_time_onchain\",\"type\":[\"int\",\"null\"]},{\"name\":\"update_time_metadata\",\"type\":[\"int\",\"null\"]},{\"name\":\"mintable\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"admin_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"jetton_content_onchain\",\"type\":[\"string\",\"null\"]},{\"name\":\"jetton_wallet_code_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"code_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"metadata_status\",\"type\":[\"int\",\"null\"]},{\"name\":\"symbol\",\"type\":[\"string\",\"null\"]},{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"image\",\"type\":[\"string\",\"null\"]},{\"name\":\"image_data\",\"type\":[\"string\",\"null\"]},{\"name\":\"decimals\",\"type\":[\"int\",\"null\"]},{\"name\":\"sources\",\"type\":[{\"type\":\"record\",\"name\":\"sources\",\"fields\":[{\"name\":\"symbol\",\"type\":[\"string\",\"null\"]},{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"image\",\"type\":[\"string\",\"null\"]},{\"name\":\"image_data\",\"type\":[\"string\",\"null\"]},{\"name\":\"decimals\",\"type\":[\"string\",\"null\"]}]},\"null\"]},{\"name\":\"tonapi_image_url\",\"type\":[\"string\",\"null\"]}]}', 
  'classification'='avro', 
  'compressionType'='none', 
  'objectCount'='1', 
  'partition_filtering.enabled'='true', 
  'recordCount'='75039', 
  'sizeKey'='100028463', 
  'transient_lastDdlTime'='1731608503', 
  'typeOfData'='file')


CREATE EXTERNAL TABLE `dex_trades`(
  `tx_hash` string COMMENT 'from deserializer', 
  `trace_id` string COMMENT 'from deserializer', 
  `project_type` string COMMENT 'from deserializer', 
  `project` string COMMENT 'from deserializer', 
  `version` int COMMENT 'from deserializer', 
  `event_time` int COMMENT 'from deserializer', 
  `event_type` string COMMENT 'from deserializer', 
  `trader_address` string COMMENT 'from deserializer', 
  `pool_address` string COMMENT 'from deserializer', 
  `router_address` string COMMENT 'from deserializer', 
  `token_sold_address` string COMMENT 'from deserializer', 
  `token_bought_address` string COMMENT 'from deserializer', 
  `amount_sold_raw` decimal(38,0) COMMENT 'from deserializer', 
  `amount_bought_raw` decimal(38,0) COMMENT 'from deserializer', 
  `referral_address` string COMMENT 'from deserializer', 
  `platform_tag` string COMMENT 'from deserializer', 
  `query_id` decimal(20,0) COMMENT 'from deserializer', 
  `volume_usd` decimal(20,6) COMMENT 'from deserializer', 
  `volume_ton` decimal(20,9) COMMENT 'from deserializer')
PARTITIONED BY ( 
  `block_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"dex_swaps\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"project_type\",\"type\":\"string\"},{\"name\":\"project\",\"type\":\"string\"},{\"name\":\"version\",\"type\":[\"int\",\"null\"]},{\"name\":\"event_time\",\"type\":\"int\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"trader_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"pool_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"router_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"token_sold_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"token_bought_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"amount_sold_raw\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"amount_bought_raw\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"referral_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"platform_tag\",\"type\":[\"string\",\"null\"]},{\"name\":\"query_id\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":0},\"null\"]},{\"name\":\"volume_usd\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":6},\"null\"]},{\"name\":\"volume_ton\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":9},\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/dex_trades'
TBLPROPERTIES (
)

CREATE EXTERNAL TABLE `dex_pools`(
  `pool` string COMMENT 'from deserializer', 
  `project` string COMMENT 'from deserializer', 
  `version` int COMMENT 'from deserializer', 
  `discovered_at` int COMMENT 'from deserializer', 
  `jetton_left` string COMMENT 'from deserializer', 
  `jetton_right` string COMMENT 'from deserializer', 
  `reserves_left` decimal(38,0) COMMENT 'from deserializer', 
  `reserves_right` decimal(38,0) COMMENT 'from deserializer', 
  `total_supply` decimal(38,0) COMMENT 'from deserializer', 
  `tvl_usd` decimal(20,6) COMMENT 'from deserializer', 
  `tvl_ton` decimal(20,9) COMMENT 'from deserializer', 
  `last_updated` int COMMENT 'from deserializer', 
  `is_liquid` boolean COMMENT 'from deserializer', 
  `lp_fee` decimal(12,10) COMMENT 'from deserializer', 
  `protocol_fee` decimal(12,10) COMMENT 'from deserializer', 
  `referral_fee` decimal(12,10) COMMENT 'from deserializer')
PARTITIONED BY ( 
  `block_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"dex_pool\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"pool\",\"type\":\"string\"},{\"name\":\"project\",\"type\":\"string\"},{\"name\":\"version\",\"type\":[\"int\",\"null\"]},{\"name\":\"discovered_at\",\"type\":[\"int\",\"null\"]},{\"name\":\"jetton_left\",\"type\":[\"string\",\"null\"]},{\"name\":\"jetton_right\",\"type\":[\"string\",\"null\"]},{\"name\":\"reserves_left\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"reserves_right\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"total_supply\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"tvl_usd\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":6},\"null\"]},{\"name\":\"tvl_ton\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":9},\"null\"]},{\"name\":\"last_updated\",\"type\":[\"int\",\"null\"]},{\"name\":\"is_liquid\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"lp_fee\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":12,\"scale\":10},\"null\"]},{\"name\":\"protocol_fee\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":12,\"scale\":10},\"null\"]},{\"name\":\"referral_fee\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":12,\"scale\":10},\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/dex_pools'
TBLPROPERTIES (
)

CREATE EXTERNAL TABLE `excluded_rows`(
  `table` string, 
  `key` string)
COMMENT 'Contains data corrections'
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/excluded_rows'
TBLPROPERTIES (
  'classification'='csv', 
  'transient_lastDdlTime'='1733881198')

CREATE EXTERNAL TABLE `balances_history`(
  `address` string COMMENT 'from deserializer', 
  `asset` string COMMENT 'from deserializer', 
  `amount` decimal(38,0) COMMENT 'from deserializer', 
  `mintless_claimed` boolean COMMENT 'from deserializer', 
  `timestamp` int COMMENT 'from deserializer', 
  `lt` bigint COMMENT 'from deserializer')
PARTITIONED BY ( 
  `block_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"balances_history\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"},{\"name\":\"asset\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"mintless_claimed\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"timestamp\",\"type\":[\"int\",\"null\"]},{\"name\":\"lt\",\"type\":[\"long\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/balances_history'
TBLPROPERTIES (
)

CREATE EXTERNAL TABLE `balances_snapshot`(
  `address` string COMMENT 'from deserializer', 
  `asset` string COMMENT 'from deserializer', 
  `amount` decimal(38,0) COMMENT 'from deserializer', 
  `mintless_claimed` boolean COMMENT 'from deserializer', 
  `timestamp` int COMMENT 'from deserializer', 
  `lt` bigint COMMENT 'from deserializer')
PARTITIONED BY ( 
  `block_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/balances_snapshot'
TBLPROPERTIES (
  'auto.purge'='false', 
  'has_encrypted_data'='false', 
  'numFiles'='-1', 
  'presto_query_id'='20250115_125839_00069_9mpf6', 
  'presto_version'='0.215-21820-g882377f', 
  'totalSize'='-1', 
  'transactional'='false', 
  'write.compression'='SNAPPY')

  CREATE EXTERNAL TABLE `nft_metadata`(
  `type` string COMMENT 'from deserializer', 
  `address` string COMMENT 'from deserializer', 
  `update_time_onchain` int COMMENT 'from deserializer', 
  `update_time_metadata` int COMMENT 'from deserializer', 
  `parent_address` string COMMENT 'from deserializer', 
  `content_onchain` string COMMENT 'from deserializer', 
  `metadata_status` int COMMENT 'from deserializer', 
  `name` string COMMENT 'from deserializer', 
  `description` string COMMENT 'from deserializer', 
  `image` string COMMENT 'from deserializer', 
  `image_data` string COMMENT 'from deserializer', 
  `attributes` string COMMENT 'from deserializer', 
  `sources` struct<name:string,description:string,image:string,image_data:string,attributes:string> COMMENT 'from deserializer', 
  `tonapi_image_url` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `adding_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"nft_metadata\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"type\",\"type\":[\"string\",\"null\"]},{\"name\":\"address\",\"type\":[\"string\",\"null\"]},{\"name\":\"update_time_onchain\",\"type\":[\"int\",\"null\"]},{\"name\":\"update_time_metadata\",\"type\":[\"int\",\"null\"]},{\"name\":\"parent_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"content_onchain\",\"type\":[\"string\",\"null\"]},{\"name\":\"metadata_status\",\"type\":[\"int\",\"null\"]},{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"image\",\"type\":[\"string\",\"null\"]},{\"name\":\"image_data\",\"type\":[\"string\",\"null\"]},{\"name\":\"attributes\",\"type\":[\"string\",\"null\"]},{\"name\":\"sources\",\"type\":[{\"type\":\"record\",\"name\":\"sources\",\"fields\":[{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"image\",\"type\":[\"string\",\"null\"]},{\"name\":\"image_data\",\"type\":[\"string\",\"null\"]},{\"name\":\"attributes\",\"type\":[\"string\",\"null\"]}]},\"null\"]},{\"name\":\"tonapi_image_url\",\"type\":[\"string\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/nft_metadata/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='archival_added_at', 
  'averageRecordSize'='999', 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"nft_metadata\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"type\",\"type\":[\"string\",\"null\"]},{\"name\":\"address\",\"type\":[\"string\",\"null\"]},{\"name\":\"update_time_onchain\",\"type\":[\"int\",\"null\"]},{\"name\":\"update_time_metadata\",\"type\":[\"int\",\"null\"]},{\"name\":\"parent_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"content_onchain\",\"type\":[\"string\",\"null\"]},{\"name\":\"metadata_status\",\"type\":[\"int\",\"null\"]},{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"image\",\"type\":[\"string\",\"null\"]},{\"name\":\"image_data\",\"type\":[\"string\",\"null\"]},{\"name\":\"attributes\",\"type\":[\"string\",\"null\"]},{\"name\":\"sources\",\"type\":[{\"type\":\"record\",\"name\":\"sources\",\"fields\":[{\"name\":\"name\",\"type\":[\"string\",\"null\"]},{\"name\":\"description\",\"type\":[\"string\",\"null\"]},{\"name\":\"image\",\"type\":[\"string\",\"null\"]},{\"name\":\"image_data\",\"type\":[\"string\",\"null\"]},{\"name\":\"attributes\",\"type\":[\"string\",\"null\"]}]},\"null\"]},{\"name\":\"tonapi_image_url\",\"type\":[\"string\",\"null\"]}]}', 
  'classification'='avro', 
  'compressionType'='none', 
  'objectCount'='3', 
  'partition_filtering.enabled'='true', 
  'recordCount'='300212', 
  'sizeKey'='300042446', 
  'typeOfData'='file')

  CREATE EXTERNAL TABLE `nft_items`(
  `address` string COMMENT 'from deserializer', 
  `is_init` boolean COMMENT 'from deserializer', 
  `index` string COMMENT 'from deserializer', 
  `collection_address` string COMMENT 'from deserializer', 
  `owner_address` string COMMENT 'from deserializer', 
  `content_onchain` string COMMENT 'from deserializer', 
  `timestamp` int COMMENT 'from deserializer', 
  `lt` bigint COMMENT 'from deserializer')
PARTITIONED BY ( 
  `block_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"nft_items_history\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"},{\"name\":\"is_init\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"index\",\"type\":[\"string\",\"null\"]},{\"name\":\"collection_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"owner_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"content_onchain\",\"type\":[\"string\",\"null\"]},{\"name\":\"timestamp\",\"type\":[\"int\",\"null\"]},{\"name\":\"lt\",\"type\":[\"long\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/nft_items'
TBLPROPERTIES (
)

CREATE EXTERNAL TABLE `nft_sales`(
  `address` string COMMENT 'from deserializer', 
  `type` string COMMENT 'from deserializer', 
  `nft_address` string COMMENT 'from deserializer', 
  `nft_owner_address` string COMMENT 'from deserializer', 
  `created_at` int COMMENT 'from deserializer', 
  `is_complete` boolean COMMENT 'from deserializer', 
  `is_canceled` boolean COMMENT 'from deserializer', 
  `end_time` int COMMENT 'from deserializer', 
  `marketplace_address` string COMMENT 'from deserializer', 
  `marketplace_fee_address` string COMMENT 'from deserializer', 
  `marketplace_fee` decimal(38,0) COMMENT 'from deserializer', 
  `price` decimal(38,0) COMMENT 'from deserializer', 
  `asset` string COMMENT 'from deserializer', 
  `royalty_address` string COMMENT 'from deserializer', 
  `royalty_amount` decimal(38,0) COMMENT 'from deserializer', 
  `max_bid` decimal(38,0) COMMENT 'from deserializer', 
  `min_bid` decimal(38,0) COMMENT 'from deserializer', 
  `min_step` decimal(38,0) COMMENT 'from deserializer', 
  `last_bid_at` int COMMENT 'from deserializer', 
  `last_member` string COMMENT 'from deserializer', 
  `timestamp` int COMMENT 'from deserializer', 
  `lt` bigint COMMENT 'from deserializer')
PARTITIONED BY ( 
  `block_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"nft_sales\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"nft_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"nft_owner_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"created_at\",\"type\":[\"int\",\"null\"]},{\"name\":\"is_complete\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"is_canceled\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"end_time\",\"type\":[\"int\",\"null\"]},{\"name\":\"marketplace_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"marketplace_fee_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"marketplace_fee\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"price\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"asset\",\"type\":[\"string\",\"null\"]},{\"name\":\"royalty_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"royalty_amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"max_bid\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"min_bid\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"min_step\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"last_bid_at\",\"type\":[\"int\",\"null\"]},{\"name\":\"last_member\",\"type\":[\"string\",\"null\"]},{\"name\":\"timestamp\",\"type\":[\"int\",\"null\"]},{\"name\":\"lt\",\"type\":[\"long\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/nft_sales'
TBLPROPERTIES (
)

CREATE EXTERNAL TABLE `nft_transfers`(
  `tx_hash` string COMMENT 'from deserializer', 
  `tx_lt` bigint COMMENT 'from deserializer', 
  `tx_now` int COMMENT 'from deserializer', 
  `tx_aborted` boolean COMMENT 'from deserializer', 
  `query_id` decimal(20,0) COMMENT 'from deserializer', 
  `nft_item_address` string COMMENT 'from deserializer', 
  `nft_item_index` string COMMENT 'from deserializer', 
  `nft_collection_address` string COMMENT 'from deserializer', 
  `old_owner` string COMMENT 'from deserializer', 
  `new_owner` string COMMENT 'from deserializer', 
  `response_destination` string COMMENT 'from deserializer', 
  `custom_payload` binary COMMENT 'from deserializer', 
  `forward_amount` decimal(38,0) COMMENT 'from deserializer', 
  `forward_payload` binary COMMENT 'from deserializer', 
  `comment` string COMMENT 'from deserializer', 
  `trace_id` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `block_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"nft_transfers\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"tx_lt\",\"type\":\"long\"},{\"name\":\"tx_now\",\"type\":\"int\"},{\"name\":\"tx_aborted\",\"type\":\"boolean\"},{\"name\":\"query_id\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":0},\"null\"]},{\"name\":\"nft_item_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"nft_item_index\",\"type\":[\"string\",\"null\"]},{\"name\":\"nft_collection_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"old_owner\",\"type\":[\"string\",\"null\"]},{\"name\":\"new_owner\",\"type\":[\"string\",\"null\"]},{\"name\":\"response_destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"custom_payload\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"forward_amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"forward_payload\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"comment\",\"type\":[\"string\",\"null\"]},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/nft_transfers'
TBLPROPERTIES (
)
  


  CREATE EXTERNAL TABLE `nft_events`(
  `type` varchar(11) COMMENT 'from deserializer', 
  `nft_item_address` string COMMENT 'from deserializer', 
  `is_init` boolean COMMENT 'from deserializer', 
  `nft_item_index` string COMMENT 'from deserializer', 
  `collection_address` string COMMENT 'from deserializer', 
  `owner_address` string COMMENT 'from deserializer', 
  `content_onchain` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `lt` bigint COMMENT 'from deserializer', 
  `tx_hash` string COMMENT 'from deserializer', 
  `trace_id` string COMMENT 'from deserializer', 
  `prev_owner` string COMMENT 'from deserializer', 
  `query_id` decimal(20,0) COMMENT 'from deserializer', 
  `forward_amount` decimal(38,0) COMMENT 'from deserializer', 
  `forward_payload` binary COMMENT 'from deserializer', 
  `comment` string COMMENT 'from deserializer', 
  `custom_payload` binary COMMENT 'from deserializer', 
  `sale_contract` string COMMENT 'from deserializer', 
  `sale_type` string COMMENT 'from deserializer', 
  `sale_end_time` bigint COMMENT 'from deserializer', 
  `marketplace_address` string COMMENT 'from deserializer', 
  `marketplace_fee_address` string COMMENT 'from deserializer', 
  `marketplace_fee` decimal(38,0) COMMENT 'from deserializer', 
  `sale_price` decimal(38,0) COMMENT 'from deserializer', 
  `payment_asset` string COMMENT 'from deserializer', 
  `royalty_address` string COMMENT 'from deserializer', 
  `royalty_amount` decimal(38,0) COMMENT 'from deserializer', 
  `auction_max_bid` decimal(38,0) COMMENT 'from deserializer', 
  `auction_min_bid` decimal(38,0) COMMENT 'from deserializer', 
  `auction_min_step` decimal(38,0) COMMENT 'from deserializer')
PARTITIONED BY ( 
  `block_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://ton-blockchain-public-datalake/v1/nft_events/'
TBLPROPERTIES (
  'auto.purge'='false', 
  'bucketing_format'='hive', 
  'bucketing_version'='1', 
  'has_encrypted_data'='false', 
  'numFiles'='-1', 
  'presto_query_id'='20250220_132704_00041_h9xpu', 
  'presto_version'='0.215-21824-g6a177b2', 
  'totalSize'='-1', 
  'transactional'='false', 
  'write.compression'='SNAPPY')


-- daily query to generate balances_snapshot
insert into datalake.balances_snapshot(address, asset, amount, mintless_claimed, timestamp, lt, block_date)
with ranks as (
 select address, asset, amount, mintless_claimed, timestamp, lt,
 row_number() over (partition by address, asset order by lt desc) as rank
 from "datalake"."balances_history"
)
select address, asset, amount, mintless_claimed, timestamp, lt,
(SELECT max(block_date) FROM "datalake"."balances_history$partitions") as block_date
from ranks where rank = 1
and (SELECT max(block_date) FROM "datalake"."balances_history$partitions") != 
(SELECT max(block_date) FROM "datalake"."balances_snapshot$partitions") 


-- views
create or replace view "jetton_metadata_latest"
as
select * from "jetton_metadata_snapshots"
where snapshot_date = (SELECT max(snapshot_date) FROM "jetton_metadata_snapshots")

-- jetton_price daily, backport from Dune - https://dune.com/queries/4438952
  
CREATE OR REPLACE VIEW jetton_price_daily
AS
WITH
DT AS (
    SELECT 
        CASE
            WHEN token_sold_address = UPPER('0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe') 
                THEN token_bought_address
            WHEN token_bought_address != UPPER('0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe')
                 AND token_sold_address IN (
                     '0:0000000000000000000000000000000000000000000000000000000000000000',
                     UPPER('0:8cdc1d7640ad5ee326527fc1ad0514f468b30dc84b0173f0e155f451b4e11f7c')
                 )
                THEN token_bought_address
            ELSE token_sold_address
        END AS token_address,
        CASE
            WHEN token_sold_address = UPPER('0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe') 
                THEN amount_bought_raw
            WHEN token_bought_address != UPPER('0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe')
                 AND token_sold_address IN ( 
                     '0:0000000000000000000000000000000000000000000000000000000000000000',
                     UPPER('0:8cdc1d7640ad5ee326527fc1ad0514f468b30dc84b0173f0e155f451b4e11f7c')
                 )
                THEN amount_bought_raw
            ELSE amount_sold_raw
        END AS amount_raw,
        volume_usd,
        volume_ton,
        from_unixtime(event_time) as block_time
    FROM datalake.dex_trades
    WHERE amount_bought_raw > 100 AND amount_sold_raw > 100
)

-- add missing USDT trades to store all prices in one table
, USDT_TRADES AS (
    SELECT 
        UPPER('0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe') AS token_address,
        CAST(amount_raw AS DOUBLE) / volume_ton * volume_usd * POWER(10, 6-9) AS amount_raw, -- ton
        volume_usd, 
        volume_ton,
        block_time
    FROM DT
    WHERE token_address = '0:0000000000000000000000000000000000000000000000000000000000000000'
)

, ALL_TRADES AS (
    SELECT *
    FROM DT
    UNION ALL
    SELECT * 
    FROM USDT_TRADES
)

SELECT
    token_address,
    DATE_TRUNC('day', block_time) AS ts,
    SUM(volume_ton) / SUM(CAST(amount_raw AS DOUBLE)) AS price_ton,
    SUM(volume_usd) / SUM(CAST(amount_raw AS DOUBLE)) AS price_usd,
    COUNT(*) AS trades_count
FROM ALL_TRADES
GROUP BY 1, 2
ORDER BY 1, 2 DESC;


-- latest values for NFT metadata

create or replace view "nft_metadata_latest"
as
with ranks as (
 select type, address, update_time_onchain, update_time_metadata,
  parent_address, content_onchain, metadata_status, name,description,
  image, image_data, attributes, sources, tonapi_image_url,
 row_number() over (partition by type, address order by update_time_metadata desc, update_time_onchain desc) as rank
 from "datalake"."nft_metadata"
)
select type, address, update_time_onchain, update_time_metadata,
  parent_address, content_onchain, metadata_status, name,description,
image, image_data, attributes, sources, tonapi_image_url from ranks where rank = 1

-- latest owners for NFT items

create or replace view datalake.nft_items_latest_state
as
with latest_ranks as (
SELECT nft_item_address, content_onchain, collection_address, owner_address, timestamp,
row_number() over(partition by nft_item_address order by timestamp desc, case when type = 'mint' then 0 else 1 end desc) as rank  FROM "datalake"."nft_events"
where type !='bid'
)
select nft_item_address, content_onchain, collection_address, owner_address
from latest_ranks where rank = 1
