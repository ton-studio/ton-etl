CREATE EXTERNAL TABLE `exporters_blocks`(
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
  `adding_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"blocks\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"workchain\",\"type\":\"int\"},{\"name\":\"shard\",\"type\":\"long\"},{\"name\":\"seqno\",\"type\":\"int\"},{\"name\":\"root_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"file_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"mc_block_workchain\",\"type\":[\"int\",\"null\"]},{\"name\":\"mc_block_shard\",\"type\":[\"long\",\"null\"]},{\"name\":\"mc_block_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"global_id\",\"type\":[\"int\",\"null\"]},{\"name\":\"version\",\"type\":[\"int\",\"null\"]},{\"name\":\"after_merge\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"before_split\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"after_split\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"want_merge\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"want_split\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"key_block\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"vert_seqno_incr\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"flags\",\"type\":[\"int\",\"null\"]},{\"name\":\"gen_utime\",\"type\":[\"long\",\"null\"]},{\"name\":\"start_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"end_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"validator_list_hash_short\",\"type\":[\"int\",\"null\"]},{\"name\":\"gen_catchain_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"min_ref_mc_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"prev_key_block_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"vert_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"master_ref_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"rand_seed\",\"type\":[\"string\",\"null\"]},{\"name\":\"created_by\",\"type\":[\"string\",\"null\"]},{\"name\":\"tx_count\",\"type\":[\"int\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://DATA_PATH/blocks'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='archival_added_at', 
  'averageRecordSize'='280', 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"blocks\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"workchain\",\"type\":\"int\"},{\"name\":\"shard\",\"type\":\"long\"},{\"name\":\"seqno\",\"type\":\"int\"},{\"name\":\"root_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"file_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"mc_block_workchain\",\"type\":[\"int\",\"null\"]},{\"name\":\"mc_block_shard\",\"type\":[\"long\",\"null\"]},{\"name\":\"mc_block_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"global_id\",\"type\":[\"int\",\"null\"]},{\"name\":\"version\",\"type\":[\"int\",\"null\"]},{\"name\":\"after_merge\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"before_split\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"after_split\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"want_merge\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"want_split\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"key_block\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"vert_seqno_incr\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"flags\",\"type\":[\"int\",\"null\"]},{\"name\":\"gen_utime\",\"type\":[\"long\",\"null\"]},{\"name\":\"start_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"end_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"validator_list_hash_short\",\"type\":[\"int\",\"null\"]},{\"name\":\"gen_catchain_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"min_ref_mc_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"prev_key_block_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"vert_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"master_ref_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"rand_seed\",\"type\":[\"string\",\"null\"]},{\"name\":\"created_by\",\"type\":[\"string\",\"null\"]},{\"name\":\"tx_count\",\"type\":[\"int\",\"null\"]}]}', 
  'classification'='avro', 
  'compressionType'='none', 
  'objectCount'='671', 
  'partition_filtering.enabled'='true', 
  'recordCount'='233382609', 
  'sizeKey'='67103747436', 
  'typeOfData'='file')


  CREATE EXTERNAL TABLE `exporters_transactions`(
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
  `adding_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"transactions\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"account\",\"type\":\"string\"},{\"name\":\"hash\",\"type\":\"string\"},{\"name\":\"lt\",\"type\":\"long\"},{\"name\":\"block_workchain\",\"type\":[\"int\",\"null\"]},{\"name\":\"block_shard\",\"type\":[\"long\",\"null\"]},{\"name\":\"block_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"mc_block_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"prev_trans_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"prev_trans_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"now\",\"type\":[\"int\",\"null\"]},{\"name\":\"orig_status\",\"type\":[{\"type\":\"enum\",\"name\":\"orig_status\",\"symbols\":[\"uninit\",\"frozen\",\"active\",\"nonexist\"]},\"null\"]},{\"name\":\"end_status\",\"type\":[{\"type\":\"enum\",\"name\":\"end_status\",\"symbols\":[\"uninit\",\"frozen\",\"active\",\"nonexist\"]},\"null\"]},{\"name\":\"total_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"account_state_hash_before\",\"type\":[\"string\",\"null\"]},{\"name\":\"account_state_hash_after\",\"type\":[\"string\",\"null\"]},{\"name\":\"account_state_code_hash_before\",\"type\":[\"string\",\"null\"]},{\"name\":\"account_state_code_hash_after\",\"type\":[\"string\",\"null\"]},{\"name\":\"account_state_balance_before\",\"type\":[\"long\",\"null\"]},{\"name\":\"account_state_balance_after\",\"type\":[\"long\",\"null\"]},{\"name\":\"descr\",\"type\":[{\"type\":\"enum\",\"name\":\"descr\",\"symbols\":[\"ord\",\"storage\",\"tick_tock\",\"split_prepare\",\"split_install\",\"merge_prepare\",\"merge_install\"]},\"null\"]},{\"name\":\"aborted\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"destroyed\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"credit_first\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"is_tock\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"installed\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"storage_fees_collected\",\"type\":[\"long\",\"null\"]},{\"name\":\"storage_fees_due\",\"type\":[\"long\",\"null\"]},{\"name\":\"storage_status_change\",\"type\":[{\"type\":\"enum\",\"name\":\"storage_status_change_type\",\"symbols\":[\"unchanged\",\"frozen\",\"deleted\"]},\"null\"]},{\"name\":\"credit_due_fees_collected\",\"type\":[\"long\",\"null\"]},{\"name\":\"credit\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_skipped\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"skipped_reason\",\"type\":[{\"type\":\"enum\",\"name\":\"skipped_reason_type\",\"symbols\":[\"no_state\",\"bad_state\",\"no_gas\",\"suspended\"]},\"null\"]},{\"name\":\"compute_success\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"compute_msg_state_used\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"compute_account_activated\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"compute_gas_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_gas_used\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_gas_limit\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_gas_credit\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_mode\",\"type\":[\"int\",\"null\"]},{\"name\":\"compute_exit_code\",\"type\":[\"int\",\"null\"]},{\"name\":\"compute_exit_arg\",\"type\":[\"int\",\"null\"]},{\"name\":\"compute_vm_steps\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_vm_init_state_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"compute_vm_final_state_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"action_success\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"action_valid\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"action_no_funds\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"action_status_change\",\"type\":[{\"type\":\"enum\",\"name\":\"action_status_change_type\",\"symbols\":[\"unchanged\",\"frozen\",\"deleted\"]},\"null\"]},{\"name\":\"action_total_fwd_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"action_total_action_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"action_result_code\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_result_arg\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_tot_actions\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_spec_actions\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_skipped_actions\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_msgs_created\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_action_list_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"action_tot_msg_size_cells\",\"type\":[\"long\",\"null\"]},{\"name\":\"action_tot_msg_size_bits\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce\",\"type\":[{\"type\":\"enum\",\"name\":\"bounce_type\",\"symbols\":[\"negfunds\",\"nofunds\",\"ok\"]},\"null\"]},{\"name\":\"bounce_msg_size_cells\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce_msg_size_bits\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce_req_fwd_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce_msg_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce_fwd_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"split_info_cur_shard_pfx_len\",\"type\":[\"int\",\"null\"]},{\"name\":\"split_info_acc_split_depth\",\"type\":[\"int\",\"null\"]},{\"name\":\"split_info_this_addr\",\"type\":[\"string\",\"null\"]},{\"name\":\"split_info_sibling_addr\",\"type\":[\"string\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://DATA_PATH/transactions'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='archival_added_at', 
  'averageRecordSize'='579', 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"transactions\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"account\",\"type\":\"string\"},{\"name\":\"hash\",\"type\":\"string\"},{\"name\":\"lt\",\"type\":\"long\"},{\"name\":\"block_workchain\",\"type\":[\"int\",\"null\"]},{\"name\":\"block_shard\",\"type\":[\"long\",\"null\"]},{\"name\":\"block_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"mc_block_seqno\",\"type\":[\"int\",\"null\"]},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"prev_trans_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"prev_trans_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"now\",\"type\":[\"int\",\"null\"]},{\"name\":\"orig_status\",\"type\":[{\"type\":\"enum\",\"name\":\"orig_status\",\"symbols\":[\"uninit\",\"frozen\",\"active\",\"nonexist\"]},\"null\"]},{\"name\":\"end_status\",\"type\":[{\"type\":\"enum\",\"name\":\"end_status\",\"symbols\":[\"uninit\",\"frozen\",\"active\",\"nonexist\"]},\"null\"]},{\"name\":\"total_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"account_state_hash_before\",\"type\":[\"string\",\"null\"]},{\"name\":\"account_state_hash_after\",\"type\":[\"string\",\"null\"]},{\"name\":\"account_state_code_hash_before\",\"type\":[\"string\",\"null\"]},{\"name\":\"account_state_code_hash_after\",\"type\":[\"string\",\"null\"]},{\"name\":\"account_state_balance_before\",\"type\":[\"long\",\"null\"]},{\"name\":\"account_state_balance_after\",\"type\":[\"long\",\"null\"]},{\"name\":\"descr\",\"type\":[{\"type\":\"enum\",\"name\":\"descr\",\"symbols\":[\"ord\",\"storage\",\"tick_tock\",\"split_prepare\",\"split_install\",\"merge_prepare\",\"merge_install\"]},\"null\"]},{\"name\":\"aborted\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"destroyed\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"credit_first\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"is_tock\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"installed\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"storage_fees_collected\",\"type\":[\"long\",\"null\"]},{\"name\":\"storage_fees_due\",\"type\":[\"long\",\"null\"]},{\"name\":\"storage_status_change\",\"type\":[{\"type\":\"enum\",\"name\":\"storage_status_change_type\",\"symbols\":[\"unchanged\",\"frozen\",\"deleted\"]},\"null\"]},{\"name\":\"credit_due_fees_collected\",\"type\":[\"long\",\"null\"]},{\"name\":\"credit\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_skipped\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"skipped_reason\",\"type\":[{\"type\":\"enum\",\"name\":\"skipped_reason_type\",\"symbols\":[\"no_state\",\"bad_state\",\"no_gas\",\"suspended\"]},\"null\"]},{\"name\":\"compute_success\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"compute_msg_state_used\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"compute_account_activated\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"compute_gas_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_gas_used\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_gas_limit\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_gas_credit\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_mode\",\"type\":[\"int\",\"null\"]},{\"name\":\"compute_exit_code\",\"type\":[\"int\",\"null\"]},{\"name\":\"compute_exit_arg\",\"type\":[\"int\",\"null\"]},{\"name\":\"compute_vm_steps\",\"type\":[\"long\",\"null\"]},{\"name\":\"compute_vm_init_state_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"compute_vm_final_state_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"action_success\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"action_valid\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"action_no_funds\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"action_status_change\",\"type\":[{\"type\":\"enum\",\"name\":\"action_status_change_type\",\"symbols\":[\"unchanged\",\"frozen\",\"deleted\"]},\"null\"]},{\"name\":\"action_total_fwd_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"action_total_action_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"action_result_code\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_result_arg\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_tot_actions\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_spec_actions\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_skipped_actions\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_msgs_created\",\"type\":[\"int\",\"null\"]},{\"name\":\"action_action_list_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"action_tot_msg_size_cells\",\"type\":[\"long\",\"null\"]},{\"name\":\"action_tot_msg_size_bits\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce\",\"type\":[{\"type\":\"enum\",\"name\":\"bounce_type\",\"symbols\":[\"negfunds\",\"nofunds\",\"ok\"]},\"null\"]},{\"name\":\"bounce_msg_size_cells\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce_msg_size_bits\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce_req_fwd_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce_msg_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"bounce_fwd_fees\",\"type\":[\"long\",\"null\"]},{\"name\":\"split_info_cur_shard_pfx_len\",\"type\":[\"int\",\"null\"]},{\"name\":\"split_info_acc_split_depth\",\"type\":[\"int\",\"null\"]},{\"name\":\"split_info_this_addr\",\"type\":[\"string\",\"null\"]},{\"name\":\"split_info_sibling_addr\",\"type\":[\"string\",\"null\"]}]}', 
  'classification'='avro', 
  'compressionType'='none', 
  'objectCount'='1760', 
  'partition_filtering.enabled'='true', 
  'recordCount'='290533102', 
  'sizeKey'='176026030408', 
  'typeOfData'='file')



  CREATE EXTERNAL TABLE `exporters_messages`(
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
  `adding_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"messages\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"tx_lt\",\"type\":\"long\"},{\"name\":\"tx_now\",\"type\":\"int\"},{\"name\":\"msg_hash\",\"type\":\"string\"},{\"name\":\"direction\",\"type\":\"string\"},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"source\",\"type\":[\"string\",\"null\"]},{\"name\":\"destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"value\",\"type\":[\"long\",\"null\"]},{\"name\":\"fwd_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"ihr_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"created_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"created_at\",\"type\":[\"long\",\"null\"]},{\"name\":\"opcode\",\"type\":[\"int\",\"null\"]},{\"name\":\"ihr_disabled\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"bounce\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"bounced\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"import_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"body_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"comment\",\"type\":[\"string\",\"null\"]},{\"name\":\"init_state_hash\",\"type\":[\"string\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://DATA_PATH/messages/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='archival_added_at', 
  'averageRecordSize'='366', 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"messages\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"tx_lt\",\"type\":\"long\"},{\"name\":\"tx_now\",\"type\":\"int\"},{\"name\":\"msg_hash\",\"type\":\"string\"},{\"name\":\"direction\",\"type\":\"string\"},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"source\",\"type\":[\"string\",\"null\"]},{\"name\":\"destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"value\",\"type\":[\"long\",\"null\"]},{\"name\":\"fwd_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"ihr_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"created_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"created_at\",\"type\":[\"long\",\"null\"]},{\"name\":\"opcode\",\"type\":[\"int\",\"null\"]},{\"name\":\"ihr_disabled\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"bounce\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"bounced\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"import_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"body_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"comment\",\"type\":[\"string\",\"null\"]},{\"name\":\"init_state_hash\",\"type\":[\"string\",\"null\"]}]}', 
  'classification'='avro', 
  'compressionType'='none', 
  'objectCount'='2142', 
  'partition_filtering.enabled'='true', 
  'recordCount'='537323540', 
  'sizeKey'='214220539031', 
  'typeOfData'='file')


  
  CREATE EXTERNAL TABLE `exporters_messages_with_data`(
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
  `adding_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"messages_with_data\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"tx_lt\",\"type\":\"long\"},{\"name\":\"tx_now\",\"type\":\"int\"},{\"name\":\"msg_hash\",\"type\":\"string\"},{\"name\":\"direction\",\"type\":\"string\"},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"source\",\"type\":[\"string\",\"null\"]},{\"name\":\"destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"value\",\"type\":[\"long\",\"null\"]},{\"name\":\"fwd_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"ihr_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"created_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"created_at\",\"type\":[\"long\",\"null\"]},{\"name\":\"opcode\",\"type\":[\"int\",\"null\"]},{\"name\":\"ihr_disabled\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"bounce\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"bounced\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"import_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"body_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"body_boc\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"comment\",\"type\":[\"string\",\"null\"]},{\"name\":\"init_state_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"init_state_boc\",\"type\":[\"bytes\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://DATA_PATH/messages_with_data/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='archival_added_at', 
  'averageRecordSize'='446', 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"messages_with_data\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"tx_lt\",\"type\":\"long\"},{\"name\":\"tx_now\",\"type\":\"int\"},{\"name\":\"msg_hash\",\"type\":\"string\"},{\"name\":\"direction\",\"type\":\"string\"},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"source\",\"type\":[\"string\",\"null\"]},{\"name\":\"destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"value\",\"type\":[\"long\",\"null\"]},{\"name\":\"fwd_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"ihr_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"created_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"created_at\",\"type\":[\"long\",\"null\"]},{\"name\":\"opcode\",\"type\":[\"int\",\"null\"]},{\"name\":\"ihr_disabled\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"bounce\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"bounced\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"import_fee\",\"type\":[\"long\",\"null\"]},{\"name\":\"body_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"body_boc\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"comment\",\"type\":[\"string\",\"null\"]},{\"name\":\"init_state_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"init_state_boc\",\"type\":[\"bytes\",\"null\"]}]}', 
  'classification'='avro', 
  'compressionType'='none', 
  'objectCount'='2603', 
  'partition_filtering.enabled'='true', 
  'recordCount'='447303429', 
  'sizeKey'='260334540168', 
  'typeOfData'='file')



  CREATE EXTERNAL TABLE `exporters_balances_history`(
  `address` string COMMENT 'from deserializer', 
  `asset` string COMMENT 'from deserializer', 
  `amount` decimal(38,0) COMMENT 'from deserializer', 
  `mintless_claimed` boolean COMMENT 'from deserializer', 
  `timestamp` int COMMENT 'from deserializer', 
  `lt` bigint COMMENT 'from deserializer')
PARTITIONED BY ( 
  `adding_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"balances_history\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"},{\"name\":\"asset\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"mintless_claimed\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"timestamp\",\"type\":[\"int\",\"null\"]},{\"name\":\"lt\",\"type\":[\"long\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://DATA_PATH/balances_history/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='archival_added_at', 
  'averageRecordSize'='130', 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"balances_history\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"},{\"name\":\"asset\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"mintless_claimed\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"timestamp\",\"type\":[\"int\",\"null\"]},{\"name\":\"lt\",\"type\":[\"long\",\"null\"]}]}', 
  'classification'='avro', 
  'compressionType'='none', 
  'objectCount'='25', 
  'partition_filtering.enabled'='true', 
  'recordCount'='18358315', 
  'sizeKey'='2500082760', 
  'typeOfData'='file')

  CREATE EXTERNAL TABLE `exporters_jetton_events`(
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
  `adding_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"jetton_events\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"tx_lt\",\"type\":\"long\"},{\"name\":\"utime\",\"type\":\"int\"},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"tx_aborted\",\"type\":\"boolean\"},{\"name\":\"query_id\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":0},\"null\"]},{\"name\":\"amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"source\",\"type\":[\"string\",\"null\"]},{\"name\":\"destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"jetton_wallet\",\"type\":[\"string\",\"null\"]},{\"name\":\"jetton_master\",\"type\":[\"string\",\"null\"]},{\"name\":\"response_destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"custom_payload\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"forward_ton_amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"forward_payload\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"comment\",\"type\":[\"string\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://DATA_PATH/jetton_events/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='archival_added_at', 
  'averageRecordSize'='482', 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"jetton_events\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"tx_lt\",\"type\":\"long\"},{\"name\":\"utime\",\"type\":\"int\"},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"tx_aborted\",\"type\":\"boolean\"},{\"name\":\"query_id\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":0},\"null\"]},{\"name\":\"amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"source\",\"type\":[\"string\",\"null\"]},{\"name\":\"destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"jetton_wallet\",\"type\":[\"string\",\"null\"]},{\"name\":\"jetton_master\",\"type\":[\"string\",\"null\"]},{\"name\":\"response_destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"custom_payload\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"forward_ton_amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"forward_payload\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"comment\",\"type\":[\"string\",\"null\"]}]}', 
  'classification'='avro', 
  'compressionType'='none', 
  'objectCount'='343', 
  'partition_filtering.enabled'='true', 
  'recordCount'='66242656', 
  'sizeKey'='34304825554', 
  'typeOfData'='file')



  CREATE EXTERNAL TABLE `exporters_jetton_metadata`(
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
  's3://DATA_PATH/jetton_metadata/'
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
  'typeOfData'='file')


CREATE EXTERNAL TABLE `exporters_latest_account_states`(
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
  `adding_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"latest_account_states\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"account\",\"type\":\"string\"},{\"name\":\"hash\",\"type\":\"string\"},{\"name\":\"balance\",\"type\":[\"long\",\"null\"]},{\"name\":\"account_status\",\"type\":[\"string\",\"null\"]},{\"name\":\"timestamp\",\"type\":[\"int\",\"null\"]},{\"name\":\"last_trans_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"last_trans_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"frozen_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"data_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"code_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"data_boc\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"code_boc\",\"type\":[\"bytes\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://DATA_PATH/latest_account_states/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='archival_added_at', 
  'averageRecordSize'='1104', 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"latest_account_states\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"account\",\"type\":\"string\"},{\"name\":\"hash\",\"type\":\"string\"},{\"name\":\"balance\",\"type\":[\"long\",\"null\"]},{\"name\":\"account_status\",\"type\":[\"string\",\"null\"]},{\"name\":\"timestamp\",\"type\":[\"int\",\"null\"]},{\"name\":\"last_trans_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"last_trans_lt\",\"type\":[\"long\",\"null\"]},{\"name\":\"frozen_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"data_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"code_hash\",\"type\":[\"string\",\"null\"]},{\"name\":\"data_boc\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"code_boc\",\"type\":[\"bytes\",\"null\"]}]}', 
  'classification'='avro', 
  'compressionType'='none', 
  'objectCount'='516', 
  'partition_filtering.enabled'='true', 
  'recordCount'='80263065', 
  'sizeKey'='98712364068', 
  'typeOfData'='file')


  CREATE EXTERNAL TABLE `exporters_nft_items_history`(
  `address` string COMMENT 'from deserializer', 
  `is_init` boolean COMMENT 'from deserializer', 
  `index` string COMMENT 'from deserializer', 
  `collection_address` string COMMENT 'from deserializer', 
  `owner_address` string COMMENT 'from deserializer', 
  `content_onchain` string COMMENT 'from deserializer', 
  `timestamp` int COMMENT 'from deserializer', 
  `lt` bigint COMMENT 'from deserializer')
PARTITIONED BY ( 
  `adding_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"nft_items_history\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"},{\"name\":\"is_init\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"index\",\"type\":[\"string\",\"null\"]},{\"name\":\"collection_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"owner_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"content_onchain\",\"type\":[\"string\",\"null\"]},{\"name\":\"timestamp\",\"type\":[\"int\",\"null\"]},{\"name\":\"lt\",\"type\":[\"long\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://DATA_PATH/nft_items_history/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='archival_added_at', 
  'averageRecordSize'='340', 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"nft_items_history\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"},{\"name\":\"is_init\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"index\",\"type\":[\"string\",\"null\"]},{\"name\":\"collection_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"owner_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"content_onchain\",\"type\":[\"string\",\"null\"]},{\"name\":\"timestamp\",\"type\":[\"int\",\"null\"]},{\"name\":\"lt\",\"type\":[\"long\",\"null\"]}]}', 
  'classification'='avro', 
  'compressionType'='none', 
  'objectCount'='140', 
  'partition_filtering.enabled'='true', 
  'recordCount'='38373372', 
  'sizeKey'='13649182436', 
  'typeOfData'='file')


  CREATE EXTERNAL TABLE `exporters_nft_sales`(
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
  `adding_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"nft_sales\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"nft_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"nft_owner_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"created_at\",\"type\":[\"int\",\"null\"]},{\"name\":\"is_complete\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"is_canceled\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"end_time\",\"type\":[\"int\",\"null\"]},{\"name\":\"marketplace_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"marketplace_fee_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"marketplace_fee\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"price\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"asset\",\"type\":[\"string\",\"null\"]},{\"name\":\"royalty_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"royalty_amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"max_bid\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"min_bid\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"min_step\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"last_bid_at\",\"type\":[\"int\",\"null\"]},{\"name\":\"last_member\",\"type\":[\"string\",\"null\"]},{\"name\":\"timestamp\",\"type\":[\"int\",\"null\"]},{\"name\":\"lt\",\"type\":[\"long\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://DATA_PATH/nft_sales/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='archival_added_at', 
  'averageRecordSize'='476', 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"nft_sales\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"address\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"nft_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"nft_owner_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"created_at\",\"type\":[\"int\",\"null\"]},{\"name\":\"is_complete\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"is_canceled\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"end_time\",\"type\":[\"int\",\"null\"]},{\"name\":\"marketplace_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"marketplace_fee_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"marketplace_fee\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"price\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"asset\",\"type\":[\"string\",\"null\"]},{\"name\":\"royalty_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"royalty_amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"max_bid\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"min_bid\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"min_step\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"last_bid_at\",\"type\":[\"int\",\"null\"]},{\"name\":\"last_member\",\"type\":[\"string\",\"null\"]},{\"name\":\"timestamp\",\"type\":[\"int\",\"null\"]},{\"name\":\"lt\",\"type\":[\"long\",\"null\"]}]}', 
  'classification'='avro', 
  'compressionType'='none', 
  'objectCount'='1', 
  'partition_filtering.enabled'='true', 
  'recordCount'='210105', 
  'sizeKey'='100011769', 
  'typeOfData'='file')


  CREATE EXTERNAL TABLE `exporters_nft_transfers`(
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
  `adding_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"nft_transfers\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"tx_lt\",\"type\":\"long\"},{\"name\":\"tx_now\",\"type\":\"int\"},{\"name\":\"tx_aborted\",\"type\":\"boolean\"},{\"name\":\"query_id\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":0},\"null\"]},{\"name\":\"nft_item_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"nft_item_index\",\"type\":[\"string\",\"null\"]},{\"name\":\"nft_collection_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"old_owner\",\"type\":[\"string\",\"null\"]},{\"name\":\"new_owner\",\"type\":[\"string\",\"null\"]},{\"name\":\"response_destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"custom_payload\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"forward_amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"forward_payload\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"comment\",\"type\":[\"string\",\"null\"]},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://DATA_PATH/nft_transfers/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='archival_added_at', 
  'averageRecordSize'='477', 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"nft_transfers\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"tx_lt\",\"type\":\"long\"},{\"name\":\"tx_now\",\"type\":\"int\"},{\"name\":\"tx_aborted\",\"type\":\"boolean\"},{\"name\":\"query_id\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":0},\"null\"]},{\"name\":\"nft_item_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"nft_item_index\",\"type\":[\"string\",\"null\"]},{\"name\":\"nft_collection_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"old_owner\",\"type\":[\"string\",\"null\"]},{\"name\":\"new_owner\",\"type\":[\"string\",\"null\"]},{\"name\":\"response_destination\",\"type\":[\"string\",\"null\"]},{\"name\":\"custom_payload\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"forward_amount\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"forward_payload\",\"type\":[\"bytes\",\"null\"]},{\"name\":\"comment\",\"type\":[\"string\",\"null\"]},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]}]}', 
  'classification'='avro', 
  'compressionType'='none', 
  'objectCount'='5', 
  'partition_filtering.enabled'='true', 
  'recordCount'='1044557', 
  'sizeKey'='500068738', 
  'typeOfData'='file')


  CREATE EXTERNAL TABLE `exporters_dex_pool`(
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
  `adding_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"dex_pool\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"pool\",\"type\":\"string\"},{\"name\":\"project\",\"type\":\"string\"},{\"name\":\"version\",\"type\":[\"int\",\"null\"]},{\"name\":\"discovered_at\",\"type\":[\"int\",\"null\"]},{\"name\":\"jetton_left\",\"type\":[\"string\",\"null\"]},{\"name\":\"jetton_right\",\"type\":[\"string\",\"null\"]},{\"name\":\"reserves_left\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"reserves_right\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"total_supply\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"tvl_usd\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":6},\"null\"]},{\"name\":\"tvl_ton\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":9},\"null\"]},{\"name\":\"last_updated\",\"type\":[\"int\",\"null\"]},{\"name\":\"is_liquid\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"lp_fee\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":12,\"scale\":10},\"null\"]},{\"name\":\"protocol_fee\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":12,\"scale\":10},\"null\"]},{\"name\":\"referral_fee\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":12,\"scale\":10},\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://DATA_PATH/dex_pool/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='archival_added_at', 
  'averageRecordSize'='507', 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"dex_pool\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"pool\",\"type\":\"string\"},{\"name\":\"project\",\"type\":\"string\"},{\"name\":\"version\",\"type\":[\"int\",\"null\"]},{\"name\":\"discovered_at\",\"type\":[\"int\",\"null\"]},{\"name\":\"jetton_left\",\"type\":[\"string\",\"null\"]},{\"name\":\"jetton_right\",\"type\":[\"string\",\"null\"]},{\"name\":\"reserves_left\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"reserves_right\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"total_supply\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"tvl_usd\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":6},\"null\"]},{\"name\":\"tvl_ton\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":9},\"null\"]},{\"name\":\"last_updated\",\"type\":[\"int\",\"null\"]},{\"name\":\"is_liquid\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"lp_fee\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":12,\"scale\":10},\"null\"]},{\"name\":\"protocol_fee\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":12,\"scale\":10},\"null\"]},{\"name\":\"referral_fee\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":12,\"scale\":10},\"null\"]}]}', 
  'classification'='avro', 
  'compressionType'='none', 
  'objectCount'='39', 
  'partition_filtering.enabled'='true', 
  'recordCount'='7364434', 
  'sizeKey'='3811828584', 
  'typeOfData'='file')


  CREATE EXTERNAL TABLE `exporters_dex_swaps`(
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
  `adding_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
WITH SERDEPROPERTIES ( 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"dex_swaps\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"project_type\",\"type\":\"string\"},{\"name\":\"project\",\"type\":\"string\"},{\"name\":\"version\",\"type\":[\"int\",\"null\"]},{\"name\":\"event_time\",\"type\":\"int\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"trader_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"pool_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"router_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"token_sold_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"token_bought_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"amount_sold_raw\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"amount_bought_raw\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"referral_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"platform_tag\",\"type\":[\"string\",\"null\"]},{\"name\":\"query_id\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":0},\"null\"]},{\"name\":\"volume_usd\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":6},\"null\"]},{\"name\":\"volume_ton\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":9},\"null\"]}]}') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  's3://DATA_PATH/dex_swaps/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='archival_added_at', 
  'averageRecordSize'='512', 
  'avro.schema.literal'='{\"type\":\"record\",\"name\":\"dex_swaps\",\"namespace\":\"ton\",\"fields\":[{\"name\":\"tx_hash\",\"type\":\"string\"},{\"name\":\"trace_id\",\"type\":[\"string\",\"null\"]},{\"name\":\"project_type\",\"type\":\"string\"},{\"name\":\"project\",\"type\":\"string\"},{\"name\":\"version\",\"type\":[\"int\",\"null\"]},{\"name\":\"event_time\",\"type\":\"int\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"trader_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"pool_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"router_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"token_sold_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"token_bought_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"amount_sold_raw\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"amount_bought_raw\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":38,\"scale\":0},\"null\"]},{\"name\":\"referral_address\",\"type\":[\"string\",\"null\"]},{\"name\":\"platform_tag\",\"type\":[\"string\",\"null\"]},{\"name\":\"query_id\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":0},\"null\"]},{\"name\":\"volume_usd\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":6},\"null\"]},{\"name\":\"volume_ton\",\"type\":[{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":9},\"null\"]}]}', 
  'classification'='avro', 
  'compressionType'='none', 
  'objectCount'='11', 
  'partition_filtering.enabled'='true', 
  'recordCount'='2134892', 
  'sizeKey'='1100170098', 
  'typeOfData'='file')