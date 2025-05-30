import json
from typing import Dict, Union
import uuid
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.operators.python import get_current_context
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.models import Variable, Connection
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition, ConsumerGroupTopicPartitions
import traceback


from datetime import datetime, timedelta
from time import time, sleep
import logging
import boto3
import pytz
import os

def results_to_df(results):
    logging.info(results)

    columns = [
        col['Label']
        for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']
    ]

    listed_results = []
    for res in results['ResultSet']['Rows'][1:]:
        values = []
        for field in res['Data']:
            try:
                values.append(list(field.values())[0])
            except:
                values.append(list(' '))

        listed_results.append(
            dict(zip(columns, values))
        )

    return listed_results

def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def send_notification(message):
    if Variable.get('SLACK_CHANNEL', default_var=None):
        slack_msg = SlackAPIPostOperator(
            task_id='send_slack_message',
            text=message,
            channel=Variable.get('SLACK_CHANNEL'),
        )
        slack_msg.execute(get_current_context())
    else:
        telegram_hook = TelegramHook(telegram_conn_id="telegram_watchdog_conn")
        telegram_hook.send_message({"text": message})

"""
Perform daily sync of data of the datalake
"""
@dag(
    schedule_interval="15 0 * * *",
    start_date=datetime(2024, 12, 1),
    catchup=False,
    concurrency=4,
    max_active_runs=1,
    tags=['ton', 'datalake']
)
def datalake_daily_sync():
    datalake_output_bucket = Variable.get("DATALAKE_ATHENA_DATALAKE_OUTPUT_BUCKET")
    datalake_exporters_bucket = Variable.get("DATALAKE_ATHENA_DATALAKE_EXPORTERS_BUCKET")
    datalake_exporters_prefix = Variable.get("DATALAKE_ATHENA_DATALAKE_EXPORTERS_PREFIX")
    datalake_athena_temp_bucket = Variable.get("DATALAKE_TMP_LOCATION")
    env_tag = Variable.get("DATALAKE_TARGET_DATABASE")
    is_testnet_mode = True if int(Variable.get("TESTNET_MODE", "0")) else False

    def safe_python_callable(func, kwargs, step_name):
        try:
            func(kwargs)
        except Exception as e:
            logging.error(f"Unable to perform {func.__name__} for *{step_name}* {e} {traceback.format_exc()}")
            send_notification(f"📛 [{env_tag}] Unable to invoke {func.__name__} for *{step_name}")
            raise e

    """
    Perform checks that datalake has last blocks after the end of the period
    """
    def perform_last_block_check(kwargs):
        task_instance = kwargs['task_instance']
        LAST_BLOCK_MATURITY_PERIOD = 10 * 60 # 10 minutes
        logical_time = kwargs['logical_time']
        logging.info(f"logical_time raw: {logical_time}")
        logical_time = logical_time[0:10]
        logging.info(f"Logical time: {logical_time}")
        start_of_the_day = datetime.strptime(logical_time, "%Y-%m-%d")
        end_of_the_day = start_of_the_day + timedelta(days=1)
        start_of_the_day_ts = int(start_of_the_day.timestamp())
        end_of_the_day_ts = int(end_of_the_day.timestamp())
        task_instance.xcom_push(key="start_of_the_day_ts", value=start_of_the_day_ts)
        task_instance.xcom_push(key="end_of_the_day_ts", value=end_of_the_day_ts)

        logging.info(f"Perform conversion for the period {start_of_the_day_ts} ({start_of_the_day}) - {end_of_the_day_ts} ({end_of_the_day})")

        postgres_hook = PostgresHook(postgres_conn_id="datalake_db")
        last_block = postgres_hook.get_first("select gen_utime from blocks where workchain = -1 and shard = -9223372036854775808 order by seqno desc limit 1")[0]
        logging.info(f"Last block: {last_block}")

        if last_block < end_of_the_day_ts + LAST_BLOCK_MATURITY_PERIOD:
            raise Exception(f"Last block is too old: {last_block}, should be at least {end_of_the_day_ts + LAST_BLOCK_MATURITY_PERIOD}")
        else:
            logging.info(f"Last block is fresh enough: {last_block}")

    def check_kafka_consumer_state(group_id: str, field: Union[str, Dict[str, str]], topic: str=None, allow_empty: int=0):
        conf = {'bootstrap.servers':  Variable.get("DATALAKE_KAFKA_BROKER")}
        logging.info(f"Connecting to kafka {conf['bootstrap.servers']}")
        admin_client = AdminClient(conf)
        consumer_conf = {
            'bootstrap.servers': conf['bootstrap.servers'],
            'group.id': 'airflow_checker',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        consumer = Consumer(consumer_conf)

        group_offsets = admin_client.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group_id)])[group_id].result()
        logging.info(group_offsets.topic_partitions)
        min_timestamp = None
        empty_count = 0
        for tp in group_offsets.topic_partitions:
            if topic is not None and tp.topic != topic or (type(field) == dict and tp.topic not in field):
                logging.info(f"Skipping topic {tp.topic}")
                continue
            # if tp.offset > 0:
            #     tp.offset -= 1 # mitigate situation when consumer offset is the last message, so rewind to the previous one
            consumer.assign([tp])
            message = consumer.poll(timeout=20.0)
            if message is not None:
                assert message.offset() == tp.offset, f"Message offset {message.offset()} does not match expected offset {tp.offset}"
                message_value = json.loads(message.value().decode('utf-8'))
                logging.info(f"Message value: {message_value}")
                field_ = field if type(field) == str else field[tp.topic]
                ts = message_value[field_]
                if ts is None:
                    empty_count += 1
                    continue
                min_timestamp = min(min_timestamp, ts) if min_timestamp is not None else ts
                logging.info(f"{tp}, {tp.offset}, {ts}")
            else:
                logging.info(f"All messages for {tp} are consumed")
                # raise Exception(f"No message for {tp}")
        if empty_count > allow_empty:
            raise Exception(f"Got {empty_count} empty partitions for {group_id}")
        if empty_count > 0:
            logging.info(f"Got {empty_count} empty partitions for {group_id}")
        return min_timestamp
    
    def transfer_s3_objects(s3, source_location: str, target_location: str):
        bucket, key = source_location.replace("s3://", "").split("/", 1)
        target_bucket, target_key = target_location.replace("s3://", "").split("/", 1)

        continuation_token = None
        output_size = 0
        output_files = 0
        while True:
            opts = {}
            if continuation_token:
                opts["ContinuationToken"] = continuation_token
            # TODO add suffix to key
            objects = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=100, **opts)
            for obj in objects.get("Contents", []):
                logging.info(f"Processing {obj['Key']}")
                suffix = "/".join(obj['Key'].split("/")[-2:])
                logging.info(f"Copying {obj['Key']} to {target_bucket}/{target_key}/{suffix}")
                s3.copy({'Bucket': bucket, 'Key': obj['Key']}, target_bucket, Key=f"{target_key}/{suffix}")
                output_files += 1
                output_size += obj['Size']
            if not objects.get("IsTruncated", False):
                break
            continuation_token = objects.get("NextContinuationToken")
        return output_size, output_files

    """
    Converts table from raw exported items into output table
    """
    def convert_table(kwargs):
        task_instance = kwargs['task_instance']

        source_database = Variable.get("DATALAKE_SOURCE_DATABASE")
        target_database = Variable.get("DATALAKE_TARGET_DATABASE")
        repartition_field = kwargs['repartition_field']
        source_table = kwargs['source_table']
        target_table = kwargs['target_table']
        table_location = kwargs['target_table_location']
        tmp_location = Variable.get("DATALAKE_TMP_LOCATION")
        workgroup = Variable.get("DATALAKE_ATHENA_WORKGROUP")
        dedup_depth = kwargs['dedup_depth']
        primary_key = kwargs.get('primary_key', None)
        allow_empty_partitions = kwargs.get('allow_empty_partitions', 0)
        start_of_the_day_ts = task_instance.xcom_pull(key="start_of_the_day_ts", task_ids='perform_last_block_check')
        end_of_the_day_ts = task_instance.xcom_pull(key="end_of_the_day_ts", task_ids='perform_last_block_check')
        logging.info(f"Start of the day: {start_of_the_day_ts}, end of the day: {end_of_the_day_ts}")

        start_of_the_day = datetime.fromtimestamp(start_of_the_day_ts, pytz.utc)
        current_table_check_partition = (start_of_the_day - timedelta(days=dedup_depth)).strftime("%Y%m%d")
        current_date = (start_of_the_day).strftime("%Y%m%d")
        source_table_since_partition = (start_of_the_day - timedelta(days=2)).strftime("%Y%m%d")
        
        EXPORTER_MATURITY_PERIOD = 5 * 60 # 5 minutes

        waited = 0
        MAX_WAIT_TIME = 7200
        while True:
            if is_testnet_mode:
                logging.info("Kafka check skipped in TESTNET_MODE")
                break
            group_id = kwargs['kafka_group_id']
            field = kwargs.get('topics_timestamp_field', kwargs['repartition_field'])
            min_timestamp = check_kafka_consumer_state(group_id=group_id, field=field, allow_empty=allow_empty_partitions)
            if min_timestamp is None:
                logging.info(f"No messages for {group_id}, will sleep for 60 seconds")
            elif min_timestamp >= end_of_the_day_ts + EXPORTER_MATURITY_PERIOD:
                logging.info(f"Found messages for {group_id} with min timestamp {min_timestamp}, it is {min_timestamp - end_of_the_day_ts} seconds since the end of the day")
                break
            else:
                logging.info(f"Found messages for {group_id} with min timestamp {min_timestamp}, it is {end_of_the_day_ts + EXPORTER_MATURITY_PERIOD - min_timestamp} seconds before the end of the day + exporter maturity period")
            sleep(60)
            waited += 60
            if waited > MAX_WAIT_TIME:
                raise Exception(f"No messages for {group_id} in the last two hours")


        athena = AthenaHook('s3_conn', region_name='us-east-1')
        def execute_athena_query(query, database=source_database):
            query_id = athena.run_query(query,
                                        query_context={"Database": database},
                                        result_configuration={'OutputLocation': f's3://{datalake_athena_temp_bucket}/'},
                                        workgroup=workgroup)
            final_state = athena.poll_query_status(query_id)
            if final_state == 'FAILED' or final_state == 'CANCELLED':
                raise Exception(f"Unable to get data from Athena: {query_id}")
            
            return query_id
        
        execute_athena_query(f"MSCK REPAIR TABLE {source_table}", database=source_database)
        execute_athena_query(f"MSCK REPAIR TABLE {target_table}", database=target_database)

        glue = boto3.client("glue", region_name="us-east-1")
        source_table_meta = glue.get_table(DatabaseName=source_database, Name=source_table)
        logging.info(source_table_meta)

        try:
            glue.get_table(DatabaseName=target_database, Name=target_table)
        except Exception as e:
            if e.__class__.__name__ != "EntityNotFoundException":
                raise e
            logging.warning(f"Table {target_table} not found in database {target_database}, creating new table")
            
            response =glue.create_table(
                DatabaseName=target_database,
                TableInput={
                    "Name": target_table,
                    "TableType": "EXTERNAL_TABLE",
                    "StorageDescriptor": {
                        'Columns': source_table_meta['Table']['StorageDescriptor']['Columns'],
                        'Location': table_location,
                        'InputFormat': 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat',
                        'SerdeInfo': source_table_meta['Table']['StorageDescriptor']['SerdeInfo'],
                    },
                    "PartitionKeys": [
                        {
                            "Name": "block_date",
                            "Type": "string",
                        }
                    ],
                    "TableType": "EXTERNAL_TABLE",
                }
            )
            logging.info(response)

        tmp_table_name = f"{target_table}_increment_{str(uuid.uuid4()).replace('-', '')}"
        tmp_table_location = f"s3://{tmp_location}/{tmp_table_name}"
        FIELDS = ", ".join([col['Name'] for col in source_table_meta['Table']['StorageDescriptor']['Columns']])
        sql = f"""
        create table "{source_database}".{tmp_table_name}
        with (
            format = 'AVRO', 
            write_compression = 'SNAPPY',
            external_location = '{tmp_table_location}',
            bucketed_by = ARRAY['{source_table_meta['Table']['StorageDescriptor']['Columns'][0]['Name']}'],
            bucket_count = 1,
            partitioned_by = ARRAY['block_date']
        )
        as
        select {FIELDS},
        date_format(from_unixtime({repartition_field}), '%Y%m%d') as block_date
        from "{source_database}".{source_table}
        where adding_date >= '{source_table_since_partition}' and date_format(from_unixtime({repartition_field}), '%Y%m%d') <= '{current_date}'
        except
        select {FIELDS}, block_date
        from "{target_database}".{target_table} where block_date >= '{current_table_check_partition}'
        """
        if primary_key is not None:
            sql = f"""
            {sql}
            and {primary_key} not in (select key from "{target_database}".excluded_rows
             where "table" = '{target_table}')
            """
        logging.info(f"Running SQL code to convert data into single file dataset {sql}")

        execute_athena_query(sql)
        glue.delete_table(DatabaseName=source_database, Name=tmp_table_name)
        logging.info(f"Time to transfer output data from {tmp_table_location} to {table_location}")
        s3_hook = S3Hook(aws_conn_id="s3_conn")

        output_size, output_files = transfer_s3_objects(s3_hook.get_conn(), tmp_table_location, table_location)

        logging.info(f"Refreshing partitions")
        execute_athena_query(f"MSCK REPAIR TABLE {target_table}", database=target_database)

        query_id = execute_athena_query(f"select count(1) as count from {target_table} where block_date = '{current_date}'", database=target_database)

        results = results_to_df(athena.get_query_results_paginator(query_id).build_full_result())
        logging.info(f"Got {len(results)} results")
        logging.info(results)
        output_count = results[0]['count']

        finished = datetime.now().strftime("%I:%M %p")
        send_notification(f"🚰👌 [{env_tag}] {target_table} finished at {finished}. {output_count} rows, {sizeof_fmt(output_size)} bytes, {output_files} files")

    """
    Checks Kafka commited offset for the consumer group
    """
    def check_kafka_offset(kwargs):
        if kwargs.get('skip_in_testnet_mode'):
            logging.info("Task skipped in TESTNET_MODE")
            return

        task_instance = kwargs['task_instance']
        group_id = kwargs['kafka_group_id']
        field = kwargs['field']
        topic = kwargs['topic']

        # start_of_the_day_ts = task_instance.xcom_pull(key="start_of_the_day_ts", task_ids='perform_last_block_check')
        end_of_the_day_ts = task_instance.xcom_pull(key="end_of_the_day_ts", task_ids='perform_last_block_check')
        # logging.info(f"Start of the day: {start_of_the_day_ts}, end of the day: {end_of_the_day_ts}")

        # start_of_the_day = datetime.fromtimestamp(start_of_the_day_ts, pytz.utc)
        # current_date = (start_of_the_day).strftime("%Y%m%d")
        # source_table_since_partition = (start_of_the_day - timedelta(days=2)).strftime("%Y%m%d")
        
        EXPORTER_MATURITY_PERIOD = 5 * 60 # 5 minutes

        waited = 0
        MAX_WAIT_TIME = 7200
        while True:
            min_timestamp = check_kafka_consumer_state(group_id=group_id, field=field, topic=topic)
            if min_timestamp is None:
                logging.info(f"No messages for {group_id}, will sleep for 60 seconds")
            elif min_timestamp >= end_of_the_day_ts + EXPORTER_MATURITY_PERIOD:
                logging.info(f"Found messages for {group_id} with min timestamp {min_timestamp}, it is {min_timestamp - end_of_the_day_ts} seconds since the end of the day")
                break
            else:
                logging.info(f"Found messages for {group_id} with min timestamp {min_timestamp}, it is {end_of_the_day_ts + EXPORTER_MATURITY_PERIOD - min_timestamp} seconds before the end of the day + exporter maturity period")
            sleep(60)
            waited += 60
            if waited > MAX_WAIT_TIME:
                raise Exception(f"No messages for {group_id} in the last two hours")

    perform_last_block_check_task = PythonOperator(
        task_id=f'perform_last_block_check',
        python_callable=lambda **kwargs: safe_python_callable(perform_last_block_check, kwargs, "last_block_check"),
        op_kwargs={
            'logical_time': '{{ data_interval_start }}'
        }
    )

    convert_blocks_task = PythonOperator(
        task_id=f'convert_blocks',
        python_callable=lambda **kwargs: safe_python_callable(convert_table, kwargs, "convert_blocks"),
        op_kwargs={
            'repartition_field': 'gen_utime',
            'source_table': 'exporters_blocks',
            'target_table': 'blocks',
            'target_table_location': f's3://{datalake_output_bucket}/v1/blocks',
            'dedup_depth': 3,
            'kafka_group_id': 'exporter_archive_blocks'
        }
    )

    convert_transactions_task = PythonOperator(
        task_id=f'convert_transactions',
        python_callable=lambda **kwargs: safe_python_callable(convert_table, kwargs, "convert_transactions"),
        op_kwargs={
            'repartition_field': 'now',
            'source_table': 'exporters_transactions',
            'target_table': 'transactions',
            'target_table_location': f's3://{datalake_output_bucket}/v1/transactions',
            'dedup_depth': 3,
            'kafka_group_id': 'exporter_archive_transactions'
        }
    )

    convert_messages_task = PythonOperator(
        task_id=f'convert_messages',
        python_callable=lambda **kwargs: safe_python_callable(convert_table, kwargs, "convert_messages"),
        op_kwargs={
            'repartition_field': 'tx_now',
            'source_table': 'exporters_messages',
            'target_table': 'messages',
            'target_table_location': f's3://{datalake_output_bucket}/v1/messages',
            'dedup_depth': 3,
            'kafka_group_id': 'exporter_archive_messages'
        }
    )

    convert_messages_with_data_task = PythonOperator(
        task_id=f'convert_messages_with_data',
        python_callable=lambda **kwargs: safe_python_callable(convert_table, kwargs, "convert_messages_with_data"),
        op_kwargs={
            'repartition_field': 'tx_now',
            'source_table': 'exporters_messages_with_data',
            'target_table': 'messages_with_data',
            'target_table_location': f's3://{datalake_output_bucket}/v1/messages_with_data',
            'dedup_depth': 3,
            'kafka_group_id': 'exporter_archive_messages_with_data'
        }
    )

    convert_accounts_task = PythonOperator(
        task_id=f'convert_accounts',
        python_callable=lambda **kwargs: safe_python_callable(convert_table, kwargs, "convert_accounts"),
        op_kwargs={
            'repartition_field': 'timestamp',
            'source_table': 'exporters_latest_account_states',
            'target_table': 'account_states',
            'target_table_location': f's3://{datalake_output_bucket}/v1/account_states',
            'dedup_depth': 3,
            'kafka_group_id': 'exporter_account_states_3'
        }
    )

    check_main_parser_offset_task = PythonOperator(
        task_id=f'check_main_parser_offset',
        python_callable=lambda **kwargs: safe_python_callable(check_kafka_offset, kwargs, "check_main_parser_offset"),
        op_kwargs={
            'kafka_group_id': 'messages_parsers',
            'topic': 'ton.public.messages',
            'field': 'tx_now'
        }
    )

    check_core_prices_offset_task = PythonOperator(
        task_id=f'check_core_prices_offset',
        python_callable=lambda **kwargs: safe_python_callable(check_kafka_offset, kwargs, "check_core_prices_offset"),
        op_kwargs={
            'kafka_group_id': 'core_prices',
            'topic': 'ton.public.latest_account_states',
            'field': 'timestamp',
            'skip_in_testnet_mode': is_testnet_mode,
        }
    )

    check_tvl_parser_offset_task = PythonOperator(
        task_id=f'check_tvl_parser_offset',
        python_callable=lambda **kwargs: safe_python_callable(check_kafka_offset, kwargs, "check_tvl_parser_offset"),
        op_kwargs={
            'kafka_group_id': 'dex_tvl_parsing',
            'topic': 'ton.public.latest_account_states',
            'field': 'timestamp',
            'skip_in_testnet_mode': is_testnet_mode,
        }
    )

    check_megatons_offset_task = PythonOperator(
        task_id=f'check_megatons_offset',
        python_callable=lambda **kwargs: safe_python_callable(check_kafka_offset, kwargs, "check_megatons_offset"),
        op_kwargs={
            'kafka_group_id': 'jettons_megaton',
            'topic': 'ton.public.jetton_transfers',
            'field': 'tx_now',
            'skip_in_testnet_mode': is_testnet_mode,
        }
    )

    convert_dex_trades_task = PythonOperator(
        task_id=f'convert_dex_trades',
        python_callable=lambda **kwargs: safe_python_callable(convert_table, kwargs, "convert_dex_trades"),
        op_kwargs={
            'repartition_field': 'event_time',
            'source_table': 'exporters_dex_swaps',
            'target_table': 'dex_trades',
            'target_table_location': f's3://{datalake_output_bucket}/v1/dex_trades',
            'dedup_depth': 10000,
            'kafka_group_id': 'exporter_dex_trades_fix',
            'primary_key': 'tx_hash',
            'topics_timestamp_field': {
                'ton.parsed.dex_swap_parsed': 'swap_utime',
                'ton.parsed.gaspump_trade': 'event_time',
                'ton.parsed.tonfun_bcl_trade': 'event_time'
            }
        }
    )

    convert_jetton_events = PythonOperator(
        task_id=f'convert_jetton_events',
        python_callable=lambda **kwargs: safe_python_callable(convert_table, kwargs, "convert_jetton_events"),
        op_kwargs={
            'repartition_field': 'utime',
            'source_table': 'exporters_jetton_events',
            'target_table': 'jetton_events',
            'target_table_location': f's3://{datalake_output_bucket}/v1/jetton_events',
            'dedup_depth': 10000,
            'kafka_group_id': 'exporter_jetton_events',
            # 'primary_key': 'tx_hash',
            'topics_timestamp_field': {
                'ton.parsed.jetton_mint': 'utime',
                'ton.public.jetton_burns': 'tx_now',
                'ton.public.jetton_transfers': 'tx_now'
            }
        }
    )


    convert_dex_tvl_task = PythonOperator(
        task_id=f'convert_dex_tvl',
        python_callable=lambda **kwargs: safe_python_callable(convert_table, kwargs, "convert_dex_tvl"),
        op_kwargs={
            'repartition_field': 'last_updated',
            'source_table': 'exporters_dex_pool',
            'target_table': 'dex_pools',
            'target_table_location': f's3://{datalake_output_bucket}/v1/dex_pools',
            'dedup_depth': 10000,
            'kafka_group_id': 'exporter_dex_pools',
            'allow_empty_partitions': 3
        }
    )

    convert_balances_history_task = PythonOperator(
        task_id=f'convert_balances_history',
        python_callable=lambda **kwargs: safe_python_callable(convert_table, kwargs, "convert_balances_history"),
        op_kwargs={
            'repartition_field': 'timestamp',
            'source_table': 'exporters_balances_history',
            'target_table': 'balances_history',
            'target_table_location': f's3://{datalake_output_bucket}/v1/balances_history',
            'dedup_depth': 10000,
            'kafka_group_id': 'exporter_balances_history',
            'topics_timestamp_field': {
                'ton.public.jetton_wallets': 'last_tx_now',
                'ton.public.latest_account_states': 'timestamp'
            }
        }
    )

    convert_nft_items_task = PythonOperator(
        task_id=f'convert_nft_items',
        python_callable=lambda **kwargs: safe_python_callable(convert_table, kwargs, "convert_nft_items"),
        op_kwargs={
            'repartition_field': 'timestamp',
            'source_table': 'exporters_nft_items_history',
            'target_table': 'nft_items',
            'target_table_location': f's3://{datalake_output_bucket}/v1/nft_items',
            'dedup_depth': 10000,
            'kafka_group_id': 'exporter_nft_items',
            'topics_timestamp_field': {
                'ton.parsed.nft_items': 'last_tx_now'
            }
        }
    )

    check_nft_parser_offset_task = PythonOperator(
        task_id=f'check_nft_parser_offset',
        python_callable=lambda **kwargs: safe_python_callable(check_kafka_offset, kwargs, "check_nft_parser_offset"),
        op_kwargs={
            'kafka_group_id': 'nft_items_parser',
            'topic': 'ton.public.latest_account_states',
            'field': 'timestamp'
        }
    )

    convert_nft_transfers_task = PythonOperator(
        task_id=f'convert_nft_transfers',
        python_callable=lambda **kwargs: safe_python_callable(convert_table, kwargs, "convert_nft_transfers"),
        op_kwargs={
            'repartition_field': 'tx_now',
            'source_table': 'exporters_nft_transfers',
            'target_table': 'nft_transfers',
            'target_table_location': f's3://{datalake_output_bucket}/v1/nft_transfers',
            'dedup_depth': 10000,
            'kafka_group_id': 'exporter_nft_transfers'
        }
    )

    convert_nft_sales_task = PythonOperator(
        task_id=f'convert_nft_sales',
        python_callable=lambda **kwargs: safe_python_callable(convert_table, kwargs, "convert_nft_sales"),
        op_kwargs={
            'repartition_field': 'timestamp',
            'source_table': 'exporters_nft_sales',
            'target_table': 'nft_sales',
            'target_table_location': f's3://{datalake_output_bucket}/v1/nft_sales',
            'dedup_depth': 10000,
            'kafka_group_id': 'exporter_nft_sales_4',
            'topics_timestamp_field': {
                'ton.public.getgems_nft_auctions': 'last_tx_now',
                'ton.public.getgems_nft_sales': 'last_tx_now'
            }
        }
    )

    def generate_balances_snapshot(kwargs):
        athena = AthenaHook('s3_conn', region_name='us-east-1')
        target_database = Variable.get("DATALAKE_TARGET_DATABASE")
        query = f"""
        insert into "{target_database}".balances_snapshot(address, asset, amount, mintless_claimed, timestamp, lt, block_date)
        with ranks as (
        select address, asset, amount, mintless_claimed, timestamp, lt,
        row_number() over (partition by address, asset order by lt desc) as rank
        from "{target_database}"."balances_history"
        )
        select address, asset, amount, mintless_claimed, timestamp, lt,
        (SELECT max(block_date) FROM "{target_database}"."balances_history$partitions") as block_date
        from ranks where rank = 1
        and (SELECT max(block_date) FROM "{target_database}"."balances_history$partitions") != 
        (SELECT max(block_date) FROM "{target_database}"."balances_snapshot$partitions") 
        """
        query_id = athena.run_query(query,
                                    query_context={"Database": Variable.get("DATALAKE_TARGET_DATABASE")},
                                    result_configuration={'OutputLocation': f's3://{datalake_athena_temp_bucket}/'},
                                    workgroup=Variable.get("DATALAKE_ATHENA_WORKGROUP"))
        final_state = athena.poll_query_status(query_id)
        if final_state == 'FAILED' or final_state == 'CANCELLED':
            raise Exception(f"Unable to get data from Athena: {query_id}")
        
    generate_balances_snapshot_task = PythonOperator(
        task_id=f'generate_balances_snapshot',
        python_callable=lambda **kwargs: safe_python_callable(generate_balances_snapshot, kwargs, "generate_balances_snapshot")
    )

    def refresh_metadata_partitions(kwargs):
        task_instance = kwargs['task_instance']

        start_of_the_day_ts = task_instance.xcom_pull(key="start_of_the_day_ts", task_ids='perform_last_block_check')
        start_of_the_day = datetime.fromtimestamp(start_of_the_day_ts, pytz.utc)
        current_date = (start_of_the_day).strftime("%Y%m%d")
        
        s3_hook = S3Hook(aws_conn_id="s3_conn")
        target_table_location = kwargs['target_table_location']
        source_table_location = kwargs['source_table_location'] + f"/adding_date={current_date}"
        logging.info(f"Starting sync of {kwargs['target_table']} from {source_table_location} to {target_table_location}")

        output_size, output_files = transfer_s3_objects(s3_hook.get_conn(), source_table_location, target_table_location)
    
        athena = AthenaHook('s3_conn', region_name='us-east-1')
        target_database = Variable.get("DATALAKE_TARGET_DATABASE")
        target_table = kwargs['target_table']
        query = f"""
        MSCK REPAIR TABLE `{target_database}`.`{target_table}`
        """
        query_id = athena.run_query(query,
                                    query_context={"Database": Variable.get("DATALAKE_TARGET_DATABASE")},
                                    result_configuration={'OutputLocation': f's3://{datalake_athena_temp_bucket}/'},
                                    workgroup=Variable.get("DATALAKE_ATHENA_WORKGROUP"))
        final_state = athena.poll_query_status(query_id)
        if final_state == 'FAILED' or final_state == 'CANCELLED':
            raise Exception(f"Unable to get data from Athena: {query_id}")
        
        send_notification(f"🚰👌 [{env_tag}] {target_table} synced {sizeof_fmt(output_size)} bytes, {output_files} files")

    refresh_nft_metadata_partitions_task = PythonOperator(
        task_id=f'refresh_nft_metadata_partitions',
        python_callable=lambda **kwargs: safe_python_callable(refresh_metadata_partitions, kwargs, "refresh_nft_metadata_partitions"),
        op_kwargs={
            'target_table': 'nft_metadata',
            'target_table_location': f's3://{datalake_output_bucket}/v1/nft_metadata',
            'source_table_location': f's3://{datalake_exporters_bucket}/{datalake_exporters_prefix}nft_metadata',
        }
    )

    refresh_jetton_metadata_partitions_task = PythonOperator(
        task_id=f'refresh_jetton_metadata_partitions',
        python_callable=lambda **kwargs: safe_python_callable(refresh_metadata_partitions, kwargs, "refresh_jetton_metadata_partitions"),
        op_kwargs={
            'target_table': 'jetton_metadata',
            'target_table_location': f's3://{datalake_output_bucket}/v1/jetton_metadata',
            'source_table_location': f's3://{datalake_exporters_bucket}/{datalake_exporters_prefix}jetton_metadata',
        }
    )

    def nft_events(kwargs):
        athena = AthenaHook('s3_conn', region_name='us-east-1')
        task_instance = kwargs['task_instance']
        end_of_the_day_ts = task_instance.xcom_pull(key="end_of_the_day_ts", task_ids='perform_last_block_check')
        start_of_the_day_ts = task_instance.xcom_pull(key="start_of_the_day_ts", task_ids='perform_last_block_check')
        start_of_the_day = datetime.fromtimestamp(start_of_the_day_ts, pytz.utc)
        current_date = (start_of_the_day).strftime("%Y%m%d")
        yesterday_date = (start_of_the_day - timedelta(days=1)).strftime("%Y%m%d")

        source_database = Variable.get("DATALAKE_SOURCE_DATABASE")
        target_database = Variable.get("DATALAKE_TARGET_DATABASE")
        tmp_location = Variable.get("DATALAKE_TMP_LOCATION")
        table_location = kwargs['target_table_location']
        tmp_table_name = f"nft_events_increment_{current_date}_{str(uuid.uuid4()).replace('-', '')}"
        tmp_table_location = f"s3://{tmp_location}/{tmp_table_name}"

        query = """MSCK REPAIR TABLE nft_events"""
        query_id = athena.run_query(query,
                            query_context={"Database": Variable.get("DATALAKE_TARGET_DATABASE")},
                            result_configuration={'OutputLocation': f's3://{datalake_athena_temp_bucket}/'},
                            workgroup=Variable.get("DATALAKE_ATHENA_WORKGROUP"))
        final_state = athena.poll_query_status(query_id)
        if final_state == 'FAILED' or final_state == 'CANCELLED':
            raise Exception(f"Unable to get data from Athena: {query_id}")

        query = f"""
        create table "{source_database}".{tmp_table_name}
        with (
            format = 'AVRO', 
            write_compression = 'SNAPPY',
            external_location = '{tmp_table_location}',
            bucketed_by = ARRAY['nft_item_address'],
            bucket_count = 1,
            partitioned_by = ARRAY['block_date']
        )
        as

        with nft_items_states_ranks as (
            select ni.*, row_number() over (partition by ni.address order by ni.lt asc ) as rank
            from "{target_database}".nft_items ni where block_date <= '{current_date}'
        ), nft_items_latest_state as (
            select * from nft_items_states_ranks where rank = 1
        ),
        raw_min_tx as (
            SELECT ni.*, t.hash as tx_hash, t.trace_id
            FROM "{target_database}".transactions t 
            join nft_items_latest_state ni on t.account = ni.address and t.block_date = ni.block_date -- must be in the same partition
            and end_status ='active' and orig_status != 'active' -- new account deployment
            where t.block_date = '{current_date}'
        ), mints as (
            select block_date, address as nft_item_address, is_init, index as nft_item_index,
            collection_address, owner_address, content_onchain, timestamp, lt, tx_hash, trace_id,
            (select account from transactions t 
            where (t.block_date = '{current_date}' or t.block_date = '{yesterday_date}') and t.trace_id = raw_min_tx.trace_id order by lt asc limit 1) as deployer
            from raw_min_tx
        ),

        transfers_to_sale_contracts as (
            select row_number() over(partition by ns.address order by ns.lt asc) as rank, t.*,
            ns.address as nft_sale_contract, type as sale_type, ns.nft_owner_address, end_time,
            marketplace_address, marketplace_fee_address,
            marketplace_fee, price, asset, royalty_address, royalty_amount, max_bid, min_bid, min_step
            from "{target_database}".nft_transfers t
            join nft_sales ns on ns.address = t.new_owner and t.nft_item_address = ns.nft_address
            where t.block_date = '{current_date}' and ns.block_date <= '{current_date}'
            and not t.tx_aborted
        ), put_on_sale_ranks as (
            select t.block_date, nft_item_address, nft_item_index, nft_collection_address as collection_address,
            nft_owner_address as owner_address,
            is_init, content_onchain, row_number() over(partition by ni.address order by ni.lt desc) as nft_state_rank, -- from nft_items
            tx_now as timestamp, tx_lt as lt, tx_hash, trace_id, forward_amount, forward_payload, comment,
            nft_sale_contract, sale_type, end_time as sale_end_time, marketplace_address, marketplace_fee_address,
            marketplace_fee, price, asset, royalty_address, royalty_amount, max_bid, min_bid, min_step, query_id, custom_payload
            from transfers_to_sale_contracts t
            join "{target_database}".nft_items ni on ni.address = nft_item_address and ni.lt <= tx_lt
            where rank = 1 -- first state of the sale contract
        ), put_on_sale as (
            select * from put_on_sale_ranks where nft_state_rank = 1
        ),
        
        transfers_from_sale_contracts_to_owner as (
            select row_number() over(partition by ns.address order by ns.lt asc) as rank,  t.*,
            ns.address as nft_sale_contract, type as sale_type, ns.nft_owner_address, end_time, marketplace_address, marketplace_fee_address,
            marketplace_fee, price, asset, royalty_address, royalty_amount, max_bid, min_bid, min_step 
            from "{target_database}".nft_transfers t
            join nft_sales ns on ns.address = t.old_owner and t.nft_item_address = ns.nft_address and ns.nft_owner_address = t.new_owner
            and (ns.is_complete or ns.is_canceled)
            where t.block_date = '{current_date}' and ns.block_date <= '{current_date}'
            and not t.tx_aborted
        ), cancel_sale_ranks as (
            select t.block_date, nft_item_address, nft_item_index, nft_collection_address as collection_address,
            nft_owner_address as owner_address, 
            is_init, content_onchain, row_number() over(partition by ni.address order by ni.lt desc) as nft_state_rank, -- from nft_items
            tx_now as timestamp, tx_lt as lt, tx_hash, trace_id, forward_amount, forward_payload, comment,
            nft_sale_contract, sale_type, end_time as sale_end_time, marketplace_address, marketplace_fee_address,
            marketplace_fee, price, asset, royalty_address, royalty_amount, max_bid, min_bid, min_step, query_id, custom_payload
            from transfers_from_sale_contracts_to_owner t
            join "{target_database}".nft_items ni on ni.address = nft_item_address and ni.lt <= tx_lt
            where rank = 1 -- first state after sale completion
        ), cancel_sale as (
            select * from cancel_sale_ranks where nft_state_rank = 1
        ),


        transfers_from_sale_contracts_to_buyer as (
            select row_number() over(partition by ns.address order by ns.lt asc) as rank,  t.*,
            ns.address as nft_sale_contract, type as sale_type, ns.nft_owner_address as seller, t.new_owner as buyer, end_time, marketplace_address, marketplace_fee_address,
            marketplace_fee, price, asset, royalty_address, royalty_amount, max_bid, min_bid, min_step 
            from "{target_database}".nft_transfers t
            join nft_sales ns on ns.address = t.old_owner and t.nft_item_address = ns.nft_address and ns.nft_owner_address != t.new_owner
            and (ns.is_complete or ns.is_canceled)
            where t.block_date = '{current_date}' and ns.block_date <= '{current_date}'
            and not t.tx_aborted
        ), sales_ranks as (
            select t.block_date, nft_item_address, nft_item_index, nft_collection_address as collection_address,
            seller, buyer,
            is_init, content_onchain, row_number() over(partition by ni.address order by ni.lt desc) as nft_state_rank, -- from nft_items
            tx_now as timestamp, tx_lt as lt, tx_hash, trace_id, forward_amount, forward_payload, comment,
            nft_sale_contract, sale_type, end_time as sale_end_time, marketplace_address, marketplace_fee_address,
            marketplace_fee, price, asset, royalty_address, royalty_amount, max_bid, min_bid, min_step, query_id, custom_payload
            from transfers_from_sale_contracts_to_buyer t
            join "{target_database}".nft_items ni on ni.address = nft_item_address and ni.lt <= tx_lt
            where rank = 1 -- the first state after the sale
        ), sales as (
            select * from sales_ranks where nft_state_rank = 1
        ),

        sales_related as (
            select tx_hash from "{target_database}".nft_events where type = 'sale' or type = 'cancel_sale' or type = 'put_on_sale'
            union all
            select tx_hash from sales
            union all
            select tx_hash from put_on_sale
            union all
            select tx_hash from cancel_sale
        ), transfers_ordinary as (
            select t.*, 
            (select is_init from "{target_database}".nft_items ni where ni.address = nft_item_address order by ni.lt desc limit 1) as is_init,
            (select content_onchain from "{target_database}".nft_items ni where ni.address = nft_item_address order by ni.lt desc limit 1) as content_onchain
            from "{target_database}".nft_transfers t
            left join sales_related s on s.tx_hash = t.tx_hash
            where t.block_date = '{current_date}'
            and s.tx_hash is null
            and not t.tx_aborted
        ),

        tondns_content_history as (
            select *, lag(content_onchain, 1) over (partition by address order by lt asc) as prev_content  from "{target_database}".nft_items 
            where collection_address ='0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF'
            and block_date <= '{current_date}'
        ), tondns_bids as (
            select address, is_init, index, collection_address, owner_address, content_onchain, timestamp, lt, block_date, json_extract_scalar(content_onchain, '$.max_bid_address') as bidder,
            cast(json_extract_scalar(content_onchain, '$.max_bid_amount') as bigint) as price
            from tondns_content_history where
            json_extract_scalar(content_onchain, '$.max_bid_address') is not null and 
            (
            json_extract_scalar(content_onchain, '$.max_bid_address') !=
            json_extract_scalar(prev_content, '$.max_bid_address') or 
            
            json_extract_scalar(content_onchain, '$.max_bid_amount') !=
            json_extract_scalar(prev_content, '$.max_bid_amount')
            
            or prev_content is null
            )
            and block_date = '{current_date}'
        ),

        tondns_owner_history as (
            select *, lag(owner_address, 1) over (partition by address order by lt asc) as prev_owner  from "{target_database}".nft_items 
            where collection_address ='0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF'
            and block_date <= '{current_date}'
        ), tondns_releases as (
            select address, is_init, index, collection_address, owner_address, content_onchain, timestamp, lt, block_date,
            prev_owner
            from  tondns_owner_history
            where owner_address is null and prev_owner is not null
            and block_date = '{current_date}'
        ),

        tondns_next_event_history as (
            select *, lag(timestamp, 1) over (partition by address order by lt desc) as next_event, lag(owner_address, 1) over (partition by address order by lt desc) as next_owner
            from "{target_database}".nft_items 
            where collection_address ='0:B774D95EB20543F186C06B371AB88AD704F7E256130CAF96189368A7D0CB6CCF'
            and block_date <= '{current_date}'
        ), tondns_winners as (
            select distinct address, is_init, index, collection_address,
            json_extract_scalar(content_onchain, '$.max_bid_address')  as owner_address,
            content_onchain, 
            cast(json_extract_scalar(content_onchain, '$.auction_end_time') as bigint) as timestamp, 0 as lt,
            date_format(from_unixtime(cast(json_extract_scalar(content_onchain, '$.auction_end_time') as bigint)), '%Y%m%d') as block_date,
            cast(json_extract_scalar(content_onchain, '$.max_bid_amount') as bigint) as price
            from tondns_next_event_history
            where next_event is not null and next_event > cast(json_extract_scalar(content_onchain, '$.auction_end_time') as bigint)
            and json_extract_scalar(content_onchain, '$.max_bid_address')  is not null
        ), tondns_winners_duplicates_ranks as (
            --  due to batch commits we can have some wrong entries, let's exlude rows if time between buy events of the same address is less then one day
            select *, lag(timestamp, 1) over (partition by address order by timestamp desc) as adjustent_winner_ts from tondns_winners
        ), tondns_implicit_finish as (
            select address, is_init, index, collection_address, owner_address, content_onchain, timestamp, lt, block_date, price from tondns_winners_duplicates_ranks
            where block_date = '{current_date}' and
            (adjustent_winner_ts is null -- last win event
            or adjustent_winner_ts - timestamp > 86400)
        ),

        telemint_collections as (
            select distinct collection_address from "{target_database}".nft_items 
            where json_extract_scalar(content_onchain, '$.bidder_address') is not null
        ), telemint_bids_history as (
            select *, lag(content_onchain, 1) over (partition by address order by lt asc) as prev_content,
            lag(owner_address, 1) over (partition by address order by lt asc) as prev_owner from "{target_database}".nft_items 
            where collection_address in (select * from telemint_collections)
            and block_date <= '{current_date}'
        ), telemint_bids as (
            select distinct address, is_init, index, collection_address, owner_address, content_onchain, timestamp, lt, block_date,
            json_extract_scalar(content_onchain, '$.bidder_address') as bidder,
            cast(json_extract_scalar(content_onchain, '$.bid') as bigint) as price
            from telemint_bids_history where
            json_extract_scalar(content_onchain, '$.bidder_address') is not null and 
            (
            json_extract_scalar(content_onchain, '$.bidder_address') !=
            json_extract_scalar(prev_content, '$.bidder_address') or 
            
            json_extract_scalar(content_onchain, '$.bid') !=
            json_extract_scalar(prev_content, '$.bid')
            or prev_content is null
            )
            and block_date = '{current_date}'
        ),

        telemint_put_on_sale as (
            select distinct address, is_init, index, collection_address, owner_address, content_onchain, timestamp, lt, block_date, 
            try(cast(json_extract_scalar(content_onchain, '$.initial_min_bid') as bigint)) as price,
            try(cast(json_extract_scalar(content_onchain, '$.max_bid') as bigint)) as max_bid,
            try(cast(json_extract_scalar(content_onchain, '$.min_bid') as bigint)) as min_bid,
            try(cast(json_extract_scalar(content_onchain, '$.min_bid_step') as bigint)) as min_bid_step,
            try(cast(json_extract_scalar(content_onchain, '$.end_time') as bigint)) as sale_end_time
            from telemint_bids_history where
            json_extract_scalar(content_onchain, '$.duration') is not null and 
            json_extract_scalar(prev_content, '$.duration') is null
            and owner_address is not null
            and try(json_extract_scalar(content_onchain, '$.initial_min_bid')) is not null
            and block_date = '{current_date}'
        ),

        telemint_cancel_sale as (
            select distinct address, is_init, index, collection_address, owner_address, content_onchain, timestamp, lt, block_date, 
            greatest(coalesce(try(cast(json_extract_scalar(prev_content, '$.bid') as bigint)), 0),
            coalesce(try(cast(json_extract_scalar(prev_content, '$.initial_min_bid') as bigint)), 0)
            ) as price,
            try(cast(json_extract_scalar(prev_content, '$.max_bid') as bigint)) as max_bid,
            try(cast(json_extract_scalar(prev_content, '$.min_bid') as bigint)) as min_bid,
            try(cast(json_extract_scalar(prev_content, '$.min_bid_step') as bigint)) as min_bid_step,
            try(cast(json_extract_scalar(prev_content, '$.end_time') as bigint)) as sale_end_time
            from telemint_bids_history where
            json_extract_scalar(prev_content, '$.duration') is not null and 
            json_extract_scalar(content_onchain, '$.duration') is null
            and owner_address is not null and owner_address = prev_owner
            
            and block_date = '{current_date}'
        ),

        telemint_sale as (
            select distinct address, is_init, index, collection_address, owner_address as buyer, prev_owner as seller, content_onchain, timestamp, lt, block_date, 
            coalesce(cast(json_extract_scalar(prev_content, '$.bid') as bigint), cast(json_extract_scalar(prev_content, '$.max_bid') as bigint), 0) as price,
            try(cast(json_extract_scalar(prev_content, '$.max_bid') as bigint)) as max_bid,
            try(cast(json_extract_scalar(prev_content, '$.min_bid') as bigint)) as min_bid,
            try(cast(json_extract_scalar(prev_content, '$.min_bid_step') as bigint)) as min_bid_step,
            try(cast(json_extract_scalar(prev_content, '$.end_time') as bigint)) as sale_end_time,
            prev_owner
            from telemint_bids_history where
            json_extract_scalar(prev_content, '$.duration') is not null and 
            json_extract_scalar(content_onchain, '$.duration') is null
            and owner_address is not null and (prev_owner is not null and owner_address != prev_owner or prev_owner is null)
            
            and block_date = '{current_date}'
        ),

        
        output as (
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
            cast(null as bigint) as auction_min_step, block_date from mints

            union all
            
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
            min_step as auction_min_step, block_date from put_on_sale
            
            union all

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
            min_step as auction_min_step, block_date from cancel_sale

            union all
            
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
            min_step as auction_min_step, block_date from sales
            
            union all
            
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
            cast(null as bigint) as auction_min_step, block_date from transfers_ordinary

            union all
            
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
            cast(null as bigint) as auction_min_step, block_date from tondns_bids

            union all

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
            cast(null as bigint) as auction_min_step, block_date from tondns_releases

            union all

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
            cast(null as bigint) as auction_min_step, block_date from tondns_implicit_finish
            
            union all
            
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
            cast(null as bigint) as auction_min_step, block_date from telemint_bids

            union all

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
            min_bid_step as auction_min_step, block_date from telemint_put_on_sale

            union all
            
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
            min_bid_step as auction_min_step, block_date from telemint_cancel_sale

            union all
            
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
            min_bid_step as auction_min_step, block_date from telemint_sale
        )

        select distinct * from output
        where block_date = '{current_date}'
        except
        select *
        from "{target_database}".nft_events where block_date = '{current_date}'
        """
        query_id = athena.run_query(query,
                                    query_context={"Database": Variable.get("DATALAKE_TARGET_DATABASE")},
                                    result_configuration={'OutputLocation': f's3://{datalake_athena_temp_bucket}/'},
                                    workgroup=Variable.get("DATALAKE_ATHENA_WORKGROUP"))
        final_state = athena.poll_query_status(query_id)
        if final_state == 'FAILED' or final_state == 'CANCELLED':
            raise Exception(f"Unable to get data from Athena: {query_id}")
        glue = boto3.client("glue", region_name="us-east-1")
        glue.delete_table(DatabaseName=source_database, Name=tmp_table_name)
        logging.info(f"Time to transfer output data from {tmp_table_location} to {table_location}")
        
        s3_hook = S3Hook(aws_conn_id="s3_conn")

        output_size, output_files = transfer_s3_objects(s3_hook.get_conn(), tmp_table_location, table_location)

        logging.info(f"Refreshing partitions")

        query_id = athena.run_query(f"MSCK REPAIR TABLE nft_events",
                                    query_context={"Database": target_database},
                                    result_configuration={'OutputLocation': f's3://{datalake_athena_temp_bucket}/'},
                                    workgroup=Variable.get("DATALAKE_ATHENA_WORKGROUP"))
        final_state = athena.poll_query_status(query_id)
        if final_state == 'FAILED' or final_state == 'CANCELLED':
            raise Exception(f"Unable to get data from Athena: {query_id}")
    

        query_id = athena.run_query(f"select count(1) as count from \"{target_database}\".nft_events where block_date = '{current_date}'",
                                    query_context={"Database": Variable.get("DATALAKE_TARGET_DATABASE")},
                                    result_configuration={'OutputLocation': f's3://{datalake_athena_temp_bucket}/'},
                                    workgroup=Variable.get("DATALAKE_ATHENA_WORKGROUP"))
        
        final_state = athena.poll_query_status(query_id)
        if final_state == 'FAILED' or final_state == 'CANCELLED':
            raise Exception(f"Unable to get data from Athena: {query_id}")

        results = results_to_df(athena.get_query_results_paginator(query_id).build_full_result())
        logging.info(f"Got {len(results)} results")
        logging.info(results)
        output_count = results[0]['count']

        query_id = athena.run_query(f"select type, count(1) as count from \"{target_database}\".nft_events where block_date = '{current_date}' group by 1",
                                    query_context={"Database": Variable.get("DATALAKE_TARGET_DATABASE")},
                                    result_configuration={'OutputLocation': f's3://{datalake_athena_temp_bucket}/'},
                                    workgroup=Variable.get("DATALAKE_ATHENA_WORKGROUP"))
        
        final_state = athena.poll_query_status(query_id)
        if final_state == 'FAILED' or final_state == 'CANCELLED':
            raise Exception(f"Unable to get data from Athena: {query_id}")

        results = results_to_df(athena.get_query_results_paginator(query_id).build_full_result())
        logging.info(f"Got {len(results)} results")
        logging.info(results)
        output_details = []
        for row in results:
            output_details.append(f"{row['type']}: {row['count']}")
        output_details = ", ".join(output_details)

        finished = datetime.now().strftime("%I:%M %p")
        send_notification(f"🚰👌 [{env_tag}] nft_events for {current_date} finished at {finished}. {output_count} rows, {sizeof_fmt(output_size)} bytes, {output_files} files. Stats: {output_details}")
        

    nft_events_task = PythonOperator(
        task_id=f'nft_events',
        python_callable=lambda **kwargs: safe_python_callable(nft_events, kwargs, "nft_events"),
        op_kwargs={
            'target_table_location': f's3://{datalake_output_bucket}/v1/nft_events',
        }
    )

    perform_last_block_check_task >> [
        convert_blocks_task,
        convert_transactions_task,
        convert_messages_task,
        convert_messages_with_data_task,
        convert_accounts_task,
        (perform_last_block_check_task >> check_main_parser_offset_task >> check_megatons_offset_task >> check_core_prices_offset_task >> convert_dex_trades_task),
        (perform_last_block_check_task >> check_main_parser_offset_task >> convert_jetton_events >> refresh_jetton_metadata_partitions_task),
        (perform_last_block_check_task >> check_main_parser_offset_task >> check_megatons_offset_task >> check_core_prices_offset_task >> check_tvl_parser_offset_task >> convert_dex_tvl_task),
        (perform_last_block_check_task >> convert_balances_history_task >> generate_balances_snapshot_task),
    ] >> check_nft_parser_offset_task >> convert_nft_items_task >> convert_nft_transfers_task >> convert_nft_sales_task >> \
        refresh_nft_metadata_partitions_task >> nft_events_task

datalake_daily_sync_dag = datalake_daily_sync()
