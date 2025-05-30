import json
from typing import Dict, Union
import uuid
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.operators.python import get_current_context
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.models import Variable
import traceback


from datetime import datetime, timedelta
from time import time, sleep
import logging
import boto3
import pendulum
import pytz


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
Utility dag to convert monthly data from AVRO to Parquet
"""
@dag(
    schedule_interval=None,
    start_date=datetime(2025, 4, 1),
    catchup=False,
    concurrency=4,
    max_active_runs=4,
    tags=['ton', 'datalake']
)
def parquet_daily_converter():

    datalake_athena_temp_bucket = Variable.get("DATALAKE_TMP_LOCATION")
    workgroup = Variable.get("DATALAKE_ATHENA_WORKGROUP")

    def safe_python_callable(func, kwargs, step_name):
        try:
            func(kwargs)
        except Exception as e:
            logging.error(f"Unable to perform {func.__name__} for *{step_name}* {e} {traceback.format_exc()}")
            send_notification(f"📛 [parquet_daily_converter] Unable to invoke {func.__name__} for *{step_name}")
            raise e
 

    """
    Converts table from raw exported items into output table
    """
    def convert_table(kwargs):
        athena = AthenaHook('s3_conn', region_name='us-east-1')
        def execute_athena_query(query):
            query_id = athena.run_query(query,
                                        query_context={"Database": "datalake_parquet"},
                                        result_configuration={'OutputLocation': f's3://{datalake_athena_temp_bucket}/'},
                                        workgroup=workgroup)
            final_state = athena.poll_query_status(query_id)
            if final_state == 'FAILED' or final_state == 'CANCELLED':
                raise Exception(f"Unable to get data from Athena: {query_id}")
            
            return query_id
        
        source_table_name = kwargs['source_table_name']
        target_table_name = kwargs['target_table_name']
        partition_field = kwargs['partition_field']
        logical_time = pendulum.parse(kwargs['logical_time'])
        day = logical_time.format('YYYYMMDD')
        day_new_format = logical_time.format('YYYY-MM-DD')

        logging.info(f"Running conversion for {source_table_name} in {day}")

        glue = boto3.client("glue", region_name="us-east-1")
        source_table_meta = glue.get_table(DatabaseName="datalake", Name=source_table_name)
        logging.info(source_table_meta)
        logging.info(f"Columns: {source_table_meta['Table']['StorageDescriptor']['Columns']}")

        FIELDS = ", ".join([col['Name'] for col in source_table_meta['Table']['StorageDescriptor']['Columns']])
        sql = f"""
        insert into "datalake_parquet".{target_table_name}
        select {FIELDS},
            substring({partition_field}, 1, 4) || '-' || substring({partition_field}, 5, 2) || '-' || substring({partition_field}, 7, 2) as date
        from datalake.{source_table_name}
        where {partition_field} like '{day}%'
        except
        select * from datalake_parquet.{target_table_name} where date = '{day_new_format}'
        """
        logging.info(f"Running SQL code to convert data into single file dataset {sql}")
        execute_athena_query(sql)

    def convert_table_task(table_name, target_table_name=None, partition_field='block_date'):
        return PythonOperator(
            task_id=table_name,
            python_callable=lambda **kwargs: safe_python_callable(convert_table, kwargs, table_name),
            op_kwargs={
                'logical_time': '{{ data_interval_start }}',
                'source_table_name': table_name,
                'target_table_name': target_table_name if target_table_name else table_name,
                'partition_field': partition_field
            }
        )

    [   
        convert_table_task("account_states"),
        convert_table_task("balances_history"),
        convert_table_task("blocks"),
        convert_table_task("dex_trades"), 
        convert_table_task("dex_pools"),
        convert_table_task("jetton_events"),
        convert_table_task("jetton_metadata", partition_field='adding_date'),
        convert_table_task("messages_with_data"),
        
        convert_table_task("nft_metadata", partition_field='adding_date'),
        convert_table_task("nft_items"),
        convert_table_task("nft_sales"),
        convert_table_task("nft_transfers"),
        convert_table_task("nft_events"),
        convert_table_task("transactions"),
    ]

parquet_daily_converter_dag = parquet_daily_converter()
