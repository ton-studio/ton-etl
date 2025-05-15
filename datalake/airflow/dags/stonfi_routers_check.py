import ast
import json
import logging
import requests
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook


STONFI_ROUTER_V1 = "EQB3ncyBUTjZUA5EnFKR5_EnOMI9V1tTEAAPaiU71gc4TiUt"
STONFI_PARSER_V2_URL = "https://raw.githubusercontent.com/ton-studio/ton-etl/refs/heads/main/parser/parsers/message/stonfi_swap_v2.py"
STONFI_POOLS_API_URL = "https://api.ston.fi/v1/pools?dex_v2=true"


@dag(
    schedule_interval="30 8 * * *",
    start_date=datetime(2025, 1, 25),
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=["ton", "dex"],
)
def stonfi_new_routers_checker():
    """
    DAG to monitor Ston.fi routers. This DAG checks for new routers by comparing
    router addresses from the parser code and the API. If new routers are found,
    an alert is sent via Telegram.
    """

    def get_data(url):
        """
        Fetches data from the given URL using an HTTP GET request.

        Args:
            url (str): The URL to fetch data from.

        Returns:
            str: The response text from the URL.

        Raises:
            Exception: If the HTTP status code is not 200 or another error occurs.
        """
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                raise Exception(f"Response status code = {response.status_code}")
            return response.text
        except Exception as e:
            telegram_hook = TelegramHook(telegram_conn_id="telegram_watchdog_conn")
            logging.error(f"Unable to get data from {url}: {e}")
            telegram_hook.send_message({"text": f"📛 Unable to get data from {url}: {e}"})
            raise e

    def extract_routers_from_code(code: str):
        """
        Extracts router addresses from the provided Python code.

        Args:
            code (str): The Python code containing router definitions.

        Returns:
            set: A set of router addresses extracted from the code.
        """
        tree = ast.parse(code)
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if (
                        isinstance(target, ast.Name)
                        and target.id == "ROUTERS"
                        and isinstance(node.value, ast.Call)
                        and isinstance(node.value.func, ast.Name)
                        and node.value.func.id == "set"
                    ):
                        map_call = node.value.args[0]
                        if (
                            isinstance(map_call, ast.Call)
                            and isinstance(map_call.func, ast.Name)
                            and map_call.func.id == "map"
                        ):
                            list_node = map_call.args[1]
                            if isinstance(list_node, ast.List):
                                return {ast.literal_eval(el) for el in list_node.elts}

    def check_routers():
        """
        Compares the router addresses from the parser code and the Ston.fi API.
        If new router addresses are found, sends an alert via Telegram.
        """
        code = get_data(STONFI_PARSER_V2_URL)
        routers_from_parser = extract_routers_from_code(code) | {STONFI_ROUTER_V1}

        stonfi_api_data = get_data(STONFI_POOLS_API_URL)
        pool_list = json.loads(stonfi_api_data).get("pool_list")
        routers_from_api = {pool.get("router_address") for pool in pool_list}

        new_routers = routers_from_api - routers_from_parser

        if new_routers:
            telegram_hook = TelegramHook(telegram_conn_id="telegram_watchdog_conn")
            logging.info(f"New Ston.fi routers have been found: {', '.join(new_routers)}")
            telegram_hook.send_message({"text": f"⚠️ New Ston.fi routers have been found: {', '.join(new_routers)}"})

    PythonOperator(
        task_id="check_routers",
        python_callable=check_routers,
    )


stonfi_checker_dag = stonfi_new_routers_checker()