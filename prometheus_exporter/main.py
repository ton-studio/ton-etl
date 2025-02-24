import json
import os

from kafka import KafkaConsumer
from loguru import logger
from prometheus_client import start_http_server

from gauges.performance.dex import DexPerformanceGauge
from gauges.performance.jetton_transfers import JettonTransfersPerformanceGauge
from gauges.performance.p2p import P2pPerformanceGauge
from gauges.performance.traces import TracesPerformanceGauge


if __name__ == "__main__":
    exporter_port = int(os.environ.get("EXPORTER_PORT", "8066"))
    commit_batch_size = int(os.environ.get("COMMIT_BATCH_SIZE", "100"))
    calc_interval = int(os.environ.get("CALC_INTERVAL", "3600"))
    traces_calc_interval = int(os.environ.get("TRACES_CALC_INTERVAL", "600"))
    update_interval = int(os.environ.get("UPDATE_INTERVAL", "5"))
    topics = os.environ.get(
        "KAFKA_TOPICS", "ton.public.blocks,ton.public.traces,ton.public.jetton_transfers,ton.parsed.dex_swap_parsed"
    )

    consumer = KafkaConsumer(
        group_id=os.environ.get("KAFKA_GROUP_ID"),
        bootstrap_servers=os.environ.get("KAFKA_BROKER"),
        auto_offset_reset=os.environ.get("KAFKA_OFFSET_RESET", "latest"),
        enable_auto_commit=False,
        max_poll_records=int(os.environ.get("KAFKA_MAX_POLL_RECORDS", "500")),
    )

    topic_list = topics.split(",")
    logger.info(f"Subscribing to: {topic_list}")
    consumer.subscribe(topic_list)

    start_http_server(exporter_port)

    gauges = [
        P2pPerformanceGauge(
            "ton_etl_common_operations_p2p",
            "TON ETL common operations metrics: p2p transfers",
            ["col"],
            calc_interval,
            update_interval,
        ),
        JettonTransfersPerformanceGauge(
            "ton_etl_common_operations_jettons",
            "TON ETL common operations metrics: simple jetton transfers",
            ["col"],
            calc_interval,
            update_interval,
        ),
        DexPerformanceGauge(
            "ton_etl_common_operations_dedust",
            "TON ETL common operations metrics: DeDust swaps",
            ["col"],
            ["dedust"],
            calc_interval,
            update_interval,
        ),
        DexPerformanceGauge(
            "ton_etl_common_operations_stonfi",
            "TON ETL common operations metrics: Ston.fi swaps",
            ["col"],
            ["ston.fi", "ston.fi_v2"],
            calc_interval,
            update_interval,
        ),
        TracesPerformanceGauge(
            "ton_etl_common_operations_traces",
            "TON ETL common operations metrics: Traces",
            ["col"],
            traces_calc_interval,
            update_interval,
        ),
    ]

    kafka_batch = 0
    for msg in consumer:
        try:
            kafka_batch += 1
            obj = json.loads(msg.value.decode("utf-8"))

            if obj.get("__op") == "d":  # ignore deletes
                continue

            for gauge in gauges:
                gauge.handle_object(obj)

        except Exception as e:
            logger.error(f"Failed to process item {msg}: {e}")
            raise

        if kafka_batch > commit_batch_size:
            logger.info(f"Processed {kafka_batch} messages, making commit")
            consumer.commit()  # commit kafka offset
            kafka_batch = 0
