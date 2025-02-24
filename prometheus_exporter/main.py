import json
import os
import sys
import time
from collections import defaultdict
from typing import Iterable

from kafka import KafkaConsumer
from loguru import logger
from prometheus_client import start_http_server, Gauge


class PerformanceGauge(Gauge):
    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Iterable[str] = (),
        interval: int = 60,
        update_interval: int = 5,
        **kwargs,
    ):
        super().__init__(name, documentation, labelnames, **kwargs)
        self._interval = interval
        self._last_timestamp = 0
        self._last_update = time.time()
        self._update_interval = update_interval
        self._tables = []
        self._trace_nodes = []
        self._trace_states = ["complete"]
        self._platforms = []
        self._data = defaultdict(dict)
        self._table_props = {
            "blocks": {"handler": self._handle_blocks},
            "traces": {"handler": self._handle_traces, "interval_factor": 1},
            "jetton_transfers": {"handler": self._handle_jetton_transfers, "interval_factor": 2},
            "dex_swap_parsed": {"handler": self._handle_dex_swap_parsed, "interval_factor": 2},
        }

    def handle_object(self, obj: dict):
        table = obj.get("__table")
        handler = self._table_props.get(table, {}).get("handler")
        if handler and table in self._tables:
            try:
                handler(obj)
            except Exception as e:
                logger.error(f"Error handling object {obj}: {e}")
        else:
            self._default_handler(obj)

        now = time.time()
        if now - self._last_update > self._update_interval:
            self._cleanup()
            metrics = self._calc_metrics()
            if metrics:
                self._update_metrics(metrics)
            self._last_update = now

    def _handle_blocks(self, obj: dict):
        if obj.get("workchain") == -1 and obj.get("shard") == -9223372036854775808:
            self._last_timestamp = max(self._last_timestamp, obj.get("gen_utime"))

    def _handle_traces(self, obj: dict):
        trace_id = obj.get("trace_id")
        start_utime = obj.get("start_utime")
        end_utime = obj.get("end_utime")
        state = obj.get("state")

        if not all([trace_id, start_utime, end_utime, state]):
            return

        if state in self._trace_states and (not self._trace_nodes or obj.get("nodes_") in self._trace_nodes):
            self._data["traces"][trace_id] = {
                "timestamp": end_utime,
                "delay": end_utime - start_utime,
                "state": state,
            }

    def _handle_jetton_transfers(self, obj: dict):
        tx_hash = obj.get("tx_hash")
        tx_now = obj.get("tx_now")
        trace_id = obj.get("trace_id")

        if not all([tx_hash, tx_now, trace_id]):
            return

        if obj.get("tx_aborted") is False:
            self._data["jetton_transfers"][tx_hash] = {
                "timestamp": tx_now,
                "trace_id": trace_id,
            }

    def _handle_dex_swap_parsed(self, obj: dict):
        tx_hash = obj.get("tx_hash")
        swap_utime = obj.get("swap_utime")
        trace_id = obj.get("trace_id")

        if not all([tx_hash, swap_utime, trace_id]):
            return

        if obj.get("platform") in self._platforms:
            self._data["dex_swap_parsed"][tx_hash] = {
                "timestamp": swap_utime,
                "trace_id": trace_id,
            }

    def _default_handler(self, obj: dict):
        logger.warning(f"No handler defined for message type '{obj.get('__table')}'")

    def _cleanup(self):
        for table, data in self._data.items():
            threshold = self._last_timestamp - self._interval * self._table_props.get(table, {}).get(
                "interval_factor", 1
            )
            self._data[table] = {key: value for key, value in data.items() if value["timestamp"] >= threshold}

    def _calc_metrics(self):
        raise NotImplementedError("_calc_metrics() must be implemented in a subclass")

    def _metrics_from_delay(self, data_list: list):
        data_list = sorted(data_list)
        return [
            {"labels": ["average"], "value": round(sum(data_list) / len(data_list))},
            {"labels": ["p50"], "value": self._percentile(data_list, 0.5)},
            {"labels": ["P75"], "value": self._percentile(data_list, 0.75)},
            {"labels": ["p95"], "value": self._percentile(data_list, 0.95)},
            {"labels": ["tx_count"], "value": len(data_list)},
        ]

    def _update_metrics(self, metrics: list):
        for metric in metrics:
            try:
                self.labels(*metric["labels"]).set(metric["value"])
                logger.info(
                    f"Metric '{self._name}' with labels {metric['labels']} has been updated to: {metric['value']}"
                )
            except Exception as e:
                logger.error(f"Metric {self._name} with labels {metric['labels']} setting error: {e}")

    def _percentile(self, sorted_data: list, fraction: float):
        if not sorted_data:
            return None
        index = int((len(sorted_data) - 1) * fraction)
        return sorted_data[index]


class P2pPerformanceGauge(PerformanceGauge):
    def __init__(self, name: str, documentation: str, interval: int = 60, update_interval: int = 5, **kwargs):
        super().__init__(name, documentation, ["column"], interval, update_interval, **kwargs)
        self._tables = ["blocks", "traces"]
        self._trace_nodes = [2]

    def _calc_metrics(self):
        if not self._data["traces"]:
            logger.warning("No trace data available for calculating metrics")
            return None

        data_list = [value["delay"] for value in self._data["traces"].values()]

        return self._metrics_from_delay(data_list)


class JettonTransfersPerformanceGauge(PerformanceGauge):
    def __init__(self, name: str, documentation: str, interval: int = 60, update_interval: int = 5, **kwargs):
        super().__init__(name, documentation, ["column"], interval, update_interval, **kwargs)
        self._tables = ["blocks", "traces", "jetton_transfers"]
        self._trace_nodes = [3, 4, 5]

    def _calc_metrics(self):
        if not self._data["jetton_transfers"]:
            logger.warning("No jetton transfer data available for calculating metrics")
            return None

        data_list = [
            self._data["traces"][value["trace_id"]]["delay"]
            for value in self._data["jetton_transfers"].values()
            if self._data["traces"].get(value["trace_id"])
        ]

        if not data_list:
            logger.warning("No matching trace data found for jetton transfers")
            return None

        return self._metrics_from_delay(data_list)


class DexPerformanceGauge(PerformanceGauge):
    def __init__(
        self,
        name: str,
        documentation: str,
        platforms: Iterable[str] = (),
        interval: int = 60,
        update_interval: int = 5,
        **kwargs,
    ):
        super().__init__(name, documentation, ["column"], interval, update_interval, **kwargs)
        self._tables = ["blocks", "traces", "dex_swap_parsed"]
        self._platforms = platforms

    def _calc_metrics(self):
        if not self._data["dex_swap_parsed"]:
            logger.warning("No DEX swap data available for calculating metrics")
            return None

        data_list = [
            self._data["traces"][value["trace_id"]]["delay"]
            for value in self._data["dex_swap_parsed"].values()
            if self._data["traces"].get(value["trace_id"])
        ]

        if not data_list:
            logger.warning("No matching trace data found for DEX swaps")
            return None

        return self._metrics_from_delay(data_list)


class TracesPerformanceGauge(PerformanceGauge):
    def __init__(self, name: str, documentation: str, interval: int = 60, update_interval: int = 5, **kwargs):
        super().__init__(name, documentation, ["column"], interval, update_interval, **kwargs)
        self._tables = ["blocks", "traces"]
        self._trace_states = ["complete", "pending"]

    def _calc_metrics(self):
        if not self._data["traces"]:
            logger.warning("No trace data available for calculating metrics")
            return None

        finished_ops_per_second = (
            len([value for value in self._data["traces"].values() if value["state"] == "complete"]) / self._interval
        )
        pending_operations = len([value for value in self._data["traces"].values() if value["state"] == "pending"])

        return [
            {"labels": ["finished_ops_per_second"], "value": finished_ops_per_second},
            {"labels": ["pending_operations"], "value": pending_operations},
        ]


if __name__ == "__main__":
    commit_batch_size = int(os.environ.get("COMMIT_BATCH_SIZE", "100"))
    calc_interval = int(os.environ.get("CALC_INTERVAL", "3600"))
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

    start_http_server(int(sys.argv[1]))

    gauges = [
        P2pPerformanceGauge(
            "ton_etl_common_operations_p2p",
            "TON ETL common operations metrics: p2p transfers",
            calc_interval,
            update_interval,
        ),
        JettonTransfersPerformanceGauge(
            "ton_etl_common_operations_jettons",
            "TON ETL common operations metrics: simple jetton transfers",
            calc_interval,
            update_interval,
        ),
        DexPerformanceGauge(
            "ton_etl_common_operations_dedust",
            "TON ETL common operations metrics: DeDust swaps",
            ["dedust"],
            calc_interval,
            update_interval,
        ),
        DexPerformanceGauge(
            "ton_etl_common_operations_stonfi",
            "TON ETL common operations metrics: Ston.fi swaps",
            ["ston.fi", "ston.fi_v2"],
            calc_interval,
            update_interval,
        ),
        TracesPerformanceGauge(
            "ton_etl_common_operations_traces",
            "TON ETL common operations metrics: Traces",
            600,
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
