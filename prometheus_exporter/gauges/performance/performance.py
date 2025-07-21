import time
from collections import defaultdict
from typing import Iterable

from loguru import logger
from prometheus_client import Gauge


class PerformanceGauge(Gauge):
    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Iterable[str] = (),
        interval: int = 600,
        update_interval: int = 5,
        **kwargs,
    ):
        super().__init__(name=name, documentation=documentation, labelnames=labelnames, **kwargs)
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
            logger.info(f"DEX swap added, total number of records = {len(self._data['dex_swap_parsed'])}, last_timestamp = {self._last_timestamp}")

    def _default_handler(self, obj: dict):
        pass

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
            {"labels": ["p75"], "value": self._percentile(data_list, 0.75)},
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
