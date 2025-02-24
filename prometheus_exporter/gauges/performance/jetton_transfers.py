from typing import Iterable
from loguru import logger

from gauges.performance.performance import PerformanceGauge


class JettonTransfersPerformanceGauge(PerformanceGauge):
    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Iterable[str],
        interval: int,
        update_interval: int,
        **kwargs,
    ):
        super().__init__(
            name=name,
            documentation=documentation,
            labelnames=labelnames,
            interval=interval,
            update_interval=update_interval,
            **kwargs,
        )
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
